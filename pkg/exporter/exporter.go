/*
Copyright 2026 Taizeng Wu.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package exporter

import (
	"archive/tar"
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
	"golang.org/x/sync/errgroup"

	modeldockv1alpha1 "github.com/wutz/modeldock/api/v1alpha1"
	"github.com/wutz/modeldock/pkg/modelmeta"
	"github.com/wutz/modeldock/pkg/oci"
)

// CommonOptions configures an export run into Harbor OCI ModelPack format.
type CommonOptions struct {
	RegistryClient        *oci.RegistryClient
	Repository            string // e.g. "models-acme/finance-qwen3-lora"
	VersionTag            string
	ArtifactType          modeldockv1alpha1.ArtifactType
	BaseModelRef          *modeldockv1alpha1.ModelArtifactReference
	Lineage               *modeldockv1alpha1.LineageSpec
	AllowUnsafeFormats    bool
	IncludeTrainingStates bool
	IncludePatterns       []string
	ExcludePatterns       []string
	Concurrency           int
	ChunkSizeBytes        int64
}

// DirExportOptions exports from a mounted local/PVC/DevBox directory.
type DirExportOptions struct {
	CommonOptions
	SourceDir string
}

// ExportResult is serialized as JSON to termination-log / output-file by modeldock-exporter.
type ExportResult struct {
	Digest              string                            `json:"digest"`
	TotalSizeBytes      int64                             `json:"totalSizeBytes"`
	BytesTransferred    int64                             `json:"bytesTransferred"`
	ShardCount          int32                             `json:"shardCount"`
	UnsafeSerialization bool                              `json:"unsafeSerialization"`
	ServingProfile      *modeldockv1alpha1.ServingProfile `json:"servingProfile,omitempty"`
	LoRASpec            *modeldockv1alpha1.LoRASpec       `json:"loraSpec,omitempty"`
	ExternalRevision    string                            `json:"externalRevision,omitempty"`
	SkippedBlobCount    int32                             `json:"skippedBlobCount"`
	UploadedBlobCount   int32                             `json:"uploadedBlobCount"`
}

// ExportFromDir runs zero-double-disk-write streaming OCI ModelPack export from SourceDir.
func ExportFromDir(ctx context.Context, opts DirExportOptions) (*ExportResult, error) {
	if opts.RegistryClient == nil {
		return nil, fmt.Errorf("registry client is required")
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = 4
	}
	if opts.ArtifactType == "" {
		opts.ArtifactType = modeldockv1alpha1.ArtifactTypeFullWeight
	}

	inspectRes, err := modelmeta.InspectDir(opts.SourceDir, opts.ArtifactType, modelmeta.InspectOptions{
		AllowUnsafeFormats:    opts.AllowUnsafeFormats,
		IncludeTrainingStates: opts.IncludeTrainingStates,
		IncludePatterns:       opts.IncludePatterns,
		ExcludePatterns:       opts.ExcludePatterns,
	})
	if err != nil {
		return nil, fmt.Errorf("pre-flight inspection failed: %w", err)
	}

	var bytesTransferred atomic.Int64
	var skippedCount atomic.Int32
	var uploadedCount atomic.Int32

	var layers []oci.Descriptor

	// 1. Build in-memory tar+zstd layer for small config/tokenizer files.
	if len(inspectRes.ConfigFiles) > 0 {
		tarBytes, names, err := packConfigFilesTarZstd(inspectRes.ConfigFiles)
		if err != nil {
			return nil, fmt.Errorf("pack config tar.zst: %w", err)
		}
		sum := sha256.Sum256(tarBytes)
		cfgLayerDigest := "sha256:" + hex.EncodeToString(sum[:])

		exists, _, err := opts.RegistryClient.HeadBlob(ctx, opts.Repository, cfgLayerDigest)
		if err != nil {
			return nil, fmt.Errorf("head config layer blob: %w", err)
		}
		if exists {
			skippedCount.Add(1)
		} else {
			err := opts.RegistryClient.PushBlobStream(
				ctx, opts.Repository, cfgLayerDigest, int64(len(tarBytes)), bytes.NewReader(tarBytes),
			)
			if err != nil {
				return nil, fmt.Errorf("push config layer blob: %w", err)
			}
			bytesTransferred.Add(int64(len(tarBytes)))
			uploadedCount.Add(1)
		}
		layers = append(layers, oci.Descriptor{
			MediaType: oci.MediaTypeWeightConfigTarZstd,
			Digest:    cfgLayerDigest,
			Size:      int64(len(tarBytes)),
			Annotations: map[string]string{
				oci.AnnotationFilepath: "configs-and-tokenizer.tar.zst",
				oci.AnnotationContents: strings.Join(names, ","),
			},
		})
	}

	// 2. Build upload tasks for weight files (splitting oversized single files if ChunkSizeBytes > 0).
	type uploadTask struct {
		index int
		wf    modelmeta.WeightFile
		// If split:
		isChunk    bool
		chunkIndex int
		offset     int64
		size       int64
		fileSHA256 string
	}

	var tasks []uploadTask
	for _, wf := range inspectRes.WeightFiles {
		if opts.ChunkSizeBytes > 0 && wf.Size > opts.ChunkSizeBytes {
			fileDigest, err := hashFileSHA256(wf.AbsPath)
			if err != nil {
				return nil, fmt.Errorf("hash oversized file %s: %w", wf.RelPath, err)
			}
			chunkIdx := 0
			for off := int64(0); off < wf.Size; off += opts.ChunkSizeBytes {
				sz := opts.ChunkSizeBytes
				if off+sz > wf.Size {
					sz = wf.Size - off
				}
				tasks = append(tasks, uploadTask{
					index:      len(tasks),
					wf:         wf,
					isChunk:    true,
					chunkIndex: chunkIdx,
					offset:     off,
					size:       sz,
					fileSHA256: fileDigest,
				})
				chunkIdx++
			}
		} else {
			tasks = append(tasks, uploadTask{
				index: len(tasks),
				wf:    wf,
				size:  wf.Size,
			})
		}
	}

	weightDescs := make([]oci.Descriptor, len(tasks))
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(opts.Concurrency)

	for i := range tasks {
		task := tasks[i]
		g.Go(func() error {
			f, err := os.Open(task.wf.AbsPath)
			if err != nil {
				return err
			}
			defer func() { _ = f.Close() }()

			// Pass 1: sequential SHA256 over the target section (zero temp disk).
			sr := io.NewSectionReader(f, task.offset, task.size)
			h := sha256.New()
			if _, err := io.Copy(h, sr); err != nil {
				return fmt.Errorf("hash %s: %w", task.wf.RelPath, err)
			}
			blobDigest := "sha256:" + hex.EncodeToString(h.Sum(nil))

			exists, _, err := opts.RegistryClient.HeadBlob(gCtx, opts.Repository, blobDigest)
			if err != nil {
				return fmt.Errorf("head blob %s: %w", task.wf.RelPath, err)
			}
			if exists {
				skippedCount.Add(1)
			} else {
				// Pass 2: seek back to offset and stream directly to Harbor.
				srUpload := io.NewSectionReader(f, task.offset, task.size)
				if err := opts.RegistryClient.PushBlobStream(gCtx, opts.Repository, blobDigest, task.size, srUpload); err != nil {
					return fmt.Errorf("push blob %s: %w", task.wf.RelPath, err)
				}
				bytesTransferred.Add(task.size)
				uploadedCount.Add(1)
			}

			ann := map[string]string{
				oci.AnnotationImageTitle: task.wf.RelPath,
				oci.AnnotationFilepath:   task.wf.RelPath,
			}
			if task.wf.SafetensorsHeaderSize > 0 && (!task.isChunk || task.chunkIndex == 0) {
				ann[oci.AnnotationSafetensorsHeaderSize] = strconv.FormatInt(task.wf.SafetensorsHeaderSize, 10)
			}
			if task.isChunk {
				ann[oci.AnnotationFileSHA256] = task.fileSHA256
				ann[oci.AnnotationChunkIndex] = strconv.Itoa(task.chunkIndex)
				ann[oci.AnnotationChunkOffset] = strconv.FormatInt(task.offset, 10)
				ann[oci.AnnotationChunkTotalSize] = strconv.FormatInt(task.wf.Size, 10)
			}
			weightDescs[task.index] = oci.Descriptor{
				MediaType:   oci.MediaTypeWeightRaw,
				Digest:      blobDigest,
				Size:        task.size,
				Annotations: ann,
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	layers = append(layers, weightDescs...)

	return finalizeManifest(
		ctx, opts.CommonOptions, layers, inspectRes, &bytesTransferred, &skippedCount, &uploadedCount, "",
	)
}

func finalizeManifest(
	ctx context.Context,
	opts CommonOptions,
	layers []oci.Descriptor,
	inspectRes *modelmeta.InspectionResult,
	bytesTransferred *atomic.Int64,
	skippedCount *atomic.Int32,
	uploadedCount *atomic.Int32,
	externalRev string,
) (*ExportResult, error) {
	var totalOCIBytes int64
	for _, l := range layers {
		totalOCIBytes += l.Size
	}

	lineage := opts.Lineage
	if externalRev != "" {
		if lineage == nil {
			lineage = &modeldockv1alpha1.LineageSpec{}
		} else {
			cp := *lineage
			lineage = &cp
		}
		lineage.ExternalRevision = externalRev
	}

	cfgPayload := oci.ModelPackConfig{
		SchemaVersion:       "1.0",
		CreatedAt:           time.Now().UTC(),
		ArtifactType:        opts.ArtifactType,
		BaseModelRef:        opts.BaseModelRef,
		LoRASpec:            inspectRes.LoRASpec,
		ServingProfile:      inspectRes.ServingProfile,
		Lineage:             lineage,
		UnsafeSerialization: inspectRes.UnsafeSerialization,
		ShardCount:          int32(len(inspectRes.WeightFiles)),
		TotalSizeBytes:      totalOCIBytes,
	}
	cfgBytes, err := json.Marshal(cfgPayload)
	if err != nil {
		return nil, err
	}
	cfgSum := sha256.Sum256(cfgBytes)
	cfgDigest := "sha256:" + hex.EncodeToString(cfgSum[:])

	exists, _, err := opts.RegistryClient.HeadBlob(ctx, opts.Repository, cfgDigest)
	if err != nil {
		return nil, err
	}
	if exists {
		skippedCount.Add(1)
	} else {
		err := opts.RegistryClient.PushBlobStream(
			ctx, opts.Repository, cfgDigest, int64(len(cfgBytes)), bytes.NewReader(cfgBytes),
		)
		if err != nil {
			return nil, fmt.Errorf("push ModelPackConfig blob: %w", err)
		}
		bytesTransferred.Add(int64(len(cfgBytes)))
		uploadedCount.Add(1)
	}

	ann := map[string]string{
		oci.AnnotationArtifactType: string(opts.ArtifactType),
	}
	if inspectRes.ServingProfile != nil {
		if inspectRes.ServingProfile.Architecture != "" {
			ann[oci.AnnotationArchitecture] = inspectRes.ServingProfile.Architecture
		}
		if inspectRes.ServingProfile.Precision != "" {
			ann[oci.AnnotationPrecision] = inspectRes.ServingProfile.Precision
		}
	}
	if lineage != nil && lineage.TrainTaskRef != nil && lineage.TrainTaskRef.TaskID != "" {
		ann[oci.AnnotationTrainTaskID] = lineage.TrainTaskRef.TaskID
	}

	manifest := &oci.Manifest{
		SchemaVersion: 2,
		MediaType:     oci.MediaTypeOCIManifest,
		ArtifactType:  oci.ArtifactTypeModelPack,
		Config: oci.Descriptor{
			MediaType: oci.MediaTypeModelConfig,
			Digest:    cfgDigest,
			Size:      int64(len(cfgBytes)),
		},
		Layers:      layers,
		Annotations: ann,
	}

	tag := opts.VersionTag
	if tag == "" {
		tag = "latest"
	}
	manifestDigest, _, err := opts.RegistryClient.PutManifest(ctx, opts.Repository, tag, manifest)
	if err != nil {
		return nil, fmt.Errorf("put manifest: %w", err)
	}

	return &ExportResult{
		Digest:              manifestDigest,
		TotalSizeBytes:      totalOCIBytes,
		BytesTransferred:    bytesTransferred.Load(),
		ShardCount:          int32(len(inspectRes.WeightFiles)),
		UnsafeSerialization: inspectRes.UnsafeSerialization,
		ServingProfile:      inspectRes.ServingProfile,
		LoRASpec:            inspectRes.LoRASpec,
		ExternalRevision:    externalRev,
		SkippedBlobCount:    skippedCount.Load(),
		UploadedBlobCount:   uploadedCount.Load(),
	}, nil
}

func packConfigFilesTarZstd(files []modelmeta.ConfigFile) ([]byte, []string, error) {
	sorted := append([]modelmeta.ConfigFile(nil), files...)
	slices.SortFunc(sorted, func(a, b modelmeta.ConfigFile) int {
		return cmp.Compare(a.RelPath, b.RelPath)
	})

	var out bytes.Buffer
	zw, err := zstd.NewWriter(&out)
	if err != nil {
		return nil, nil, err
	}
	tw := tar.NewWriter(zw)

	names := make([]string, 0, len(sorted))
	for _, cf := range sorted {
		data, err := os.ReadFile(cf.AbsPath)
		if err != nil {
			_ = tw.Close()
			_ = zw.Close()
			return nil, nil, err
		}
		if err := tw.WriteHeader(&tar.Header{
			Name: cf.RelPath,
			Mode: 0o644,
			Size: int64(len(data)),
		}); err != nil {
			_ = tw.Close()
			_ = zw.Close()
			return nil, nil, err
		}
		if _, err := tw.Write(data); err != nil {
			_ = tw.Close()
			_ = zw.Close()
			return nil, nil, err
		}
		names = append(names, cf.RelPath)
	}
	if err := tw.Close(); err != nil {
		_ = zw.Close()
		return nil, nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, nil, err
	}
	return out.Bytes(), names, nil
}

func packInMemoryConfigFilesTarZstd(files map[string][]byte) ([]byte, []string, error) {
	names := make([]string, 0, len(files))
	for k := range files {
		names = append(names, k)
	}
	slices.Sort(names)

	var out bytes.Buffer
	zw, err := zstd.NewWriter(&out)
	if err != nil {
		return nil, nil, err
	}
	tw := tar.NewWriter(zw)
	for _, name := range names {
		data := files[name]
		if err := tw.WriteHeader(&tar.Header{
			Name: name,
			Mode: 0o644,
			Size: int64(len(data)),
		}); err != nil {
			_ = tw.Close()
			_ = zw.Close()
			return nil, nil, err
		}
		if _, err := tw.Write(data); err != nil {
			_ = tw.Close()
			_ = zw.Close()
			return nil, nil, err
		}
	}
	if err := tw.Close(); err != nil {
		_ = zw.Close()
		return nil, nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, nil, err
	}
	return out.Bytes(), names, nil
}

func hashFileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil)), nil
}

var _ = sync.Mutex{}
