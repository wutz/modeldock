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

package puller

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
	"golang.org/x/sync/errgroup"

	modeldockv1alpha1 "github.com/wutz/modeldock/api/v1alpha1"
	"github.com/wutz/modeldock/pkg/oci"
)

const ReadyMarkerFile = ".modeldock-ready"

// PullOptions configures OCI ModelPack retrieval into CAS cache directories.
type PullOptions struct {
	RegistryClient *oci.RegistryClient
	Repository     string
	Reference      string // digest (sha256:...) or tag
	CacheRoot      string
	Strategy       modeldockv1alpha1.CacheStrategy
	Concurrency    int
}

// PullResult is returned by Pull to report target directory and transfer metrics.
type PullResult struct {
	ResolvedDigest  string `json:"resolvedDigest"`
	TargetDir       string `json:"targetDir"`
	SubPath         string `json:"subPath"`
	AlreadyCached   bool   `json:"alreadyCached"`
	BytesDownloaded int64  `json:"bytesDownloaded"`
	DurationSeconds int64  `json:"durationSeconds"`
	HardlinkedFiles int32  `json:"hardlinkedFiles,omitempty"`
}

type readyMarker struct {
	ManifestDigest string    `json:"manifestDigest"`
	Repository     string    `json:"repository"`
	CompletedAt    time.Time `json:"completedAt"`
}

// Pull downloads and verifies an OCI ModelPack artifact into either SharedFilesystem CAS
// or NodeLocalNVMe hardlink-deduplicated CAS.
func Pull(ctx context.Context, opts PullOptions) (*PullResult, error) {
	if opts.RegistryClient == nil {
		return nil, errors.New("registry client is required")
	}
	if opts.CacheRoot == "" {
		return nil, errors.New("cacheRoot is required")
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = 4
	}
	if opts.Strategy == "" {
		opts.Strategy = modeldockv1alpha1.CacheStrategySharedFilesystem
	}

	start := time.Now()
	manifest, manifestDigest, err := opts.RegistryClient.GetManifest(ctx, opts.Repository, opts.Reference)
	if err != nil {
		return nil, fmt.Errorf("fetch manifest: %w", err)
	}
	hexDigest := strings.TrimPrefix(manifestDigest, "sha256:")

	var subPath, targetDir string
	if opts.Strategy == modeldockv1alpha1.CacheStrategyNodeLocalNVMe {
		subPath = filepath.ToSlash(filepath.Join("manifests", "sha256", hexDigest))
		targetDir = filepath.Join(opts.CacheRoot, "manifests", "sha256", hexDigest)
	} else {
		subPath = filepath.ToSlash(filepath.Join("sha256", hexDigest))
		targetDir = filepath.Join(opts.CacheRoot, "sha256", hexDigest)
	}

	// Fast idempotent check.
	if _, err := os.Stat(filepath.Join(targetDir, ReadyMarkerFile)); err == nil {
		return &PullResult{
			ResolvedDigest:  manifestDigest,
			TargetDir:       targetDir,
			SubPath:         subPath,
			AlreadyCached:   true,
			DurationSeconds: int64(time.Since(start).Seconds()),
		}, nil
	}

	stagingRoot := filepath.Join(opts.CacheRoot, "staging")
	if err := os.MkdirAll(stagingRoot, 0o755); err != nil {
		return nil, fmt.Errorf("create staging root: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(targetDir), 0o755); err != nil {
		return nil, fmt.Errorf("create target parent dir: %w", err)
	}

	// Acquire simple atomic lock dir for this digest on the same filesystem.
	lockDir := filepath.Join(stagingRoot, ".lock-"+hexDigest)
	for {
		err := os.Mkdir(lockDir, 0o755)
		if err == nil {
			break
		}
		if !os.IsExist(err) {
			return nil, fmt.Errorf("acquire pull lock: %w", err)
		}
		// Another worker is pulling; check if target became ready.
		if _, readyErr := os.Stat(filepath.Join(targetDir, ReadyMarkerFile)); readyErr == nil {
			return &PullResult{
				ResolvedDigest:  manifestDigest,
				TargetDir:       targetDir,
				SubPath:         subPath,
				AlreadyCached:   true,
				DurationSeconds: int64(time.Since(start).Seconds()),
			}, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(150 * time.Millisecond):
		}
	}
	defer func() { _ = os.RemoveAll(lockDir) }()

	// Double-check after lock acquisition.
	if _, err := os.Stat(filepath.Join(targetDir, ReadyMarkerFile)); err == nil {
		return &PullResult{
			ResolvedDigest:  manifestDigest,
			TargetDir:       targetDir,
			SubPath:         subPath,
			AlreadyCached:   true,
			DurationSeconds: int64(time.Since(start).Seconds()),
		}, nil
	}

	stagingDir, err := os.MkdirTemp(stagingRoot, "tmp-"+hexDigest[:12]+"-")
	if err != nil {
		return nil, fmt.Errorf("create staging dir: %w", err)
	}
	cleanupStaging := true
	defer func() {
		if cleanupStaging {
			_ = os.RemoveAll(stagingDir)
		}
	}()

	var downloaded atomic.Int64
	var linkCount atomic.Int32

	if opts.Strategy == modeldockv1alpha1.CacheStrategyNodeLocalNVMe {
		if err := pullNodeLocalNVMe(ctx, opts, manifest, stagingDir, &downloaded, &linkCount); err != nil {
			return nil, err
		}
	} else {
		if err := pullSharedFilesystem(ctx, opts, manifest, stagingDir, &downloaded); err != nil {
			return nil, err
		}
	}

	markerRaw, _ := json.MarshalIndent(readyMarker{
		ManifestDigest: manifestDigest,
		Repository:     opts.Repository,
		CompletedAt:    time.Now().UTC(),
	}, "", "  ")
	markerPath := filepath.Join(stagingDir, ReadyMarkerFile)
	if err := writeFileSynced(markerPath, markerRaw, 0o444); err != nil {
		return nil, err
	}

	// Atomically promote stagingDir -> targetDir.
	if _, err := os.Stat(targetDir); err == nil {
		_ = os.RemoveAll(targetDir)
	}
	if err := os.Rename(stagingDir, targetDir); err != nil {
		return nil, fmt.Errorf("atomic rename %s -> %s: %w", stagingDir, targetDir, err)
	}
	cleanupStaging = false

	return &PullResult{
		ResolvedDigest:  manifestDigest,
		TargetDir:       targetDir,
		SubPath:         subPath,
		AlreadyCached:   false,
		BytesDownloaded: downloaded.Load(),
		DurationSeconds: int64(time.Since(start).Seconds()),
		HardlinkedFiles: linkCount.Load(),
	}, nil
}

func pullSharedFilesystem(ctx context.Context, opts PullOptions, manifest *oci.Manifest, stagingDir string, downloaded *atomic.Int64) error {
	// Pre-create target files for chunked layers so concurrent workers can WriteAt cleanly.
	chunkedFiles := map[string]struct {
		totalSize  int64
		fileSHA256 string
	}{}
	for _, layer := range manifest.Layers {
		if layer.MediaType != oci.MediaTypeWeightRaw {
			continue
		}
		if offStr := layer.Annotations[oci.AnnotationChunkOffset]; offStr != "" {
			relPath := layerFilePath(layer)
			totalSz, _ := strconv.ParseInt(layer.Annotations[oci.AnnotationChunkTotalSize], 10, 64)
			chunkedFiles[relPath] = struct {
				totalSize  int64
				fileSHA256 string
			}{
				totalSize:  totalSz,
				fileSHA256: layer.Annotations[oci.AnnotationFileSHA256],
			}
		}
	}
	for relPath, info := range chunkedFiles {
		dstPath, err := safeJoin(stagingDir, relPath)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
			return err
		}
		f, err := os.OpenFile(dstPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
		if err != nil {
			return err
		}
		if info.totalSize > 0 {
			_ = f.Truncate(info.totalSize)
		}
		_ = f.Close()
	}

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(opts.Concurrency)

	var fileMu sync.Mutex
	for i := range manifest.Layers {
		layer := manifest.Layers[i]
		g.Go(func() error {
			switch layer.MediaType {
			case oci.MediaTypeWeightConfigTarZstd:
				raw, err := opts.RegistryClient.GetBlobBytes(gCtx, opts.Repository, layer.Digest)
				if err != nil {
					return fmt.Errorf("pull config layer %s: %w", layer.Digest, err)
				}
				downloaded.Add(int64(len(raw)))
				return extractTarZstd(raw, stagingDir)

			case oci.MediaTypeWeightRaw:
				relPath := layerFilePath(layer)
				dstPath, err := safeJoin(stagingDir, relPath)
				if err != nil {
					return err
				}
				if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
					return err
				}

				if offStr := layer.Annotations[oci.AnnotationChunkOffset]; offStr != "" {
					offset, _ := strconv.ParseInt(offStr, 10, 64)
					var buf bytes.Buffer
					if err := opts.RegistryClient.PullBlob(gCtx, opts.Repository, layer.Digest, &buf, 0); err != nil {
						return fmt.Errorf("pull chunk %s: %w", layer.Digest, err)
					}
					downloaded.Add(int64(buf.Len()))
					fileMu.Lock()
					f, err := os.OpenFile(dstPath, os.O_RDWR, 0o644)
					if err == nil {
						_, err = f.WriteAt(buf.Bytes(), offset)
						if syncErr := f.Sync(); err == nil {
							err = syncErr
						}
						_ = f.Close()
					}
					fileMu.Unlock()
					return err
				}

				f, err := os.OpenFile(dstPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o444)
				if err != nil {
					return err
				}
				pullErr := opts.RegistryClient.PullBlob(gCtx, opts.Repository, layer.Digest, f, 0)
				if syncErr := f.Sync(); pullErr == nil && syncErr != nil {
					pullErr = syncErr
				}
				_ = f.Close()
				if pullErr != nil {
					return fmt.Errorf("pull weight layer %s (%s): %w", relPath, layer.Digest, pullErr)
				}
				downloaded.Add(layer.Size)
				return nil

			default:
				return nil
			}
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	// Verify full reassembled SHA256 for any chunked files and lock permissions read-only.
	for relPath, info := range chunkedFiles {
		dstPath, _ := safeJoin(stagingDir, relPath)
		if info.fileSHA256 != "" {
			got, err := fileSHA256(dstPath)
			if err != nil {
				return err
			}
			if got != info.fileSHA256 {
				return fmt.Errorf("reassembled file %s digest mismatch: got %s, want %s", relPath, got, info.fileSHA256)
			}
		}
		_ = os.Chmod(dstPath, 0o444)
	}
	return nil
}

func pullNodeLocalNVMe(ctx context.Context, opts PullOptions, manifest *oci.Manifest, stagingDir string, downloaded *atomic.Int64, linkCount *atomic.Int32) error {
	blobsDir := filepath.Join(opts.CacheRoot, "blobs", "sha256")
	if err := os.MkdirAll(blobsDir, 0o755); err != nil {
		return err
	}

	// If any chunked layers exist, fall back to SharedFilesystem staging for chunk reassembly.
	for _, layer := range manifest.Layers {
		if layer.Annotations[oci.AnnotationChunkOffset] != "" {
			return pullSharedFilesystem(ctx, opts, manifest, stagingDir, downloaded)
		}
	}

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(opts.Concurrency)

	for i := range manifest.Layers {
		layer := manifest.Layers[i]
		g.Go(func() error {
			switch layer.MediaType {
			case oci.MediaTypeWeightConfigTarZstd:
				raw, err := opts.RegistryClient.GetBlobBytes(gCtx, opts.Repository, layer.Digest)
				if err != nil {
					return err
				}
				downloaded.Add(int64(len(raw)))
				return extractTarZstd(raw, stagingDir)

			case oci.MediaTypeWeightRaw:
				blobHex := strings.TrimPrefix(layer.Digest, "sha256:")
				blobPath := filepath.Join(blobsDir, blobHex)

				// Pull into blob store if not already cached by an earlier model version.
				if _, err := os.Stat(blobPath); os.IsNotExist(err) {
					tmpBlob := blobPath + ".tmp-" + strconv.FormatInt(time.Now().UnixNano(), 10)
					f, err := os.OpenFile(tmpBlob, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o444)
					if err != nil {
						return err
					}
					pullErr := opts.RegistryClient.PullBlob(gCtx, opts.Repository, layer.Digest, f, 0)
					if syncErr := f.Sync(); pullErr == nil && syncErr != nil {
						pullErr = syncErr
					}
					_ = f.Close()
					if pullErr != nil {
						_ = os.Remove(tmpBlob)
						return pullErr
					}
					if err := os.Rename(tmpBlob, blobPath); err != nil {
						_ = os.Remove(tmpBlob)
						// Another concurrent layer with same digest might have completed first.
						if _, statErr := os.Stat(blobPath); statErr != nil {
							return err
						}
					} else {
						downloaded.Add(layer.Size)
					}
				}

				relPath := layerFilePath(layer)
				dstPath, err := safeJoin(stagingDir, relPath)
				if err != nil {
					return err
				}
				if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
					return err
				}
				if err := os.Link(blobPath, dstPath); err != nil {
					return fmt.Errorf("hardlink %s -> %s: %w", blobPath, dstPath, err)
				}
				linkCount.Add(1)
				return nil
			}
			return nil
		})
	}
	return g.Wait()
}

// EvictUnused removes cached manifest directories older than maxAge that are not in keepDigests.
func EvictUnused(cacheRoot string, strategy modeldockv1alpha1.CacheStrategy, keepDigests map[string]struct{}, maxAge time.Duration) (int, error) {
	base := filepath.Join(cacheRoot, "sha256")
	if strategy == modeldockv1alpha1.CacheStrategyNodeLocalNVMe {
		base = filepath.Join(cacheRoot, "manifests", "sha256")
	}
	entries, err := os.ReadDir(base)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	evicted := 0
	cutoff := time.Now().Add(-maxAge)
	for _, ent := range entries {
		if !ent.IsDir() {
			continue
		}
		fullDigest := "sha256:" + ent.Name()
		if _, keep := keepDigests[fullDigest]; keep {
			continue
		}
		info, err := ent.Info()
		if err != nil {
			continue
		}
		if info.ModTime().Before(cutoff) {
			if err := os.RemoveAll(filepath.Join(base, ent.Name())); err == nil {
				evicted++
			}
		}
	}
	return evicted, nil
}

func extractTarZstd(compressed []byte, dstDir string) error {
	zr, err := zstd.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return err
	}
	defer zr.Close()

	tr := tar.NewReader(zr)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if hdr.Typeflag == tar.TypeDir {
			target, err := safeJoin(dstDir, hdr.Name)
			if err != nil {
				return err
			}
			if err := os.MkdirAll(target, 0o755); err != nil {
				return err
			}
			continue
		}
		if hdr.Typeflag != tar.TypeReg {
			continue
		}
		target, err := safeJoin(dstDir, hdr.Name)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		f, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o444)
		if err != nil {
			return err
		}
		if _, err := io.Copy(f, io.LimitReader(tr, 256*1024*1024)); err != nil {
			_ = f.Close()
			return err
		}
		_ = f.Sync()
		_ = f.Close()
	}
	return nil
}

func layerFilePath(layer oci.Descriptor) string {
	if p := layer.Annotations[oci.AnnotationFilepath]; p != "" {
		return p
	}
	if p := layer.Annotations[oci.AnnotationImageTitle]; p != "" {
		return p
	}
	return strings.TrimPrefix(layer.Digest, "sha256:") + ".safetensors"
}

func safeJoin(root, rel string) (string, error) {
	clean := filepath.Clean(rel)
	if filepath.IsAbs(clean) || strings.HasPrefix(clean, "..") || clean == "." {
		return "", fmt.Errorf("unsafe archive path %q", rel)
	}
	joined := filepath.Join(root, clean)
	check, err := filepath.Rel(root, joined)
	if err != nil || strings.HasPrefix(check, "..") {
		return "", fmt.Errorf("path escapes cache root: %q", rel)
	}
	return joined, nil
}

func writeFileSynced(path string, data []byte, mode os.FileMode) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func fileSHA256(path string) (string, error) {
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
