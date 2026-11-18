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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"

	"golang.org/x/sync/errgroup"

	modeldockv1alpha1 "github.com/wutz/modeldock/api/v1alpha1"
	"github.com/wutz/modeldock/pkg/modelmeta"
	"github.com/wutz/modeldock/pkg/oci"
)

// HubExportOptions configures streaming relay from HuggingFace or ModelScope directly into Harbor.
type HubExportOptions struct {
	CommonOptions
	Provider       modeldockv1alpha1.HubProvider
	RepoID         string
	Revision       string
	EndpointMirror string
	AuthToken      string
	HTTPClient     *http.Client
}

type hubFileEntry struct {
	Path        string
	Size        int64
	SHA256      string // optional LFS OID
	DownloadURL string
}

// ExportFromExternalHub streams public or gated models from HuggingFace or ModelScope directly to Harbor.
func ExportFromExternalHub(ctx context.Context, opts HubExportOptions) (*ExportResult, error) {
	if opts.RegistryClient == nil {
		return nil, errors.New("registry client is required")
	}
	if opts.RepoID == "" {
		return nil, errors.New("external hub repoID is required")
	}
	if opts.Concurrency <= 0 {
		opts.Concurrency = 4
	}
	if opts.ArtifactType == "" {
		opts.ArtifactType = modeldockv1alpha1.ArtifactTypeFullWeight
	}
	hc := opts.HTTPClient
	if hc == nil {
		hc = &http.Client{}
	}

	resolvedRev, files, err := listExternalHubFiles(ctx, hc, opts)
	if err != nil {
		return nil, err
	}

	// Check if safetensors exist so we can skip duplicate legacy .bin/.onnx/.h5 files automatically.
	hasSafetensors := false
	for _, f := range files {
		if strings.HasSuffix(strings.ToLower(f.Path), ".safetensors") {
			hasSafetensors = true
			break
		}
	}

	var configEntries []hubFileEntry
	var weightEntries []hubFileEntry
	unsafeSerialization := false

	for _, f := range files {
		if !opts.IncludeTrainingStates && isHubExcludedDefault(f.Path) {
			continue
		}
		if matchHubPatterns(f.Path, opts.ExcludePatterns) {
			continue
		}
		if len(opts.IncludePatterns) > 0 && !matchHubPatterns(f.Path, opts.IncludePatterns) {
			continue
		}
		low := strings.ToLower(f.Path)
		if isHubWeight(low) {
			if hasSafetensors && len(opts.IncludePatterns) == 0 && (strings.HasSuffix(low, ".bin") || strings.HasSuffix(low, ".pt") || strings.HasSuffix(low, ".pth") || strings.HasSuffix(low, ".h5") || strings.HasSuffix(low, ".onnx")) {
				// Auto-deduplicate redundant legacy weights when safetensors are present.
				continue
			}
			if isHubUnsafeWeight(low) {
				if !opts.AllowUnsafeFormats {
					return nil, fmt.Errorf("unsafe pickle weight %q blocked (set allowUnsafeFormats=true to override)", f.Path)
				}
				unsafeSerialization = true
			}
			weightEntries = append(weightEntries, f)
		} else {
			configEntries = append(configEntries, f)
		}
	}

	if len(weightEntries) == 0 {
		return nil, errors.New("no valid weight files matched from external hub repository")
	}

	var bytesTransferred atomic.Int64
	var skippedCount atomic.Int32
	var uploadedCount atomic.Int32

	// Download small config files into memory (usually < 20 MiB total) and extract ServingProfile.
	configPayloads := make(map[string][]byte, len(configEntries))
	inspectRes := &modelmeta.InspectionResult{
		UnsafeSerialization: unsafeSerialization,
		ServingProfile:      &modeldockv1alpha1.ServingProfile{RecommendedEngine: "vllm"},
	}

	for _, ce := range configEntries {
		if ce.Size > 64*1024*1024 {
			continue
		}
		data, err := downloadHubBytes(ctx, hc, ce.DownloadURL, opts.AuthToken)
		if err != nil {
			return nil, fmt.Errorf("fetch config file %s: %w", ce.Path, err)
		}
		configPayloads[ce.Path] = data
		switch filepath.Base(ce.Path) {
		case "config.json":
			modelmeta.ParseModelConfigJSON(data, inspectRes.ServingProfile)
		case "tokenizer_config.json":
			var tok map[string]interface{}
			if json.Unmarshal(data, &tok) == nil {
				if ct, ok := tok["chat_template"]; ok && ct != nil && ct != "" {
					inspectRes.ServingProfile.HasChatTemplate = true
				}
			}
		case "chat_template.jinja":
			inspectRes.ServingProfile.HasChatTemplate = true
		case "adapter_config.json":
			lora, baseArch := modelmeta.ParseAdapterConfigJSON(data)
			inspectRes.LoRASpec = lora
			if inspectRes.ServingProfile.Architecture == "" && baseArch != "" {
				inspectRes.ServingProfile.Architecture = baseArch
			}
		}
	}
	if opts.ArtifactType == modeldockv1alpha1.ArtifactTypeLoRAAdapter && inspectRes.LoRASpec == nil {
		return nil, errors.New("adapter_config.json is required for LoRAAdapter artifacts")
	}

	var layers []oci.Descriptor
	if len(configPayloads) > 0 {
		tarBytes, names, err := packInMemoryConfigFilesTarZstd(configPayloads)
		if err != nil {
			return nil, err
		}
		sum := sha256.Sum256(tarBytes)
		cfgLayerDigest := "sha256:" + hex.EncodeToString(sum[:])

		exists, _, err := opts.RegistryClient.HeadBlob(ctx, opts.Repository, cfgLayerDigest)
		if err != nil {
			return nil, err
		}
		if exists {
			skippedCount.Add(1)
		} else {
			if err := opts.RegistryClient.PushBlobStream(ctx, opts.Repository, cfgLayerDigest, int64(len(tarBytes)), bytes.NewReader(tarBytes)); err != nil {
				return nil, err
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

	// Stream weight shards directly from ExternalHub HTTP response -> Harbor OCI PATCH (zero temp disk).
	weightDescs := make([]oci.Descriptor, len(weightEntries))
	weightFilesMeta := make([]modelmeta.WeightFile, len(weightEntries))
	var totalWeightBytes int64
	for i, we := range weightEntries {
		totalWeightBytes += we.Size
		weightFilesMeta[i] = modelmeta.WeightFile{
			RelPath: we.Path,
			Size:    we.Size,
		}
	}

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(opts.Concurrency)
	for i := range weightEntries {
		idx := i
		we := weightEntries[i]
		g.Go(func() error {
			expectedDigest := ""
			if we.SHA256 != "" {
				if strings.HasPrefix(we.SHA256, "sha256:") {
					expectedDigest = we.SHA256
				} else {
					expectedDigest = "sha256:" + we.SHA256
				}
				exists, _, err := opts.RegistryClient.HeadBlob(gCtx, opts.Repository, expectedDigest)
				if err == nil && exists {
					skippedCount.Add(1)
					weightDescs[idx] = oci.Descriptor{
						MediaType: oci.MediaTypeWeightRaw,
						Digest:    expectedDigest,
						Size:      we.Size,
						Annotations: map[string]string{
							oci.AnnotationImageTitle: we.Path,
							oci.AnnotationFilepath:   we.Path,
						},
					}
					return nil
				}
			}

			req, err := http.NewRequestWithContext(gCtx, http.MethodGet, we.DownloadURL, nil)
			if err != nil {
				return err
			}
			if opts.AuthToken != "" {
				req.Header.Set("Authorization", "Bearer "+opts.AuthToken)
			}
			resp, err := hc.Do(req)
			if err != nil {
				return fmt.Errorf("stream %s from hub: %w", we.Path, err)
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusOK {
				return fmt.Errorf("hub GET %s returned %d", we.Path, resp.StatusCode)
			}

			size := we.Size
			if size <= 0 && resp.ContentLength > 0 {
				size = resp.ContentLength
			}

			// Peek first bytes if safetensors to capture header-size annotation without second pass.
			var streamReader io.Reader = resp.Body
			var hdrSize int64
			if strings.HasSuffix(strings.ToLower(we.Path), ".safetensors") {
				var lenBuf [8]byte
				if _, err := io.ReadFull(resp.Body, lenBuf[:]); err == nil {
					hdrSize = int64(binary.LittleEndian.Uint64(lenBuf[:]))
					streamReader = io.MultiReader(bytes.NewReader(lenBuf[:]), resp.Body)
				}
			}

			digest, err := opts.RegistryClient.PushBlobStreamDynamic(gCtx, opts.Repository, size, streamReader, expectedDigest)
			if err != nil {
				return fmt.Errorf("relay %s to harbor: %w", we.Path, err)
			}
			bytesTransferred.Add(size)
			uploadedCount.Add(1)

			ann := map[string]string{
				oci.AnnotationImageTitle: we.Path,
				oci.AnnotationFilepath:   we.Path,
			}
			if hdrSize > 0 {
				ann[oci.AnnotationSafetensorsHeaderSize] = strconv.FormatInt(hdrSize, 10)
			}
			weightDescs[idx] = oci.Descriptor{
				MediaType:   oci.MediaTypeWeightRaw,
				Digest:      digest,
				Size:        size,
				Annotations: ann,
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	inspectRes.WeightFiles = weightFilesMeta
	inspectRes.WeightSizeBytes = totalWeightBytes
	modelmeta.FinalizeServingProfile(opts.ArtifactType, weightFilesMeta, totalWeightBytes, inspectRes.ServingProfile)
	layers = append(layers, weightDescs...)

	return finalizeManifest(ctx, opts.CommonOptions, layers, inspectRes, &bytesTransferred, &skippedCount, &uploadedCount, resolvedRev)
}

func listExternalHubFiles(ctx context.Context, hc *http.Client, opts HubExportOptions) (string, []hubFileEntry, error) {
	rev := opts.Revision
	switch opts.Provider {
	case modeldockv1alpha1.HubProviderModelScope:
		if rev == "" {
			rev = "master"
		}
		base := strings.TrimRight(opts.EndpointMirror, "/")
		if base == "" {
			base = "https://modelscope.cn"
		}
		apiURL := fmt.Sprintf("%s/api/v1/models/%s/repo/files?Revision=%s&Recursive=True",
			base, opts.RepoID, url.QueryEscape(rev))
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL, nil)
		if err != nil {
			return "", nil, err
		}
		if opts.AuthToken != "" {
			req.Header.Set("Authorization", "Bearer "+opts.AuthToken)
		}
		resp, err := hc.Do(req)
		if err != nil {
			return "", nil, err
		}
		defer func() { _ = resp.Body.Close() }()
		body, err := io.ReadAll(io.LimitReader(resp.Body, 16*1024*1024))
		if err != nil {
			return "", nil, err
		}
		if resp.StatusCode != http.StatusOK {
			return "", nil, fmt.Errorf("ModelScope API returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		var msResp struct {
			Data struct {
				Files []struct {
					Name     string `json:"Name"`
					Path     string `json:"Path"`
					Type     string `json:"Type"`
					Size     int64  `json:"Size"`
					Sha256   string `json:"Sha256"`
					Revision string `json:"Revision"`
				} `json:"Files"`
			} `json:"Data"`
		}
		if err := json.Unmarshal(body, &msResp); err != nil {
			return "", nil, err
		}
		resolved := rev
		var entries []hubFileEntry
		for _, f := range msResp.Data.Files {
			if f.Type == "tree" {
				continue
			}
			p := f.Path
			if p == "" {
				p = f.Name
			}
			if f.Revision != "" {
				resolved = f.Revision
			}
			dl := fmt.Sprintf("%s/api/v1/models/%s/repo?Revision=%s&FilePath=%s",
				base, opts.RepoID, url.QueryEscape(rev), url.QueryEscape(p))
			entries = append(entries, hubFileEntry{
				Path:        p,
				Size:        f.Size,
				SHA256:      f.Sha256,
				DownloadURL: dl,
			})
		}
		return resolved, entries, nil

	default: // HuggingFace
		if rev == "" {
			rev = "main"
		}
		base := strings.TrimRight(opts.EndpointMirror, "/")
		if base == "" {
			base = "https://huggingface.co"
		}
		// Resolve commit SHA via /api/models/<repo>/revision/<rev> if available.
		resolved := rev
		revReq, _ := http.NewRequestWithContext(ctx, http.MethodGet,
			fmt.Sprintf("%s/api/models/%s/revision/%s", base, opts.RepoID, url.PathEscape(rev)), nil)
		if revReq != nil {
			if opts.AuthToken != "" {
				revReq.Header.Set("Authorization", "Bearer "+opts.AuthToken)
			}
			if revResp, err := hc.Do(revReq); err == nil {
				if revResp.StatusCode == http.StatusOK {
					var meta struct {
						SHA string `json:"sha"`
					}
					if json.NewDecoder(revResp.Body).Decode(&meta) == nil && meta.SHA != "" {
						resolved = meta.SHA
					}
				}
				_ = revResp.Body.Close()
			}
		}

		treeURL := fmt.Sprintf("%s/api/models/%s/tree/%s?recursive=true", base, opts.RepoID, url.PathEscape(resolved))
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, treeURL, nil)
		if err != nil {
			return "", nil, err
		}
		if opts.AuthToken != "" {
			req.Header.Set("Authorization", "Bearer "+opts.AuthToken)
		}
		resp, err := hc.Do(req)
		if err != nil {
			return "", nil, err
		}
		defer func() { _ = resp.Body.Close() }()
		body, err := io.ReadAll(io.LimitReader(resp.Body, 16*1024*1024))
		if err != nil {
			return "", nil, err
		}
		if resp.StatusCode != http.StatusOK {
			return "", nil, fmt.Errorf("HuggingFace API returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
		}
		var items []struct {
			Type string `json:"type"`
			Path string `json:"path"`
			Size int64  `json:"size"`
			LFS  *struct {
				OID  string `json:"oid"`
				Size int64  `json:"size"`
			} `json:"lfs"`
		}
		if err := json.Unmarshal(body, &items); err != nil {
			return "", nil, err
		}
		var entries []hubFileEntry
		for _, it := range items {
			if it.Type != "file" {
				continue
			}
			sz := it.Size
			sha := ""
			if it.LFS != nil {
				if it.LFS.Size > 0 {
					sz = it.LFS.Size
				}
				sha = it.LFS.OID
			}
			dl := fmt.Sprintf("%s/%s/resolve/%s/%s", base, opts.RepoID, url.PathEscape(resolved), it.Path)
			entries = append(entries, hubFileEntry{
				Path:        it.Path,
				Size:        sz,
				SHA256:      sha,
				DownloadURL: dl,
			})
		}
		return resolved, entries, nil
	}
}

func downloadHubBytes(ctx context.Context, hc *http.Client, rawURL, token string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s returned %d", rawURL, resp.StatusCode)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 64*1024*1024))
}

func isHubExcludedDefault(path string) bool {
	low := strings.ToLower(path)
	base := filepath.Base(low)
	if strings.HasPrefix(low, ".git") || base == ".gitattributes" || base == ".gitignore" {
		return true
	}
	return strings.HasPrefix(base, "optimizer") || strings.HasPrefix(base, "rng_state") || strings.HasPrefix(base, "zero_pp_rank")
}

func isHubWeight(low string) bool {
	for _, ext := range []string{".safetensors", ".gguf", ".bin", ".pt", ".pth", ".ckpt", ".pkl", ".h5", ".onnx"} {
		if strings.HasSuffix(low, ext) {
			return true
		}
	}
	return false
}

func isHubUnsafeWeight(low string) bool {
	for _, ext := range []string{".bin", ".pt", ".pth", ".ckpt", ".pkl"} {
		if strings.HasSuffix(low, ext) {
			return true
		}
	}
	return false
}

func matchHubPatterns(path string, patterns []string) bool {
	base := filepath.Base(path)
	for _, p := range patterns {
		if ok, _ := filepath.Match(p, path); ok {
			return true
		}
		if ok, _ := filepath.Match(p, base); ok {
			return true
		}
	}
	return false
}
