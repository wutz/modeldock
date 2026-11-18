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

package exporter_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	modeldockv1alpha1 "github.com/wutz/modeldock/api/v1alpha1"
	"github.com/wutz/modeldock/pkg/exporter"
	"github.com/wutz/modeldock/pkg/harbor"
	"github.com/wutz/modeldock/pkg/harbor/harbortest"
	"github.com/wutz/modeldock/pkg/oci"
	"github.com/wutz/modeldock/pkg/puller"
)

func buildSafetensors(t *testing.T, byteLen int) []byte {
	t.Helper()
	hdrRaw, _ := json.Marshal(map[string]any{
		"layer.0.weight": map[string]any{
			"dtype":        "BF16",
			"shape":        []int{64, byteLen / 128},
			"data_offsets": []int{0, byteLen},
		},
	})
	var buf bytes.Buffer
	var lenBuf [8]byte
	binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(hdrRaw)))
	buf.Write(lenBuf[:])
	buf.Write(hdrRaw)
	for i := range byteLen {
		buf.WriteByte(byte((i*31 + 7) & 0xff))
	}
	return buf.Bytes()
}

func TestEndToEndExportAndPullDataPlane(t *testing.T) {
	const testLoRARepo = "models-acme/qwen3-lora"
	ctx := context.Background()
	srv := harbortest.NewServer()
	defer srv.Close()

	hc, err := harbor.NewHTTPClient(srv.URL(), srv.AdminUser, srv.AdminPass, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hc.EnsureProject(ctx, "models-acme", false, nil); err != nil {
		t.Fatal(err)
	}
	pullRobot, pushRobot, err := hc.EnsureProjectRobots(ctx, "models-acme", "qwen3-lora", false)
	if err != nil {
		t.Fatal(err)
	}

	// Build local model dir (LoRA + shard1 + shard2)
	srcDir := t.TempDir()
	_ = os.WriteFile(filepath.Join(srcDir, "config.json"),
		[]byte(`{"architectures":["Qwen3ForCausalLM"],"torch_dtype":"bfloat16","max_position_embeddings":32768}`),
		0o644)
	_ = os.WriteFile(filepath.Join(srcDir, "adapter_config.json"),
		[]byte(`{"r":32,"lora_alpha":64,"target_modules":["q_proj","v_proj"],"base_model_name_or_path":"Qwen/Qwen3-32B"}`),
		0o644)
	shard1 := buildSafetensors(t, 64*1024)
	shard2 := buildSafetensors(t, 48*1024)
	_ = os.WriteFile(filepath.Join(srcDir, "adapter_model-00001.safetensors"), shard1, 0o644)
	_ = os.WriteFile(filepath.Join(srcDir, "adapter_model-00002.safetensors"), shard2, 0o644)

	pushClient, err := oci.NewRegistryClient(oci.ClientOptions{
		RegistryURL:      srv.URL(),
		Username:         pushRobot.Name,
		Password:         pushRobot.Secret,
		UploadChunkBytes: 16 * 1024, // exercise multi-chunk PATCH streaming
	})
	if err != nil {
		t.Fatal(err)
	}

	// 1. Export with ChunkSizeBytes=24KB to also exercise oversized file splitting & reassembly!
	res, err := exporter.ExportFromDir(ctx, exporter.DirExportOptions{
		SourceDir: srcDir,
		CommonOptions: exporter.CommonOptions{
			RegistryClient: pushClient,
			Repository:     testLoRARepo,
			VersionTag:     "v1.0.0",
			ArtifactType:   modeldockv1alpha1.ArtifactTypeLoRAAdapter,
			Concurrency:    4,
			ChunkSizeBytes: 24 * 1024,
			Lineage: &modeldockv1alpha1.LineageSpec{
				SourceType: modeldockv1alpha1.SourceTypeTrainTask,
				TrainTaskRef: &modeldockv1alpha1.TrainTaskReference{
					TaskID: "task-98a7c1",
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("ExportFromDir failed: %v", err)
	}
	if res.Digest == "" || res.ShardCount != 2 || res.LoRASpec == nil || res.LoRASpec.Rank != 32 {
		t.Fatalf("unexpected export result: %+v", res)
	}
	if res.UploadedBlobCount == 0 {
		t.Fatalf("expected uploaded blobs > 0")
	}

	// 2. Re-export unchanged v1.0.1 -> all blobs must hit HEAD dedup (0 bytes uploaded!)
	res2, err := exporter.ExportFromDir(ctx, exporter.DirExportOptions{
		SourceDir: srcDir,
		CommonOptions: exporter.CommonOptions{
			RegistryClient: pushClient,
			Repository:     testLoRARepo,
			VersionTag:     "v1.0.1",
			ArtifactType:   modeldockv1alpha1.ArtifactTypeLoRAAdapter,
			Concurrency:    4,
			ChunkSizeBytes: 24 * 1024,
		},
	})
	if err != nil {
		t.Fatalf("re-export failed: %v", err)
	}
	if res2.SkippedBlobCount != 7 || res2.BytesTransferred > 1024 {
		t.Fatalf(
			"expected all 7 layer blobs skipped on dedup and only small config uploaded, got bytes=%d skipped=%d",
			res2.BytesTransferred, res2.SkippedBlobCount,
		)
	}

	// 3. Verify PullRobot is strictly read-only (cannot push)
	pullClient, err := oci.NewRegistryClient(oci.ClientOptions{
		RegistryURL: srv.URL(),
		Username:    pullRobot.Name,
		Password:    pullRobot.Secret,
	})
	if err != nil {
		t.Fatal(err)
	}
	zeroDigest := "sha256:0000000000000000000000000000000000000000000000000000000000000000"
	if err := pullClient.PushBlobBytes(ctx, testLoRARepo, zeroDigest, []byte("x")); err == nil {
		t.Fatalf("expected pull-only robot to be rejected on push")
	}

	// 4. Pull with SharedFilesystem via S3 307 Redirect and verify byte-exact reassembly
	casRoot := t.TempDir()
	pullRes, err := puller.Pull(ctx, puller.PullOptions{
		RegistryClient: pullClient,
		Repository:     testLoRARepo,
		Reference:      res.Digest,
		CacheRoot:      casRoot,
		Strategy:       modeldockv1alpha1.CacheStrategySharedFilesystem,
		Concurrency:    4,
	})
	if err != nil {
		t.Fatalf("Pull SharedFilesystem failed: %v", err)
	}
	if srv.BlobS3PullCount.Load() == 0 {
		t.Fatalf("expected S3 redirect presigned pulls to be exercised")
	}
	gotShard1, err := os.ReadFile(filepath.Join(pullRes.TargetDir, "adapter_model-00001.safetensors"))
	if err != nil || !bytes.Equal(gotShard1, shard1) {
		t.Fatalf("reassembled shard1 mismatch! err=%v len=%d/%d", err, len(gotShard1), len(shard1))
	}
	gotShard2, err := os.ReadFile(filepath.Join(pullRes.TargetDir, "adapter_model-00002.safetensors"))
	if err != nil || !bytes.Equal(gotShard2, shard2) {
		t.Fatalf("reassembled shard2 mismatch! err=%v", err)
	}

	// Second Pull must be idempotent 0-byte fast hit
	pullRes2, err := puller.Pull(ctx, puller.PullOptions{
		RegistryClient: pullClient,
		Repository:     testLoRARepo,
		Reference:      res.Digest,
		CacheRoot:      casRoot,
		Strategy:       modeldockv1alpha1.CacheStrategySharedFilesystem,
	})
	if err != nil || !pullRes2.AlreadyCached {
		t.Fatalf("expected idempotent AlreadyCached=true, got %+v err=%v", pullRes2, err)
	}

	// 5. Export without chunking and test NodeLocalNVMe hardlink deduplication across 2 versions!
	resNoChunk, err := exporter.ExportFromDir(ctx, exporter.DirExportOptions{
		SourceDir: srcDir,
		CommonOptions: exporter.CommonOptions{
			RegistryClient: pushClient,
			Repository:     testLoRARepo,
			VersionTag:     "v2.0.0",
			ArtifactType:   modeldockv1alpha1.ArtifactTypeLoRAAdapter,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	nvmeRoot := t.TempDir()
	nvmeRes, err := puller.Pull(ctx, puller.PullOptions{
		RegistryClient: pullClient,
		Repository:     testLoRARepo,
		Reference:      resNoChunk.Digest,
		CacheRoot:      nvmeRoot,
		Strategy:       modeldockv1alpha1.CacheStrategyNodeLocalNVMe,
	})
	if err != nil || nvmeRes.HardlinkedFiles != 2 {
		t.Fatalf("expected NodeLocalNVMe 2 hard-linked files, got %+v err=%v", nvmeRes, err)
	}

	// Evict test
	evicted, err := puller.EvictUnused(
		casRoot, modeldockv1alpha1.CacheStrategySharedFilesystem, map[string]struct{}{}, -time.Second,
	)
	if err != nil || evicted != 1 {
		t.Fatalf("expected 1 evicted dir, got %d err=%v", evicted, err)
	}
}

func TestExternalHubHuggingFaceRelay(t *testing.T) {
	ctx := context.Background()
	srv := harbortest.NewServer()
	defer srv.Close()

	hc, _ := harbor.NewHTTPClient(srv.URL(), srv.AdminUser, srv.AdminPass, true)
	_, _ = hc.EnsureProject(ctx, "models-public", true, nil)

	stData := buildSafetensors(t, 8192)
	cfgData := []byte(`{"architectures":["LlamaForCausalLM"],"torch_dtype":"bfloat16","max_position_embeddings":8192}`)

	// Mock HuggingFace API server
	hfSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/models/meta-llama/Llama-Test/revision/main":
			_, _ = w.Write([]byte(`{"sha":"deadbeef12345678"}`))
		case "/api/models/meta-llama/Llama-Test/tree/deadbeef12345678":
			_, _ = w.Write([]byte(`[
				{"type":"file","path":"config.json","size":` + jsonInt(len(cfgData)) + `},
				{"type":"file","path":"model.safetensors","size":` + jsonInt(len(stData)) + `},
				{"type":"file","path":"pytorch_model.bin","size":999999}
			]`))
		case "/meta-llama/Llama-Test/resolve/deadbeef12345678/config.json":
			_, _ = w.Write(cfgData)
		case "/meta-llama/Llama-Test/resolve/deadbeef12345678/model.safetensors":
			_, _ = w.Write(stData)
		case "/meta-llama/Llama-Test/resolve/deadbeef12345678/pytorch_model.bin":
			http.Error(w, "should have been auto-deduplicated!", http.StatusBadRequest)
		default:
			http.NotFound(w, r)
		}
	}))
	defer hfSrv.Close()

	regClient, err := oci.NewRegistryClient(oci.ClientOptions{
		RegistryURL: srv.URL(),
		Username:    srv.AdminUser,
		Password:    srv.AdminPass,
	})
	if err != nil {
		t.Fatal(err)
	}

	res, err := exporter.ExportFromExternalHub(ctx, exporter.HubExportOptions{
		Provider:       modeldockv1alpha1.HubProviderHuggingFace,
		RepoID:         "meta-llama/Llama-Test",
		Revision:       "main",
		EndpointMirror: hfSrv.URL,
		CommonOptions: exporter.CommonOptions{
			RegistryClient: regClient,
			Repository:     "models-public/llama-test",
			VersionTag:     "v1",
			ArtifactType:   modeldockv1alpha1.ArtifactTypeFullWeight,
		},
	})
	if err != nil {
		t.Fatalf("ExportFromExternalHub failed: %v", err)
	}
	if res.ExternalRevision != "deadbeef12345678" {
		t.Fatalf("expected resolved commit SHA deadbeef12345678, got %q", res.ExternalRevision)
	}
	if res.ServingProfile == nil || res.ServingProfile.Architecture != "LlamaForCausalLM" {
		t.Fatalf("unexpected ServingProfile: %+v", res.ServingProfile)
	}
	if res.UnsafeSerialization {
		t.Fatalf("expected legacy pytorch_model.bin to be skipped when safetensors exists")
	}
}

func jsonInt(n int) string {
	b, _ := json.Marshal(n)
	return string(b)
}
