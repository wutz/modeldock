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

package modelmeta_test

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	modeldockv1alpha1 "github.com/wutz/modeldock/api/v1alpha1"
	"github.com/wutz/modeldock/pkg/modelmeta"
)

// BuildValidSafetensors creates a minimal valid .safetensors binary blob with tensor metadata.
func BuildValidSafetensors(t *testing.T, tensorName string, dtype string, shape []int64, byteLen int) []byte {
	t.Helper()
	hdr := map[string]interface{}{
		tensorName: map[string]interface{}{
			"dtype":        dtype,
			"shape":        shape,
			"data_offsets": []int{0, byteLen},
		},
	}
	hdrRaw, err := json.Marshal(hdr)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	var lenBuf [8]byte
	binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(hdrRaw)))
	buf.Write(lenBuf[:])
	buf.Write(hdrRaw)
	buf.Write(bytes.Repeat([]byte{0x42}, byteLen))
	return buf.Bytes()
}

func TestInspectDirFullModelAndSecurity(t *testing.T) {
	dir := t.TempDir()

	// Write config.json & tokenizer_config.json
	cfg := `{"architectures":["Qwen3ForCausalLM"],"torch_dtype":"bfloat16","max_position_embeddings":32768,"hidden_size":1024,"num_hidden_layers":4,"vocab_size":32000,"intermediate_size":2048}`
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "tokenizer_config.json"), []byte(`{"chat_template":"{% for m in messages %}{{ m.content }}{% endfor %}"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	// Training artifacts that must be filtered out automatically
	if err := os.WriteFile(filepath.Join(dir, "optimizer.pt"), []byte("big optimizer state"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "rng_state_0.pth"), []byte("rng"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Valid safetensors shard
	stBytes := BuildValidSafetensors(t, "model.embed_tokens.weight", "BF16", []int64{1024, 1024}, 2048)
	if err := os.WriteFile(filepath.Join(dir, "model-00001-of-00001.safetensors"), stBytes, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "model.safetensors.index.json"), []byte(`{"weight_map":{"model.embed_tokens.weight":"model-00001-of-00001.safetensors"}}`), 0o644); err != nil {
		t.Fatal(err)
	}

	res, err := modelmeta.InspectDir(dir, modeldockv1alpha1.ArtifactTypeFullWeight, modelmeta.InspectOptions{})
	if err != nil {
		t.Fatalf("InspectDir failed: %v", err)
	}
	if res.UnsafeSerialization {
		t.Fatalf("expected UnsafeSerialization=false")
	}
	if len(res.WeightFiles) != 1 || res.WeightFiles[0].SafetensorsHeaderSize == 0 {
		t.Fatalf("expected 1 safetensors shard with header size > 0, got %+v", res.WeightFiles)
	}
	if res.ServingProfile.Architecture != "Qwen3ForCausalLM" || res.ServingProfile.Precision != "BF16" || !res.ServingProfile.HasChatTemplate {
		t.Fatalf("unexpected ServingProfile: %+v", res.ServingProfile)
	}
	for _, cf := range res.ConfigFiles {
		if cf.RelPath == "optimizer.pt" || cf.RelPath == "rng_state_0.pth" {
			t.Fatalf("training state %s was not filtered", cf.RelPath)
		}
	}

	// Test blocking unsafe .bin format when AllowUnsafeFormats=false
	if err := os.WriteFile(filepath.Join(dir, "pytorch_model.bin"), []byte("pickle-payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := modelmeta.InspectDir(dir, modeldockv1alpha1.ArtifactTypeFullWeight, modelmeta.InspectOptions{AllowUnsafeFormats: false}); err == nil {
		t.Fatalf("expected InspectDir to reject unsafe .bin file when allowUnsafeFormats=false")
	}
	resAllow, err := modelmeta.InspectDir(dir, modeldockv1alpha1.ArtifactTypeFullWeight, modelmeta.InspectOptions{AllowUnsafeFormats: true})
	if err != nil || !resAllow.UnsafeSerialization {
		t.Fatalf("expected AllowUnsafeFormats=true to succeed and flag UnsafeSerialization, err=%v", err)
	}
	_ = os.Remove(filepath.Join(dir, "pytorch_model.bin"))

	// Test symlink escape defense
	outsideFile := filepath.Join(t.TempDir(), "secret.txt")
	_ = os.WriteFile(outsideFile, []byte("secret"), 0o644)
	if err := os.Symlink(outsideFile, filepath.Join(dir, "escape.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := modelmeta.InspectDir(dir, modeldockv1alpha1.ArtifactTypeFullWeight, modelmeta.InspectOptions{}); err == nil {
		t.Fatalf("expected InspectDir to block symlink escaping root")
	}
}
