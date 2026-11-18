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
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/wutz/modeldock/pkg/exporter"
	"github.com/wutz/modeldock/pkg/harbor"
	"github.com/wutz/modeldock/pkg/harbor/harbortest"
	"github.com/wutz/modeldock/pkg/puller"
)

func TestCLIBinariesEndToEnd(t *testing.T) {
	ctx := context.Background()
	srv := harbortest.NewServer()
	defer srv.Close()

	hc, err := harbor.NewHTTPClient(srv.URL(), srv.AdminUser, srv.AdminPass, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hc.EnsureProject(ctx, "models-cli", false, nil); err != nil {
		t.Fatal(err)
	}
	pullRobot, pushRobot, err := hc.EnsureProjectRobots(ctx, "models-cli", "demo", false)
	if err != nil {
		t.Fatal(err)
	}

	repoRoot, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	binDir := t.TempDir()
	exporterBin := filepath.Join(binDir, "modeldock-exporter")
	pullerBin := filepath.Join(binDir, "modeldock-puller")

	buildExp := exec.CommandContext(ctx, "go", "build", "-o", exporterBin, "./cmd/modeldock-exporter/main.go")
	buildExp.Dir = repoRoot
	if out, err := buildExp.CombinedOutput(); err != nil {
		t.Fatalf("build exporter: %v\n%s", err, out)
	}
	buildPull := exec.CommandContext(ctx, "go", "build", "-o", pullerBin, "./cmd/modeldock-puller/main.go")
	buildPull.Dir = repoRoot
	if out, err := buildPull.CombinedOutput(); err != nil {
		t.Fatalf("build puller: %v\n%s", err, out)
	}

	pushCreds := t.TempDir()
	_ = os.WriteFile(filepath.Join(pushCreds, "username"), []byte(pushRobot.Name), 0o600)
	_ = os.WriteFile(filepath.Join(pushCreds, "password"), []byte(pushRobot.Secret), 0o600)

	pullCreds := t.TempDir()
	_ = os.WriteFile(filepath.Join(pullCreds, "username"), []byte(pullRobot.Name), 0o600)
	_ = os.WriteFile(filepath.Join(pullCreds, "password"), []byte(pullRobot.Secret), 0o600)

	srcDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(srcDir, "model.safetensors"), buildSafetensors(t, 128*1024), 0o644); err != nil {
		t.Fatal(err)
	}
	cfgBytes := []byte(`{"model_type":"qwen2","architectures":["Qwen2ForCausalLM"]}`)
	if err := os.WriteFile(filepath.Join(srcDir, "config.json"), cfgBytes, 0o644); err != nil {
		t.Fatal(err)
	}
	origWeights, _ := os.ReadFile(filepath.Join(srcDir, "model.safetensors"))

	exportOut := filepath.Join(t.TempDir(), "export-result.json")
	expCmd := exec.CommandContext(ctx, exporterBin,
		"--mode=dir",
		"--registry-url="+srv.URL(),
		"--repository=models-cli/demo",
		"--version-tag=v1.0.0",
		"--source-dir="+srcDir,
		"--credentials-dir="+pushCreds,
		"--chunk-size-bytes=50000",
		"--output-file="+exportOut,
	)
	if out, err := expCmd.CombinedOutput(); err != nil {
		t.Fatalf("exporter CLI failed: %v\n%s", err, out)
	}
	rawExp, err := os.ReadFile(exportOut)
	if err != nil {
		t.Fatal(err)
	}
	var expRes exporter.ExportResult
	if err := json.Unmarshal(rawExp, &expRes); err != nil {
		t.Fatalf("unmarshal export result: %v (%s)", err, rawExp)
	}
	if expRes.Digest == "" {
		t.Fatalf("expected non-empty digest in ExportResult: %+v", expRes)
	}

	casRoot := t.TempDir()
	pullOut := filepath.Join(t.TempDir(), "pull-result.json")
	pullCmd := exec.CommandContext(ctx, pullerBin,
		"--registry-url="+srv.URL(),
		"--repository=models-cli/demo",
		"--reference="+expRes.Digest,
		"--strategy=SharedFilesystem",
		"--cache-root="+casRoot,
		"--credentials-dir="+pullCreds,
		"--output-file="+pullOut,
	)
	if out, err := pullCmd.CombinedOutput(); err != nil {
		t.Fatalf("puller CLI failed: %v\n%s", err, out)
	}
	rawPull, err := os.ReadFile(pullOut)
	if err != nil {
		t.Fatal(err)
	}
	var pullRes puller.PullResult
	if err := json.Unmarshal(rawPull, &pullRes); err != nil {
		t.Fatalf("unmarshal pull result: %v (%s)", err, rawPull)
	}
	pulledWeights, err := os.ReadFile(filepath.Join(pullRes.TargetDir, "model.safetensors"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(origWeights, pulledWeights) {
		t.Fatalf("pulled weights mismatch: got %d bytes, want %d", len(pulledWeights), len(origWeights))
	}
	if _, err := os.Stat(filepath.Join(pullRes.TargetDir, puller.ReadyMarkerFile)); err != nil {
		t.Fatalf("expected ready marker: %v", err)
	}
}
