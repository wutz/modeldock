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

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	modeldockv1alpha1 "github.com/wutz/modeldock/api/v1alpha1"
	"github.com/wutz/modeldock/pkg/exporter"
	"github.com/wutz/modeldock/pkg/oci"
)

func main() {
	var (
		mode                  string
		registryURL           string
		repository            string
		versionTag            string
		artifactType          string
		credentialsDir        string
		username              string
		password              string
		insecureSkipTLS       bool
		sourceDir             string
		hubProvider           string
		hubRepoID             string
		hubRevision           string
		hubMirror             string
		hubAuthFile           string
		includePatterns       string
		excludePatterns       string
		allowUnsafeFormats    bool
		includeTrainingStates bool
		concurrency           int
		chunkSizeBytes        int64
		baseModelJSON         string
		lineageJSON           string
		outputFile            string
	)

	flag.StringVar(&mode, "mode", "dir", "Export mode: dir | external-hub")
	flag.StringVar(&registryURL, "registry-url", "", "Harbor registry base URL")
	flag.StringVar(&repository, "repository", "", "Harbor repository path (project/repo)")
	flag.StringVar(&versionTag, "version-tag", "latest", "OCI version tag")
	flag.StringVar(&artifactType, "artifact-type", "FullWeight", "FullWeight | LoRAAdapter | DraftModel")
	flag.StringVar(&credentialsDir, "credentials-dir", "", "Directory containing username and password files")
	flag.StringVar(&username, "username", os.Getenv("MODELDOCK_USERNAME"), "Harbor robot username")
	flag.StringVar(&password, "password", os.Getenv("MODELDOCK_PASSWORD"), "Harbor robot password/token")
	flag.BoolVar(&insecureSkipTLS, "insecure-skip-tls", false, "Skip TLS verification")
	flag.StringVar(&sourceDir, "source-dir", "/mnt/source", "Local source directory (mode=dir)")
	flag.StringVar(&hubProvider, "hub-provider", "HuggingFace", "HuggingFace | ModelScope")
	flag.StringVar(&hubRepoID, "hub-repo-id", "", "External Hub repo ID")
	flag.StringVar(&hubRevision, "hub-revision", "", "External Hub branch, tag, or commit")
	flag.StringVar(&hubMirror, "hub-mirror", "", "External Hub mirror endpoint")
	flag.StringVar(&hubAuthFile, "hub-auth-file", "", "File containing External Hub auth token")
	flag.StringVar(&includePatterns, "include-patterns", "", "Comma-separated glob include patterns")
	flag.StringVar(&excludePatterns, "exclude-patterns", "", "Comma-separated glob exclude patterns")
	flag.BoolVar(&allowUnsafeFormats, "allow-unsafe-formats", false, "Allow pickle .bin/.pt weights")
	flag.BoolVar(&includeTrainingStates, "include-training-states", false, "Include optimizer/rng checkpoint files")
	flag.IntVar(&concurrency, "concurrency", 4, "Worker concurrency")
	flag.Int64Var(&chunkSizeBytes, "chunk-size-bytes", 0, "Split weight files larger than this byte threshold (0=disabled)")
	flag.StringVar(&baseModelJSON, "base-model-json", "", "JSON-encoded ModelArtifactReference")
	flag.StringVar(&lineageJSON, "lineage-json", "", "JSON-encoded LineageSpec")
	flag.StringVar(&outputFile, "output-file", "", "Optional path (e.g. /dev/termination-log) to write ExportResult JSON")
	flag.Parse()

	if credentialsDir != "" {
		if u, err := os.ReadFile(filepath.Join(credentialsDir, "username")); err == nil && username == "" {
			username = strings.TrimSpace(string(u))
		}
		if p, err := os.ReadFile(filepath.Join(credentialsDir, "password")); err == nil && password == "" {
			password = strings.TrimSpace(string(p))
		}
	}

	regClient, err := oci.NewRegistryClient(oci.ClientOptions{
		RegistryURL:     registryURL,
		Username:        username,
		Password:        password,
		InsecureSkipTLS: insecureSkipTLS,
	})
	if err != nil {
		fatal(outputFile, err)
	}

	var baseRef *modeldockv1alpha1.ModelArtifactReference
	if baseModelJSON != "" {
		baseRef = &modeldockv1alpha1.ModelArtifactReference{}
		if err := json.Unmarshal([]byte(baseModelJSON), baseRef); err != nil {
			fatal(outputFile, fmt.Errorf("invalid --base-model-json: %w", err))
		}
	}

	var lineage *modeldockv1alpha1.LineageSpec
	if lineageJSON != "" {
		lineage = &modeldockv1alpha1.LineageSpec{}
		if err := json.Unmarshal([]byte(lineageJSON), lineage); err != nil {
			fatal(outputFile, fmt.Errorf("invalid --lineage-json: %w", err))
		}
	}

	common := exporter.CommonOptions{
		RegistryClient:        regClient,
		Repository:            repository,
		VersionTag:            versionTag,
		ArtifactType:          modeldockv1alpha1.ArtifactType(artifactType),
		BaseModelRef:          baseRef,
		Lineage:               lineage,
		AllowUnsafeFormats:    allowUnsafeFormats,
		IncludeTrainingStates: includeTrainingStates,
		IncludePatterns:       splitCSV(includePatterns),
		ExcludePatterns:       splitCSV(excludePatterns),
		Concurrency:           concurrency,
		ChunkSizeBytes:        chunkSizeBytes,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var res *exporter.ExportResult
	switch mode {
	case "dir":
		res, err = exporter.ExportFromDir(ctx, exporter.DirExportOptions{
			CommonOptions: common,
			SourceDir:     sourceDir,
		})
	case "external-hub":
		var hubToken string
		if hubAuthFile != "" {
			if raw, rerr := os.ReadFile(hubAuthFile); rerr == nil {
				hubToken = strings.TrimSpace(string(raw))
			}
		}
		res, err = exporter.ExportFromExternalHub(ctx, exporter.HubExportOptions{
			CommonOptions:  common,
			Provider:       modeldockv1alpha1.HubProvider(hubProvider),
			RepoID:         hubRepoID,
			Revision:       hubRevision,
			EndpointMirror: hubMirror,
			AuthToken:      hubToken,
		})
	default:
		err = fmt.Errorf("unknown export mode %q", mode)
	}
	if err != nil {
		fatal(outputFile, err)
	}

	outBytes, err := json.Marshal(res)
	if err != nil {
		fatal(outputFile, err)
	}
	if outputFile != "" {
		if werr := os.WriteFile(outputFile, outBytes, 0o644); werr != nil {
			fatal("", werr)
		}
	}
	fmt.Println(string(outBytes))
}

func splitCSV(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

func fatal(outputFile string, err error) {
	msg := fmt.Sprintf("modeldock-exporter error: %v", err)
	if outputFile != "" {
		_ = os.WriteFile(outputFile, []byte(msg), 0o644)
	}
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}
