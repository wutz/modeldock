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
	"github.com/wutz/modeldock/pkg/oci"
	"github.com/wutz/modeldock/pkg/puller"
)

func main() {
	var (
		registryURL     string
		repository      string
		reference       string
		strategy        string
		cacheRoot       string
		credentialsDir  string
		username        string
		password        string
		proxyURL        string
		insecureSkipTLS bool
		concurrency     int
		outputFile      string
	)

	flag.StringVar(&registryURL, "registry-url", "", "Harbor registry base URL")
	flag.StringVar(&repository, "repository", "", "Harbor repository path (project/repo)")
	flag.StringVar(&reference, "reference", "", "OCI manifest digest or tag")
	flag.StringVar(&strategy, "strategy", string(modeldockv1alpha1.CacheStrategySharedFilesystem),
		"SharedFilesystem | NodeLocalNVMe")
	flag.StringVar(&cacheRoot, "cache-root", "/modeldock-cas", "CAS cache root directory")
	flag.StringVar(&credentialsDir, "credentials-dir", "", "Directory containing username and password files")
	flag.StringVar(&username, "username", os.Getenv("MODELDOCK_USERNAME"), "Harbor pull robot username")
	flag.StringVar(&password, "password", os.Getenv("MODELDOCK_PASSWORD"), "Harbor pull robot password")
	flag.StringVar(&proxyURL, "proxy-url", os.Getenv("HTTP_PROXY"), "Optional Dragonfly P2P HTTP proxy URL")
	flag.BoolVar(&insecureSkipTLS, "insecure-skip-tls", false, "Skip TLS verification")
	flag.IntVar(&concurrency, "concurrency", 4, "Concurrent layer pull workers")
	flag.StringVar(&outputFile, "output-file", "", "Optional path (e.g. /dev/termination-log) to write PullResult JSON")
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
		ProxyURL:        proxyURL,
		InsecureSkipTLS: insecureSkipTLS,
	})
	if err != nil {
		fatal(outputFile, err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	res, err := puller.Pull(ctx, puller.PullOptions{
		RegistryClient: regClient,
		Repository:     repository,
		Reference:      reference,
		CacheRoot:      cacheRoot,
		Strategy:       modeldockv1alpha1.CacheStrategy(strategy),
		Concurrency:    concurrency,
	})
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

func fatal(outputFile string, err error) {
	msg := fmt.Sprintf("modeldock-puller error: %v", err)
	if outputFile != "" {
		_ = os.WriteFile(outputFile, []byte(msg), 0o644)
	}
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}
