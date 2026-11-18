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

package ui

import (
	"embed"
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	modeldockv1alpha1 "github.com/wutz/modeldock/api/v1alpha1"
)

//go:embed static/index.html
var staticFS embed.FS

// Server serves the lightweight read-only ModelDock catalog UI and JSON API.
// When RestConfig is set and a caller supplies a Kubernetes Bearer token, API requests
// impersonate/execute using that token so cluster RBAC permissions are enforced.
type Server struct {
	FallbackReader     client.Reader
	RestConfig         *rest.Config
	Scheme             *runtime.Scheme
	RequireBearerToken bool
}

// CatalogResponse bundles repositories, artifacts, and caches visible to the caller.
type CatalogResponse struct {
	Repositories []modeldockv1alpha1.ModelRepository `json:"repositories"`
	Artifacts    []modeldockv1alpha1.ModelArtifact   `json:"artifacts"`
	Caches       []modeldockv1alpha1.ModelCache      `json:"caches"`
}

// Handler returns an http.Handler exposing the embedded UI and /api/v1/catalog endpoint.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	sub, _ := fs.Sub(staticFS, "static")
	fileServer := http.FileServer(http.FS(sub))

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/api/v1/catalog", s.handleCatalog)
	mux.Handle("/", fileServer)
	return mux
}

func (s *Server) handleCatalog(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	reader, err := s.readerForRequest(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	ns := r.URL.Query().Get("namespace")
	var listOpts []client.ListOption
	if ns != "" {
		listOpts = append(listOpts, client.InNamespace(ns))
	}

	ctx := r.Context()
	var repos modeldockv1alpha1.ModelRepositoryList
	if err := reader.List(ctx, &repos, listOpts...); err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	var arts modeldockv1alpha1.ModelArtifactList
	if err := reader.List(ctx, &arts, listOpts...); err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	var caches modeldockv1alpha1.ModelCacheList
	if err := reader.List(ctx, &caches, listOpts...); err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(CatalogResponse{
		Repositories: repos.Items,
		Artifacts:    arts.Items,
		Caches:       caches.Items,
	})
}

func (s *Server) readerForRequest(r *http.Request) (client.Reader, error) {
	auth := r.Header.Get("Authorization")
	if strings.HasPrefix(auth, "Bearer ") {
		token := strings.TrimSpace(strings.TrimPrefix(auth, "Bearer "))
		if token != "" && s.RestConfig != nil {
			cfg := rest.CopyConfig(s.RestConfig)
			cfg.BearerToken = token
			cfg.BearerTokenFile = ""
			cfg.CertData = nil
			cfg.KeyData = nil
			cfg.CertFile = ""
			cfg.KeyFile = ""
			c, err := client.New(cfg, client.Options{Scheme: s.Scheme})
			if err != nil {
				return nil, err
			}
			return c, nil
		}
	}
	if s.RequireBearerToken {
		return nil, errors.New("missing Kubernetes Bearer token")
	}
	return s.FallbackReader, nil
}
