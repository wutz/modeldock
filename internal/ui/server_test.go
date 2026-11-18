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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	modeldockv1alpha1 "github.com/wutz/modeldock/api/v1alpha1"
)

func TestCatalogUIServer(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = modeldockv1alpha1.AddToScheme(scheme)

	art := &modeldockv1alpha1.ModelArtifact{
		ObjectMeta: metav1.ObjectMeta{Name: "qwen3-32b", Namespace: "modeldock-public"},
		Spec: modeldockv1alpha1.ModelArtifactSpec{
			RepositoryRef: "qwen3",
			VersionTag:    "v1.0.0",
			OCIDigest:     "sha256:4b227777d4dd1fc61c6f884f48641d02b4d121d3fd328cb08b5531fcacdabf8a",
			Stage:         modeldockv1alpha1.StageProduction,
		},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(art).Build()
	srv := &Server{
		FallbackReader: cl,
		Scheme:         scheme,
	}
	h := srv.Handler()

	// Embedded HTML page
	recHTML := httptest.NewRecorder()
	h.ServeHTTP(recHTML, httptest.NewRequest(http.MethodGet, "/", nil))
	if recHTML.Code != http.StatusOK || !strings.Contains(recHTML.Body.String(), "ModelDock Registry") {
		t.Fatalf("unexpected UI HTML response: %d", recHTML.Code)
	}

	// JSON catalog API
	recAPI := httptest.NewRecorder()
	h.ServeHTTP(recAPI, httptest.NewRequest(http.MethodGet, "/api/v1/catalog", nil))
	if recAPI.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", recAPI.Code)
	}
	var res CatalogResponse
	if err := json.Unmarshal(recAPI.Body.Bytes(), &res); err != nil {
		t.Fatal(err)
	}
	if len(res.Artifacts) != 1 || res.Artifacts[0].Name != "qwen3-32b" {
		t.Fatalf("unexpected catalog artifacts: %+v", res.Artifacts)
	}

	// Enforce Bearer token when RequireBearerToken=true
	srvAuth := &Server{FallbackReader: cl, Scheme: scheme, RequireBearerToken: true}
	rec401 := httptest.NewRecorder()
	srvAuth.Handler().ServeHTTP(rec401, httptest.NewRequest(http.MethodGet, "/api/v1/catalog", nil))
	if rec401.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 when RequireBearerToken=true, got %d", rec401.Code)
	}
}
