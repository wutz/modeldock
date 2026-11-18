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

package harborwebhook

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	modeldockv1alpha1 "github.com/wutz/modeldock/api/v1alpha1"
)

func TestHarborWebhookHandler(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = modeldockv1alpha1.AddToScheme(scheme)

	repo := &modeldockv1alpha1.ModelRepository{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "qwen3-32b",
			Namespace: modeldockv1alpha1.PublicNamespace,
		},
		Spec: modeldockv1alpha1.ModelRepositorySpec{
			Visibility: modeldockv1alpha1.VisibilityPublic,
		},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(repo).Build()
	h := &Handler{
		Client:       cl,
		SharedSecret: "top-secret",
	}

	payload := `{
		"type": "REPLICATION",
		"event_data": {
			"repository": {"name": "qwen3-32b", "namespace": "models-public", "repo_full_name": "models-public/qwen3-32b"},
			"resources": [{"digest": "sha256:4b227777d4dd1fc61c6f884f48641d02b4d121d3fd328cb08b5531fcacdabf8a", "tag": "stage-production", "resource_url": "harbor.infer/models-public/qwen3-32b:stage-production"}]
		}
	}`

	// Unauthorized without Bearer token
	reqBad := httptest.NewRequest(http.MethodPost, "/webhooks/harbor", bytes.NewBufferString(payload))
	recBad := httptest.NewRecorder()
	h.ServeHTTP(recBad, reqBad)
	if recBad.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 without secret, got %d", recBad.Code)
	}

	// Authorized Harbor REPLICATION event registers ModelArtifact with stage=Production
	reqOK := httptest.NewRequest(http.MethodPost, "/webhooks/harbor", bytes.NewBufferString(payload))
	reqOK.Header.Set("Authorization", "Bearer top-secret")
	recOK := httptest.NewRecorder()
	h.ServeHTTP(recOK, reqOK)
	if recOK.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", recOK.Code, recOK.Body.String())
	}

	var list modeldockv1alpha1.ModelArtifactList
	if err := cl.List(context.Background(), &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 || list.Items[0].Spec.Stage != modeldockv1alpha1.StageProduction {
		t.Fatalf("expected 1 Production ModelArtifact, got %+v", list.Items)
	}
}
