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
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	modeldockv1alpha1 "github.com/wutz/modeldock/api/v1alpha1"
	"github.com/wutz/modeldock/internal/controller"
	"github.com/wutz/modeldock/pkg/harbor"
)

// Handler processes Harbor v2 PUSH_ARTIFACT / REPLICATION webhooks and registers ModelArtifact CRs.
type Handler struct {
	Client       client.Client
	SharedSecret string
}

// ServeHTTP handles Harbor webhook POST requests.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	log := logf.Log.WithName("harbor-webhook")
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if h.SharedSecret != "" {
		auth := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if subtle.ConstantTimeCompare([]byte(auth), []byte(h.SharedSecret)) != 1 {
			http.Error(w, "unauthorized webhook token", http.StatusUnauthorized)
			return
		}
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, 4*1024*1024))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var payload harbor.WebhookPayload
	if err := json.Unmarshal(body, &payload); err != nil {
		http.Error(w, fmt.Sprintf("invalid webhook payload: %v", err), http.StatusBadRequest)
		return
	}

	// Only handle push / replication arrival events.
	evType := strings.ToUpper(payload.Type)
	if evType != "PUSH_ARTIFACT" && evType != "REPLICATION" {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ignored"}`))
		return
	}

	ctx := r.Context()
	proj := payload.EventData.Repository.Namespace
	repo := payload.EventData.Repository.Name
	fullRepo := payload.EventData.Repository.RepoFullName
	if fullRepo == "" && proj != "" && repo != "" {
		fullRepo = proj + "/" + repo
	}

	var repoList modeldockv1alpha1.ModelRepositoryList
	if err := h.Client.List(ctx, &repoList); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var matched *modeldockv1alpha1.ModelRepository
	for i := range repoList.Items {
		mr := &repoList.Items[i]
		p, rn := controller.ResolveHarborProjectAndRepo(ctx, h.Client, mr)
		if p+"/"+rn == fullRepo || (proj == p && repo == rn) {
			matched = mr
			break
		}
	}
	if matched == nil {
		log.Info("no matching ModelRepository for Harbor event", "repo", fullRepo)
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"status":"no-matching-repository"}`))
		return
	}

	registered := 0
	for _, res := range payload.EventData.Resources {
		if res.Digest == "" {
			continue
		}
		tag := res.Tag
		if tag == "" {
			tag = "latest"
		}
		stage := modeldockv1alpha1.StageStaging
		switch tag {
		case "stage-production":
			stage = modeldockv1alpha1.StageProduction
		case "stage-verified":
			stage = modeldockv1alpha1.StageVerified
		}

		artName := controller.SanitizeK8sName(matched.Name, tag, strings.TrimPrefix(res.Digest, "sha256:")[:8])
		mart := &modeldockv1alpha1.ModelArtifact{
			ObjectMeta: metav1.ObjectMeta{
				Name:      artName,
				Namespace: matched.Namespace,
				Labels: map[string]string{
					modeldockv1alpha1.LabelRepository: matched.Name,
					modeldockv1alpha1.LabelStage:      string(stage),
				},
			},
			Spec: modeldockv1alpha1.ModelArtifactSpec{
				RepositoryRef: matched.Name,
				VersionTag:    tag,
				OCIDigest:     res.Digest,
				Stage:         stage,
				ArtifactType:  modeldockv1alpha1.ArtifactTypeFullWeight,
				Lineage: &modeldockv1alpha1.LineageSpec{
					SourceType:       modeldockv1alpha1.SourceTypeExternalOCI,
					ExternalRevision: res.ResourceURL,
				},
			},
		}
		_, err := controllerutil.CreateOrUpdate(ctx, h.Client, mart, func() error {
			if mart.CreationTimestamp.IsZero() {
				return nil
			}
			// If replication delivered a production tag for an existing digest, promote stage.
			if stage == modeldockv1alpha1.StageProduction && mart.Spec.Stage != modeldockv1alpha1.StageProduction {
				mart.Spec.Stage = modeldockv1alpha1.StageProduction
			}
			return nil
		})
		if err != nil {
			log.Error(err, "failed to register ModelArtifact from Harbor webhook", "artifact", artName)
			continue
		}
		registered++
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(w, `{"status":"ok","registered":%d}`, registered)
}
