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

package controller

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	modeldockv1alpha1 "github.com/wutz/modeldock/api/v1alpha1"
	"github.com/wutz/modeldock/pkg/harbor"
)

// OperatorConfig holds shared runtime configuration for ModelDock controllers.
type OperatorConfig struct {
	SystemNamespace   string
	ExporterImage     string
	PullerImage       string
	DefaultCASPVC     string
	DragonflyProxyURL string
	HarborResolver    *harbor.Resolver
}

func (c OperatorConfig) withDefaults() OperatorConfig {
	if c.SystemNamespace == "" {
		c.SystemNamespace = modeldockv1alpha1.DefaultSystemNamespace
	}
	if c.ExporterImage == "" {
		c.ExporterImage = "ghcr.io/wutz/modeldock-exporter:latest"
	}
	if c.PullerImage == "" {
		c.PullerImage = "ghcr.io/wutz/modeldock-puller:latest"
	}
	if c.DefaultCASPVC == "" {
		c.DefaultCASPVC = "modeldock-cas-shared"
	}
	return c
}

// ResolveHarborProjectAndRepo computes the Harbor project name and repo name for a ModelRepository.
func ResolveHarborProjectAndRepo(ctx context.Context, c client.Reader, mrepo *modeldockv1alpha1.ModelRepository) (string, string) {
	projectName := mrepo.Spec.Harbor.ProjectOverride
	if projectName == "" {
		if mrepo.Namespace == modeldockv1alpha1.PublicNamespace {
			projectName = modeldockv1alpha1.PublicHarborProject
		} else {
			tenant := mrepo.Labels[modeldockv1alpha1.LabelWorkspaceProject]
			if tenant == "" && c != nil {
				var ns corev1.Namespace
				if err := c.Get(ctx, types.NamespacedName{Name: mrepo.Namespace}, &ns); err == nil {
					tenant = ns.Labels[modeldockv1alpha1.LabelWorkspaceProject]
				}
			}
			if tenant == "" {
				tenant = mrepo.Namespace
			}
			projectName = "models-" + tenant
		}
	}

	repoName := mrepo.Spec.Harbor.RepositoryName
	if repoName == "" {
		repoName = mrepo.Name
	}
	return projectName, repoName
}

// BuildDockerConfigJSON encodes registry credentials into a Kubernetes dockerconfigjson payload.
func BuildDockerConfigJSON(registryHost, username, password string) ([]byte, error) {
	auth := base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
	payload := map[string]interface{}{
		"auths": map[string]interface{}{
			registryHost: map[string]string{
				"username": username,
				"password": password,
				"auth":     auth,
			},
		},
	}
	return json.Marshal(payload)
}

// SanitizeK8sName builds a valid <=63 char Kubernetes resource name with a deterministic short hash suffix if truncated.
func SanitizeK8sName(parts ...string) string {
	joined := strings.ToLower(strings.Join(parts, "-"))
	var b strings.Builder
	for _, r := range joined {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteRune('-')
		}
	}
	clean := strings.Trim(b.String(), "-")
	if len(clean) <= 63 {
		return clean
	}
	sum := sha256.Sum256([]byte(clean))
	suffix := hex.EncodeToString(sum[:4])
	return strings.TrimRight(clean[:54], "-") + "-" + suffix
}

func setCondition(conditions *[]metav1.Condition, condType string, status metav1.ConditionStatus, reason, message string) {
	now := metav1.Now()
	for i := range *conditions {
		if (*conditions)[i].Type == condType {
			if (*conditions)[i].Status != status {
				(*conditions)[i].LastTransitionTime = now
			}
			(*conditions)[i].Status = status
			(*conditions)[i].Reason = reason
			(*conditions)[i].Message = message
			return
		}
	}
	*conditions = append(*conditions, metav1.Condition{
		Type:               condType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		LastTransitionTime: now,
	})
}

func resolveArtifactNamespace(ref modeldockv1alpha1.ModelArtifactReference, defaultNS string) string {
	if ref.Namespace != "" {
		return ref.Namespace
	}
	return defaultNS
}

func ensureNamespaceLabelsForArtifact(mart *modeldockv1alpha1.ModelArtifact) bool {
	changed := false
	if mart.Labels == nil {
		mart.Labels = map[string]string{}
	}
	want := map[string]string{
		modeldockv1alpha1.LabelRepository:   mart.Spec.RepositoryRef,
		modeldockv1alpha1.LabelArtifactType: string(mart.Spec.ArtifactType),
		modeldockv1alpha1.LabelStage:        string(mart.Spec.Stage),
	}
	for k, v := range want {
		if v != "" && mart.Labels[k] != v {
			mart.Labels[k] = v
			changed = true
		}
	}
	return changed
}

func cacheNameForArtifact(mart *modeldockv1alpha1.ModelArtifact) string {
	shortDigest := strings.TrimPrefix(mart.Spec.OCIDigest, "sha256:")
	if len(shortDigest) > 8 {
		shortDigest = shortDigest[:8]
	}
	return SanitizeK8sName("mcache", mart.Name, shortDigest)
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return fmt.Sprintf("%v", err)
}
