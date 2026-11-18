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

package v1alpha1

import (
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	modeldockv1alpha1 "github.com/wutz/modeldock/api/v1alpha1"
)

var _ = Describe("ModelDock Admission Webhooks", func() {
	const digestA = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const digestB = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

	It("enforces visibility, defaults, immutability, and Production deletion protection via live kube-apiserver webhooks", func() {
		for _, ns := range []string{"modeldock-public", "tenant-a", "tenant-b", "tenant-c"} {
			Expect(k8sClient.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})).To(Succeed())
		}

		By("1. ModelRepository projectOverride restricted to modeldock-public")
		badRepo := &modeldockv1alpha1.ModelRepository{
			ObjectMeta: metav1.ObjectMeta{Name: "bad-override", Namespace: "tenant-a"},
			Spec: modeldockv1alpha1.ModelRepositorySpec{
				Harbor: modeldockv1alpha1.HarborRepositoryConfig{ProjectOverride: "models-public"},
			},
		}
		Expect(k8sClient.Create(ctx, badRepo)).NotTo(Succeed())

		pubRepo := &modeldockv1alpha1.ModelRepository{
			ObjectMeta: metav1.ObjectMeta{Name: "qwen3-base", Namespace: "modeldock-public"},
			Spec: modeldockv1alpha1.ModelRepositorySpec{
				Visibility: modeldockv1alpha1.VisibilityPublic,
				Harbor:     modeldockv1alpha1.HarborRepositoryConfig{ProjectOverride: "models-public"},
			},
		}
		Expect(k8sClient.Create(ctx, pubRepo)).To(Succeed())

		privRepoB := &modeldockv1alpha1.ModelRepository{
			ObjectMeta: metav1.ObjectMeta{Name: "secret-b", Namespace: "tenant-b"},
			Spec:       modeldockv1alpha1.ModelRepositorySpec{Visibility: modeldockv1alpha1.VisibilityPrivate},
		}
		Expect(k8sClient.Create(ctx, privRepoB)).To(Succeed())

		sharedRepoB := &modeldockv1alpha1.ModelRepository{
			ObjectMeta: metav1.ObjectMeta{Name: "shared-b", Namespace: "tenant-b"},
			Spec: modeldockv1alpha1.ModelRepositorySpec{
				Visibility:       modeldockv1alpha1.VisibilityWorkspaceShared,
				SharedNamespaces: []string{"tenant-a"},
			},
		}
		Expect(k8sClient.Create(ctx, sharedRepoB)).To(Succeed())

		repoA := &modeldockv1alpha1.ModelRepository{
			ObjectMeta: metav1.ObjectMeta{Name: "lora-a", Namespace: "tenant-a"},
			Spec:       modeldockv1alpha1.ModelRepositorySpec{Visibility: modeldockv1alpha1.VisibilityPrivate},
		}
		Expect(k8sClient.Create(ctx, repoA)).To(Succeed())

		By("2. Creating public & private base ModelArtifacts and verifying defaulting")
		pubBase := &modeldockv1alpha1.ModelArtifact{
			ObjectMeta: metav1.ObjectMeta{Name: "qwen3-32b", Namespace: "modeldock-public"},
			Spec: modeldockv1alpha1.ModelArtifactSpec{
				RepositoryRef: "qwen3-base",
				VersionTag:    "bf16",
				OCIDigest:     digestA,
			},
		}
		Expect(k8sClient.Create(ctx, pubBase)).To(Succeed())
		Expect(pubBase.Spec.Stage).To(Equal(modeldockv1alpha1.StageStaging))
		Expect(pubBase.Spec.ArtifactType).To(Equal(modeldockv1alpha1.ArtifactTypeFullWeight))

		privArtB := &modeldockv1alpha1.ModelArtifact{
			ObjectMeta: metav1.ObjectMeta{Name: "priv-base-b", Namespace: "tenant-b"},
			Spec: modeldockv1alpha1.ModelArtifactSpec{
				RepositoryRef: "secret-b",
				VersionTag:    "v1",
				OCIDigest:     digestA,
			},
		}
		Expect(k8sClient.Create(ctx, privArtB)).To(Succeed())

		sharedArtB := &modeldockv1alpha1.ModelArtifact{
			ObjectMeta: metav1.ObjectMeta{Name: "shared-base-b", Namespace: "tenant-b"},
			Spec: modeldockv1alpha1.ModelArtifactSpec{
				RepositoryRef: "shared-b",
				VersionTag:    "v1",
				OCIDigest:     digestA,
			},
		}
		Expect(k8sClient.Create(ctx, sharedArtB)).To(Succeed())

		By("3. Enforcing LoRA baseModelRef requirement and cross-namespace Anti-Confused-Deputy rules")
		loraNoBase := &modeldockv1alpha1.ModelArtifact{
			ObjectMeta: metav1.ObjectMeta{Name: "lora-missing-base", Namespace: "tenant-a"},
			Spec: modeldockv1alpha1.ModelArtifactSpec{
				RepositoryRef: "lora-a",
				VersionTag:    "v1",
				OCIDigest:     digestA,
				ArtifactType:  modeldockv1alpha1.ArtifactTypeLoRAAdapter,
			},
		}
		Expect(k8sClient.Create(ctx, loraNoBase)).NotTo(Succeed())

		// Tenant A referencing Tenant B's Private model -> MUST DENY
		loraSteal := &modeldockv1alpha1.ModelArtifact{
			ObjectMeta: metav1.ObjectMeta{Name: "lora-steal", Namespace: "tenant-a"},
			Spec: modeldockv1alpha1.ModelArtifactSpec{
				RepositoryRef: "lora-a",
				VersionTag:    "v1",
				OCIDigest:     digestA,
				ArtifactType:  modeldockv1alpha1.ArtifactTypeLoRAAdapter,
				BaseModelRef: &modeldockv1alpha1.ModelArtifactReference{
					Namespace: "tenant-b",
					Name:      "priv-base-b",
				},
			},
		}
		err := k8sClient.Create(ctx, loraSteal)
		Expect(err).To(HaveOccurred())
		Expect(strings.Contains(err.Error(), "Private")).To(BeTrue())

		// Tenant A referencing Tenant B's WorkspaceShared model (whitelisted) -> ALLOW
		loraSharedOK := &modeldockv1alpha1.ModelArtifact{
			ObjectMeta: metav1.ObjectMeta{Name: "lora-shared-ok", Namespace: "tenant-a"},
			Spec: modeldockv1alpha1.ModelArtifactSpec{
				RepositoryRef: "lora-a",
				VersionTag:    "v1",
				OCIDigest:     digestA,
				ArtifactType:  modeldockv1alpha1.ArtifactTypeLoRAAdapter,
				BaseModelRef: &modeldockv1alpha1.ModelArtifactReference{
					Namespace: "tenant-b",
					Name:      "shared-base-b",
				},
			},
		}
		Expect(k8sClient.Create(ctx, loraSharedOK)).To(Succeed())

		// Tenant A referencing Public model -> ALLOW
		loraPubOK := &modeldockv1alpha1.ModelArtifact{
			ObjectMeta: metav1.ObjectMeta{Name: "lora-pub-ok", Namespace: "tenant-a"},
			Spec: modeldockv1alpha1.ModelArtifactSpec{
				RepositoryRef: "lora-a",
				VersionTag:    "v2",
				OCIDigest:     digestA,
				ArtifactType:  modeldockv1alpha1.ArtifactTypeLoRAAdapter,
				Stage:         modeldockv1alpha1.StageProduction,
				BaseModelRef: &modeldockv1alpha1.ModelArtifactReference{
					Namespace: "modeldock-public",
					Name:      "qwen3-32b",
					OCIDigest: digestA,
				},
			},
		}
		Expect(k8sClient.Create(ctx, loraPubOK)).To(Succeed())

		By("4. Enforcing immutability once Ready & Production deletion protection")
		loraPubOK.Status.Phase = modeldockv1alpha1.ArtifactPhaseReady
		Expect(k8sClient.Status().Update(ctx, loraPubOK)).To(Succeed())

		var cur modeldockv1alpha1.ModelArtifact
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "tenant-a", Name: "lora-pub-ok"}, &cur)).To(Succeed())
		cur.Spec.OCIDigest = digestB
		Expect(k8sClient.Update(ctx, &cur)).NotTo(Succeed())

		// Create active ModelCache with activeConsumers=2 -> deletion must be blocked!
		mc := &modeldockv1alpha1.ModelCache{
			ObjectMeta: metav1.ObjectMeta{Name: "mc-lora-pub", Namespace: "tenant-a"},
			Spec: modeldockv1alpha1.ModelCacheSpec{
				ArtifactRef: modeldockv1alpha1.ModelArtifactReference{Name: "lora-pub-ok"},
			},
		}
		Expect(k8sClient.Create(ctx, mc)).To(Succeed())
		mc.Status.ActiveConsumers = 2
		Expect(k8sClient.Status().Update(ctx, mc)).To(Succeed())

		Expect(k8sClient.Delete(ctx, loraPubOK)).NotTo(Succeed())

		// After draining consumers & demoting to Deprecated, deletion succeeds.
		mc.Status.ActiveConsumers = 0
		Expect(k8sClient.Status().Update(ctx, mc)).To(Succeed())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "tenant-a", Name: "lora-pub-ok"}, &cur)).To(Succeed())
		cur.Spec.Stage = modeldockv1alpha1.StageDeprecated
		Expect(k8sClient.Update(ctx, &cur)).To(Succeed())
		Expect(k8sClient.Delete(ctx, &cur)).To(Succeed())

		By("5. Validating ModelExport defaulting & single-source rules")
		badExport := &modeldockv1alpha1.ModelExport{
			ObjectMeta: metav1.ObjectMeta{Name: "bad-exp", Namespace: "tenant-a"},
			Spec: modeldockv1alpha1.ModelExportSpec{
				TargetRepository: "lora-a",
				VersionTag:       "v1",
			},
		}
		Expect(k8sClient.Create(ctx, badExport)).NotTo(Succeed())

		goodExport := &modeldockv1alpha1.ModelExport{
			ObjectMeta: metav1.ObjectMeta{Name: "good-exp", Namespace: "tenant-a"},
			Spec: modeldockv1alpha1.ModelExportSpec{
				TargetRepository: "lora-a",
				VersionTag:       "v1",
				Source: modeldockv1alpha1.ExportSource{
					ExternalHubSource: &modeldockv1alpha1.ExternalHubSource{
						Provider: modeldockv1alpha1.HubProviderModelScope,
						RepoID:   "Qwen/Qwen3-32B",
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, goodExport)).To(Succeed())
		Expect(goodExport.Spec.Concurrency).To(Equal(int32(4)))
		Expect(*goodExport.Spec.TTLSecondsAfterFinished).To(Equal(int32(86400)))
	})
})
