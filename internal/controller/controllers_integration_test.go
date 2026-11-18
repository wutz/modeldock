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
	"bytes"
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	modeldockv1alpha1 "github.com/wutz/modeldock/api/v1alpha1"
	"github.com/wutz/modeldock/pkg/exporter"
	"github.com/wutz/modeldock/pkg/harbor"
	"github.com/wutz/modeldock/pkg/oci"
)

func buildTestSafetensors(byteLen int) []byte {
	hdrRaw, _ := json.Marshal(map[string]interface{}{
		"model.weight": map[string]interface{}{
			"dtype":        "BF16",
			"shape":        []int{32, byteLen / 64},
			"data_offsets": []int{0, byteLen},
		},
	})
	var buf bytes.Buffer
	var lenBuf [8]byte
	binary.LittleEndian.PutUint64(lenBuf[:], uint64(len(hdrRaw)))
	buf.Write(lenBuf[:])
	buf.Write(hdrRaw)
	buf.Write(bytes.Repeat([]byte{0x11}, byteLen))
	return buf.Bytes()
}

var _ = Describe("ModelDock Controllers End-to-End Integration", func() {
	const timeout = 15 * time.Second
	const interval = 200 * time.Millisecond

	It("reconciles ModelRepository -> ModelExport -> ModelArtifact -> auto-warmup ModelCache -> consumer tracking", func() {
		ns := &corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{
				Name: "forge-acme",
				Labels: map[string]string{
					modeldockv1alpha1.LabelWorkspaceProject: "acme",
				},
			},
		}
		Expect(k8sClient.Create(ctx, ns)).To(Succeed())

		By("1. Creating ModelRepository and verifying Harbor project + Robot secrets")
		mrepo := &modeldockv1alpha1.ModelRepository{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "qwen3-sft",
				Namespace: "forge-acme",
			},
			Spec: modeldockv1alpha1.ModelRepositorySpec{
				DisplayName:      "Qwen3 SFT",
				Category:         modeldockv1alpha1.CategoryLLM,
				Visibility:       modeldockv1alpha1.VisibilityPrivate,
				AutoWarmupStages: []modeldockv1alpha1.ArtifactStage{modeldockv1alpha1.StageProduction},
				RetentionPolicy: &modeldockv1alpha1.RetentionPolicy{
					KeepLastStagingVersions: ptr.To(int32(1)),
				},
			},
		}
		Expect(k8sClient.Create(ctx, mrepo)).To(Succeed())

		Eventually(func(g Gomega) {
			var cur modeldockv1alpha1.ModelRepository
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "forge-acme", Name: "qwen3-sft"}, &cur)).To(Succeed())
			g.Expect(cur.Status.Phase).To(Equal(modeldockv1alpha1.RepositoryPhaseReady))
			g.Expect(cur.Status.OCIRepository).To(ContainSubstring("/models-acme/qwen3-sft"))
			g.Expect(cur.Status.PullSecretName).To(Equal("mrepo-qwen3-sft-pull"))
			g.Expect(cur.Status.PushSecretName).To(Equal("mrepo-qwen3-sft-push"))
		}, timeout, interval).Should(Succeed())

		var pullSec, pushSec corev1.Secret
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "forge-acme", Name: "mrepo-qwen3-sft-pull"}, &pullSec)).To(Succeed())
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "forge-acme", Name: "mrepo-qwen3-sft-push"}, &pushSec)).To(Succeed())
		oldPullUser := string(pullSec.Data["username"])

		By("2. Rotating Robot credentials via annotation")
		Eventually(func(g Gomega) {
			var cur modeldockv1alpha1.ModelRepository
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "forge-acme", Name: "qwen3-sft"}, &cur)).To(Succeed())
			if cur.Annotations == nil {
				cur.Annotations = map[string]string{}
			}
			cur.Annotations[modeldockv1alpha1.AnnotationAction] = modeldockv1alpha1.ActionRotateCredentials
			g.Expect(k8sClient.Update(ctx, &cur)).To(Succeed())
		}, timeout, interval).Should(Succeed())

		Eventually(func(g Gomega) {
			var sec corev1.Secret
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "forge-acme", Name: "mrepo-qwen3-sft-pull"}, &sec)).To(Succeed())
			// Secret gets updated with new token
			g.Expect(string(sec.Data["username"])).To(Equal(oldPullUser))
			var cur modeldockv1alpha1.ModelRepository
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "forge-acme", Name: "qwen3-sft"}, &cur)).To(Succeed())
			_, stillAnnotated := cur.Annotations[modeldockv1alpha1.AnnotationAction]
			g.Expect(stillAnnotated).To(BeFalse())
		}, timeout, interval).Should(Succeed())

		By("3. Exporting a real model directory into Harbor and reconciling ModelExport Job")
		// Re-read rotated push secret and run actual exporter against Harbor test server!
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "forge-acme", Name: "mrepo-qwen3-sft-push"}, &pushSec)).To(Succeed())
		modelDir := GinkgoT().TempDir()
		Expect(os.WriteFile(filepath.Join(modelDir, "config.json"), []byte(`{"architectures":["Qwen3ForCausalLM"],"torch_dtype":"bfloat16","max_position_embeddings":32768}`), 0o644)).To(Succeed())
		Expect(os.WriteFile(filepath.Join(modelDir, "model.safetensors"), buildTestSafetensors(4096), 0o644)).To(Succeed())

		regClient, err := oci.NewRegistryClient(oci.ClientOptions{
			RegistryURL: harborSrv.URL(),
			Username:    string(pushSec.Data["username"]),
			Password:    string(pushSec.Data["password"]),
		})
		Expect(err).NotTo(HaveOccurred())

		expRes, err := exporter.ExportFromDir(ctx, exporter.DirExportOptions{
			SourceDir: modelDir,
			CommonOptions: exporter.CommonOptions{
				RegistryClient: regClient,
				Repository:     "models-acme/qwen3-sft",
				VersionTag:     "v1.0.0",
				ArtifactType:   modeldockv1alpha1.ArtifactTypeFullWeight,
			},
		})
		Expect(err).NotTo(HaveOccurred())

		mexp := &modeldockv1alpha1.ModelExport{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "export-v1",
				Namespace: "forge-acme",
			},
			Spec: modeldockv1alpha1.ModelExportSpec{
				TargetRepository: "qwen3-sft",
				VersionTag:       "v1.0.0",
				InitialStage:     modeldockv1alpha1.StageStaging,
				ArtifactType:     modeldockv1alpha1.ArtifactTypeFullWeight,
				Source: modeldockv1alpha1.ExportSource{
					PVCSource: &modeldockv1alpha1.PVCSource{
						ClaimName: "train-ckpt-pvc",
						SubPath:   "run-1/final",
					},
				},
			},
		}
		Expect(k8sClient.Create(ctx, mexp)).To(Succeed())

		// Verify ModelExportReconciler schedules non-privileged Job with ReadOnly PVC mount.
		var job batchv1.Job
		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "forge-acme", Name: "mexp-export-v1"}, &job)).To(Succeed())
			c := job.Spec.Template.Spec.Containers[0]
			g.Expect(*c.SecurityContext.Privileged).To(BeFalse())
			g.Expect(*c.SecurityContext.AllowPrivilegeEscalation).To(BeFalse())
			g.Expect(*c.SecurityContext.RunAsNonRoot).To(BeTrue())
		}, timeout, interval).Should(Succeed())

		// Simulate Job pod finishing with ExportResult in termination-log and JobComplete condition.
		resJSON, _ := json.Marshal(expRes)
		jobPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "mexp-export-v1-pod",
				Namespace: "forge-acme",
				Labels:    map[string]string{"job-name": "mexp-export-v1"},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "exporter", Image: "ghcr.io/wutz/modeldock-exporter:test"}},
			},
		}
		Expect(k8sClient.Create(ctx, jobPod)).To(Succeed())
		jobPod.Status = corev1.PodStatus{
			Phase: corev1.PodSucceeded,
			ContainerStatuses: []corev1.ContainerStatus{
				{
					Name: "exporter",
					State: corev1.ContainerState{
						Terminated: &corev1.ContainerStateTerminated{
							ExitCode: 0,
							Message:  string(resJSON),
						},
					},
				},
			},
		}
		Expect(k8sClient.Status().Update(ctx, jobPod)).To(Succeed())

		now := metav1.Now()
		job.Status.StartTime = &now
		job.Status.CompletionTime = &now
		job.Status.Succeeded = 1
		job.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
			{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, &job)).To(Succeed())

		var createdArtName string
		Eventually(func(g Gomega) {
			var cur modeldockv1alpha1.ModelExport
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "forge-acme", Name: "export-v1"}, &cur)).To(Succeed())
			g.Expect(cur.Status.Phase).To(Equal(modeldockv1alpha1.ExportPhaseSucceeded))
			g.Expect(cur.Status.ExportedDigest).To(Equal(expRes.Digest))
			createdArtName = cur.Status.ArtifactName
			g.Expect(createdArtName).NotTo(BeEmpty())
		}, timeout, interval).Should(Succeed())

		By("4. Verifying ModelArtifact becomes Ready and promoting to Production triggers ModelCache")
		Eventually(func(g Gomega) {
			var mart modeldockv1alpha1.ModelArtifact
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "forge-acme", Name: createdArtName}, &mart)).To(Succeed())
			g.Expect(mart.Status.Phase).To(Equal(modeldockv1alpha1.ArtifactPhaseReady))
			g.Expect(mart.Status.ShardCount).To(Equal(int32(1)))
		}, timeout, interval).Should(Succeed())

		Eventually(func(g Gomega) {
			var mart modeldockv1alpha1.ModelArtifact
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "forge-acme", Name: createdArtName}, &mart)).To(Succeed())
			mart.Spec.Stage = modeldockv1alpha1.StageProduction
			g.Expect(k8sClient.Update(ctx, &mart)).To(Succeed())
		}, timeout, interval).Should(Succeed())

		// Verify Harbor received stage-production tag and ModelCache was auto-created!
		hc, err := harbor.NewHTTPClient(harborSrv.URL(), harborSrv.AdminUser, harborSrv.AdminPass, true)
		Expect(err).NotTo(HaveOccurred())
		Eventually(func(g Gomega) {
			ha, err := hc.GetArtifact(ctx, "models-acme", "qwen3-sft", expRes.Digest)
			g.Expect(err).NotTo(HaveOccurred())
			g.Expect(ha).NotTo(BeNil())
			hasProdTag := false
			for _, tg := range ha.Tags {
				if tg.Name == "stage-production" {
					hasProdTag = true
				}
			}
			g.Expect(hasProdTag).To(BeTrue())
		}, timeout, interval).Should(Succeed())

		var caches modeldockv1alpha1.ModelCacheList
		Eventually(func(g Gomega) {
			g.Expect(k8sClient.List(ctx, &caches, client.InNamespace("forge-acme"))).To(Succeed())
			g.Expect(caches.Items).To(HaveLen(1))
		}, timeout, interval).Should(Succeed())
		mcName := caches.Items[0].Name

		By("5. Completing ModelCache puller Job and tracking active inference Pod consumers")
		cacheJobName := SanitizeK8sName("mcache", mcName)
		var cacheJob batchv1.Job
		Eventually(func(g Gomega) {
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "forge-acme", Name: cacheJobName}, &cacheJob)).To(Succeed())
		}, timeout, interval).Should(Succeed())

		// Add a simulated running vLLM consumer pod referencing this cache.
		vllmPod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "vllm-worker-0",
				Namespace: "forge-acme",
				Labels:    map[string]string{modeldockv1alpha1.LabelConsumerCache: mcName},
			},
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{{Name: "vllm", Image: "vllm:latest"}},
			},
		}
		Expect(k8sClient.Create(ctx, vllmPod)).To(Succeed())
		vllmPod.Status.Phase = corev1.PodRunning
		Expect(k8sClient.Status().Update(ctx, vllmPod)).To(Succeed())

		// Complete puller job.
		cacheJob.Status.StartTime = &now
		cacheJob.Status.CompletionTime = &now
		cacheJob.Status.Succeeded = 1
		cacheJob.Status.Conditions = []batchv1.JobCondition{
			{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
			{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
		}
		Expect(k8sClient.Status().Update(ctx, &cacheJob)).To(Succeed())

		Eventually(func(g Gomega) {
			var mc modeldockv1alpha1.ModelCache
			g.Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "forge-acme", Name: mcName}, &mc)).To(Succeed())
			g.Expect(mc.Status.Phase).To(Equal(modeldockv1alpha1.CachePhaseReady))
			g.Expect(mc.Status.MountSpec).NotTo(BeNil())
			g.Expect(mc.Status.MountSpec.Mode).To(Equal(modeldockv1alpha1.MountModeReadOnlyManyPVC))
			g.Expect(mc.Status.MountSpec.SubPath).To(HavePrefix("sha256/"))
			g.Expect(mc.Status.ActiveConsumers).To(Equal(int32(1)))
		}, timeout, interval).Should(Succeed())
	})
})
