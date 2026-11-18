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
	"encoding/json"
	"fmt"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	modeldockv1alpha1 "github.com/wutz/modeldock/api/v1alpha1"
	"github.com/wutz/modeldock/pkg/harbor"
	"github.com/wutz/modeldock/pkg/puller"
)

const casCacheVolumeName = "cas-cache"

// ModelCacheReconciler reconciles a ModelCache object, pre-warming Harbor OCI artifacts into CAS pools.
type ModelCacheReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Config OperatorConfig
}

// +kubebuilder:rbac:groups=modeldock.wutz.dev,resources=modelcaches,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=modeldock.wutz.dev,resources=modelcaches/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=modeldock.wutz.dev,resources=modelcaches/finalizers,verbs=update
// +kubebuilder:rbac:groups=modeldock.wutz.dev,resources=modelartifacts;modelrepositories,verbs=get;list;watch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods;nodes;persistentvolumeclaims;secrets,verbs=get;list;watch

// Reconcile manages CAS warmup Jobs, MountSpec publication, active consumer tracking, and TTL/LRU eviction.
func (r *ModelCacheReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	cfg := r.Config.withDefaults()

	var mc modeldockv1alpha1.ModelCache
	if err := r.Get(ctx, req.NamespacedName, &mc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !mc.DeletionTimestamp.IsZero() || mc.Status.Phase == modeldockv1alpha1.CachePhaseEvicted {
		return ctrl.Result{}, nil
	}

	artNS := resolveArtifactNamespace(mc.Spec.ArtifactRef, mc.Namespace)
	var mart modeldockv1alpha1.ModelArtifact
	if err := r.Get(ctx, types.NamespacedName{Namespace: artNS, Name: mc.Spec.ArtifactRef.Name}, &mart); err != nil {
		if apierrors.IsNotFound(err) {
			return r.markPending(ctx, &mc, "ArtifactNotFound", fmt.Sprintf("Waiting for ModelArtifact %s/%s", artNS, mc.Spec.ArtifactRef.Name))
		}
		return ctrl.Result{}, err
	}
	if mart.Status.Phase != modeldockv1alpha1.ArtifactPhaseReady {
		return r.markPending(ctx, &mc, "ArtifactNotReady", "Waiting for ModelArtifact to become Ready")
	}

	var mrepo modeldockv1alpha1.ModelRepository
	if err := r.Get(ctx, types.NamespacedName{Namespace: mart.Namespace, Name: mart.Spec.RepositoryRef}, &mrepo); err != nil {
		return r.markFailed(ctx, &mc, "RepositoryNotFound", err.Error())
	}

	strategy := mc.Spec.Strategy
	if strategy == "" {
		strategy = modeldockv1alpha1.CacheStrategySharedFilesystem
	}

	// Fast path for Kubernetes native OCI ImageVolume.
	if strategy == modeldockv1alpha1.CacheStrategyImageVolume {
		return r.reconcileImageVolume(ctx, &mc, &mart)
	}

	resolver := cfg.HarborResolver
	if resolver == nil {
		resolver = &harbor.Resolver{Client: r.Client, SystemNamespace: cfg.SystemNamespace}
	}
	instCfg, err := resolver.Resolve(ctx, mrepo.Spec.Harbor.InstanceRef)
	if err != nil {
		return r.markFailed(ctx, &mc, "HarborConfigError", err.Error())
	}

	// If already Ready, track ActiveConsumers and evaluate TTL/LRU eviction.
	if mc.Status.Phase == modeldockv1alpha1.CachePhaseReady {
		return r.reconcileReadyCache(ctx, &mc, &mart)
	}

	jobName := SanitizeK8sName("mcache", mc.Name)
	var job batchv1.Job
	err = r.Get(ctx, types.NamespacedName{Namespace: mc.Namespace, Name: jobName}, &job)
	if apierrors.IsNotFound(err) {
		return r.createPullerJob(ctx, &mc, &mart, &mrepo, instCfg, cfg, strategy, jobName)
	} else if err != nil {
		return ctrl.Result{}, err
	}

	return r.reconcilePullerJobStatus(ctx, &mc, &mart, &job, strategy, cfg, jobName)
}

func (r *ModelCacheReconciler) reconcileImageVolume(
	ctx context.Context,
	mc *modeldockv1alpha1.ModelCache,
	mart *modeldockv1alpha1.ModelArtifact,
) (ctrl.Result, error) {
	now := metav1.Now()
	mc.Status.Phase = modeldockv1alpha1.CachePhaseReady
	mc.Status.ResolvedDigest = mart.Spec.OCIDigest
	mc.Status.MountSpec = &modeldockv1alpha1.CacheMountSpec{
		Mode:         modeldockv1alpha1.MountModeOCIImageVolume,
		OCIReference: mart.Status.OCIReference,
	}
	if mc.Status.LastUsedTime == nil {
		mc.Status.LastUsedTime = &now
	}
	setCondition(&mc.Status.Conditions, "CacheReady", metav1.ConditionTrue, "ImageVolumeReady", "Native OCI ImageVolume reference ready")
	if err := r.Status().Update(ctx, mc); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *ModelCacheReconciler) reconcileReadyCache(
	ctx context.Context,
	mc *modeldockv1alpha1.ModelCache,
	mart *modeldockv1alpha1.ModelArtifact,
) (ctrl.Result, error) {
	consumers := r.countActiveConsumers(ctx, mc, mart)
	mc.Status.ActiveConsumers = consumers
	now := metav1.Now()
	if consumers > 0 {
		mc.Status.LastUsedTime = &now
	} else if mc.Spec.TTLHoursAfterUnused != nil && *mc.Spec.TTLHoursAfterUnused > 0 && mc.Status.LastUsedTime != nil {
		ttl := time.Duration(*mc.Spec.TTLHoursAfterUnused) * time.Hour
		if time.Since(mc.Status.LastUsedTime.Time) > ttl {
			mc.Status.Phase = modeldockv1alpha1.CachePhaseEvicted
			mc.Status.EvictedAt = &now
			mc.Status.Message = fmt.Sprintf("Evicted after %dh with 0 active consumers", *mc.Spec.TTLHoursAfterUnused)
			setCondition(&mc.Status.Conditions, "CacheReady", metav1.ConditionFalse, "EvictedByTTL", mc.Status.Message)
			if err := r.Status().Update(ctx, mc); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
	}
	if err := r.Status().Update(ctx, mc); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
}

func (r *ModelCacheReconciler) createPullerJob(
	ctx context.Context,
	mc *modeldockv1alpha1.ModelCache,
	mart *modeldockv1alpha1.ModelArtifact,
	mrepo *modeldockv1alpha1.ModelRepository,
	instCfg *harbor.InstanceConfig,
	cfg OperatorConfig,
	strategy modeldockv1alpha1.CacheStrategy,
	jobName string,
) (ctrl.Result, error) {
	newJob, pvcName := r.buildPullerJob(ctx, mc, mart, mrepo, instCfg, cfg, jobName)
	if err := controllerutil.SetControllerReference(mc, newJob, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	if err := r.Create(ctx, newJob); err != nil {
		return ctrl.Result{}, err
	}
	mc.Status.Phase = modeldockv1alpha1.CachePhaseWarmingUp
	mc.Status.ResolvedDigest = mart.Spec.OCIDigest
	mc.Status.Message = fmt.Sprintf("Warming up digest %s via %s (pvc=%s)", mart.Spec.OCIDigest, strategy, pvcName)
	setCondition(&mc.Status.Conditions, "CacheReady", metav1.ConditionFalse, "WarmingUp", mc.Status.Message)
	if err := r.Status().Update(ctx, mc); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
}

func (r *ModelCacheReconciler) reconcilePullerJobStatus(
	ctx context.Context,
	mc *modeldockv1alpha1.ModelCache,
	mart *modeldockv1alpha1.ModelArtifact,
	job *batchv1.Job,
	strategy modeldockv1alpha1.CacheStrategy,
	cfg OperatorConfig,
	jobName string,
) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	for _, cond := range job.Status.Conditions {
		if cond.Type == batchv1.JobFailed && cond.Status == corev1.ConditionTrue {
			return r.markFailed(ctx, mc, "PullerJobFailed", cond.Message)
		}
		if cond.Type != batchv1.JobComplete || cond.Status != corev1.ConditionTrue {
			continue
		}

		dur := int64(0)
		if job.Status.StartTime != nil && job.Status.CompletionTime != nil {
			dur = int64(job.Status.CompletionTime.Sub(job.Status.StartTime.Time).Seconds())
		}
		if pullRes := r.readPullResult(ctx, mc.Namespace, jobName); pullRes != nil && pullRes.DurationSeconds > 0 {
			dur = pullRes.DurationSeconds
		}

		mountSpec, cachedNodes := r.buildMountSpec(mc, mart, strategy, cfg)
		now := metav1.Now()
		consumers := r.countActiveConsumers(ctx, mc, mart)
		mc.Status.Phase = modeldockv1alpha1.CachePhaseReady
		mc.Status.ResolvedDigest = mart.Spec.OCIDigest
		mc.Status.MountSpec = &mountSpec
		mc.Status.CachedNodesCount = cachedNodes
		mc.Status.WarmupDurationSeconds = dur
		mc.Status.ActiveConsumers = consumers
		mc.Status.LastUsedTime = &now
		mc.Status.Message = fmt.Sprintf("Cache ready at %s%s", mountSpec.SubPath, mountSpec.HostPath)
		setCondition(&mc.Status.Conditions, "CacheReady", metav1.ConditionTrue, "WarmupSucceeded", mc.Status.Message)

		if err := r.Status().Update(ctx, mc); err != nil {
			return ctrl.Result{}, err
		}
		log.Info("ModelCache ready", "cache", mc.Name, "digest", mart.Spec.OCIDigest, "durationSeconds", dur)
		return ctrl.Result{RequeueAfter: 5 * time.Minute}, nil
	}
	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

func (r *ModelCacheReconciler) buildMountSpec(
	mc *modeldockv1alpha1.ModelCache,
	mart *modeldockv1alpha1.ModelArtifact,
	strategy modeldockv1alpha1.CacheStrategy,
	cfg OperatorConfig,
) (modeldockv1alpha1.CacheMountSpec, int32) {
	hexDigest := strings.TrimPrefix(mart.Spec.OCIDigest, "sha256:")
	if strategy == modeldockv1alpha1.CacheStrategyNodeLocalNVMe {
		hostRoot := "/var/lib/modeldock/cas"
		if mc.Spec.NodeLocalNVMe != nil && mc.Spec.NodeLocalNVMe.HostCacheRoot != "" {
			hostRoot = strings.TrimRight(mc.Spec.NodeLocalNVMe.HostCacheRoot, "/")
		}
		return modeldockv1alpha1.CacheMountSpec{
			Mode:     modeldockv1alpha1.MountModeNodeLocalHostPath,
			HostPath: fmt.Sprintf("%s/manifests/sha256/%s", hostRoot, hexDigest),
		}, 1
	}
	pvcName := r.resolveSharedPVCName(mc, cfg)
	return modeldockv1alpha1.CacheMountSpec{
		Mode:      modeldockv1alpha1.MountModeReadOnlyManyPVC,
		ClaimName: pvcName,
		SubPath:   "sha256/" + hexDigest,
	}, 0
}

func (r *ModelCacheReconciler) buildPullerJob(
	ctx context.Context,
	mc *modeldockv1alpha1.ModelCache,
	mart *modeldockv1alpha1.ModelArtifact,
	mrepo *modeldockv1alpha1.ModelRepository,
	instCfg *harbor.InstanceConfig,
	cfg OperatorConfig,
	jobName string,
) (*batchv1.Job, string) {
	projectName, repoName := ResolveHarborProjectAndRepo(ctx, r.Client, mrepo)
	fullRepo := projectName + "/" + repoName
	strategy := mc.Spec.Strategy
	if strategy == "" {
		strategy = modeldockv1alpha1.CacheStrategySharedFilesystem
	}

	args := []string{
		"--registry-url=" + instCfg.Endpoint,
		"--repository=" + fullRepo,
		"--reference=" + mart.Spec.OCIDigest,
		"--strategy=" + string(strategy),
		"--cache-root=/modeldock-cas",
		"--credentials-dir=/etc/modeldock/pull-secret",
		fmt.Sprintf("--insecure-skip-tls=%t", instCfg.InsecureSkipTLS),
		"--output-file=/dev/termination-log",
	}
	if mc.Spec.EnableP2P {
		proxyURL := cfg.DragonflyProxyURL
		if proxyURL == "" {
			proxyURL = instCfg.ProxyURL
		}
		if proxyURL != "" {
			args = append(args, "--proxy-url="+proxyURL)
		}
	}

	volumes := []corev1.Volume{
		{
			Name: "pull-secret",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: mrepo.Status.PullSecretName,
				},
			},
		},
	}
	mounts := []corev1.VolumeMount{
		{
			Name:      "pull-secret",
			MountPath: "/etc/modeldock/pull-secret",
			ReadOnly:  true,
		},
		{
			Name:      casCacheVolumeName,
			MountPath: "/modeldock-cas",
		},
	}

	var pvcName string
	var nodeSelector map[string]string
	if strategy == modeldockv1alpha1.CacheStrategyNodeLocalNVMe {
		hostRoot := "/var/lib/modeldock/cas"
		if mc.Spec.NodeLocalNVMe != nil {
			if mc.Spec.NodeLocalNVMe.HostCacheRoot != "" {
				hostRoot = mc.Spec.NodeLocalNVMe.HostCacheRoot
			}
			nodeSelector = mc.Spec.NodeLocalNVMe.NodeSelector
		}
		hostPathType := corev1.HostPathDirectoryOrCreate
		volumes = append(volumes, corev1.Volume{
			Name: casCacheVolumeName,
			VolumeSource: corev1.VolumeSource{
				HostPath: &corev1.HostPathVolumeSource{
					Path: hostRoot,
					Type: &hostPathType,
				},
			},
		})
	} else {
		pvcName = r.resolveSharedPVCName(mc, cfg)
		volumes = append(volumes, corev1.Volume{
			Name: casCacheVolumeName,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: pvcName,
					ReadOnly:  false,
				},
			},
		})
	}

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: mc.Namespace,
			Labels: map[string]string{
				modeldockv1alpha1.LabelCacheJob: mc.Name,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            new(int32(2)),
			TTLSecondsAfterFinished: new(int32(3600)),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						modeldockv1alpha1.LabelCacheJob: mc.Name,
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					NodeSelector:  nodeSelector,
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: new(true),
					},
					Containers: []corev1.Container{
						{
							Name:            "puller",
							Image:           cfg.PullerImage,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Args:            args,
							VolumeMounts:    mounts,
							SecurityContext: &corev1.SecurityContext{
								Privileged:               new(false),
								AllowPrivilegeEscalation: new(false),
								RunAsNonRoot:             new(true),
								Capabilities: &corev1.Capabilities{
									Drop: []corev1.Capability{"ALL"},
								},
							},
						},
					},
					Volumes: volumes,
				},
			},
		},
	}, pvcName
}

func (r *ModelCacheReconciler) resolveSharedPVCName(mc *modeldockv1alpha1.ModelCache, cfg OperatorConfig) string {
	if mc.Spec.SharedFilesystem != nil {
		if mc.Spec.SharedFilesystem.PVCName != "" {
			return mc.Spec.SharedFilesystem.PVCName
		}
		if mc.Spec.SharedFilesystem.CachePoolName != "" && mc.Spec.SharedFilesystem.CachePoolName != "default-rwx-model-pool" {
			return mc.Spec.SharedFilesystem.CachePoolName
		}
	}
	return cfg.DefaultCASPVC
}

func (r *ModelCacheReconciler) readPullResult(ctx context.Context, ns, jobName string) *puller.PullResult {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(ns), client.MatchingLabels{"job-name": jobName}); err != nil {
		return nil
	}
	for i := range pods.Items {
		for _, cs := range pods.Items[i].Status.ContainerStatuses {
			if cs.State.Terminated != nil && cs.State.Terminated.Message != "" {
				var res puller.PullResult
				if json.Unmarshal([]byte(cs.State.Terminated.Message), &res) == nil {
					return &res
				}
			}
		}
	}
	return nil
}

func (r *ModelCacheReconciler) countActiveConsumers(ctx context.Context, mc *modeldockv1alpha1.ModelCache, mart *modeldockv1alpha1.ModelArtifact) int32 {
	var pods corev1.PodList
	// List pods referencing this cache or artifact across namespaces (for Public/WorkspaceShared base models).
	if err := r.List(ctx, &pods, client.MatchingLabels{modeldockv1alpha1.LabelConsumerCache: mc.Name}); err != nil {
		return mc.Status.ActiveConsumers
	}
	seen := make(map[string]struct{})
	var count int32
	for i := range pods.Items {
		p := &pods.Items[i]
		if p.Status.Phase == corev1.PodRunning || p.Status.Phase == corev1.PodPending {
			seen[p.Namespace+"/"+p.Name] = struct{}{}
			count++
		}
	}

	var artPods corev1.PodList
	if err := r.List(ctx, &artPods, client.MatchingLabels{modeldockv1alpha1.LabelConsumerArtifact: mart.Name}); err == nil {
		for i := range artPods.Items {
			p := &artPods.Items[i]
			if _, ok := seen[p.Namespace+"/"+p.Name]; ok {
				continue
			}
			if p.Status.Phase == corev1.PodRunning || p.Status.Phase == corev1.PodPending {
				count++
			}
		}
	}
	return count
}

func (r *ModelCacheReconciler) markPending(ctx context.Context, mc *modeldockv1alpha1.ModelCache, reason, msg string) (ctrl.Result, error) {
	mc.Status.Phase = modeldockv1alpha1.CachePhasePending
	mc.Status.Message = msg
	setCondition(&mc.Status.Conditions, "CacheReady", metav1.ConditionFalse, reason, msg)
	_ = r.Status().Update(ctx, mc)
	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

func (r *ModelCacheReconciler) markFailed(ctx context.Context, mc *modeldockv1alpha1.ModelCache, reason, msg string) (ctrl.Result, error) {
	mc.Status.Phase = modeldockv1alpha1.CachePhaseFailed
	mc.Status.Message = msg
	setCondition(&mc.Status.Conditions, "CacheReady", metav1.ConditionFalse, reason, msg)
	_ = r.Status().Update(ctx, mc)
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ModelCacheReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&modeldockv1alpha1.ModelCache{}).
		Owns(&batchv1.Job{}).
		Named("modelcache").
		Complete(r)
}
