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
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	modeldockv1alpha1 "github.com/wutz/modeldock/api/v1alpha1"
	"github.com/wutz/modeldock/pkg/exporter"
	"github.com/wutz/modeldock/pkg/harbor"
)

// ModelExportReconciler reconciles a ModelExport object by scheduling unprivileged streaming exporter Jobs.
type ModelExportReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Config OperatorConfig
}

// +kubebuilder:rbac:groups=modeldock.wutz.dev,resources=modelexports,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=modeldock.wutz.dev,resources=modelexports/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=modeldock.wutz.dev,resources=modelexports/finalizers,verbs=update
// +kubebuilder:rbac:groups=modeldock.wutz.dev,resources=modelrepositories,verbs=get;list;watch
// +kubebuilder:rbac:groups=modeldock.wutz.dev,resources=modelartifacts,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods;secrets,verbs=get;list;watch

// Reconcile drives ModelExport through Pending -> Validating/Pushing -> Succeeded/Failed and registers ModelArtifact.
func (r *ModelExportReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	cfg := r.Config.withDefaults()

	var mexp modeldockv1alpha1.ModelExport
	if err := r.Get(ctx, req.NamespacedName, &mexp); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !mexp.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// Terminal state reached; nothing more to reconcile.
	if mexp.Status.Phase == modeldockv1alpha1.ExportPhaseSucceeded || mexp.Status.Phase == modeldockv1alpha1.ExportPhaseFailed {
		return ctrl.Result{}, nil
	}

	var mrepo modeldockv1alpha1.ModelRepository
	if err := r.Get(ctx, types.NamespacedName{Namespace: mexp.Namespace, Name: mexp.Spec.TargetRepository}, &mrepo); err != nil {
		if apierrors.IsNotFound(err) {
			return r.markPending(ctx, &mexp, "RepositoryNotFound", fmt.Sprintf("Waiting for ModelRepository %q", mexp.Spec.TargetRepository))
		}
		return ctrl.Result{}, err
	}
	if mrepo.Status.Phase != modeldockv1alpha1.RepositoryPhaseReady {
		return r.markPending(ctx, &mexp, "RepositoryNotReady", "Waiting for ModelRepository to become Ready")
	}

	resolver := cfg.HarborResolver
	if resolver == nil {
		resolver = &harbor.Resolver{Client: r.Client, SystemNamespace: cfg.SystemNamespace}
	}
	instCfg, err := resolver.Resolve(ctx, mrepo.Spec.Harbor.InstanceRef)
	if err != nil {
		return r.markFailed(ctx, &mexp, "HarborConfigError", err.Error())
	}

	jobName := SanitizeK8sName("mexp", mexp.Name)
	var job batchv1.Job
	err = r.Get(ctx, types.NamespacedName{Namespace: mexp.Namespace, Name: jobName}, &job)
	if apierrors.IsNotFound(err) {
		newJob, buildErr := r.buildExporterJob(ctx, &mexp, &mrepo, instCfg, cfg, jobName)
		if buildErr != nil {
			return r.markFailed(ctx, &mexp, "InvalidExportSpec", buildErr.Error())
		}
		if err := controllerutil.SetControllerReference(&mexp, newJob, r.Scheme); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.Create(ctx, newJob); err != nil {
			return ctrl.Result{}, err
		}
		now := metav1.Now()
		mexp.Status.Phase = modeldockv1alpha1.ExportPhaseValidating
		mexp.Status.JobName = jobName
		if mexp.Status.StartTime == nil {
			mexp.Status.StartTime = &now
		}
		setCondition(&mexp.Status.Conditions, "Exported", metav1.ConditionFalse, "JobScheduled", "Created unprivileged modeldock-exporter Job")
		if err := r.Status().Update(ctx, &mexp); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
	} else if err != nil {
		return ctrl.Result{}, err
	}

	// Evaluate Job status.
	for _, cond := range job.Status.Conditions {
		if cond.Type == batchv1.JobFailed && cond.Status == corev1.ConditionTrue {
			msg := r.readJobTerminationOrFailure(ctx, mexp.Namespace, jobName)
			if msg == "" {
				msg = cond.Message
			}
			return r.markFailed(ctx, &mexp, "ExporterJobFailed", msg)
		}
		if cond.Type == batchv1.JobComplete && cond.Status == corev1.ConditionTrue {
			res, err := r.readExportResult(ctx, &mexp, jobName)
			if err != nil {
				log.Error(err, "failed to parse exporter result")
				return r.markFailed(ctx, &mexp, "ResultParseFailed", err.Error())
			}
			artifactName, err := r.ensureModelArtifact(ctx, &mexp, res)
			if err != nil {
				return ctrl.Result{}, err
			}
			now := metav1.Now()
			mexp.Status.Phase = modeldockv1alpha1.ExportPhaseSucceeded
			mexp.Status.JobName = jobName
			mexp.Status.CompletionTime = &now
			mexp.Status.ExportedDigest = res.Digest
			mexp.Status.ArtifactName = artifactName
			mexp.Status.BytesTransferred = res.BytesTransferred
			mexp.Status.Message = fmt.Sprintf("Exported %s (%d shards)", res.Digest, res.ShardCount)
			setCondition(&mexp.Status.Conditions, "Exported", metav1.ConditionTrue, "Succeeded", mexp.Status.Message)
			if err := r.Status().Update(ctx, &mexp); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, nil
		}
	}

	// Job still running -> Pushing.
	if mexp.Status.Phase != modeldockv1alpha1.ExportPhasePushing {
		mexp.Status.Phase = modeldockv1alpha1.ExportPhasePushing
		mexp.Status.JobName = jobName
		_ = r.Status().Update(ctx, &mexp)
	}
	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

func (r *ModelExportReconciler) buildExporterJob(
	ctx context.Context,
	mexp *modeldockv1alpha1.ModelExport,
	mrepo *modeldockv1alpha1.ModelRepository,
	instCfg *harbor.InstanceConfig,
	cfg OperatorConfig,
	jobName string,
) (*batchv1.Job, error) {
	projectName, repoName := ResolveHarborProjectAndRepo(ctx, r.Client, mrepo)
	fullRepo := projectName + "/" + repoName

	concurrency := mexp.Spec.Concurrency
	if concurrency <= 0 {
		concurrency = 4
	}

	args := []string{
		"--registry-url=" + instCfg.Endpoint,
		"--repository=" + fullRepo,
		"--version-tag=" + mexp.Spec.VersionTag,
		"--artifact-type=" + string(defaultArtifactType(mexp.Spec.ArtifactType)),
		"--credentials-dir=/etc/modeldock/push-secret",
		fmt.Sprintf("--concurrency=%d", concurrency),
		fmt.Sprintf("--allow-unsafe-formats=%t", mexp.Spec.AllowUnsafeFormats),
		fmt.Sprintf("--include-training-states=%t", mexp.Spec.IncludeTrainingStates),
		fmt.Sprintf("--insecure-skip-tls=%t", instCfg.InsecureSkipTLS),
		"--output-file=/dev/termination-log",
	}
	if mexp.Spec.ChunkSizeBytes > 0 {
		args = append(args, fmt.Sprintf("--chunk-size-bytes=%d", mexp.Spec.ChunkSizeBytes))
	}
	if mexp.Spec.BaseModelRef != nil {
		if raw, err := json.Marshal(mexp.Spec.BaseModelRef); err == nil {
			args = append(args, "--base-model-json="+string(raw))
		}
	}
	if mexp.Spec.Lineage != nil {
		if raw, err := json.Marshal(mexp.Spec.Lineage); err == nil {
			args = append(args, "--lineage-json="+string(raw))
		}
	}

	volumes := []corev1.Volume{
		{
			Name: "push-secret",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: mrepo.Status.PushSecretName,
				},
			},
		},
	}
	mounts := []corev1.VolumeMount{
		{
			Name:      "push-secret",
			MountPath: "/etc/modeldock/push-secret",
			ReadOnly:  true,
		},
	}

	switch {
	case mexp.Spec.Source.PVCSource != nil:
		src := mexp.Spec.Source.PVCSource
		args = append(args, "--mode=dir", "--source-dir=/mnt/source")
		if len(src.IncludePatterns) > 0 {
			args = append(args, "--include-patterns="+strings.Join(src.IncludePatterns, ","))
		}
		if len(src.ExcludePatterns) > 0 {
			args = append(args, "--exclude-patterns="+strings.Join(src.ExcludePatterns, ","))
		}
		volumes = append(volumes, corev1.Volume{
			Name: "source-pvc",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: src.ClaimName,
					ReadOnly:  true,
				},
			},
		})
		mounts = append(mounts, corev1.VolumeMount{
			Name:      "source-pvc",
			MountPath: "/mnt/source",
			SubPath:   src.SubPath,
			ReadOnly:  true,
		})

	case mexp.Spec.Source.DevBoxSource != nil:
		src := mexp.Spec.Source.DevBoxSource
		args = append(args, "--mode=dir", "--source-dir=/mnt/source")
		if len(src.ExcludePatterns) > 0 {
			args = append(args, "--exclude-patterns="+strings.Join(src.ExcludePatterns, ","))
		}
		volumes = append(volumes, corev1.Volume{
			Name: "source-devbox",
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: src.ClaimName,
					ReadOnly:  true,
				},
			},
		})
		mounts = append(mounts, corev1.VolumeMount{
			Name:      "source-devbox",
			MountPath: "/mnt/source",
			SubPath:   src.SubPath,
			ReadOnly:  true,
		})

	case mexp.Spec.Source.ExternalHubSource != nil:
		src := mexp.Spec.Source.ExternalHubSource
		args = append(args,
			"--mode=external-hub",
			"--hub-provider="+string(src.Provider),
			"--hub-repo-id="+src.RepoID,
		)
		if src.Revision != "" {
			args = append(args, "--hub-revision="+src.Revision)
		}
		if src.EndpointMirror != "" {
			args = append(args, "--hub-mirror="+src.EndpointMirror)
		}
		if len(src.IncludePatterns) > 0 {
			args = append(args, "--include-patterns="+strings.Join(src.IncludePatterns, ","))
		}
		if len(src.ExcludePatterns) > 0 {
			args = append(args, "--exclude-patterns="+strings.Join(src.ExcludePatterns, ","))
		}
		if src.AuthSecretRef != "" {
			args = append(args, "--hub-auth-file=/etc/modeldock/hub-auth/token")
			volumes = append(volumes, corev1.Volume{
				Name: "hub-auth",
				VolumeSource: corev1.VolumeSource{
					Secret: &corev1.SecretVolumeSource{
						SecretName: src.AuthSecretRef,
					},
				},
			})
			mounts = append(mounts, corev1.VolumeMount{
				Name:      "hub-auth",
				MountPath: "/etc/modeldock/hub-auth",
				ReadOnly:  true,
			})
		}

	default:
		return nil, fmt.Errorf("ModelExport %s/%s must specify one of pvcSource, externalHubSource, or devBoxSource", mexp.Namespace, mexp.Name)
	}

	ttl := int32(86400)
	if mexp.Spec.TTLSecondsAfterFinished != nil {
		ttl = *mexp.Spec.TTLSecondsAfterFinished
	}

	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: mexp.Namespace,
			Labels: map[string]string{
				modeldockv1alpha1.LabelExportJob: mexp.Name,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit:            ptr.To(int32(2)),
			TTLSecondsAfterFinished: ptr.To(ttl),
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						modeldockv1alpha1.LabelExportJob: mexp.Name,
					},
				},
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: ptr.To(true),
					},
					Containers: []corev1.Container{
						{
							Name:            "exporter",
							Image:           cfg.ExporterImage,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Args:            args,
							VolumeMounts:    mounts,
							SecurityContext: &corev1.SecurityContext{
								Privileged:               ptr.To(false),
								AllowPrivilegeEscalation: ptr.To(false),
								RunAsNonRoot:             ptr.To(true),
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
	}, nil
}

func (r *ModelExportReconciler) readExportResult(ctx context.Context, mexp *modeldockv1alpha1.ModelExport, jobName string) (*exporter.ExportResult, error) {
	msg := r.readJobTerminationOrFailure(ctx, mexp.Namespace, jobName)
	if msg == "" {
		return nil, fmt.Errorf("exporter pod termination message is empty")
	}
	var res exporter.ExportResult
	if err := json.Unmarshal([]byte(msg), &res); err != nil {
		return nil, fmt.Errorf("decode exporter result JSON %q: %w", msg, err)
	}
	return &res, nil
}

func (r *ModelExportReconciler) readJobTerminationOrFailure(ctx context.Context, ns, jobName string) string {
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.InNamespace(ns), client.MatchingLabels{"job-name": jobName}); err != nil {
		return ""
	}
	for i := range pods.Items {
		for _, cs := range pods.Items[i].Status.ContainerStatuses {
			if cs.State.Terminated != nil && cs.State.Terminated.Message != "" {
				return cs.State.Terminated.Message
			}
		}
	}
	return ""
}

func (r *ModelExportReconciler) ensureModelArtifact(ctx context.Context, mexp *modeldockv1alpha1.ModelExport, res *exporter.ExportResult) (string, error) {
	artName := SanitizeK8sName(mexp.Spec.TargetRepository, mexp.Spec.VersionTag)
	initialStage := mexp.Spec.InitialStage
	if initialStage == "" {
		initialStage = modeldockv1alpha1.StageStaging
	}
	artType := defaultArtifactType(mexp.Spec.ArtifactType)

	lineage := mexp.Spec.Lineage
	if res.ExternalRevision != "" {
		if lineage == nil {
			lineage = &modeldockv1alpha1.LineageSpec{}
		} else {
			cp := *lineage
			lineage = &cp
		}
		lineage.ExternalRevision = res.ExternalRevision
	}

	mart := &modeldockv1alpha1.ModelArtifact{
		ObjectMeta: metav1.ObjectMeta{
			Name:      artName,
			Namespace: mexp.Namespace,
			Labels: map[string]string{
				modeldockv1alpha1.LabelRepository:   mexp.Spec.TargetRepository,
				modeldockv1alpha1.LabelArtifactType: string(artType),
				modeldockv1alpha1.LabelStage:        string(initialStage),
			},
		},
		Spec: modeldockv1alpha1.ModelArtifactSpec{
			RepositoryRef:  mexp.Spec.TargetRepository,
			VersionTag:     mexp.Spec.VersionTag,
			OCIDigest:      res.Digest,
			Stage:          initialStage,
			ArtifactType:   artType,
			BaseModelRef:   mexp.Spec.BaseModelRef,
			LoRASpec:       res.LoRASpec,
			ServingProfile: res.ServingProfile,
			Lineage:        lineage,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, mart, func() error {
		if mart.CreationTimestamp.IsZero() {
			return nil
		}
		// Keep immutable fields intact if already created.
		return nil
	})
	return artName, err
}

func defaultArtifactType(t modeldockv1alpha1.ArtifactType) modeldockv1alpha1.ArtifactType {
	if t == "" {
		return modeldockv1alpha1.ArtifactTypeFullWeight
	}
	return t
}

func (r *ModelExportReconciler) markPending(ctx context.Context, mexp *modeldockv1alpha1.ModelExport, reason, msg string) (ctrl.Result, error) {
	mexp.Status.Phase = modeldockv1alpha1.ExportPhasePending
	mexp.Status.Message = msg
	setCondition(&mexp.Status.Conditions, "Exported", metav1.ConditionFalse, reason, msg)
	_ = r.Status().Update(ctx, mexp)
	return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
}

func (r *ModelExportReconciler) markFailed(ctx context.Context, mexp *modeldockv1alpha1.ModelExport, reason, msg string) (ctrl.Result, error) {
	now := metav1.Now()
	mexp.Status.Phase = modeldockv1alpha1.ExportPhaseFailed
	mexp.Status.CompletionTime = &now
	mexp.Status.Message = msg
	setCondition(&mexp.Status.Conditions, "Exported", metav1.ConditionFalse, reason, msg)
	_ = r.Status().Update(ctx, mexp)
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ModelExportReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&modeldockv1alpha1.ModelExport{}).
		Owns(&batchv1.Job{}).
		Named("modelexport").
		Complete(r)
}
