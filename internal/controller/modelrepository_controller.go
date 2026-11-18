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
	"fmt"
	"sort"
	"time"

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
)

const defaultRotationPeriod = 30 * 24 * time.Hour

// ModelRepositoryReconciler reconciles a ModelRepository object against Harbor.
type ModelRepositoryReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Config OperatorConfig
}

// +kubebuilder:rbac:groups=modeldock.wutz.dev,resources=modelrepositories,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=modeldock.wutz.dev,resources=modelrepositories/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=modeldock.wutz.dev,resources=modelrepositories/finalizers,verbs=update
// +kubebuilder:rbac:groups=modeldock.wutz.dev,resources=modelartifacts,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=secrets;namespaces,verbs=get;list;watch;create;update;patch

// Reconcile synchronizes Harbor project, quotas, least-privilege pull/push Robot Secrets, and retention policies.
func (r *ModelRepositoryReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	cfg := r.Config.withDefaults()

	var mrepo modeldockv1alpha1.ModelRepository
	if err := r.Get(ctx, req.NamespacedName, &mrepo); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !mrepo.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(&mrepo, modeldockv1alpha1.FinalizerModelDock) {
			controllerutil.RemoveFinalizer(&mrepo, modeldockv1alpha1.FinalizerModelDock)
			if err := r.Update(ctx, &mrepo); err != nil {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(&mrepo, modeldockv1alpha1.FinalizerModelDock) {
		controllerutil.AddFinalizer(&mrepo, modeldockv1alpha1.FinalizerModelDock)
		if err := r.Update(ctx, &mrepo); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: time.Millisecond}, nil
	}

	resolver := cfg.HarborResolver
	if resolver == nil {
		resolver = &harbor.Resolver{Client: r.Client, SystemNamespace: cfg.SystemNamespace}
	}
	instCfg, err := resolver.Resolve(ctx, mrepo.Spec.Harbor.InstanceRef)
	if err != nil {
		return r.markError(ctx, &mrepo, "HarborConfigError", err)
	}
	hc, err := instCfg.NewHarborClient()
	if err != nil {
		return r.markError(ctx, &mrepo, "HarborClientError", err)
	}

	projectName, repoName := ResolveHarborProjectAndRepo(ctx, r.Client, &mrepo)
	isPublic := mrepo.Spec.Visibility == modeldockv1alpha1.VisibilityPublic
	var storageLimit *int64
	if mrepo.Spec.Harbor.StorageLimitBytes != nil {
		storageLimit = mrepo.Spec.Harbor.StorageLimitBytes
	}

	proj, err := hc.EnsureProject(ctx, projectName, isPublic, storageLimit)
	if err != nil {
		log.Error(err, "failed to ensure Harbor project", "project", projectName)
		return r.markError(ctx, &mrepo, "HarborProjectSyncFailed", err)
	}

	// Determine whether Robot credentials need initial issuance or rotation.
	pullSecretName := fmt.Sprintf("mrepo-%s-pull", mrepo.Name)
	pushSecretName := fmt.Sprintf("mrepo-%s-push", mrepo.Name)

	rotateRequested := mrepo.Annotations[modeldockv1alpha1.AnnotationAction] == modeldockv1alpha1.ActionRotateCredentials
	pullMissing := r.secretMissing(ctx, mrepo.Namespace, pullSecretName)
	pushMissing := r.secretMissing(ctx, mrepo.Namespace, pushSecretName)
	periodicRotate := mrepo.Status.LastCredentialRotationTime != nil &&
		time.Since(mrepo.Status.LastCredentialRotationTime.Time) > defaultRotationPeriod

	rotate := rotateRequested || pullMissing || pushMissing || periodicRotate
	pullRobot, pushRobot, err := hc.EnsureProjectRobots(ctx, projectName, mrepo.Name, rotate)
	if err != nil {
		log.Error(err, "failed to ensure Harbor robot accounts", "project", projectName)
		return r.markError(ctx, &mrepo, "HarborRobotSyncFailed", err)
	}

	if pullRobot != nil && pullRobot.Secret != "" {
		if err := r.reconcileRobotSecret(ctx, &mrepo, pullSecretName, instCfg, pullRobot); err != nil {
			return r.markError(ctx, &mrepo, "PullSecretSyncFailed", err)
		}
	}
	if pushRobot != nil && pushRobot.Secret != "" {
		if err := r.reconcileRobotSecret(ctx, &mrepo, pushSecretName, instCfg, pushRobot); err != nil {
			return r.markError(ctx, &mrepo, "PushSecretSyncFailed", err)
		}
	}

	// Clear one-shot rotate-credentials annotation once rotated.
	if rotateRequested {
		delete(mrepo.Annotations, modeldockv1alpha1.AnnotationAction)
		if err := r.Update(ctx, &mrepo); err != nil {
			return ctrl.Result{}, err
		}
	}

	// Aggregate ModelArtifact stats & enforce Staging retention policy.
	artifactCount, latestProd, err := r.reconcileArtifactsAndRetention(ctx, &mrepo)
	if err != nil {
		log.Error(err, "failed to reconcile artifact retention")
	}

	mrepo.Status.Phase = modeldockv1alpha1.RepositoryPhaseReady
	mrepo.Status.OCIRepository = fmt.Sprintf("%s/%s/%s", instCfg.RegistryHost, projectName, repoName)
	mrepo.Status.HarborProjectID = proj.ProjectID
	mrepo.Status.PullSecretName = pullSecretName
	mrepo.Status.PushSecretName = pushSecretName
	mrepo.Status.ArtifactCount = artifactCount
	mrepo.Status.LatestProductionArtifact = latestProd
	if rotate || mrepo.Status.LastCredentialRotationTime == nil {
		now := metav1.Now()
		mrepo.Status.LastCredentialRotationTime = &now
	}
	setCondition(&mrepo.Status.Conditions, "HarborSynced", metav1.ConditionTrue, "Reconciled",
		fmt.Sprintf("Synced Harbor project %s (ID=%d)", projectName, proj.ProjectID))

	if err := r.Status().Update(ctx, &mrepo); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: 10 * time.Minute}, nil
}

func (r *ModelRepositoryReconciler) secretMissing(ctx context.Context, ns, name string) bool {
	var sec corev1.Secret
	err := r.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, &sec)
	return apierrors.IsNotFound(err)
}

func (r *ModelRepositoryReconciler) reconcileRobotSecret(
	ctx context.Context,
	mrepo *modeldockv1alpha1.ModelRepository,
	secretName string,
	instCfg *harbor.InstanceConfig,
	robot *harbor.Robot,
) error {
	dockerCfg, err := BuildDockerConfigJSON(instCfg.RegistryHost, robot.Name, robot.Secret)
	if err != nil {
		return err
	}
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: mrepo.Namespace,
		},
	}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, sec, func() error {
		sec.Type = corev1.SecretTypeDockerConfigJson
		sec.Data = map[string][]byte{
			corev1.DockerConfigJsonKey: dockerCfg,
			"username":                 []byte(robot.Name),
			"password":                 []byte(robot.Secret),
			"endpoint":                 []byte(instCfg.Endpoint),
		}
		return controllerutil.SetControllerReference(mrepo, sec, r.Scheme)
	})
	return err
}

func (r *ModelRepositoryReconciler) reconcileArtifactsAndRetention(ctx context.Context, mrepo *modeldockv1alpha1.ModelRepository) (int32, string, error) {
	var list modeldockv1alpha1.ModelArtifactList
	if err := r.List(ctx, &list, client.InNamespace(mrepo.Namespace)); err != nil {
		return 0, "", err
	}

	var repoArtifacts []modeldockv1alpha1.ModelArtifact
	var stagingArtifacts []modeldockv1alpha1.ModelArtifact
	var prodArtifacts []modeldockv1alpha1.ModelArtifact

	for i := range list.Items {
		art := list.Items[i]
		if art.Spec.RepositoryRef != mrepo.Name {
			continue
		}
		if art.Spec.Stage == modeldockv1alpha1.StageArchived {
			continue
		}
		repoArtifacts = append(repoArtifacts, art)
		switch art.Spec.Stage {
		case modeldockv1alpha1.StageStaging:
			stagingArtifacts = append(stagingArtifacts, art)
		case modeldockv1alpha1.StageProduction:
			prodArtifacts = append(prodArtifacts, art)
		}
	}

	// Enforce keepLastStagingVersions by archiving oldest Staging artifacts beyond limit.
	if mrepo.Spec.RetentionPolicy != nil && mrepo.Spec.RetentionPolicy.KeepLastStagingVersions != nil {
		keep := int(*mrepo.Spec.RetentionPolicy.KeepLastStagingVersions)
		if len(stagingArtifacts) > keep {
			sort.Slice(stagingArtifacts, func(i, j int) bool {
				return stagingArtifacts[i].CreationTimestamp.After(stagingArtifacts[j].CreationTimestamp.Time)
			})
			for i := keep; i < len(stagingArtifacts); i++ {
				stale := stagingArtifacts[i].DeepCopy()
				stale.Spec.Stage = modeldockv1alpha1.StageArchived
				_ = r.Update(ctx, stale)
			}
		}
	}

	latestProd := ""
	if len(prodArtifacts) > 0 {
		sort.Slice(prodArtifacts, func(i, j int) bool {
			return prodArtifacts[i].CreationTimestamp.After(prodArtifacts[j].CreationTimestamp.Time)
		})
		latestProd = prodArtifacts[0].Name
	}

	return int32(len(repoArtifacts)), latestProd, nil
}

func (r *ModelRepositoryReconciler) markError(ctx context.Context, mrepo *modeldockv1alpha1.ModelRepository, reason string, err error) (ctrl.Result, error) {
	mrepo.Status.Phase = modeldockv1alpha1.RepositoryPhaseError
	setCondition(&mrepo.Status.Conditions, "HarborSynced", metav1.ConditionFalse, reason, errorString(err))
	_ = r.Status().Update(ctx, mrepo)
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ModelRepositoryReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&modeldockv1alpha1.ModelRepository{}).
		Owns(&corev1.Secret{}).
		Named("modelrepository").
		Complete(r)
}
