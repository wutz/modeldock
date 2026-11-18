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
	"github.com/wutz/modeldock/pkg/oci"
)

// ModelArtifactReconciler reconciles a ModelArtifact object.
type ModelArtifactReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Config OperatorConfig
}

// +kubebuilder:rbac:groups=modeldock.wutz.dev,resources=modelartifacts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=modeldock.wutz.dev,resources=modelartifacts/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=modeldock.wutz.dev,resources=modelartifacts/finalizers,verbs=update
// +kubebuilder:rbac:groups=modeldock.wutz.dev,resources=modelrepositories,verbs=get;list;watch
// +kubebuilder:rbac:groups=modeldock.wutz.dev,resources=modelcaches,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconcile verifies OCI Manifest/ModelPackConfig against Harbor, syncs stage tags, checks LoRA base compatibility, and triggers ModelCache warmup.
func (r *ModelArtifactReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	cfg := r.Config.withDefaults()

	var mart modeldockv1alpha1.ModelArtifact
	if err := r.Get(ctx, req.NamespacedName, &mart); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !mart.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// Keep label selectors synchronized with spec.
	if ensureNamespaceLabelsForArtifact(&mart) {
		if err := r.Update(ctx, &mart); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: time.Millisecond}, nil
	}

	var mrepo modeldockv1alpha1.ModelRepository
	if err := r.Get(ctx, types.NamespacedName{Namespace: mart.Namespace, Name: mart.Spec.RepositoryRef}, &mrepo); err != nil {
		if apierrors.IsNotFound(err) {
			return r.markInvalid(ctx, &mart, "RepositoryNotFound", fmt.Sprintf("ModelRepository %q not found", mart.Spec.RepositoryRef))
		}
		return ctrl.Result{}, err
	}

	if mrepo.Status.Phase != modeldockv1alpha1.RepositoryPhaseReady {
		mart.Status.Phase = modeldockv1alpha1.ArtifactPhaseSyncing
		setCondition(&mart.Status.Conditions, "ManifestVerified", metav1.ConditionFalse, "RepositoryNotReady", "Waiting for ModelRepository to become Ready")
		_ = r.Status().Update(ctx, &mart)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	resolver := cfg.HarborResolver
	if resolver == nil {
		resolver = &harbor.Resolver{Client: r.Client, SystemNamespace: cfg.SystemNamespace}
	}
	instCfg, err := resolver.Resolve(ctx, mrepo.Spec.Harbor.InstanceRef)
	if err != nil {
		return r.markInvalid(ctx, &mart, "HarborConfigError", err.Error())
	}

	// Read pull robot credentials from the repository's pull Secret.
	pullUser, pullPass := instCfg.Username, instCfg.Password
	if mrepo.Status.PullSecretName != "" {
		var pullSec corev1.Secret
		if err := r.Get(ctx, types.NamespacedName{Namespace: mart.Namespace, Name: mrepo.Status.PullSecretName}, &pullSec); err == nil {
			if u := string(pullSec.Data["username"]); u != "" {
				pullUser = u
			}
			if p := string(pullSec.Data["password"]); p != "" {
				pullPass = p
			}
		}
	}

	projectName, repoName := ResolveHarborProjectAndRepo(ctx, r.Client, &mrepo)
	fullRepo := projectName + "/" + repoName

	regClient, err := oci.NewRegistryClient(oci.ClientOptions{
		RegistryURL:     instCfg.Endpoint,
		Username:        pullUser,
		Password:        pullPass,
		InsecureSkipTLS: instCfg.InsecureSkipTLS,
	})
	if err != nil {
		return r.markInvalid(ctx, &mart, "RegistryClientError", err.Error())
	}

	manifest, resolvedDigest, err := regClient.GetManifest(ctx, fullRepo, mart.Spec.OCIDigest)
	if err != nil {
		log.Error(err, "failed to fetch OCI manifest from Harbor", "repo", fullRepo, "digest", mart.Spec.OCIDigest)
		return r.markInvalid(ctx, &mart, "ManifestFetchFailed", err.Error())
	}
	if resolvedDigest != mart.Spec.OCIDigest {
		return r.markInvalid(ctx, &mart, "DigestMismatch",
			fmt.Sprintf("Harbor returned digest %s, expected %s", resolvedDigest, mart.Spec.OCIDigest))
	}

	var totalSize int64
	var shardCount int32
	for _, l := range manifest.Layers {
		totalSize += l.Size
		if l.MediaType == oci.MediaTypeWeightRaw {
			shardCount++
		}
	}

	mpCfg, cfgErr := regClient.GetModelPackConfig(ctx, fullRepo, manifest)
	unsafeSer := false
	if cfgErr == nil && mpCfg != nil {
		unsafeSer = mpCfg.UnsafeSerialization
		if mpCfg.ShardCount > 0 {
			shardCount = mpCfg.ShardCount
		}
		// Hydrate Spec metadata if created externally (e.g. via Harbor cross-cluster replication webhook).
		specUpdated := false
		if mart.Spec.ServingProfile == nil && mpCfg.ServingProfile != nil {
			mart.Spec.ServingProfile = mpCfg.ServingProfile
			specUpdated = true
		}
		if mart.Spec.LoRASpec == nil && mpCfg.LoRASpec != nil {
			mart.Spec.LoRASpec = mpCfg.LoRASpec
			specUpdated = true
		}
		if mart.Spec.Lineage == nil && mpCfg.Lineage != nil {
			mart.Spec.Lineage = mpCfg.Lineage
			specUpdated = true
		}
		if specUpdated {
			if err := r.Update(ctx, &mart); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{RequeueAfter: time.Millisecond}, nil
		}
	}

	// Verify BaseModelRef compatibility if LoRAAdapter or DraftModel.
	if mart.Spec.ArtifactType == modeldockv1alpha1.ArtifactTypeLoRAAdapter || mart.Spec.ArtifactType == modeldockv1alpha1.ArtifactTypeDraftModel {
		if err := r.verifyBaseModel(ctx, &mart); err != nil {
			setCondition(&mart.Status.Conditions, "BaseModelCompatible", metav1.ConditionFalse, "BaseModelIncompatible", err.Error())
		} else {
			setCondition(&mart.Status.Conditions, "BaseModelCompatible", metav1.ConditionTrue, "VerifiedAgainstBase", "BaseModelRef resolved and Ready")
		}
	}

	// Sync Harbor stage tags (stage-verified / stage-production) for cross-cluster Harbor Replication filtering.
	if hc, err := instCfg.NewHarborClient(); err == nil {
		switch mart.Spec.Stage {
		case modeldockv1alpha1.StageVerified:
			_ = hc.AddArtifactTag(ctx, projectName, repoName, mart.Spec.OCIDigest, "stage-verified")
			_ = hc.DeleteArtifactTag(ctx, projectName, repoName, mart.Spec.OCIDigest, "stage-production")
		case modeldockv1alpha1.StageProduction:
			_ = hc.AddArtifactTag(ctx, projectName, repoName, mart.Spec.OCIDigest, "stage-production")
		case modeldockv1alpha1.StageDeprecated, modeldockv1alpha1.StageArchived:
			_ = hc.DeleteArtifactTag(ctx, projectName, repoName, mart.Spec.OCIDigest, "stage-production")
			_ = hc.DeleteArtifactTag(ctx, projectName, repoName, mart.Spec.OCIDigest, "stage-verified")
		}
	}

	// Handle warmup trigger (autoWarmupStages or manual annotation).
	warmupAnnotated := mart.Annotations[modeldockv1alpha1.AnnotationAction] == modeldockv1alpha1.ActionWarmup
	shouldWarmup := warmupAnnotated || stageInList(mart.Spec.Stage, mrepo.Spec.AutoWarmupStages)
	var activeCacheName string
	if shouldWarmup && mart.Spec.Stage != modeldockv1alpha1.StageArchived {
		mc, err := r.ensureModelCache(ctx, &mart)
		if err != nil {
			log.Error(err, "failed to ensure ModelCache")
		} else if mc != nil {
			if mc.Status.Phase == modeldockv1alpha1.CachePhaseReady {
				activeCacheName = mc.Name
			}
			if warmupAnnotated {
				delete(mart.Annotations, modeldockv1alpha1.AnnotationAction)
				_ = r.Update(ctx, &mart)
			}
		}
	} else {
		// Check if an existing ModelCache exists for this artifact.
		var mc modeldockv1alpha1.ModelCache
		if err := r.Get(ctx, types.NamespacedName{Namespace: mart.Namespace, Name: cacheNameForArtifact(&mart)}, &mc); err == nil {
			if mc.Status.Phase == modeldockv1alpha1.CachePhaseReady {
				activeCacheName = mc.Name
			}
		}
	}

	if mart.Spec.Stage == modeldockv1alpha1.StageArchived {
		mart.Status.Phase = modeldockv1alpha1.ArtifactPhaseArchived
	} else {
		mart.Status.Phase = modeldockv1alpha1.ArtifactPhaseReady
	}
	mart.Status.OCIReference = fmt.Sprintf("%s@%s", mrepo.Status.OCIRepository, mart.Spec.OCIDigest)
	mart.Status.TotalSizeBytes = totalSize
	mart.Status.ShardCount = shardCount
	mart.Status.UnsafeSerialization = unsafeSer
	mart.Status.ActiveCacheRef = activeCacheName
	if mart.Spec.Stage == modeldockv1alpha1.StageProduction && mart.Status.PromotedToProductionAt == nil {
		now := metav1.Now()
		mart.Status.PromotedToProductionAt = &now
	}
	setCondition(&mart.Status.Conditions, "ManifestVerified", metav1.ConditionTrue, "DigestMatched",
		fmt.Sprintf("Verified OCI ModelPack manifest %s (%d shards)", mart.Spec.OCIDigest, shardCount))

	if err := r.Status().Update(ctx, &mart); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *ModelArtifactReconciler) verifyBaseModel(ctx context.Context, mart *modeldockv1alpha1.ModelArtifact) error {
	if mart.Spec.BaseModelRef == nil {
		return fmt.Errorf("baseModelRef is required for %s", mart.Spec.ArtifactType)
	}
	baseNS := resolveArtifactNamespace(*mart.Spec.BaseModelRef, mart.Namespace)
	var base martTypeAlias
	if err := r.Get(ctx, types.NamespacedName{Namespace: baseNS, Name: mart.Spec.BaseModelRef.Name}, &base); err != nil {
		return fmt.Errorf("lookup base model %s/%s: %w", baseNS, mart.Spec.BaseModelRef.Name, err)
	}
	if mart.Spec.BaseModelRef.OCIDigest != "" && base.Spec.OCIDigest != mart.Spec.BaseModelRef.OCIDigest {
		return fmt.Errorf("base model digest mismatch: got %s, expected %s", base.Spec.OCIDigest, mart.Spec.BaseModelRef.OCIDigest)
	}
	return nil
}

type martTypeAlias = modeldockv1alpha1.ModelArtifact

func (r *ModelArtifactReconciler) ensureModelCache(ctx context.Context, mart *modeldockv1alpha1.ModelArtifact) (*modeldockv1alpha1.ModelCache, error) {
	name := cacheNameForArtifact(mart)
	mc := &modeldockv1alpha1.ModelCache{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: mart.Namespace,
		},
	}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, mc, func() error {
		if mc.CreationTimestamp.IsZero() {
			mc.Spec = modeldockv1alpha1.ModelCacheSpec{
				ArtifactRef: modeldockv1alpha1.ModelArtifactReference{
					Namespace: mart.Namespace,
					Name:      mart.Name,
					OCIDigest: mart.Spec.OCIDigest,
				},
				Strategy: modeldockv1alpha1.CacheStrategySharedFilesystem,
				SharedFilesystem: &modeldockv1alpha1.SharedFilesystemCacheConfig{
					CachePoolName: "default-rwx-model-pool",
				},
				EnableP2P: true,
			}
		}
		return controllerutil.SetControllerReference(mart, mc, r.Scheme)
	})
	return mc, err
}

func (r *ModelArtifactReconciler) markInvalid(ctx context.Context, mart *modeldockv1alpha1.ModelArtifact, reason, msg string) (ctrl.Result, error) {
	mart.Status.Phase = modeldockv1alpha1.ArtifactPhaseInvalid
	setCondition(&mart.Status.Conditions, "ManifestVerified", metav1.ConditionFalse, reason, msg)
	_ = r.Status().Update(ctx, mart)
	return ctrl.Result{RequeueAfter: 30 * time.Second}, nil
}

func stageInList(s modeldockv1alpha1.ArtifactStage, list []modeldockv1alpha1.ArtifactStage) bool {
	for _, item := range list {
		if item == s {
			return true
		}
	}
	return false
}

// SetupWithManager sets up the controller with the Manager.
func (r *ModelArtifactReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&modeldockv1alpha1.ModelArtifact{}).
		Owns(&modeldockv1alpha1.ModelCache{}).
		Named("modelartifact").
		Complete(r)
}
