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
	"context"
	"fmt"
	"reflect"

	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	modeldockv1alpha1 "github.com/wutz/modeldock/api/v1alpha1"
)

var modelartifactlog = logf.Log.WithName("modelartifact-resource")

// SetupModelArtifactWebhookWithManager registers the webhook for ModelArtifact in the manager.
func SetupModelArtifactWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &modeldockv1alpha1.ModelArtifact{}).
		WithValidator(&ModelArtifactValidator{Client: mgr.GetClient()}).
		WithDefaulter(&ModelArtifactDefaulter{}).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-modeldock-wutz-dev-v1alpha1-modelartifact,mutating=true,failurePolicy=fail,sideEffects=None,groups=modeldock.wutz.dev,resources=modelartifacts,verbs=create;update,versions=v1alpha1,name=mmodelartifact-v1alpha1.kb.io,admissionReviewVersions=v1

// ModelArtifactDefaulter defaults Stage and ArtifactType on ModelArtifact.
// +kubebuilder:object:generate=false
type ModelArtifactDefaulter struct{}

func (d *ModelArtifactDefaulter) Default(_ context.Context, obj *modeldockv1alpha1.ModelArtifact) error {
	if obj.Spec.Stage == "" {
		obj.Spec.Stage = modeldockv1alpha1.StageStaging
	}
	if obj.Spec.ArtifactType == "" {
		obj.Spec.ArtifactType = modeldockv1alpha1.ArtifactTypeFullWeight
	}
	return nil
}

// +kubebuilder:webhook:path=/validate-modeldock-wutz-dev-v1alpha1-modelartifact,mutating=false,failurePolicy=fail,sideEffects=None,groups=modeldock.wutz.dev,resources=modelartifacts,verbs=create;update;delete,versions=v1alpha1,name=vmodelartifact-v1alpha1.kb.io,admissionReviewVersions=v1

// ModelArtifactValidator enforces immutability, LoRA base compatibility, cross-namespace visibility, and Production deletion protection.
// +kubebuilder:object:generate=false
type ModelArtifactValidator struct {
	Client client.Reader
}

func (v *ModelArtifactValidator) ValidateCreate(ctx context.Context, obj *modeldockv1alpha1.ModelArtifact) (admission.Warnings, error) {
	modelartifactlog.Info("Validation for ModelArtifact upon creation", "name", obj.GetName())
	return v.validateSpec(ctx, obj)
}

func (v *ModelArtifactValidator) ValidateUpdate(ctx context.Context, oldObj, newObj *modeldockv1alpha1.ModelArtifact) (admission.Warnings, error) {
	modelartifactlog.Info("Validation for ModelArtifact upon update", "name", newObj.GetName())

	// Once Ready, core identity fields are strictly immutable.
	if oldObj.Status.Phase == modeldockv1alpha1.ArtifactPhaseReady {
		if oldObj.Spec.OCIDigest != newObj.Spec.OCIDigest {
			return nil, fmt.Errorf("spec.ociDigest is immutable once ModelArtifact is Ready")
		}
		if oldObj.Spec.ArtifactType != newObj.Spec.ArtifactType {
			return nil, fmt.Errorf("spec.artifactType is immutable once ModelArtifact is Ready")
		}
		if oldObj.Spec.RepositoryRef != newObj.Spec.RepositoryRef {
			return nil, fmt.Errorf("spec.repositoryRef is immutable once ModelArtifact is Ready")
		}
		if !reflect.DeepEqual(oldObj.Spec.BaseModelRef, newObj.Spec.BaseModelRef) {
			return nil, fmt.Errorf("spec.baseModelRef is immutable once ModelArtifact is Ready")
		}
	}

	return v.validateSpec(ctx, newObj)
}

func (v *ModelArtifactValidator) ValidateDelete(ctx context.Context, obj *modeldockv1alpha1.ModelArtifact) (admission.Warnings, error) {
	modelartifactlog.Info("Validation for ModelArtifact upon deletion", "name", obj.GetName())

	if obj.Spec.Stage == modeldockv1alpha1.StageProduction && v.Client != nil {
		var caches modeldockv1alpha1.ModelCacheList
		if err := v.Client.List(ctx, &caches, client.InNamespace(obj.Namespace)); err == nil {
			for i := range caches.Items {
				mc := &caches.Items[i]
				if mc.Spec.ArtifactRef.Name == obj.Name && mc.Status.ActiveConsumers > 0 {
					return nil, fmt.Errorf("cannot delete Production ModelArtifact %q: ModelCache %q still has %d active consumers; demote to Deprecated first",
						obj.Name, mc.Name, mc.Status.ActiveConsumers)
				}
			}
		}
	}
	return nil, nil
}

func (v *ModelArtifactValidator) validateSpec(ctx context.Context, obj *modeldockv1alpha1.ModelArtifact) (admission.Warnings, error) {
	if obj.Spec.ArtifactType == modeldockv1alpha1.ArtifactTypeLoRAAdapter || obj.Spec.ArtifactType == modeldockv1alpha1.ArtifactTypeDraftModel {
		if obj.Spec.BaseModelRef == nil {
			return nil, fmt.Errorf("spec.baseModelRef is required when artifactType is %s", obj.Spec.ArtifactType)
		}
	}
	if v.Client != nil {
		var repo modeldockv1alpha1.ModelRepository
		if err := v.Client.Get(ctx, types.NamespacedName{Namespace: obj.Namespace, Name: obj.Spec.RepositoryRef}, &repo); err != nil {
			return nil, fmt.Errorf("spec.repositoryRef %q not found in namespace %q: %w", obj.Spec.RepositoryRef, obj.Namespace, err)
		}
		if err := ValidateCrossNamespaceModelReference(ctx, v.Client, obj.Namespace, obj.Spec.BaseModelRef); err != nil {
			return nil, err
		}
	}
	return nil, nil
}
