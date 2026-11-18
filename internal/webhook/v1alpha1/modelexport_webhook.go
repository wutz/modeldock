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

	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	modeldockv1alpha1 "github.com/wutz/modeldock/api/v1alpha1"
)

var modelexportlog = logf.Log.WithName("modelexport-resource")

// SetupModelExportWebhookWithManager registers the webhook for ModelExport in the manager.
func SetupModelExportWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &modeldockv1alpha1.ModelExport{}).
		WithValidator(&ModelExportValidator{Client: mgr.GetClient()}).
		WithDefaulter(&ModelExportDefaulter{}).
		Complete()
}

// +kubebuilder:webhook:path=/mutate-modeldock-wutz-dev-v1alpha1-modelexport,mutating=true,failurePolicy=fail,sideEffects=None,groups=modeldock.wutz.dev,resources=modelexports,verbs=create;update,versions=v1alpha1,name=mmodelexport-v1alpha1.kb.io,admissionReviewVersions=v1

// ModelExportDefaulter populates sensible defaults on ModelExport.
// +kubebuilder:object:generate=false
type ModelExportDefaulter struct{}

func (d *ModelExportDefaulter) Default(_ context.Context, obj *modeldockv1alpha1.ModelExport) error {
	if obj.Spec.InitialStage == "" {
		obj.Spec.InitialStage = modeldockv1alpha1.StageStaging
	}
	if obj.Spec.ArtifactType == "" {
		obj.Spec.ArtifactType = modeldockv1alpha1.ArtifactTypeFullWeight
	}
	if obj.Spec.Concurrency == 0 {
		obj.Spec.Concurrency = 4
	}
	if obj.Spec.TTLSecondsAfterFinished == nil {
		obj.Spec.TTLSecondsAfterFinished = new(int32(86400))
	}
	return nil
}

// +kubebuilder:webhook:path=/validate-modeldock-wutz-dev-v1alpha1-modelexport,mutating=false,failurePolicy=fail,sideEffects=None,groups=modeldock.wutz.dev,resources=modelexports,verbs=create;update,versions=v1alpha1,name=vmodelexport-v1alpha1.kb.io,admissionReviewVersions=v1

// ModelExportValidator validates source selection, target repository existence, and baseModelRef cross-namespace visibility.
// +kubebuilder:object:generate=false
type ModelExportValidator struct {
	Client client.Reader
}

func (v *ModelExportValidator) ValidateCreate(ctx context.Context, obj *modeldockv1alpha1.ModelExport) (admission.Warnings, error) {
	modelexportlog.Info("Validation for ModelExport upon creation", "name", obj.GetName())
	return v.validate(ctx, obj)
}

func (v *ModelExportValidator) ValidateUpdate(ctx context.Context, _, newObj *modeldockv1alpha1.ModelExport) (admission.Warnings, error) {
	modelexportlog.Info("Validation for ModelExport upon update", "name", newObj.GetName())
	return v.validate(ctx, newObj)
}

func (v *ModelExportValidator) ValidateDelete(_ context.Context, _ *modeldockv1alpha1.ModelExport) (admission.Warnings, error) {
	return nil, nil
}

func (v *ModelExportValidator) validate(ctx context.Context, obj *modeldockv1alpha1.ModelExport) (admission.Warnings, error) {
	srcCount := 0
	if obj.Spec.Source.PVCSource != nil {
		srcCount++
	}
	if obj.Spec.Source.ExternalHubSource != nil {
		srcCount++
	}
	if obj.Spec.Source.DevBoxSource != nil {
		srcCount++
	}
	if srcCount != 1 {
		return nil, fmt.Errorf("spec.source must specify exactly one of pvcSource, externalHubSource, or devBoxSource (got %d)", srcCount)
	}

	if obj.Spec.ArtifactType == modeldockv1alpha1.ArtifactTypeLoRAAdapter || obj.Spec.ArtifactType == modeldockv1alpha1.ArtifactTypeDraftModel {
		if obj.Spec.BaseModelRef == nil {
			return nil, fmt.Errorf("spec.baseModelRef is required when artifactType is %s", obj.Spec.ArtifactType)
		}
	}

	var warnings admission.Warnings
	if obj.Spec.AllowUnsafeFormats {
		warnings = append(warnings, "allowUnsafeFormats=true allows pickle-based weights which carry arbitrary code execution risk")
	}
	if obj.Spec.IncludeTrainingStates {
		warnings = append(warnings, "includeTrainingStates=true may export large optimizer states into Harbor")
	}

	if v.Client != nil {
		var repo modeldockv1alpha1.ModelRepository
		if err := v.Client.Get(ctx, types.NamespacedName{Namespace: obj.Namespace, Name: obj.Spec.TargetRepository}, &repo); err != nil {
			return nil, fmt.Errorf("spec.targetRepository %q not found in namespace %q: %w", obj.Spec.TargetRepository, obj.Namespace, err)
		}
		if err := ValidateCrossNamespaceModelReference(ctx, v.Client, obj.Namespace, obj.Spec.BaseModelRef); err != nil {
			return nil, err
		}
	}
	return warnings, nil
}
