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

	ctrl "sigs.k8s.io/controller-runtime"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	modeldockv1alpha1 "github.com/wutz/modeldock/api/v1alpha1"
)

var modelrepositorylog = logf.Log.WithName("modelrepository-resource")

// SetupModelRepositoryWebhookWithManager registers the webhook for ModelRepository in the manager.
func SetupModelRepositoryWebhookWithManager(mgr ctrl.Manager) error {
	return ctrl.NewWebhookManagedBy(mgr, &modeldockv1alpha1.ModelRepository{}).
		WithValidator(&ModelRepositoryValidator{}).
		Complete()
}

// +kubebuilder:webhook:path=/validate-modeldock-wutz-dev-v1alpha1-modelrepository,mutating=false,failurePolicy=fail,sideEffects=None,groups=modeldock.wutz.dev,resources=modelrepositories,verbs=create;update,versions=v1alpha1,name=vmodelrepository-v1alpha1.kb.io,admissionReviewVersions=v1

// ModelRepositoryValidator validates ModelRepository creation and updates.
// +kubebuilder:object:generate=false
type ModelRepositoryValidator struct{}

func (v *ModelRepositoryValidator) ValidateCreate(_ context.Context, obj *modeldockv1alpha1.ModelRepository) (admission.Warnings, error) {
	modelrepositorylog.Info("Validation for ModelRepository upon creation", "name", obj.GetName())
	return v.validate(obj)
}

func (v *ModelRepositoryValidator) ValidateUpdate(_ context.Context, _, newObj *modeldockv1alpha1.ModelRepository) (admission.Warnings, error) {
	modelrepositorylog.Info("Validation for ModelRepository upon update", "name", newObj.GetName())
	return v.validate(newObj)
}

func (v *ModelRepositoryValidator) ValidateDelete(_ context.Context, _ *modeldockv1alpha1.ModelRepository) (admission.Warnings, error) {
	return nil, nil
}

func (v *ModelRepositoryValidator) validate(obj *modeldockv1alpha1.ModelRepository) (admission.Warnings, error) {
	var warnings admission.Warnings
	if obj.Spec.Harbor.ProjectOverride != "" && obj.Namespace != modeldockv1alpha1.PublicNamespace {
		return nil, fmt.Errorf("spec.harbor.projectOverride is only permitted in namespace %q", modeldockv1alpha1.PublicNamespace)
	}
	if obj.Spec.Visibility == modeldockv1alpha1.VisibilityPublic && obj.Namespace != modeldockv1alpha1.PublicNamespace {
		warnings = append(warnings, fmt.Sprintf("visibility=Public outside %q exposes models to all cluster namespaces", modeldockv1alpha1.PublicNamespace))
	}
	if obj.Spec.Visibility == modeldockv1alpha1.VisibilityWorkspaceShared && len(obj.Spec.SharedNamespaces) == 0 {
		warnings = append(warnings, "visibility=WorkspaceShared has empty sharedNamespaces list")
	}
	return warnings, nil
}
