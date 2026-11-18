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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// RepositoryPhase represents the lifecycle phase of a ModelRepository.
// +kubebuilder:validation:Enum=Pending;Ready;Error
type RepositoryPhase string

const (
	RepositoryPhasePending RepositoryPhase = "Pending"
	RepositoryPhaseReady   RepositoryPhase = "Ready"
	RepositoryPhaseError   RepositoryPhase = "Error"
)

// HarborRepositoryConfig configures mapping to a Harbor Project and Repository.
type HarborRepositoryConfig struct {
	// InstanceRef references the platform Harbor instance configuration Secret/ConfigMap name.
	// +kubebuilder:default=default-harbor
	// +optional
	InstanceRef string `json:"instanceRef,omitempty"`

	// ProjectOverride overrides the Harbor project name. Only permitted in modeldock-public.
	// +optional
	ProjectOverride string `json:"projectOverride,omitempty"`

	// RepositoryName overrides the Harbor repository name inside the project. Defaults to metadata.name.
	// +optional
	RepositoryName string `json:"repositoryName,omitempty"`

	// StorageLimitBytes configures Harbor project storage quota in bytes (-1 for unlimited).
	// +optional
	StorageLimitBytes *int64 `json:"storageLimitBytes,omitempty"`
}

// RetentionPolicy defines automatic staging pruning and Harbor GC policies.
type RetentionPolicy struct {
	// KeepLastStagingVersions retains at most N Staging artifacts ordered by creation time.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=5
	// +optional
	KeepLastStagingVersions *int32 `json:"keepLastStagingVersions,omitempty"`

	// UntaggedGarbageCollectDays sets the retention window for untagged Harbor manifests.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=3
	// +optional
	UntaggedGarbageCollectDays *int32 `json:"untaggedGarbageCollectDays,omitempty"`
}

// ModelRepositorySpec defines the desired state of ModelRepository.
type ModelRepositorySpec struct {
	// DisplayName is a human-friendly title for the repository.
	// +optional
	DisplayName string `json:"displayName,omitempty"`

	// Description explains the purpose and lineage of models stored in this repository.
	// +optional
	Description string `json:"description,omitempty"`

	// Category classifies the model family.
	// +kubebuilder:default=LLM
	// +optional
	Category ModelCategory `json:"category,omitempty"`

	// Visibility controls cross-namespace access permissions.
	// +kubebuilder:default=Private
	// +optional
	Visibility RepositoryVisibility `json:"visibility,omitempty"`

	// SharedNamespaces lists consumer namespaces permitted when visibility is WorkspaceShared.
	// +optional
	SharedNamespaces []string `json:"sharedNamespaces,omitempty"`

	// Harbor defines the underlying Harbor project and repository mapping.
	// +optional
	Harbor HarborRepositoryConfig `json:"harbor,omitempty"`

	// AutoWarmupStages lists artifact stages that automatically trigger ModelCache creation.
	// +optional
	AutoWarmupStages []ArtifactStage `json:"autoWarmupStages,omitempty"`

	// RetentionPolicy governs automatic cleanup of old Staging artifacts.
	// +optional
	RetentionPolicy *RetentionPolicy `json:"retentionPolicy,omitempty"`
}

// ModelRepositoryStatus defines the observed state of ModelRepository.
type ModelRepositoryStatus struct {
	// Phase summarizes repository reconciliation health.
	// +optional
	Phase RepositoryPhase `json:"phase,omitempty"`

	// OCIRepository is the full OCI repository URI (e.g. harbor.example.com/models-acme/finance-qwen3-lora).
	// +optional
	OCIRepository string `json:"ociRepository,omitempty"`

	// HarborProjectID is the numeric Harbor Project ID.
	// +optional
	HarborProjectID int64 `json:"harborProjectID,omitempty"`

	// PullSecretName is the K8s Secret containing read-only Harbor Robot credentials.
	// +optional
	PullSecretName string `json:"pullSecretName,omitempty"`

	// PushSecretName is the K8s Secret containing pull+push (no delete) Harbor Robot credentials.
	// +optional
	PushSecretName string `json:"pushSecretName,omitempty"`

	// ArtifactCount tracks active ModelArtifacts associated with this repository.
	// +optional
	ArtifactCount int32 `json:"artifactCount,omitempty"`

	// LatestProductionArtifact records the most recently promoted Production ModelArtifact name.
	// +optional
	LatestProductionArtifact string `json:"latestProductionArtifact,omitempty"`

	// LastCredentialRotationTime records when pull/push Robot credentials were last rotated.
	// +optional
	LastCredentialRotationTime *metav1.Time `json:"lastCredentialRotationTime,omitempty"`

	// Conditions represent the current state of the ModelRepository resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=mrepo
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Visibility",type=string,JSONPath=`.spec.visibility`
// +kubebuilder:printcolumn:name="OCI Repository",type=string,JSONPath=`.status.ociRepository`
// +kubebuilder:printcolumn:name="Artifacts",type=integer,JSONPath=`.status.artifactCount`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ModelRepository is the Schema for the modelrepositories API.
type ModelRepository struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// +required
	Spec ModelRepositorySpec `json:"spec"`

	// +optional
	Status ModelRepositoryStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ModelRepositoryList contains a list of ModelRepository.
type ModelRepositoryList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []ModelRepository `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &ModelRepository{}, &ModelRepositoryList{})
		return nil
	})
}
