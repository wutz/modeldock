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

// ArtifactPhase represents the synchronization/verification phase of a ModelArtifact.
// +kubebuilder:validation:Enum=Syncing;Ready;Invalid;Archived
type ArtifactPhase string

const (
	ArtifactPhaseSyncing  ArtifactPhase = "Syncing"
	ArtifactPhaseReady    ArtifactPhase = "Ready"
	ArtifactPhaseInvalid  ArtifactPhase = "Invalid"
	ArtifactPhaseArchived ArtifactPhase = "Archived"
)

// ModelArtifactSpec defines the desired state of ModelArtifact.
type ModelArtifactSpec struct {
	// RepositoryRef references the parent ModelRepository in the same namespace.
	// +kubebuilder:validation:MinLength=1
	RepositoryRef string `json:"repositoryRef"`

	// VersionTag is the semantic or release tag mapped in Harbor.
	// +kubebuilder:validation:MinLength=1
	VersionTag string `json:"versionTag"`

	// OCIDigest locks the immutable sha256 manifest digest (e.g. sha256:...).
	// +kubebuilder:validation:Pattern=`^sha256:[a-f0-9]{64}$`
	OCIDigest string `json:"ociDigest"`

	// Stage controls the lifecycle promotion state machine.
	// +kubebuilder:default=Staging
	// +optional
	Stage ArtifactStage `json:"stage,omitempty"`

	// ArtifactType classifies this model artifact.
	// +kubebuilder:default=FullWeight
	// +optional
	ArtifactType ArtifactType `json:"artifactType,omitempty"`

	// BaseModelRef is required when ArtifactType is LoRAAdapter or DraftModel.
	// +optional
	BaseModelRef *ModelArtifactReference `json:"baseModelRef,omitempty"`

	// LoRASpec contains adapter parameters extracted from adapter_config.json.
	// +optional
	LoRASpec *LoRASpec `json:"loraSpec,omitempty"`

	// ServingProfile defines runtime inference specifications.
	// +optional
	ServingProfile *ServingProfile `json:"servingProfile,omitempty"`

	// Lineage tracks training provenance, parent artifacts, and evaluation records.
	// +optional
	Lineage *LineageSpec `json:"lineage,omitempty"`
}

// ModelArtifactStatus defines the observed state of ModelArtifact.
type ModelArtifactStatus struct {
	// Phase summarizes artifact verification status.
	// +optional
	Phase ArtifactPhase `json:"phase,omitempty"`

	// OCIReference is the canonical digest-pinned OCI reference.
	// +optional
	OCIReference string `json:"ociReference,omitempty"`

	// TotalSizeBytes is the total byte size of all OCI layers in the manifest.
	// +optional
	TotalSizeBytes int64 `json:"totalSizeBytes,omitempty"`

	// ShardCount is the number of weight shards in the artifact.
	// +optional
	ShardCount int32 `json:"shardCount,omitempty"`

	// UnsafeSerialization is true if the artifact contains pickle-based formats (.bin/.pt).
	// +optional
	UnsafeSerialization bool `json:"unsafeSerialization,omitempty"`

	// ActiveCacheRef points to an active ready ModelCache in the same namespace.
	// +optional
	ActiveCacheRef string `json:"activeCacheRef,omitempty"`

	// PromotedToProductionAt records when the artifact first entered Production stage.
	// +optional
	PromotedToProductionAt *metav1.Time `json:"promotedToProductionAt,omitempty"`

	// Conditions represent the current state of the ModelArtifact resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=mart
// +kubebuilder:printcolumn:name="Stage",type=string,JSONPath=`.spec.stage`
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.artifactType`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Size",type=integer,JSONPath=`.status.totalSizeBytes`
// +kubebuilder:printcolumn:name="Digest",type=string,JSONPath=`.spec.ociDigest`,priority=1
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ModelArtifact is the Schema for the modelartifacts API.
type ModelArtifact struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// +required
	Spec ModelArtifactSpec `json:"spec"`

	// +optional
	Status ModelArtifactStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ModelArtifactList contains a list of ModelArtifact.
type ModelArtifactList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []ModelArtifact `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &ModelArtifact{}, &ModelArtifactList{})
		return nil
	})
}
