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

// ExportPhase represents the execution phase of a ModelExport job.
// +kubebuilder:validation:Enum=Pending;Validating;Pushing;Succeeded;Failed
type ExportPhase string

const (
	ExportPhasePending    ExportPhase = "Pending"
	ExportPhaseValidating ExportPhase = "Validating"
	ExportPhasePushing    ExportPhase = "Pushing"
	ExportPhaseSucceeded  ExportPhase = "Succeeded"
	ExportPhaseFailed     ExportPhase = "Failed"
)

// HubProvider identifies an external open-source model registry.
// +kubebuilder:validation:Enum=HuggingFace;ModelScope
type HubProvider string

const (
	HubProviderHuggingFace HubProvider = "HuggingFace"
	HubProviderModelScope  HubProvider = "ModelScope"
)

// PVCSource exports model weights directly from a Kubernetes PersistentVolumeClaim.
type PVCSource struct {
	// ClaimName is the PVC name in the same namespace.
	// +kubebuilder:validation:MinLength=1
	ClaimName string `json:"claimName"`

	// SubPath specifies the subdirectory inside the PVC containing the checkpoint/model.
	// +optional
	SubPath string `json:"subPath,omitempty"`

	// IncludePatterns optionally restricts exported files by glob patterns.
	// +optional
	IncludePatterns []string `json:"includePatterns,omitempty"`

	// ExcludePatterns excludes matching files (in addition to default training state filters).
	// +optional
	ExcludePatterns []string `json:"excludePatterns,omitempty"`
}

// ExternalHubSource streams model weights from HuggingFace or ModelScope into Harbor.
type ExternalHubSource struct {
	// Provider specifies HuggingFace or ModelScope.
	Provider HubProvider `json:"provider"`

	// RepoID is the upstream model repository ID (e.g. "Qwen/Qwen3-32B").
	// +kubebuilder:validation:MinLength=1
	RepoID string `json:"repoID"`

	// Revision locks a branch, tag, or commit SHA (defaults to "main" or "master").
	// +optional
	Revision string `json:"revision,omitempty"`

	// EndpointMirror optionally specifies a mirror base URL (e.g. https://hf-mirror.com).
	// +optional
	EndpointMirror string `json:"endpointMirror,omitempty"`

	// AuthSecretRef optionally references a Secret containing a "token" key for gated repos.
	// +optional
	AuthSecretRef string `json:"authSecretRef,omitempty"`

	// IncludePatterns optionally restricts downloaded files by glob patterns.
	// +optional
	IncludePatterns []string `json:"includePatterns,omitempty"`

	// ExcludePatterns excludes matching files from relay.
	// +optional
	ExcludePatterns []string `json:"excludePatterns,omitempty"`
}

// DevBoxSource exports model snapshots from an interactive DevPilot workspace PVC.
type DevBoxSource struct {
	// ClaimName is the DevBox workspace PVC name.
	// +kubebuilder:validation:MinLength=1
	ClaimName string `json:"claimName"`

	// SubPath specifies the model directory inside the DevBox PVC.
	// +optional
	SubPath string `json:"subPath,omitempty"`

	// PodName optionally records the DevBox pod name for provenance.
	// +optional
	PodName string `json:"podName,omitempty"`

	// ExcludePatterns excludes matching files.
	// +optional
	ExcludePatterns []string `json:"excludePatterns,omitempty"`
}

// ExportSource selects exactly one model input source.
type ExportSource struct {
	// +optional
	PVCSource *PVCSource `json:"pvcSource,omitempty"`
	// +optional
	ExternalHubSource *ExternalHubSource `json:"externalHubSource,omitempty"`
	// +optional
	DevBoxSource *DevBoxSource `json:"devBoxSource,omitempty"`
}

// ModelExportSpec defines the desired state of ModelExport.
type ModelExportSpec struct {
	// TargetRepository names the target ModelRepository in the same namespace.
	// +kubebuilder:validation:MinLength=1
	TargetRepository string `json:"targetRepository"`

	// VersionTag assigns the semantic version tag in Harbor and the resulting ModelArtifact.
	// +kubebuilder:validation:MinLength=1
	VersionTag string `json:"versionTag"`

	// InitialStage defines the initial lifecycle stage for the generated ModelArtifact.
	// +kubebuilder:default=Staging
	// +optional
	InitialStage ArtifactStage `json:"initialStage,omitempty"`

	// ArtifactType classifies the exported artifact.
	// +kubebuilder:default=FullWeight
	// +optional
	ArtifactType ArtifactType `json:"artifactType,omitempty"`

	// BaseModelRef is required when ArtifactType is LoRAAdapter or DraftModel.
	// +optional
	BaseModelRef *ModelArtifactReference `json:"baseModelRef,omitempty"`

	// Source defines where weights are streamed from.
	Source ExportSource `json:"source"`

	// Lineage injects training or external provenance into the resulting ModelArtifact.
	// +optional
	Lineage *LineageSpec `json:"lineage,omitempty"`

	// AllowUnsafeFormats permits exporting pickle-based files (.bin/.pt/.pth).
	// +optional
	AllowUnsafeFormats bool `json:"allowUnsafeFormats,omitempty"`

	// IncludeTrainingStates disables default filtering of optimizer/rng checkpoint files.
	// +optional
	IncludeTrainingStates bool `json:"includeTrainingStates,omitempty"`

	// Concurrency controls bounded worker concurrency for shard hashing and upload.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=32
	// +kubebuilder:default=4
	// +optional
	Concurrency int32 `json:"concurrency,omitempty"`

	// ChunkSizeBytes splits any single weight file exceeding this size into virtual chunked OCI layers (0 disables splitting).
	// +optional
	ChunkSizeBytes int64 `json:"chunkSizeBytes,omitempty"`

	// TTLSecondsAfterFinished configures automatic Job/export cleanup TTL.
	// +kubebuilder:default=86400
	// +optional
	TTLSecondsAfterFinished *int32 `json:"ttlSecondsAfterFinished,omitempty"`
}

// ModelExportStatus defines the observed state of ModelExport.
type ModelExportStatus struct {
	// Phase tracks export pipeline execution state.
	// +optional
	Phase ExportPhase `json:"phase,omitempty"`

	// JobName records the backing Kubernetes Job name.
	// +optional
	JobName string `json:"jobName,omitempty"`

	// StartTime records when export execution began.
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty"`

	// CompletionTime records when export finished.
	// +optional
	CompletionTime *metav1.Time `json:"completionTime,omitempty"`

	// ExportedDigest records the resulting OCI manifest digest.
	// +optional
	ExportedDigest string `json:"exportedDigest,omitempty"`

	// ArtifactName records the name of the created ModelArtifact.
	// +optional
	ArtifactName string `json:"artifactName,omitempty"`

	// BytesTransferred records total uploaded bytes.
	// +optional
	BytesTransferred int64 `json:"bytesTransferred,omitempty"`

	// Message provides human-readable progress or failure details.
	// +optional
	Message string `json:"message,omitempty"`

	// Conditions represent the current state of the ModelExport resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=mexp
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Type",type=string,JSONPath=`.spec.artifactType`
// +kubebuilder:printcolumn:name="Repository",type=string,JSONPath=`.spec.targetRepository`
// +kubebuilder:printcolumn:name="Bytes",type=integer,JSONPath=`.status.bytesTransferred`
// +kubebuilder:printcolumn:name="Artifact",type=string,JSONPath=`.status.artifactName`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ModelExport is the Schema for the modelexports API.
type ModelExport struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// +required
	Spec ModelExportSpec `json:"spec"`

	// +optional
	Status ModelExportStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ModelExportList contains a list of ModelExport.
type ModelExportList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []ModelExport `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &ModelExport{}, &ModelExportList{})
		return nil
	})
}
