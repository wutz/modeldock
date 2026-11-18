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

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

const (
	// PublicNamespace is the platform namespace hosting public base models.
	PublicNamespace = "modeldock-public"
	// PublicHarborProject is the Harbor project name mapped to PublicNamespace.
	PublicHarborProject = "models-public"
	// DefaultSystemNamespace is the namespace where ModelDock control plane runs.
	DefaultSystemNamespace = "modeldock-system"

	// LabelWorkspaceProject identifies the tenant project name on a Kubernetes Namespace or CR.
	LabelWorkspaceProject = "workspace.wutz.dev/project"
	// LabelRepository indexes ModelArtifact by its parent ModelRepository name.
	LabelRepository = "modeldock.wutz.dev/repository"
	// LabelArtifactType indexes ModelArtifact by artifact type.
	LabelArtifactType = "modeldock.wutz.dev/artifact-type"
	// LabelStage indexes ModelArtifact by its lifecycle stage.
	LabelStage = "modeldock.wutz.dev/stage"
	// LabelExportJob links an exporter Job to a ModelExport resource.
	LabelExportJob = "modeldock.wutz.dev/export"
	// LabelCacheJob links a puller Job to a ModelCache resource.
	LabelCacheJob = "modeldock.wutz.dev/cache"
	// LabelConsumerCache is placed on consumer Pods using a ModelCache.
	LabelConsumerCache = "modeldock.wutz.dev/cache-ref"
	// LabelConsumerArtifact is placed on consumer Pods using a ModelArtifact.
	LabelConsumerArtifact = "modeldock.wutz.dev/artifact-ref"

	// AnnotationAction triggers one-shot operational actions on resources ("warmup", "rotate-credentials").
	AnnotationAction = "modeldock.wutz.dev/action"
	// ActionWarmup triggers immediate ModelCache creation/warmup for a ModelArtifact.
	ActionWarmup = "warmup"
	// ActionRotateCredentials triggers immediate Harbor Robot credential rotation on a ModelRepository.
	ActionRotateCredentials = "rotate-credentials"

	// FinalizerModelDock is the standard finalizer used for external Harbor / cache cleanup.
	FinalizerModelDock = "modeldock.wutz.dev/finalizer"
)

// ModelCategory enumerates supported model domains.
// +kubebuilder:validation:Enum=LLM;VLM;Embedding;Reranker;Audio;Custom
type ModelCategory string

const (
	CategoryLLM       ModelCategory = "LLM"
	CategoryVLM       ModelCategory = "VLM"
	CategoryEmbedding ModelCategory = "Embedding"
	CategoryReranker  ModelCategory = "Reranker"
	CategoryAudio     ModelCategory = "Audio"
	CategoryCustom    ModelCategory = "Custom"
)

// RepositoryVisibility controls cross-namespace sharing policy.
// +kubebuilder:validation:Enum=Private;WorkspaceShared;Public
type RepositoryVisibility string

const (
	VisibilityPrivate         RepositoryVisibility = "Private"
	VisibilityWorkspaceShared RepositoryVisibility = "WorkspaceShared"
	VisibilityPublic          RepositoryVisibility = "Public"
)

// ArtifactStage defines the lifecycle promotion stage of a ModelArtifact.
// +kubebuilder:validation:Enum=Staging;Verified;Production;Deprecated;Archived
type ArtifactStage string

const (
	StageStaging    ArtifactStage = "Staging"
	StageVerified   ArtifactStage = "Verified"
	StageProduction ArtifactStage = "Production"
	StageDeprecated ArtifactStage = "Deprecated"
	StageArchived   ArtifactStage = "Archived"
)

// ArtifactType classifies whether the artifact is a full weight, LoRA adapter, or draft model.
// +kubebuilder:validation:Enum=FullWeight;LoRAAdapter;DraftModel
type ArtifactType string

const (
	ArtifactTypeFullWeight  ArtifactType = "FullWeight"
	ArtifactTypeLoRAAdapter ArtifactType = "LoRAAdapter"
	ArtifactTypeDraftModel  ArtifactType = "DraftModel"
)

// SourceType identifies the origin of a ModelArtifact or ModelExport.
// +kubebuilder:validation:Enum=TrainTask;DevBox;HuggingFace;ModelScope;Quantization;ExternalOCI
type SourceType string

const (
	SourceTypeTrainTask    SourceType = "TrainTask"
	SourceTypeDevBox       SourceType = "DevBox"
	SourceTypeHuggingFace  SourceType = "HuggingFace"
	SourceTypeModelScope   SourceType = "ModelScope"
	SourceTypeQuantization SourceType = "Quantization"
	SourceTypeExternalOCI  SourceType = "ExternalOCI"
)

// ModelArtifactReference points to a ModelArtifact (e.g. baseModelRef or parentArtifactRef).
type ModelArtifactReference struct {
	// Namespace of the referenced ModelArtifact. Defaults to the referencing object's namespace if empty.
	// +optional
	Namespace string `json:"namespace,omitempty"`

	// Name of the referenced ModelArtifact.
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// OCIDigest optionally pins the exact immutable manifest digest of the referenced artifact.
	// +optional
	OCIDigest string `json:"ociDigest,omitempty"`
}

// LoRASpec captures adapter rank, alpha, and target transformer modules.
type LoRASpec struct {
	// Rank is the LoRA decomposition rank r.
	// +kubebuilder:validation:Minimum=1
	Rank int32 `json:"rank"`

	// Alpha is the LoRA scaling factor.
	// +optional
	Alpha int32 `json:"alpha,omitempty"`

	// TargetModules lists module suffixes adapted by LoRA.
	// +optional
	TargetModules []string `json:"targetModules,omitempty"`
}

// ServingProfile defines the runtime inference contract consumed by InferPilot / vLLM / SGLang.
type ServingProfile struct {
	// Architecture is the model architecture class (e.g. Qwen3ForCausalLM).
	// +optional
	Architecture string `json:"architecture,omitempty"`

	// ParameterSize is a human-readable parameter count (e.g. "32B", "671B").
	// +optional
	ParameterSize string `json:"parameterSize,omitempty"`

	// Precision denotes numerical dtype or quantization format.
	// +kubebuilder:validation:Enum=FP32;BF16;FP16;FP8;INT8;INT4-AWQ;INT4-GPTQ;Unknown
	// +optional
	Precision string `json:"precision,omitempty"`

	// SerializationFormat denotes weight serialization format.
	// +kubebuilder:validation:Enum=safetensors;gguf;pytorch-bin;unknown
	// +optional
	SerializationFormat string `json:"serializationFormat,omitempty"`

	// RequiresTrustRemoteCode indicates whether custom Python modeling code is needed.
	// +optional
	RequiresTrustRemoteCode bool `json:"requiresTrustRemoteCode,omitempty"`

	// MinVRAMGiB estimates minimum GPU memory required to load the weights.
	// +optional
	MinVRAMGiB int32 `json:"minVRAMGiB,omitempty"`

	// MaxPositionEmbeddings records maximum context window length from config.json.
	// +optional
	MaxPositionEmbeddings int32 `json:"maxPositionEmbeddings,omitempty"`

	// HasChatTemplate indicates whether tokenizer_config.json or chat_template.jinja is present.
	// +optional
	HasChatTemplate bool `json:"hasChatTemplate,omitempty"`

	// RecommendedEngine names the preferred inference engine (vllm | sglang | triton).
	// +kubebuilder:validation:Enum=vllm;sglang;triton
	// +optional
	RecommendedEngine string `json:"recommendedEngine,omitempty"`

	// RecommendedTensorParallel specifies recommended tensor parallel degree.
	// +optional
	RecommendedTensorParallel int32 `json:"recommendedTensorParallel,omitempty"`

	// RecommendedExtraArgs holds extra CLI flags passed to the inference engine.
	// +optional
	RecommendedExtraArgs []string `json:"recommendedExtraArgs,omitempty"`
}

// TrainTaskReference records lineage back to a TrainPilot TrainTask.
type TrainTaskReference struct {
	// +optional
	Namespace string `json:"namespace,omitempty"`
	// +optional
	Name string `json:"name,omitempty"`
	// +optional
	TaskID string `json:"taskID,omitempty"`
	// +optional
	RunID string `json:"runID,omitempty"`
	// +optional
	GlobalStep int64 `json:"globalStep,omitempty"`
}

// EvaluationRecord captures benchmark evaluation results attached to an artifact.
type EvaluationRecord struct {
	Benchmark   string       `json:"benchmark"`
	Score       string       `json:"score"`
	EvaluatedAt *metav1.Time `json:"evaluatedAt,omitempty"`
	// +optional
	ReportURI string `json:"reportURI,omitempty"`
}

// LineageSpec records training, fine-tuning, and evaluation provenance.
type LineageSpec struct {
	// SourceType identifies how this model was produced.
	// +optional
	SourceType SourceType `json:"sourceType,omitempty"`

	// TrainTaskRef links to the originating TrainPilot task.
	// +optional
	TrainTaskRef *TrainTaskReference `json:"trainTaskRef,omitempty"`

	// ParentArtifactRef links to the parent model artifact (e.g. SFT checkpoint or base model).
	// +optional
	ParentArtifactRef *ModelArtifactReference `json:"parentArtifactRef,omitempty"`

	// ExternalRevision captures the resolved upstream commit SHA or external OCI reference.
	// +optional
	ExternalRevision string `json:"externalRevision,omitempty"`

	// Evaluations stores benchmark evaluation scores used for promotion gates.
	// +optional
	Evaluations []EvaluationRecord `json:"evaluations,omitempty"`
}
