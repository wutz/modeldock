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

package oci

import (
	"time"

	modeldockv1alpha1 "github.com/wutz/modeldock/api/v1alpha1"
)

const (
	// MediaTypeOCIManifest is the standard OCI Image Manifest v1 media type.
	MediaTypeOCIManifest = "application/vnd.oci.image.manifest.v1+json"
	// MediaTypeOCIEmptyConfig is used if no config payload is provided.
	MediaTypeOCIEmptyConfig = "application/vnd.oci.empty.v1+json"

	// ArtifactTypeModelPack is the CNCF ModelPack artifact type.
	ArtifactTypeModelPack = "application/vnd.cncf.model.manifest.v1+json"
	// MediaTypeModelConfig is the CNCF ModelPack config blob media type.
	MediaTypeModelConfig = "application/vnd.cncf.model.config.v1+json"
	// MediaTypeWeightConfigTarZstd is the zstd-compressed tar layer containing config/tokenizer JSONs.
	MediaTypeWeightConfigTarZstd = "application/vnd.cncf.model.weight.config.v1.tar+zstd"
	// MediaTypeWeightRaw is the uncompressed single-file raw weight layer media type.
	MediaTypeWeightRaw = "application/vnd.cncf.model.weight.v1.raw"

	// AnnotationFilepath records the logical file path inside the ModelPack layout.
	AnnotationFilepath = "org.cncf.model.filepath"
	// AnnotationImageTitle records the original file name (OCI image title).
	AnnotationImageTitle = "org.opencontainers.image.title"
	// AnnotationContents lists packed config files inside the config tar+zstd layer.
	AnnotationContents = "modeldock.wutz.dev/contents"
	// AnnotationSafetensorsHeaderSize records byte length of the safetensors JSON header for HTTP Range reads.
	AnnotationSafetensorsHeaderSize = "modeldock.wutz.dev/safetensors.header-size"
	// AnnotationFileSHA256 records the whole-file SHA256 hex digest when a large file is split into chunks.
	AnnotationFileSHA256 = "modeldock.wutz.dev/file.sha256"
	// AnnotationChunkIndex records 0-based chunk index for split oversized weight layers.
	AnnotationChunkIndex = "modeldock.wutz.dev/chunk.index"
	// AnnotationChunkOffset records byte offset into target file for split oversized weight layers.
	AnnotationChunkOffset = "modeldock.wutz.dev/chunk.offset"
	// AnnotationChunkTotalSize records total original file size for split oversized weight layers.
	AnnotationChunkTotalSize = "modeldock.wutz.dev/chunk.total-size"

	// AnnotationArtifactType records FullWeight | LoRAAdapter | DraftModel.
	AnnotationArtifactType = "modeldock.wutz.dev/artifact-type"
	// AnnotationArchitecture records model architecture.
	AnnotationArchitecture = "modeldock.wutz.dev/architecture"
	// AnnotationPrecision records weight precision.
	AnnotationPrecision = "modeldock.wutz.dev/precision"
	// AnnotationTrainTaskID records TrainPilot TASK_ID.
	AnnotationTrainTaskID = "modeldock.wutz.dev/train-task-id"
)

// Descriptor describes an OCI content-addressable blob.
type Descriptor struct {
	MediaType   string            `json:"mediaType"`
	Digest      string            `json:"digest"`
	Size        int64             `json:"size"`
	Annotations map[string]string `json:"annotations,omitempty"`
}

// Manifest represents an OCI Image Manifest v1.1 with CNCF ModelPack artifactType.
type Manifest struct {
	SchemaVersion int               `json:"schemaVersion"`
	MediaType     string            `json:"mediaType"`
	ArtifactType  string            `json:"artifactType,omitempty"`
	Config        Descriptor        `json:"config"`
	Layers        []Descriptor      `json:"layers"`
	Annotations   map[string]string `json:"annotations,omitempty"`
}

// ModelPackConfig is stored as the OCI Config Blob so remote clusters can reconstruct ModelArtifact CRs
// without a shared DB.
type ModelPackConfig struct {
	SchemaVersion       string                                    `json:"schemaVersion"`
	CreatedAt           time.Time                                 `json:"createdAt"`
	ArtifactType        modeldockv1alpha1.ArtifactType            `json:"artifactType"`
	BaseModelRef        *modeldockv1alpha1.ModelArtifactReference `json:"baseModelRef,omitempty"`
	LoRASpec            *modeldockv1alpha1.LoRASpec               `json:"loraSpec,omitempty"`
	ServingProfile      *modeldockv1alpha1.ServingProfile         `json:"servingProfile,omitempty"`
	Lineage             *modeldockv1alpha1.LineageSpec            `json:"lineage,omitempty"`
	UnsafeSerialization bool                                      `json:"unsafeSerialization,omitempty"`
	ShardCount          int32                                     `json:"shardCount"`
	TotalSizeBytes      int64                                     `json:"totalSizeBytes"`
}
