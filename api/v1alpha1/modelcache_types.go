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

// CacheStrategy defines how model weights are materialized for inference pods.
// +kubebuilder:validation:Enum=SharedFilesystem;NodeLocalNVMe;ImageVolume
type CacheStrategy string

const (
	CacheStrategySharedFilesystem CacheStrategy = "SharedFilesystem"
	CacheStrategyNodeLocalNVMe    CacheStrategy = "NodeLocalNVMe"
	CacheStrategyImageVolume      CacheStrategy = "ImageVolume"
)

// CachePhase represents the warmup/eviction state of a ModelCache.
// +kubebuilder:validation:Enum=Pending;WarmingUp;Ready;Evicted;Failed
type CachePhase string

const (
	CachePhasePending   CachePhase = "Pending"
	CachePhaseWarmingUp CachePhase = "WarmingUp"
	CachePhaseReady     CachePhase = "Ready"
	CachePhaseEvicted   CachePhase = "Evicted"
	CachePhaseFailed    CachePhase = "Failed"
)

// MountMode defines the volume mount mechanism advertised in status.mountSpec.
// +kubebuilder:validation:Enum=ReadOnlyManyPVC;NodeLocalHostPath;OCIImageVolume
type MountMode string

const (
	MountModeReadOnlyManyPVC   MountMode = "ReadOnlyManyPVC"
	MountModeNodeLocalHostPath MountMode = "NodeLocalHostPath"
	MountModeOCIImageVolume    MountMode = "OCIImageVolume"
)

// SharedFilesystemCacheConfig configures shared RWX CAS volume delivery.
type SharedFilesystemCacheConfig struct {
	// CachePoolName names the platform RWX storage pool (e.g. default-rwx-model-pool).
	// +kubebuilder:default=default-rwx-model-pool
	// +optional
	CachePoolName string `json:"cachePoolName,omitempty"`

	// PVCName explicitly specifies the RWX PVC storing the CAS directory (defaults to pool mapping).
	// +optional
	PVCName string `json:"pvcName,omitempty"`
}

// NodeLocalNVMeCacheConfig configures GPU node local NVMe cache pre-pulling.
type NodeLocalNVMeCacheConfig struct {
	// NodeSelector filters target GPU nodes for cache warmup.
	// +optional
	NodeSelector map[string]string `json:"nodeSelector,omitempty"`

	// HostCacheRoot sets the host directory root for CAS blobs and hardlink manifest views.
	// +kubebuilder:default=/var/lib/modeldock/cas
	// +optional
	HostCacheRoot string `json:"hostCacheRoot,omitempty"`
}

// CacheMountSpec describes how InferPilot/TrainPilot pods should mount the cached model.
type CacheMountSpec struct {
	// Mode specifies ReadOnlyManyPVC, NodeLocalHostPath, or OCIImageVolume.
	Mode MountMode `json:"mode"`

	// ClaimName is set when Mode is ReadOnlyManyPVC.
	// +optional
	ClaimName string `json:"claimName,omitempty"`

	// SubPath is the digest-addressed directory path inside the PVC (sha256/<hex>).
	// +optional
	SubPath string `json:"subPath,omitempty"`

	// HostPath is set when Mode is NodeLocalHostPath.
	// +optional
	HostPath string `json:"hostPath,omitempty"`

	// OCIReference is set when Mode is OCIImageVolume.
	// +optional
	OCIReference string `json:"ociReference,omitempty"`
}

// ModelCacheSpec defines the desired state of ModelCache.
type ModelCacheSpec struct {
	// ArtifactRef identifies the target ModelArtifact to warm up.
	ArtifactRef ModelArtifactReference `json:"artifactRef"`

	// Strategy selects SharedFilesystem, NodeLocalNVMe, or ImageVolume.
	// +kubebuilder:default=SharedFilesystem
	// +optional
	Strategy CacheStrategy `json:"strategy,omitempty"`

	// SharedFilesystem configures shared CAS volume caching.
	// +optional
	SharedFilesystem *SharedFilesystemCacheConfig `json:"sharedFilesystem,omitempty"`

	// NodeLocalNVMe configures node-local NVMe hardlink caching.
	// +optional
	NodeLocalNVMe *NodeLocalNVMeCacheConfig `json:"nodeLocalNVMe,omitempty"`

	// EnableP2P routes Harbor blob pulls through Dragonfly P2P HTTP proxy when configured.
	// +kubebuilder:default=true
	// +optional
	EnableP2P bool `json:"enableP2P,omitempty"`

	// TTLHoursAfterUnused permits LRU eviction after activeConsumers stays 0 for this duration.
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:default=168
	// +optional
	TTLHoursAfterUnused *int32 `json:"ttlHoursAfterUnused,omitempty"`
}

// ModelCacheStatus defines the observed state of ModelCache.
type ModelCacheStatus struct {
	// Phase tracks warmup and lifecycle state.
	// +optional
	Phase CachePhase `json:"phase,omitempty"`

	// ResolvedDigest records the immutable OCI manifest digest materialized in cache.
	// +optional
	ResolvedDigest string `json:"resolvedDigest,omitempty"`

	// MountSpec exposes ready-to-use mount coordinates for InferPilot / TrainPilot.
	// +optional
	MountSpec *CacheMountSpec `json:"mountSpec,omitempty"`

	// CachedNodesCount records how many nodes have warmed the cache in NodeLocalNVMe mode.
	// +optional
	CachedNodesCount int32 `json:"cachedNodesCount,omitempty"`

	// WarmupDurationSeconds records total warmup duration.
	// +optional
	WarmupDurationSeconds int64 `json:"warmupDurationSeconds,omitempty"`

	// ActiveConsumers counts running pods actively referencing this cache.
	// +optional
	ActiveConsumers int32 `json:"activeConsumers,omitempty"`

	// LastUsedTime records the last timestamp when ActiveConsumers > 0 or warmup completed.
	// +optional
	LastUsedTime *metav1.Time `json:"lastUsedTime,omitempty"`

	// EvictedAt records when unused cache content was evicted.
	// +optional
	EvictedAt *metav1.Time `json:"evictedAt,omitempty"`

	// Message gives progress or failure context.
	// +optional
	Message string `json:"message,omitempty"`

	// Conditions represent the current state of the ModelCache resource.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:shortName=mcache
// +kubebuilder:printcolumn:name="Strategy",type=string,JSONPath=`.spec.strategy`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Consumers",type=integer,JSONPath=`.status.activeConsumers`
// +kubebuilder:printcolumn:name="Duration(s)",type=integer,JSONPath=`.status.warmupDurationSeconds`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ModelCache is the Schema for the modelcaches API.
type ModelCache struct {
	metav1.TypeMeta `json:",inline"`

	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// +required
	Spec ModelCacheSpec `json:"spec"`

	// +optional
	Status ModelCacheStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// ModelCacheList contains a list of ModelCache.
type ModelCacheList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []ModelCache `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &ModelCache{}, &ModelCacheList{})
		return nil
	})
}
