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

package harbor

import "time"

// Project represents a Harbor v2 project.
type Project struct {
	ProjectID    int64             `json:"project_id,omitempty"`
	Name         string            `json:"name"`
	Public       bool              `json:"public"`
	Metadata     map[string]string `json:"metadata,omitempty"`
	StorageLimit int64             `json:"storage_limit,omitempty"`
}

// RobotPermission defines an action/resource pair on a Harbor Robot Account.
type RobotPermission struct {
	Kind      string   `json:"kind"`
	Namespace string   `json:"namespace"`
	Access    []Access `json:"access"`
}

// Access defines resource + action permission.
type Access struct {
	Resource string `json:"resource"`
	Action   string `json:"action"`
	Effect   string `json:"effect,omitempty"`
}

// RobotRequest creates a project-scoped Harbor Robot Account.
type RobotRequest struct {
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	Level       string            `json:"level"`
	Duration    int64             `json:"duration"`
	Permissions []RobotPermission `json:"permissions"`
}

// Robot represents a Harbor Robot Account response (Secret is only populated on creation).
type Robot struct {
	ID          int64             `json:"id,omitempty"`
	Name        string            `json:"name"`
	Secret      string            `json:"secret,omitempty"`
	Level       string            `json:"level,omitempty"`
	Duration    int64             `json:"duration,omitempty"`
	ExpiresAt   int64             `json:"expires_at,omitempty"`
	Disable     bool              `json:"disable,omitempty"`
	Permissions []RobotPermission `json:"permissions,omitempty"`
}

// Tag represents an OCI tag attached to a Harbor artifact.
type Tag struct {
	ID         int64     `json:"id,omitempty"`
	Name       string    `json:"name"`
	PushTime   time.Time `json:"push_time,omitempty"`
	PullTime   time.Time `json:"pull_time,omitempty"`
	ArtifactID int64     `json:"artifact_id,omitempty"`
}

// Artifact represents a Harbor v2 artifact summary.
type Artifact struct {
	ID                int64                  `json:"id,omitempty"`
	Type              string                 `json:"type,omitempty"`
	MediaType         string                 `json:"media_type,omitempty"`
	ManifestMediaType string                 `json:"manifest_media_type,omitempty"`
	Digest            string                 `json:"digest"`
	Size              int64                  `json:"size"`
	PushTime          time.Time              `json:"push_time,omitempty"`
	Tags              []Tag                  `json:"tags,omitempty"`
	ExtraAttrs        map[string]interface{} `json:"extra_attrs,omitempty"`
}

// WebhookPayload models Harbor v2 webhook notification events.
type WebhookPayload struct {
	Type      string           `json:"type"`
	OccurAt   int64            `json:"occur_at,omitempty"`
	Operator  string           `json:"operator,omitempty"`
	EventData WebhookEventData `json:"event_data"`
}

// WebhookEventData holds resources and repository metadata from a Harbor webhook.
type WebhookEventData struct {
	Resources  []WebhookResource `json:"resources"`
	Repository WebhookRepository `json:"repository"`
}

// WebhookResource identifies an artifact affected by a Harbor event.
type WebhookResource struct {
	Digest      string `json:"digest"`
	Tag         string `json:"tag"`
	ResourceURL string `json:"resource_url"`
}

// WebhookRepository identifies the Harbor project and repo in a webhook event.
type WebhookRepository struct {
	Name         string `json:"name"`
	Namespace    string `json:"namespace"`
	RepoFullName string `json:"repo_full_name"`
	RepoType     string `json:"repo_type"`
}
