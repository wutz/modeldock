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

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client defines the Harbor v2.10+ management operations used by ModelDock controllers.
type Client interface {
	Ping(ctx context.Context) error
	EnsureProject(ctx context.Context, name string, public bool, storageLimitBytes *int64) (*Project, error)
	EnsureProjectRobots(ctx context.Context, projectName, repoBaseName string, rotate bool) (pullRobot *Robot, pushRobot *Robot, err error)
	ListArtifacts(ctx context.Context, projectName, repoName string) ([]Artifact, error)
	GetArtifact(ctx context.Context, projectName, repoName, reference string) (*Artifact, error)
	AddArtifactTag(ctx context.Context, projectName, repoName, reference, tag string) error
	DeleteArtifactTag(ctx context.Context, projectName, repoName, reference, tag string) error
	DeleteArtifact(ctx context.Context, projectName, repoName, reference string) error
}

// HTTPClient implements Client against Harbor v2 REST API.
type HTTPClient struct {
	baseURL    string
	username   string
	password   string
	httpClient *http.Client
}

// NewHTTPClient creates a Harbor v2 REST client.
func NewHTTPClient(baseURL, username, password string, insecureSkipTLS bool) (*HTTPClient, error) {
	if baseURL == "" {
		return nil, errors.New("harbor baseURL is required")
	}
	u := strings.TrimRight(baseURL, "/")
	if !strings.HasPrefix(u, "http://") && !strings.HasPrefix(u, "https://") {
		u = "https://" + u
	}
	return &HTTPClient{
		baseURL:  u,
		username: username,
		password: password,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{
					InsecureSkipVerify: insecureSkipTLS, //nolint:gosec
				},
			},
		},
	}, nil
}

func (c *HTTPClient) Ping(ctx context.Context) error {
	resp, err := c.doJSON(ctx, http.MethodGet, "/api/v2.0/ping", nil, nil)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("harbor ping returned %d", resp.StatusCode)
	}
	return nil
}

func (c *HTTPClient) EnsureProject(ctx context.Context, name string, public bool, storageLimitBytes *int64) (*Project, error) {
	if name == "" {
		return nil, errors.New("project name cannot be empty")
	}

	proj, err := c.getProjectByName(ctx, name)
	if err != nil {
		return nil, err
	}
	if proj == nil {
		reqBody := map[string]interface{}{
			"project_name": name,
			"public":       public,
			"metadata": map[string]string{
				"public": strconv.FormatBool(public),
			},
		}
		if storageLimitBytes != nil {
			reqBody["storage_limit"] = *storageLimitBytes
		}
		resp, err := c.doJSON(ctx, http.MethodPost, "/api/v2.0/projects", reqBody, nil)
		if err != nil {
			return nil, err
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusConflict {
			return nil, fmt.Errorf("create harbor project %q returned %d: %s", name, resp.StatusCode, strings.TrimSpace(string(body)))
		}
		proj, err = c.getProjectByName(ctx, name)
		if err != nil {
			return nil, err
		}
		if proj == nil {
			return nil, fmt.Errorf("project %q created but not found on readback", name)
		}
	}

	// Update quota if requested and project exists.
	if storageLimitBytes != nil && proj.ProjectID > 0 {
		_ = c.updateProjectQuota(ctx, proj.ProjectID, *storageLimitBytes)
	}
	return proj, nil
}

func (c *HTTPClient) getProjectByName(ctx context.Context, name string) (*Project, error) {
	resp, err := c.doJSON(ctx, http.MethodGet, "/api/v2.0/projects?name="+url.QueryEscape(name), nil, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("list harbor projects returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var projects []Project
	if err := json.Unmarshal(body, &projects); err != nil {
		return nil, err
	}
	for i := range projects {
		if projects[i].Name == name {
			return &projects[i], nil
		}
	}
	return nil, nil
}

func (c *HTTPClient) updateProjectQuota(ctx context.Context, projectID, storageLimitBytes int64) error {
	// List quotas for project and update if found.
	path := fmt.Sprintf("/api/v2.0/quotas?reference=project&reference_id=%d", projectID)
	resp, err := c.doJSON(ctx, http.MethodGet, path, nil, nil)
	if err != nil {
		return err
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 256*1024))
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil
	}
	var quotas []struct {
		ID int64 `json:"id"`
	}
	if err := json.Unmarshal(body, &quotas); err != nil || len(quotas) == 0 {
		return nil
	}
	putBody := map[string]interface{}{
		"hard": map[string]int64{
			"storage": storageLimitBytes,
		},
	}
	putResp, err := c.doJSON(ctx, http.MethodPut, fmt.Sprintf("/api/v2.0/quotas/%d", quotas[0].ID), putBody, nil)
	if err != nil {
		return err
	}
	_ = putResp.Body.Close()
	return nil
}

// EnsureProjectRobots creates or rotates least-privilege project-scoped Pull and Push Robot Accounts.
// Pull Robot: repository:pull only.
// Push Robot: repository:pull + repository:push (NO delete permission).
func (c *HTTPClient) EnsureProjectRobots(ctx context.Context, projectName, repoBaseName string, rotate bool) (*Robot, *Robot, error) {
	existing, err := c.listProjectRobots(ctx, projectName)
	if err != nil {
		return nil, nil, err
	}

	pullName := fmt.Sprintf("modeldock-%s-pull", sanitizeRobotSuffix(repoBaseName))
	pushName := fmt.Sprintf("modeldock-%s-push", sanitizeRobotSuffix(repoBaseName))

	findRobot := func(shortName string) *Robot {
		suffix := "$" + shortName
		for i := range existing {
			if existing[i].Name == shortName || strings.HasSuffix(existing[i].Name, suffix) {
				return &existing[i]
			}
		}
		return nil
	}

	ensureOne := func(shortName string, actions []string) (*Robot, error) {
		cur := findRobot(shortName)
		if cur != nil && !rotate {
			// Robot already exists and no rotation requested; caller keeps existing K8s Secret token.
			return cur, nil
		}
		if cur != nil && rotate {
			if err := c.deleteRobot(ctx, cur.ID); err != nil {
				return nil, fmt.Errorf("delete old robot %s: %w", cur.Name, err)
			}
		}
		accessList := make([]Access, 0, len(actions))
		for _, a := range actions {
			accessList = append(accessList, Access{
				Resource: "repository",
				Action:   a,
				Effect:   "allow",
			})
		}
		req := RobotRequest{
			Name:        shortName,
			Description: "Managed by ModelDock operator (least-privilege project robot)",
			Level:       "project",
			Duration:    90, // days
			Permissions: []RobotPermission{
				{
					Kind:      "project",
					Namespace: projectName,
					Access:    accessList,
				},
			},
		}
		resp, err := c.doJSON(ctx, http.MethodPost, "/api/v2.0/robots", req, nil)
		if err != nil {
			return nil, err
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 256*1024))
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("create robot %s returned %d: %s", shortName, resp.StatusCode, strings.TrimSpace(string(body)))
		}
		var created Robot
		if err := json.Unmarshal(body, &created); err != nil {
			return nil, err
		}
		return &created, nil
	}

	pullRobot, err := ensureOne(pullName, []string{"pull"})
	if err != nil {
		return nil, nil, err
	}
	pushRobot, err := ensureOne(pushName, []string{"pull", "push"})
	if err != nil {
		return nil, nil, err
	}
	return pullRobot, pushRobot, nil
}

func (c *HTTPClient) listProjectRobots(ctx context.Context, projectName string) ([]Robot, error) {
	q := fmt.Sprintf("/api/v2.0/robots?q=%s&page_size=100", url.QueryEscape("Level=project"))
	resp, err := c.doJSON(ctx, http.MethodGet, q, nil, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("list robots returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var robots []Robot
	if err := json.Unmarshal(body, &robots); err != nil {
		return nil, err
	}
	var filtered []Robot
	for _, r := range robots {
		if len(r.Permissions) == 0 || r.Permissions[0].Namespace == projectName {
			filtered = append(filtered, r)
		}
	}
	return filtered, nil
}

func (c *HTTPClient) deleteRobot(ctx context.Context, id int64) error {
	resp, err := c.doJSON(ctx, http.MethodDelete, fmt.Sprintf("/api/v2.0/robots/%d", id), nil, nil)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("delete robot %d returned %d", id, resp.StatusCode)
	}
	return nil
}

func (c *HTTPClient) ListArtifacts(ctx context.Context, projectName, repoName string) ([]Artifact, error) {
	path := fmt.Sprintf("/api/v2.0/projects/%s/repositories/%s/artifacts?with_tag=true&page_size=100",
		url.PathEscape(projectName), doubleEscapeRepo(repoName))
	resp, err := c.doJSON(ctx, http.MethodGet, path, nil, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4*1024*1024))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("list artifacts returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var artifacts []Artifact
	if err := json.Unmarshal(body, &artifacts); err != nil {
		return nil, err
	}
	return artifacts, nil
}

func (c *HTTPClient) GetArtifact(ctx context.Context, projectName, repoName, reference string) (*Artifact, error) {
	path := fmt.Sprintf("/api/v2.0/projects/%s/repositories/%s/artifacts/%s?with_tag=true",
		url.PathEscape(projectName), doubleEscapeRepo(repoName), url.PathEscape(reference))
	resp, err := c.doJSON(ctx, http.MethodGet, path, nil, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get artifact returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var art Artifact
	if err := json.Unmarshal(body, &art); err != nil {
		return nil, err
	}
	return &art, nil
}

func (c *HTTPClient) AddArtifactTag(ctx context.Context, projectName, repoName, reference, tag string) error {
	path := fmt.Sprintf("/api/v2.0/projects/%s/repositories/%s/artifacts/%s/tags",
		url.PathEscape(projectName), doubleEscapeRepo(repoName), url.PathEscape(reference))
	resp, err := c.doJSON(ctx, http.MethodPost, path, map[string]string{"name": tag}, nil)
	if err != nil {
		return err
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusConflict {
		return fmt.Errorf("add tag %q returned %d: %s", tag, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func (c *HTTPClient) DeleteArtifactTag(ctx context.Context, projectName, repoName, reference, tag string) error {
	path := fmt.Sprintf("/api/v2.0/projects/%s/repositories/%s/artifacts/%s/tags/%s",
		url.PathEscape(projectName), doubleEscapeRepo(repoName), url.PathEscape(reference), url.PathEscape(tag))
	resp, err := c.doJSON(ctx, http.MethodDelete, path, nil, nil)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("delete tag %q returned %d", tag, resp.StatusCode)
	}
	return nil
}

func (c *HTTPClient) DeleteArtifact(ctx context.Context, projectName, repoName, reference string) error {
	path := fmt.Sprintf("/api/v2.0/projects/%s/repositories/%s/artifacts/%s",
		url.PathEscape(projectName), doubleEscapeRepo(repoName), url.PathEscape(reference))
	resp, err := c.doJSON(ctx, http.MethodDelete, path, nil, nil)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("delete artifact %q returned %d", reference, resp.StatusCode)
	}
	return nil
}

func (c *HTTPClient) doJSON(ctx context.Context, method, path string, body interface{}, headers map[string]string) (*http.Response, error) {
	var bodyReader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		bodyReader = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, bodyReader)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return c.httpClient.Do(req)
}

func doubleEscapeRepo(repo string) string {
	// Harbor v2 requires double URL-encoding if repository name contains '/'.
	if strings.Contains(repo, "/") {
		return url.PathEscape(url.PathEscape(repo))
	}
	return url.PathEscape(repo)
}

func sanitizeRobotSuffix(s string) string {
	s = strings.ToLower(s)
	s = strings.ReplaceAll(s, "/", "-")
	if len(s) > 40 {
		return s[:40]
	}
	return s
}
