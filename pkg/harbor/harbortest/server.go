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

package harbortest

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wutz/modeldock/pkg/harbor"
	"github.com/wutz/modeldock/pkg/oci"
)

// Server is an in-process Harbor v2.10+ REST API, Token Service, OCI v1.1 Registry,
// and S3 HTTP 307 Redirect Object Storage emulator for unit and integration testing.
type Server struct {
	HarborServer *httptest.Server
	S3Server     *httptest.Server

	AdminUser string
	AdminPass string

	// EnableS3Redirect causes GET /v2/<repo>/blobs/<digest> to respond with HTTP 307 to S3Server.
	EnableS3Redirect bool

	BlobUploadCount  atomic.Int64
	BlobHeadHitCount atomic.Int64
	BlobS3PullCount  atomic.Int64

	mu        sync.RWMutex
	nextID    int64
	projects  map[string]*harbor.Project
	quotas    map[int64]*quotaEntry
	robots    map[int64]*robotEntry
	uploads   map[string]*uploadSession
	blobs     map[string][]byte
	manifests map[string]*manifestEntry // key: "<repo>@<digest>"
	tags      map[string]string         // key: "<repo>:<tag>" -> digest
}

type quotaEntry struct {
	ID           int64
	ProjectID    int64
	StorageLimit int64
}

type robotEntry struct {
	harbor.Robot
	ProjectName string
}

type uploadSession struct {
	ID   string
	Repo string
	Buf  bytes.Buffer
}

type manifestEntry struct {
	Repo      string
	Digest    string
	Raw       []byte
	MediaType string
	Manifest  oci.Manifest
	PushTime  time.Time
}

// NewServer starts a full Harbor + S3 Redirect test server pair.
func NewServer() *Server {
	s := &Server{
		AdminUser:        "admin",
		AdminPass:        "Harbor12345",
		EnableS3Redirect: true,
		nextID:           1,
		projects:         make(map[string]*harbor.Project),
		quotas:           make(map[int64]*quotaEntry),
		robots:           make(map[int64]*robotEntry),
		uploads:          make(map[string]*uploadSession),
		blobs:            make(map[string][]byte),
		manifests:        make(map[string]*manifestEntry),
		tags:             make(map[string]string),
	}

	s.S3Server = httptest.NewServer(http.HandlerFunc(s.handleS3))
	s.HarborServer = httptest.NewServer(http.HandlerFunc(s.handleHarbor))
	return s
}

// URL returns the Harbor server base URL.
func (s *Server) URL() string {
	return s.HarborServer.URL
}

// RegistryHost returns the host:port portion of the Harbor server URL.
func (s *Server) RegistryHost() string {
	u, _ := url.Parse(s.HarborServer.URL)
	return u.Host
}

// Close shuts down both Harbor and S3 test servers.
func (s *Server) Close() {
	s.HarborServer.Close()
	s.S3Server.Close()
}

// PutBlobForTest seeds a blob directly into Harbor's object storage.
func (s *Server) PutBlobForTest(data []byte) string {
	sum := sha256.Sum256(data)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	s.mu.Lock()
	s.blobs[digest] = append([]byte(nil), data...)
	s.mu.Unlock()
	return digest
}

func (s *Server) handleS3(w http.ResponseWriter, r *http.Request) {
	// Ensure Harbor Authorization headers are never forwarded to S3 presigned URLs.
	if r.Header.Get("Authorization") != "" {
		http.Error(w, "Authorization header must not be forwarded to S3 presigned URL", http.StatusBadRequest)
		return
	}
	if r.URL.Query().Get("X-Amz-Signature") == "" {
		http.Error(w, "missing presigned signature", http.StatusForbidden)
		return
	}
	digest := strings.TrimPrefix(r.URL.Path, "/s3-blobs/")
	s.mu.RLock()
	data, ok := s.blobs[digest]
	s.mu.RUnlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	s.BlobS3PullCount.Add(1)
	http.ServeContent(w, r, digest, time.Unix(0, 0), bytes.NewReader(data))
}

func (s *Server) handleHarbor(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	switch {
	case path == "/api/v2.0/ping":
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`"Pong"`))
	case path == "/service/token":
		s.handleToken(w, r)
	case strings.HasPrefix(path, "/api/v2.0/"):
		s.handleAPI(w, r)
	case path == "/v2/" || strings.HasPrefix(path, "/v2/"):
		s.handleOCI(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) authenticateBasic(r *http.Request) (string, *robotEntry, bool) {
	user, pass, ok := r.BasicAuth()
	if !ok {
		return "", nil, false
	}
	if user == s.AdminUser && pass == s.AdminPass {
		return user, nil, true
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, rb := range s.robots {
		if !rb.Disable && rb.Name == user && rb.Secret == pass {
			return user, rb, true
		}
	}
	return "", nil, false
}

func (s *Server) handleToken(w http.ResponseWriter, r *http.Request) {
	user, rb, ok := s.authenticateBasic(r)
	if !ok {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	scope := r.URL.Query().Get("scope")
	// Check robot permissions against requested scope if robot.
	if rb != nil && scope != "" {
		parts := strings.Split(scope, ":")
		if len(parts) == 3 && parts[0] == "repository" {
			repo := parts[1]
			actions := strings.Split(parts[2], ",")
			proj := strings.Split(repo, "/")[0]
			if !robotAllows(rb, proj, actions) {
				http.Error(w, "forbidden scope for robot", http.StatusForbidden)
				return
			}
		}
	}
	payload := fmt.Sprintf("%s|%s", user, scope)
	tok := base64.StdEncoding.EncodeToString([]byte(payload))
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"token":      tok,
		"expires_in": 900,
	})
}

func robotAllows(rb *robotEntry, project string, actions []string) bool {
	for _, perm := range rb.Permissions {
		if perm.Namespace != project {
			continue
		}
		for _, reqAction := range actions {
			allowed := false
			for _, acc := range perm.Access {
				if acc.Action == reqAction && (acc.Effect == "" || acc.Effect == "allow") {
					allowed = true
					break
				}
			}
			if !allowed {
				return false
			}
		}
		return true
	}
	return false
}

func (s *Server) authorizeOCI(w http.ResponseWriter, r *http.Request, repo, action string) bool {
	auth := r.Header.Get("Authorization")
	if strings.HasPrefix(auth, "Bearer ") {
		raw, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(auth, "Bearer "))
		if err == nil {
			parts := strings.SplitN(string(raw), "|", 2)
			if len(parts) == 2 {
				user := parts[0]
				if user == s.AdminUser {
					return true
				}
				s.mu.RLock()
				var matched *robotEntry
				for _, rb := range s.robots {
					if rb.Name == user && !rb.Disable {
						matched = rb
						break
					}
				}
				s.mu.RUnlock()
				if matched != nil {
					proj := strings.Split(repo, "/")[0]
					if robotAllows(matched, proj, []string{action}) {
						return true
					}
					http.Error(w, "insufficient scope", http.StatusForbidden)
					return false
				}
			}
		}
	}
	if _, rb, ok := s.authenticateBasic(r); ok {
		if rb == nil {
			return true
		}
		proj := strings.Split(repo, "/")[0]
		if robotAllows(rb, proj, []string{action}) {
			return true
		}
	}

	scope := ""
	if repo != "" {
		scope = fmt.Sprintf(`,scope="repository:%s:%s"`, repo, action)
	}
	w.Header().Set("Www-Authenticate", fmt.Sprintf(`Bearer realm="%s/service/token",service="harbor-registry"%s`, s.URL(), scope))
	http.Error(w, "unauthorized", http.StatusUnauthorized)
	return false
}

func (s *Server) handleAPI(w http.ResponseWriter, r *http.Request) {
	if user, _, ok := s.authenticateBasic(r); !ok || user != s.AdminUser {
		http.Error(w, "admin auth required", http.StatusUnauthorized)
		return
	}
	p := strings.TrimPrefix(r.URL.Path, "/api/v2.0")

	switch {
	case p == "/projects" && r.Method == http.MethodGet:
		nameFilter := r.URL.Query().Get("name")
		s.mu.RLock()
		var out []harbor.Project
		for _, proj := range s.projects {
			if nameFilter == "" || proj.Name == nameFilter {
				out = append(out, *proj)
			}
		}
		s.mu.RUnlock()
		writeJSON(w, http.StatusOK, out)

	case p == "/projects" && r.Method == http.MethodPost:
		var req struct {
			ProjectName  string            `json:"project_name"`
			Public       bool              `json:"public"`
			Metadata     map[string]string `json:"metadata"`
			StorageLimit int64             `json:"storage_limit"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		if _, exists := s.projects[req.ProjectName]; exists {
			s.mu.Unlock()
			http.Error(w, "project already exists", http.StatusConflict)
			return
		}
		s.nextID++
		pid := s.nextID
		s.projects[req.ProjectName] = &harbor.Project{
			ProjectID:    pid,
			Name:         req.ProjectName,
			Public:       req.Public,
			Metadata:     req.Metadata,
			StorageLimit: req.StorageLimit,
		}
		s.nextID++
		qid := s.nextID
		s.quotas[qid] = &quotaEntry{
			ID:           qid,
			ProjectID:    pid,
			StorageLimit: req.StorageLimit,
		}
		s.mu.Unlock()
		w.WriteHeader(http.StatusCreated)

	case p == "/quotas" && r.Method == http.MethodGet:
		refID, _ := strconv.ParseInt(r.URL.Query().Get("reference_id"), 10, 64)
		s.mu.RLock()
		var out []map[string]interface{}
		for _, q := range s.quotas {
			if refID == 0 || q.ProjectID == refID {
				out = append(out, map[string]interface{}{
					"id":           q.ID,
					"reference_id": q.ProjectID,
					"hard":         map[string]int64{"storage": q.StorageLimit},
				})
			}
		}
		s.mu.RUnlock()
		writeJSON(w, http.StatusOK, out)

	case strings.HasPrefix(p, "/quotas/") && r.Method == http.MethodPut:
		id, _ := strconv.ParseInt(strings.TrimPrefix(p, "/quotas/"), 10, 64)
		var req struct {
			Hard map[string]int64 `json:"hard"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		if q, ok := s.quotas[id]; ok {
			if v, has := req.Hard["storage"]; has {
				q.StorageLimit = v
			}
		}
		s.mu.Unlock()
		w.WriteHeader(http.StatusOK)

	case p == "/robots" && r.Method == http.MethodGet:
		s.mu.RLock()
		out := make([]harbor.Robot, 0, len(s.robots))
		for _, rb := range s.robots {
			copyRb := rb.Robot
			copyRb.Secret = "" // Harbor never returns secret on GET
			out = append(out, copyRb)
		}
		s.mu.RUnlock()
		writeJSON(w, http.StatusOK, out)

	case p == "/robots" && r.Method == http.MethodPost:
		var req harbor.RobotRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		projName := ""
		if len(req.Permissions) > 0 {
			projName = req.Permissions[0].Namespace
		}
		s.mu.Lock()
		s.nextID++
		rid := s.nextID
		fullName := fmt.Sprintf("robot$%s+%s", projName, req.Name)
		secret := fmt.Sprintf("tok-%d-%d", rid, time.Now().UnixNano())
		entry := &robotEntry{
			ProjectName: projName,
			Robot: harbor.Robot{
				ID:          rid,
				Name:        fullName,
				Secret:      secret,
				Level:       req.Level,
				Duration:    req.Duration,
				Permissions: req.Permissions,
			},
		}
		s.robots[rid] = entry
		s.mu.Unlock()
		writeJSON(w, http.StatusCreated, entry.Robot)

	case strings.HasPrefix(p, "/robots/") && r.Method == http.MethodDelete:
		id, _ := strconv.ParseInt(strings.TrimPrefix(p, "/robots/"), 10, 64)
		s.mu.Lock()
		delete(s.robots, id)
		s.mu.Unlock()
		w.WriteHeader(http.StatusOK)

	case strings.HasPrefix(p, "/projects/"):
		s.handleProjectArtifactsAPI(w, r, p)

	default:
		http.NotFound(w, r)
	}
}

func (s *Server) handleProjectArtifactsAPI(w http.ResponseWriter, r *http.Request, p string) {
	// /projects/{project}/repositories/{repo}/artifacts...
	trimmed := strings.TrimPrefix(p, "/projects/")
	parts := strings.Split(trimmed, "/")
	if len(parts) < 4 || parts[1] != "repositories" || parts[3] != "artifacts" {
		http.NotFound(w, r)
		return
	}
	project := parts[0]
	repoSeg, _ := url.PathUnescape(parts[2])
	fullRepo := project + "/" + repoSeg

	if len(parts) == 4 && r.Method == http.MethodGet {
		s.mu.RLock()
		var out []harbor.Artifact
		for _, m := range s.manifests {
			if m.Repo == fullRepo {
				out = append(out, s.toHarborArtifactLocked(m))
			}
		}
		s.mu.RUnlock()
		writeJSON(w, http.StatusOK, out)
		return
	}

	if len(parts) >= 5 {
		ref, _ := url.PathUnescape(parts[4])
		s.mu.Lock()
		defer s.mu.Unlock()

		digest := ref
		if !strings.HasPrefix(ref, "sha256:") {
			if d, ok := s.tags[fullRepo+":"+ref]; ok {
				digest = d
			}
		}
		m := s.manifests[fullRepo+"@"+digest]

		if len(parts) == 5 && r.Method == http.MethodGet {
			if m == nil {
				http.NotFound(w, r)
				return
			}
			writeJSON(w, http.StatusOK, s.toHarborArtifactLocked(m))
			return
		}
		if len(parts) == 5 && r.Method == http.MethodDelete {
			delete(s.manifests, fullRepo+"@"+digest)
			for tk, tv := range s.tags {
				if strings.HasPrefix(tk, fullRepo+":") && tv == digest {
					delete(s.tags, tk)
				}
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		if len(parts) == 6 && parts[5] == "tags" && r.Method == http.MethodPost {
			if m == nil {
				http.NotFound(w, r)
				return
			}
			var req struct {
				Name string `json:"name"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			s.tags[fullRepo+":"+req.Name] = digest
			w.WriteHeader(http.StatusCreated)
			return
		}
		if len(parts) == 7 && parts[5] == "tags" && r.Method == http.MethodDelete {
			tagName, _ := url.PathUnescape(parts[6])
			delete(s.tags, fullRepo+":"+tagName)
			w.WriteHeader(http.StatusOK)
			return
		}
	}
	http.NotFound(w, r)
}

func (s *Server) toHarborArtifactLocked(m *manifestEntry) harbor.Artifact {
	var total int64
	for _, l := range m.Manifest.Layers {
		total += l.Size
	}
	var tags []harbor.Tag
	prefix := m.Repo + ":"
	for k, d := range s.tags {
		if strings.HasPrefix(k, prefix) && d == m.Digest {
			tags = append(tags, harbor.Tag{
				Name:     strings.TrimPrefix(k, prefix),
				PushTime: m.PushTime,
			})
		}
	}
	return harbor.Artifact{
		Type:              "MODEL",
		MediaType:         m.Manifest.ArtifactType,
		ManifestMediaType: m.Manifest.MediaType,
		Digest:            m.Digest,
		Size:              total,
		PushTime:          m.PushTime,
		Tags:              tags,
	}
}

func (s *Server) handleOCI(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/v2/" {
		if !s.authorizeOCI(w, r, "", "pull") {
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	}

	rest := strings.TrimPrefix(r.URL.Path, "/v2/")
	// Match /v2/<repo>/blobs/... or /v2/<repo>/manifests/...
	if idx := strings.Index(rest, "/blobs/uploads"); idx != -1 {
		repo := rest[:idx]
		if !s.authorizeOCI(w, r, repo, "push") {
			return
		}
		s.handleBlobUpload(w, r, repo, rest[idx+len("/blobs/uploads"):])
		return
	}
	if idx := strings.Index(rest, "/blobs/"); idx != -1 {
		repo := rest[:idx]
		digest := rest[idx+len("/blobs/"):]
		if !s.authorizeOCI(w, r, repo, "pull") {
			return
		}
		s.handleBlobDownload(w, r, digest)
		return
	}
	if idx := strings.Index(rest, "/manifests/"); idx != -1 {
		repo := rest[:idx]
		ref := rest[idx+len("/manifests/"):]
		action := "pull"
		if r.Method == http.MethodPut || r.Method == http.MethodDelete {
			action = "push"
		}
		if !s.authorizeOCI(w, r, repo, action) {
			return
		}
		s.handleManifest(w, r, repo, ref)
		return
	}
	http.NotFound(w, r)
}

func (s *Server) handleBlobUpload(w http.ResponseWriter, r *http.Request, repo, sub string) {
	if r.Method == http.MethodPost && (sub == "" || sub == "/") {
		s.mu.Lock()
		s.nextID++
		uid := fmt.Sprintf("upload-%d", s.nextID)
		s.uploads[uid] = &uploadSession{ID: uid, Repo: repo}
		s.mu.Unlock()
		w.Header().Set("Location", fmt.Sprintf("/v2/%s/blobs/uploads/%s", repo, uid))
		w.Header().Set("Docker-Upload-UUID", uid)
		w.WriteHeader(http.StatusAccepted)
		return
	}
	uid := strings.TrimPrefix(sub, "/")
	s.mu.Lock()
	sess, ok := s.uploads[uid]
	s.mu.Unlock()
	if !ok {
		http.NotFound(w, r)
		return
	}

	if r.Method == http.MethodPatch {
		if _, err := io.Copy(&sess.Buf, r.Body); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Location", fmt.Sprintf("/v2/%s/blobs/uploads/%s", repo, uid))
		w.Header().Set("Range", fmt.Sprintf("0-%d", sess.Buf.Len()-1))
		w.WriteHeader(http.StatusAccepted)
		return
	}

	if r.Method == http.MethodPut {
		if r.Body != nil {
			_, _ = io.Copy(&sess.Buf, r.Body)
		}
		digest := r.URL.Query().Get("digest")
		data := append([]byte(nil), sess.Buf.Bytes()...)
		sum := sha256.Sum256(data)
		computed := "sha256:" + hex.EncodeToString(sum[:])
		if digest != "" && digest != computed {
			http.Error(w, fmt.Sprintf("digest mismatch: got %s want %s", computed, digest), http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.blobs[computed] = data
		delete(s.uploads, uid)
		s.mu.Unlock()
		s.BlobUploadCount.Add(1)
		w.Header().Set("Docker-Content-Digest", computed)
		w.WriteHeader(http.StatusCreated)
		return
	}
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
}

func (s *Server) handleBlobDownload(w http.ResponseWriter, r *http.Request, digest string) {
	s.mu.RLock()
	data, ok := s.blobs[digest]
	s.mu.RUnlock()
	if !ok {
		http.NotFound(w, r)
		return
	}
	if r.Method == http.MethodHead {
		s.BlobHeadHitCount.Add(1)
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.Header().Set("Docker-Content-Digest", digest)
		w.WriteHeader(http.StatusOK)
		return
	}
	if s.EnableS3Redirect {
		presigned := fmt.Sprintf("%s/s3-blobs/%s?X-Amz-Signature=mock-sig", s.S3Server.URL, url.PathEscape(digest))
		w.Header().Set("Location", presigned)
		w.WriteHeader(http.StatusTemporaryRedirect)
		return
	}
	w.Header().Set("Docker-Content-Digest", digest)
	http.ServeContent(w, r, digest, time.Unix(0, 0), bytes.NewReader(data))
}

func (s *Server) handleManifest(w http.ResponseWriter, r *http.Request, repo, ref string) {
	if r.Method == http.MethodPut {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var m oci.Manifest
		if err := json.Unmarshal(raw, &m); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		sum := sha256.Sum256(raw)
		digest := "sha256:" + hex.EncodeToString(sum[:])

		s.mu.Lock()
		s.manifests[repo+"@"+digest] = &manifestEntry{
			Repo:      repo,
			Digest:    digest,
			Raw:       raw,
			MediaType: m.MediaType,
			Manifest:  m,
			PushTime:  time.Now(),
		}
		if !strings.HasPrefix(ref, "sha256:") {
			s.tags[repo+":"+ref] = digest
		}
		s.mu.Unlock()

		w.Header().Set("Docker-Content-Digest", digest)
		w.WriteHeader(http.StatusCreated)
		return
	}

	if r.Method == http.MethodGet {
		s.mu.RLock()
		digest := ref
		if !strings.HasPrefix(ref, "sha256:") {
			digest = s.tags[repo+":"+ref]
		}
		m := s.manifests[repo+"@"+digest]
		s.mu.RUnlock()
		if m == nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", m.MediaType)
		w.Header().Set("Docker-Content-Digest", m.Digest)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(m.Raw)
		return
	}
	http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
}

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
