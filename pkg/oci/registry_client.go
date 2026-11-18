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
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

var bearerChallengeRe = regexp.MustCompile(`([a-zA-Z0-9_]+)="([^"]*)"`)

// ClientOptions configures the OCI Distribution v1.1 registry client.
type ClientOptions struct {
	// RegistryURL is the base URL, e.g. "https://harbor.example.com" or "http://127.0.0.1:8080".
	RegistryURL string
	// Username for Basic / Harbor Robot auth.
	Username string
	// Password / Robot token.
	Password string
	// InsecureSkipTLS skips TLS certificate verification when true.
	InsecureSkipTLS bool
	// ProxyURL optionally routes blob downloads through Dragonfly P2P HTTP proxy.
	ProxyURL string
	// UploadChunkBytes controls chunk size for PATCH streaming uploads (default 16 MiB).
	UploadChunkBytes int64
	// HTTPClient optionally overrides the underlying HTTP client.
	HTTPClient *http.Client
}

// RegistryClient interacts with Harbor OCI v2 / Distribution v1.1 endpoints.
type RegistryClient struct {
	baseURL          string
	username         string
	password         string
	uploadChunkBytes int64
	httpClient       *http.Client
	blobClient       *http.Client // separate client when ProxyURL (Dragonfly P2P) is configured

	tokenMu sync.RWMutex
	tokens  map[string]cachedToken
}

type cachedToken struct {
	token     string
	expiresAt time.Time
}

// NewRegistryClient constructs a RegistryClient.
func NewRegistryClient(opts ClientOptions) (*RegistryClient, error) {
	if opts.RegistryURL == "" {
		return nil, errors.New("registry URL is required")
	}
	baseURL := strings.TrimRight(opts.RegistryURL, "/")
	if !strings.HasPrefix(baseURL, "http://") && !strings.HasPrefix(baseURL, "https://") {
		baseURL = "https://" + baseURL
	}
	chunkSize := opts.UploadChunkBytes
	if chunkSize <= 0 {
		chunkSize = 16 * 1024 * 1024
	}

	httpClient := opts.HTTPClient
	if httpClient == nil {
		tr := &http.Transport{
			MaxIdleConns:        64,
			MaxIdleConnsPerHost: 32,
			IdleConnTimeout:     90 * time.Second,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: opts.InsecureSkipTLS, //nolint:gosec
			},
		}
		httpClient = &http.Client{
			Transport: tr,
			// Handle redirects explicitly so Authorization header is never leaked to S3 presigned URLs.
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	}

	blobClient := httpClient
	if opts.ProxyURL != "" {
		proxyParsed, err := url.Parse(opts.ProxyURL)
		if err != nil {
			return nil, fmt.Errorf("invalid proxy URL %q: %w", opts.ProxyURL, err)
		}
		tr := &http.Transport{
			Proxy:               http.ProxyURL(proxyParsed),
			MaxIdleConns:        64,
			MaxIdleConnsPerHost: 32,
			IdleConnTimeout:     90 * time.Second,
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: opts.InsecureSkipTLS, //nolint:gosec
			},
		}
		blobClient = &http.Client{
			Transport: tr,
			CheckRedirect: func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	}

	return &RegistryClient{
		baseURL:          baseURL,
		username:         opts.Username,
		password:         opts.Password,
		uploadChunkBytes: chunkSize,
		httpClient:       httpClient,
		blobClient:       blobClient,
		tokens:           make(map[string]cachedToken),
	}, nil
}

// HeadBlob checks whether a blob already exists in Harbor (returns exists, size, err).
func (c *RegistryClient) HeadBlob(ctx context.Context, repository, digest string) (bool, int64, error) {
	u := fmt.Sprintf("%s/v2/%s/blobs/%s", c.baseURL, strings.Trim(repository, "/"), digest)
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, u, nil)
	if err != nil {
		return false, 0, err
	}
	resp, err := c.doAuthenticated(ctx, c.httpClient, req, fmt.Sprintf("repository:%s:pull", repository))
	if err != nil {
		return false, 0, err
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusOK:
		size, _ := strconv.ParseInt(resp.Header.Get("Content-Length"), 10, 64)
		return true, size, nil
	case http.StatusTemporaryRedirect, http.StatusFound:
		// Harbor S3 redirect may return 307 even on HEAD.
		return true, 0, nil
	case http.StatusNotFound:
		return false, 0, nil
	default:
		return false, 0, fmt.Errorf("HEAD blob %s unexpected status %d", digest, resp.StatusCode)
	}
}

// PushBlobBytes uploads an in-memory blob (such as ModelPackConfig or small config tar.zst).
func (c *RegistryClient) PushBlobBytes(ctx context.Context, repository, digest string, data []byte) error {
	exists, _, err := c.HeadBlob(ctx, repository, digest)
	if err == nil && exists {
		return nil
	}
	return c.PushBlobStream(ctx, repository, digest, int64(len(data)), bytes.NewReader(data))
}

// PushBlobStream performs a zero-copy chunked OCI blob upload (POST -> PATCH chunks -> PUT ?digest=...)
// directly from r without buffering the entire payload in memory or temp disk.
func (c *RegistryClient) PushBlobStream(
	ctx context.Context,
	repository, digest string,
	totalSize int64,
	r io.Reader,
) error {
	_, err := c.PushBlobStreamDynamic(ctx, repository, totalSize, r, digest)
	return err
}

// PushBlobStreamDynamic streams r in chunks via PATCH while computing SHA256 on the fly.
// If expectedDigest is non-empty it verifies the computed digest matches before committing PUT ?digest=...
func (c *RegistryClient) PushBlobStreamDynamic(
	ctx context.Context,
	repository string,
	totalSize int64,
	r io.Reader,
	expectedDigest string,
) (string, error) {
	repo := strings.Trim(repository, "/")
	scope := fmt.Sprintf("repository:%s:pull,push", repo)

	// 1. Initiate upload session (POST /v2/<repo>/blobs/uploads/)
	initURL := fmt.Sprintf("%s/v2/%s/blobs/uploads/", c.baseURL, repo)
	initReq, err := http.NewRequestWithContext(ctx, http.MethodPost, initURL, nil)
	if err != nil {
		return "", err
	}
	initResp, err := c.doAuthenticated(ctx, c.httpClient, initReq, scope)
	if err != nil {
		return "", fmt.Errorf("initiate blob upload: %w", err)
	}
	initBody, _ := io.ReadAll(io.LimitReader(initResp.Body, 4096))
	_ = initResp.Body.Close()

	if initResp.StatusCode != http.StatusAccepted && initResp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf(
			"initiate blob upload returned %d: %s",
			initResp.StatusCode, strings.TrimSpace(string(initBody)),
		)
	}
	if initResp.StatusCode == http.StatusCreated {
		return expectedDigest, nil
	}

	uploadLoc := resolveLocation(c.baseURL, initResp.Header.Get("Location"))
	if uploadLoc == "" {
		return "", errors.New("initiate blob upload missing Location header")
	}

	// 2. Stream chunks via PATCH with Content-Range while computing SHA256
	h := sha256.New()
	buf := make([]byte, c.uploadChunkBytes)
	var offset int64
	for {
		n, readErr := io.ReadFull(r, buf)
		if n > 0 {
			chunk := buf[:n]
			_, _ = h.Write(chunk)

			patchReq, err := http.NewRequestWithContext(ctx, http.MethodPatch, uploadLoc, bytes.NewReader(chunk))
			if err != nil {
				return "", err
			}
			patchReq.Header.Set("Content-Type", "application/octet-stream")
			patchReq.Header.Set("Content-Range", fmt.Sprintf("%d-%d", offset, offset+int64(n)-1))
			patchReq.ContentLength = int64(n)

			patchResp, err := c.doAuthenticated(ctx, c.httpClient, patchReq, scope)
			if err != nil {
				return "", fmt.Errorf("patch blob chunk at offset %d: %w", offset, err)
			}
			patchBody, _ := io.ReadAll(io.LimitReader(patchResp.Body, 4096))
			_ = patchResp.Body.Close()

			if patchResp.StatusCode != http.StatusAccepted && patchResp.StatusCode != http.StatusNoContent {
				return "", fmt.Errorf(
					"patch blob chunk returned %d: %s",
					patchResp.StatusCode, strings.TrimSpace(string(patchBody)),
				)
			}
			if nextLoc := patchResp.Header.Get("Location"); nextLoc != "" {
				uploadLoc = resolveLocation(c.baseURL, nextLoc)
			}
			offset += int64(n)
		}
		if readErr == io.EOF || readErr == io.ErrUnexpectedEOF {
			break
		}
		if readErr != nil {
			return "", fmt.Errorf("read upload stream: %w", readErr)
		}
	}

	if totalSize >= 0 && offset != totalSize {
		return "", fmt.Errorf("uploaded byte count %d does not match expected %d", offset, totalSize)
	}
	digest := "sha256:" + hex.EncodeToString(h.Sum(nil))
	if expectedDigest != "" && digest != expectedDigest {
		return "", fmt.Errorf("computed digest %s does not match expected %s", digest, expectedDigest)
	}

	// 3. Finalize upload with digest (PUT <location>&digest=sha256:...)
	sep := "?"
	if strings.Contains(uploadLoc, "?") {
		sep = "&"
	}
	commitURL := uploadLoc + sep + "digest=" + url.QueryEscape(digest)
	commitReq, err := http.NewRequestWithContext(ctx, http.MethodPut, commitURL, nil)
	if err != nil {
		return "", err
	}
	commitResp, err := c.doAuthenticated(ctx, c.httpClient, commitReq, scope)
	if err != nil {
		return "", fmt.Errorf("commit blob upload %s: %w", digest, err)
	}
	commitBody, _ := io.ReadAll(io.LimitReader(commitResp.Body, 4096))
	_ = commitResp.Body.Close()
	if commitResp.StatusCode != http.StatusCreated && commitResp.StatusCode != http.StatusOK {
		return "", fmt.Errorf(
			"commit blob upload returned %d: %s",
			commitResp.StatusCode, strings.TrimSpace(string(commitBody)),
		)
	}
	return digest, nil
}

// PutManifest pushes an OCI manifest at reference (tag or digest) and returns its canonical sha256 digest.
func (c *RegistryClient) PutManifest(
	ctx context.Context,
	repository, reference string,
	manifest *Manifest,
) (string, int64, error) {
	if manifest.SchemaVersion == 0 {
		manifest.SchemaVersion = 2
	}
	if manifest.MediaType == "" {
		manifest.MediaType = MediaTypeOCIManifest
	}
	raw, err := json.Marshal(manifest)
	if err != nil {
		return "", 0, err
	}
	sum := sha256.Sum256(raw)
	digest := "sha256:" + hex.EncodeToString(sum[:])

	repo := strings.Trim(repository, "/")
	u := fmt.Sprintf("%s/v2/%s/manifests/%s", c.baseURL, repo, reference)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, u, bytes.NewReader(raw))
	if err != nil {
		return "", 0, err
	}
	req.Header.Set("Content-Type", manifest.MediaType)

	resp, err := c.doAuthenticated(ctx, c.httpClient, req, fmt.Sprintf("repository:%s:pull,push", repo))
	if err != nil {
		return "", 0, fmt.Errorf("put manifest: %w", err)
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	_ = resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return "", 0, fmt.Errorf("put manifest returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if headerDigest := resp.Header.Get("Docker-Content-Digest"); headerDigest != "" {
		digest = headerDigest
	}
	return digest, int64(len(raw)), nil
}

// GetManifest retrieves and parses an OCI manifest by tag or digest, returning manifest and canonical digest.
func (c *RegistryClient) GetManifest(ctx context.Context, repository, reference string) (*Manifest, string, error) {
	repo := strings.Trim(repository, "/")
	u := fmt.Sprintf("%s/v2/%s/manifests/%s", c.baseURL, repo, reference)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, "", err
	}
	req.Header.Set("Accept", strings.Join([]string{
		MediaTypeOCIManifest,
		"application/vnd.docker.distribution.manifest.v2+json",
		"application/json",
	}, ", "))

	resp, err := c.doAuthenticated(ctx, c.httpClient, req, fmt.Sprintf("repository:%s:pull", repo))
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 16*1024*1024))
	if err != nil {
		return nil, "", err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf(
			"GET manifest %s returned %d: %s",
			reference, resp.StatusCode, strings.TrimSpace(string(body)),
		)
	}

	var m Manifest
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, "", fmt.Errorf("decode manifest: %w", err)
	}
	sum := sha256.Sum256(body)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	if hdr := resp.Header.Get("Docker-Content-Digest"); hdr != "" {
		digest = hdr
	}
	return &m, digest, nil
}

// GetBlobBytes downloads a small blob (such as ModelPackConfig) into memory and verifies its digest.
func (c *RegistryClient) GetBlobBytes(ctx context.Context, repository, digest string) ([]byte, error) {
	var buf bytes.Buffer
	if err := c.PullBlob(ctx, repository, digest, &buf, 0); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// PullBlob downloads a blob, following HTTP 307/302 S3 redirects safely (without forwarding Harbor credentials to S3),
// optionally routing through Dragonfly P2P HTTP proxy, and verifying the full sha256 digest.
func (c *RegistryClient) PullBlob(
	ctx context.Context,
	repository, digest string,
	dst io.Writer,
	rangeStart int64,
) error {
	repo := strings.Trim(repository, "/")
	u := fmt.Sprintf("%s/v2/%s/blobs/%s", c.baseURL, repo, digest)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	if rangeStart > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", rangeStart))
	}

	resp, err := c.doAuthenticated(ctx, c.blobClient, req, fmt.Sprintf("repository:%s:pull", repo))
	if err != nil {
		return err
	}

	// Follow up to 3 cross-origin redirects (e.g. Harbor HTTP 307 -> S3 presigned URL)
	// WITHOUT forwarding the Harbor Authorization header.
	redirects := 0
	for isRedirectStatus(resp.StatusCode) && redirects < 3 {
		loc := resp.Header.Get("Location")
		_ = resp.Body.Close()
		if loc == "" {
			return errors.New("redirect response missing Location header")
		}
		targetURL := resolveLocation(c.baseURL, loc)
		redirReq, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, nil)
		if err != nil {
			return err
		}
		if rangeStart > 0 {
			redirReq.Header.Set("Range", fmt.Sprintf("bytes=%d-", rangeStart))
		}
		resp, err = c.blobClient.Do(redirReq)
		if err != nil {
			return fmt.Errorf("follow blob redirect: %w", err)
		}
		redirects++
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("pull blob %s returned %d: %s", digest, resp.StatusCode, strings.TrimSpace(string(errBody)))
	}

	if rangeStart > 0 {
		_, err = io.Copy(dst, resp.Body)
		return err
	}

	h := sha256.New()
	if _, err := io.Copy(io.MultiWriter(dst, h), resp.Body); err != nil {
		return fmt.Errorf("stream blob %s: %w", digest, err)
	}
	got := "sha256:" + hex.EncodeToString(h.Sum(nil))
	if got != digest {
		return fmt.Errorf("blob digest mismatch: got %s, expected %s", got, digest)
	}
	return nil
}

func isRedirectStatus(code int) bool {
	return code == http.StatusTemporaryRedirect ||
		code == http.StatusFound ||
		code == http.StatusMovedPermanently ||
		code == http.StatusSeeOther
}

// GetModelPackConfig fetches and decodes the ModelPackConfig referenced by a manifest.
func (c *RegistryClient) GetModelPackConfig(
	ctx context.Context,
	repository string,
	manifest *Manifest,
) (*ModelPackConfig, error) {
	if manifest == nil || manifest.Config.Digest == "" {
		return nil, errors.New("manifest has no config digest")
	}
	raw, err := c.GetBlobBytes(ctx, repository, manifest.Config.Digest)
	if err != nil {
		return nil, err
	}
	var cfg ModelPackConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("decode ModelPackConfig: %w", err)
	}
	return &cfg, nil
}

func (c *RegistryClient) doAuthenticated(
	ctx context.Context,
	client *http.Client,
	req *http.Request,
	scope string,
) (*http.Response, error) {
	if tok := c.getCachedToken(scope); tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	} else if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	// Preserve body for 401 retry if small/seekable.
	var bodyBytes []byte
	if req.Body != nil && req.GetBody == nil {
		var err error
		bodyBytes, err = io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		_ = req.Body.Close()
		req.GetBody = func() (io.ReadCloser, error) {
			return io.NopCloser(bytes.NewReader(bodyBytes)), nil
		}
		req.Body = io.NopCloser(bytes.NewReader(bodyBytes))
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusUnauthorized {
		return resp, nil
	}

	challenge := resp.Header.Get("Www-Authenticate")
	if !strings.HasPrefix(strings.ToLower(challenge), "bearer ") {
		return resp, nil
	}
	_ = resp.Body.Close()

	tok, err := c.fetchBearerToken(ctx, challenge, scope)
	if err != nil {
		return nil, fmt.Errorf("bearer token exchange failed: %w", err)
	}

	retryReq := req.Clone(ctx)
	if req.GetBody != nil {
		retryReq.Body, err = req.GetBody()
		if err != nil {
			return nil, err
		}
	}
	retryReq.Header.Set("Authorization", "Bearer "+tok)
	return client.Do(retryReq)
}

func (c *RegistryClient) fetchBearerToken(ctx context.Context, challenge, defaultScope string) (string, error) {
	params := map[string]string{}
	for _, m := range bearerChallengeRe.FindAllStringSubmatch(challenge, -1) {
		if len(m) == 3 {
			params[m[1]] = m[2]
		}
	}
	realm := params["realm"]
	if realm == "" {
		return "", fmt.Errorf("bearer challenge missing realm: %q", challenge)
	}
	u, err := url.Parse(realm)
	if err != nil {
		return "", err
	}
	q := u.Query()
	if svc := params["service"]; svc != "" {
		q.Set("service", svc)
	}
	scope := params["scope"]
	if scope == "" {
		scope = defaultScope
	}
	if scope != "" {
		q.Set("scope", scope)
	}
	u.RawQuery = q.Encode()

	tokenReq, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return "", err
	}
	if c.username != "" {
		tokenReq.SetBasicAuth(c.username, c.password)
	}
	resp, err := c.httpClient.Do(tokenReq)
	if err != nil {
		return "", err
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("token endpoint returned %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var parsed struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return "", err
	}
	tok := parsed.Token
	if tok == "" {
		tok = parsed.AccessToken
	}
	if tok == "" {
		return "", errors.New("token response contained empty token")
	}
	ttl := time.Duration(parsed.ExpiresIn) * time.Second
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	c.tokenMu.Lock()
	c.tokens[scope] = cachedToken{
		token:     tok,
		expiresAt: time.Now().Add(ttl - 15*time.Second),
	}
	c.tokenMu.Unlock()
	return tok, nil
}

func (c *RegistryClient) getCachedToken(scope string) string {
	c.tokenMu.RLock()
	defer c.tokenMu.RUnlock()
	ct, ok := c.tokens[scope]
	if !ok || time.Now().After(ct.expiresAt) {
		return ""
	}
	return ct.token
}

func resolveLocation(baseURL, loc string) string {
	if loc == "" {
		return ""
	}
	if strings.HasPrefix(loc, "http://") || strings.HasPrefix(loc, "https://") {
		return loc
	}
	base, err := url.Parse(baseURL)
	if err != nil {
		return loc
	}
	ref, err := url.Parse(loc)
	if err != nil {
		return loc
	}
	return base.ResolveReference(ref).String()
}
