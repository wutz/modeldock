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
	"context"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	modeldockv1alpha1 "github.com/wutz/modeldock/api/v1alpha1"
)

// InstanceConfig describes connection settings for a Harbor registry instance.
type InstanceConfig struct {
	Name            string
	Endpoint        string
	RegistryHost    string
	Username        string
	Password        string
	InsecureSkipTLS bool
	ProxyURL        string
}

// Resolver resolves Harbor InstanceConfig from a Kubernetes Secret in the system namespace
// (or falls back to environment variables for local development/testing).
type Resolver struct {
	Client          client.Reader
	SystemNamespace string
}

// Resolve loads Harbor connection details for instanceRef (defaults to "default-harbor").
func (r *Resolver) Resolve(ctx context.Context, instanceRef string) (*InstanceConfig, error) {
	if instanceRef == "" {
		instanceRef = "default-harbor"
	}
	sysNS := r.SystemNamespace
	if sysNS == "" {
		sysNS = modeldockv1alpha1.DefaultSystemNamespace
	}

	if r.Client != nil {
		var sec corev1.Secret
		err := r.Client.Get(ctx, types.NamespacedName{Namespace: sysNS, Name: instanceRef}, &sec)
		if err == nil {
			endpoint := string(sec.Data["endpoint"])
			if endpoint == "" {
				endpoint = string(sec.Data["url"])
			}
			insecure, _ := strconv.ParseBool(string(sec.Data["insecureSkipTLS"]))
			cfg := &InstanceConfig{
				Name:            instanceRef,
				Endpoint:        strings.TrimRight(endpoint, "/"),
				RegistryHost:    string(sec.Data["registryHost"]),
				Username:        string(sec.Data["username"]),
				Password:        string(sec.Data["password"]),
				InsecureSkipTLS: insecure,
				ProxyURL:        string(sec.Data["proxyURL"]),
			}
			if cfg.RegistryHost == "" {
				cfg.RegistryHost = hostFromURL(cfg.Endpoint)
			}
			return cfg, nil
		}
		if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("lookup harbor secret %s/%s: %w", sysNS, instanceRef, err)
		}
	}

	// Fallback to environment variables when Secret is not present (dev/test).
	endpoint := os.Getenv("MODELDOCK_HARBOR_ENDPOINT")
	if endpoint == "" {
		return nil, fmt.Errorf("harbor instance %q not found in namespace %q and MODELDOCK_HARBOR_ENDPOINT is unset", instanceRef, sysNS)
	}
	insecure, _ := strconv.ParseBool(os.Getenv("MODELDOCK_HARBOR_INSECURE"))
	return &InstanceConfig{
		Name:            instanceRef,
		Endpoint:        strings.TrimRight(endpoint, "/"),
		RegistryHost:    hostFromURL(endpoint),
		Username:        os.Getenv("MODELDOCK_HARBOR_USERNAME"),
		Password:        os.Getenv("MODELDOCK_HARBOR_PASSWORD"),
		InsecureSkipTLS: insecure,
		ProxyURL:        os.Getenv("MODELDOCK_DRAGONFLY_PROXY"),
	}, nil
}

// NewHarborClient builds a Harbor v2 REST Client from InstanceConfig.
func (cfg *InstanceConfig) NewHarborClient() (Client, error) {
	return NewHTTPClient(cfg.Endpoint, cfg.Username, cfg.Password, cfg.InsecureSkipTLS)
}

func hostFromURL(raw string) string {
	if raw == "" {
		return ""
	}
	if !strings.HasPrefix(raw, "http://") && !strings.HasPrefix(raw, "https://") {
		return strings.Split(raw, "/")[0]
	}
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	return u.Host
}
