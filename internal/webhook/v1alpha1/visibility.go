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
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	modeldockv1alpha1 "github.com/wutz/modeldock/api/v1alpha1"
)

// ValidateCrossNamespaceModelReference enforces Private / WorkspaceShared / Public access rules
// when an object in callerNS references a ModelArtifact via ref (anti-confused-deputy guard).
func ValidateCrossNamespaceModelReference(
	ctx context.Context,
	c client.Reader,
	callerNS string,
	ref *modeldockv1alpha1.ModelArtifactReference,
) error {
	if ref == nil {
		return nil
	}
	targetNS := ref.Namespace
	if targetNS == "" {
		targetNS = callerNS
	}

	if c == nil {
		return nil
	}

	var targetArt modeldockv1alpha1.ModelArtifact
	if err := c.Get(ctx, types.NamespacedName{Namespace: targetNS, Name: ref.Name}, &targetArt); err != nil {
		return fmt.Errorf("baseModelRef %s/%s not found: %w", targetNS, ref.Name, err)
	}
	if ref.OCIDigest != "" && targetArt.Spec.OCIDigest != ref.OCIDigest {
		return fmt.Errorf("baseModelRef digest %s does not match artifact digest %s", ref.OCIDigest, targetArt.Spec.OCIDigest)
	}

	if targetNS == callerNS {
		return nil
	}

	var targetRepo modeldockv1alpha1.ModelRepository
	if err := c.Get(ctx, types.NamespacedName{Namespace: targetNS, Name: targetArt.Spec.RepositoryRef}, &targetRepo); err != nil {
		return fmt.Errorf("repository %s/%s for baseModelRef not found: %w", targetNS, targetArt.Spec.RepositoryRef, err)
	}

	switch targetRepo.Spec.Visibility {
	case modeldockv1alpha1.VisibilityPublic:
		return nil
	case modeldockv1alpha1.VisibilityWorkspaceShared:
		for _, allowed := range targetRepo.Spec.SharedNamespaces {
			if allowed == callerNS {
				return nil
			}
		}
		return fmt.Errorf("namespace %q is not in sharedNamespaces whitelist of repository %s/%s", callerNS, targetNS, targetRepo.Name)
	default:
		return fmt.Errorf("cross-namespace reference to %s/%s denied: repository %q visibility is Private", targetNS, ref.Name, targetRepo.Name)
	}
}
