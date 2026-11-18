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

package controller

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	modeldockv1alpha1 "github.com/wutz/modeldock/api/v1alpha1"
	"github.com/wutz/modeldock/pkg/harbor"
	"github.com/wutz/modeldock/pkg/harbor/harbortest"
	// +kubebuilder:scaffold:imports
)

var (
	ctx       context.Context
	cancel    context.CancelFunc
	testEnv   *envtest.Environment
	cfg       *rest.Config
	k8sClient client.Client
	harborSrv *harbortest.Server
)

func TestControllers(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Controller Suite")
}

var _ = BeforeSuite(func() {
	logf.SetLogger(zap.New(zap.WriteTo(GinkgoWriter), zap.UseDevMode(true)))

	ctx, cancel = context.WithCancel(context.TODO())

	Expect(modeldockv1alpha1.AddToScheme(scheme.Scheme)).To(Succeed())

	By("starting in-process Harbor + OCI + S3 redirect test server")
	harborSrv = harbortest.NewServer()

	By("bootstrapping envtest Kubernetes API server")
	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd", "bases")},
		ErrorIfCRDPathMissing: true,
	}
	if dir := getFirstFoundEnvTestBinaryDir(); dir != "" {
		testEnv.BinaryAssetsDirectory = dir
	}

	var err error
	cfg, err = testEnv.Start()
	Expect(err).NotTo(HaveOccurred())
	Expect(cfg).NotTo(BeNil())

	k8sClient, err = client.New(cfg, client.Options{Scheme: scheme.Scheme})
	Expect(err).NotTo(HaveOccurred())

	// Seed modeldock-system namespace and default-harbor Secret.
	Expect(k8sClient.Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: modeldockv1alpha1.DefaultSystemNamespace},
	})).To(Succeed())
	Expect(k8sClient.Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "default-harbor",
			Namespace: modeldockv1alpha1.DefaultSystemNamespace,
		},
		Data: map[string][]byte{
			"endpoint":        []byte(harborSrv.URL()),
			"registryHost":    []byte(harborSrv.RegistryHost()),
			"username":        []byte(harborSrv.AdminUser),
			"password":        []byte(harborSrv.AdminPass),
			"insecureSkipTLS": []byte("true"),
		},
	})).To(Succeed())

	// Start real controller-runtime manager with all 4 reconcilers.
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:  scheme.Scheme,
		Metrics: metricsserver.Options{BindAddress: "0"},
	})
	Expect(err).NotTo(HaveOccurred())

	opCfg := OperatorConfig{
		SystemNamespace: modeldockv1alpha1.DefaultSystemNamespace,
		ExporterImage:   "ghcr.io/wutz/modeldock-exporter:test",
		PullerImage:     "ghcr.io/wutz/modeldock-puller:test",
		DefaultCASPVC:   "modeldock-cas-shared",
		HarborResolver: &harbor.Resolver{
			Client:          mgr.GetAPIReader(),
			SystemNamespace: modeldockv1alpha1.DefaultSystemNamespace,
		},
	}

	Expect((&ModelRepositoryReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), Config: opCfg}).SetupWithManager(mgr)).To(Succeed())
	Expect((&ModelArtifactReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), Config: opCfg}).SetupWithManager(mgr)).To(Succeed())
	Expect((&ModelExportReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), Config: opCfg}).SetupWithManager(mgr)).To(Succeed())
	Expect((&ModelCacheReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(), Config: opCfg}).SetupWithManager(mgr)).To(Succeed())

	go func() {
		defer GinkgoRecover()
		Expect(mgr.Start(ctx)).To(Succeed())
	}()
})

var _ = AfterSuite(func() {
	By("tearing down the test environment")
	cancel()
	if harborSrv != nil {
		harborSrv.Close()
	}
	Expect(testEnv.Stop()).To(Succeed())
})

func getFirstFoundEnvTestBinaryDir() string {
	basePath := filepath.Join("..", "..", "bin", "k8s")
	entries, err := os.ReadDir(basePath)
	if err != nil {
		return ""
	}
	for _, entry := range entries {
		if entry.IsDir() {
			return filepath.Join(basePath, entry.Name())
		}
	}
	return ""
}
