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

package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"net/http"
	"os"
	"time"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	modeldockv1alpha1 "github.com/wutz/modeldock/api/v1alpha1"
	"github.com/wutz/modeldock/internal/controller"
	"github.com/wutz/modeldock/internal/harborwebhook"
	"github.com/wutz/modeldock/internal/ui"
	webhookv1alpha1 "github.com/wutz/modeldock/internal/webhook/v1alpha1"
	"github.com/wutz/modeldock/pkg/harbor"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(modeldockv1alpha1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

// nolint:gocyclo
func main() {
	var metricsAddr string
	var metricsCertPath, metricsCertName, metricsCertKey string
	var webhookCertPath, webhookCertName, webhookCertKey string
	var webhookPort int
	var enableLeaderElection bool
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	var systemNamespace string
	var exporterImage string
	var pullerImage string
	var defaultCASPVC string
	var dragonflyProxyURL string
	var harborWebhookAddr string
	var harborWebhookSecret string
	var uiAddr string
	var uiRequireBearerToken bool
	var tlsOpts []func(*tls.Config)

	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager.")
	flag.BoolVar(&secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS.")
	flag.StringVar(&webhookCertPath, "webhook-cert-path", "", "The directory that contains the webhook certificate.")
	flag.StringVar(&webhookCertName, "webhook-cert-name", "tls.crt", "The name of the webhook certificate file.")
	flag.StringVar(&webhookCertKey, "webhook-cert-key", "tls.key", "The name of the webhook key file.")
	flag.IntVar(&webhookPort, "webhook-port", 9443, "Port the webhook server listens on. Set -1 to disable.")
	flag.StringVar(&metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	flag.StringVar(&metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	flag.StringVar(&metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	flag.BoolVar(&enableHTTP2, "enable-http2", false, "If set, HTTP/2 will be enabled for metrics and webhook servers.")
	flag.StringVar(&systemNamespace, "system-namespace", modeldockv1alpha1.DefaultSystemNamespace,
		"Namespace holding Harbor instance secrets.")
	flag.StringVar(&exporterImage, "exporter-image", "ghcr.io/wutz/modeldock-exporter:latest",
		"Container image for modeldock-exporter Jobs.")
	flag.StringVar(&pullerImage, "puller-image", "ghcr.io/wutz/modeldock-puller:latest",
		"Container image for modeldock-puller Jobs.")
	flag.StringVar(&defaultCASPVC, "default-cas-pvc", "modeldock-cas-shared",
		"Default RWX PVC for SharedFilesystem ModelCache.")
	flag.StringVar(&dragonflyProxyURL, "dragonfly-proxy-url", "",
		"Optional Dragonfly P2P HTTP proxy URL for ModelCache pulls.")
	flag.StringVar(&harborWebhookAddr, "harbor-webhook-bind-address", ":8090",
		"Bind address for Harbor event webhook receiver (0 to disable).")
	flag.StringVar(&harborWebhookSecret, "harbor-webhook-secret", os.Getenv("MODELDOCK_HARBOR_WEBHOOK_SECRET"),
		"Optional shared Bearer secret for Harbor webhooks.")
	flag.StringVar(&uiAddr, "ui-bind-address", ":8091",
		"Bind address for read-only embedded ModelDock Catalog UI (0 to disable).")
	flag.BoolVar(&uiRequireBearerToken, "ui-require-bearer-token", false,
		"Require caller K8s Bearer token for /api/v1/catalog.")

	opts := zap.Options{Development: true}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("Disabling HTTP/2")
		c.NextProtos = []string{"http/1.1"}
	}
	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	webhookServerOptions := webhook.Options{
		TLSOpts: tlsOpts,
		Port:    webhookPort,
	}
	if len(webhookCertPath) > 0 {
		webhookServerOptions.CertDir = webhookCertPath
		webhookServerOptions.CertName = webhookCertName
		webhookServerOptions.KeyName = webhookCertKey
	}
	webhookServer := webhook.NewServer(webhookServerOptions)

	metricsServerOptions := metricsserver.Options{
		BindAddress:   metricsAddr,
		SecureServing: secureMetrics,
		TLSOpts:       tlsOpts,
	}
	if secureMetrics {
		metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}
	if len(metricsCertPath) > 0 {
		metricsServerOptions.CertDir = metricsCertPath
		metricsServerOptions.CertName = metricsCertName
		metricsServerOptions.KeyName = metricsCertKey
	}

	restCfg := ctrl.GetConfigOrDie()
	mgr, err := ctrl.NewManager(restCfg, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsServerOptions,
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "84a690c1.wutz.dev",
	})
	if err != nil {
		setupLog.Error(err, "Failed to start manager")
		os.Exit(1)
	}

	opCfg := controller.OperatorConfig{
		SystemNamespace:   systemNamespace,
		ExporterImage:     exporterImage,
		PullerImage:       pullerImage,
		DefaultCASPVC:     defaultCASPVC,
		DragonflyProxyURL: dragonflyProxyURL,
		HarborResolver: &harbor.Resolver{
			Client:          mgr.GetAPIReader(),
			SystemNamespace: systemNamespace,
		},
	}

	if err := (&controller.ModelRepositoryReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Config: opCfg,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "modelrepository")
		os.Exit(1)
	}
	if err := (&controller.ModelArtifactReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Config: opCfg,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "modelartifact")
		os.Exit(1)
	}
	if err := (&controller.ModelExportReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Config: opCfg,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "modelexport")
		os.Exit(1)
	}
	if err := (&controller.ModelCacheReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
		Config: opCfg,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "modelcache")
		os.Exit(1)
	}

	if os.Getenv("ENABLE_WEBHOOKS") != "false" {
		if err := webhookv1alpha1.SetupModelRepositoryWebhookWithManager(mgr); err != nil {
			setupLog.Error(err, "Failed to create webhook", "webhook", "ModelRepository")
			os.Exit(1)
		}
		if err := webhookv1alpha1.SetupModelArtifactWebhookWithManager(mgr); err != nil {
			setupLog.Error(err, "Failed to create webhook", "webhook", "ModelArtifact")
			os.Exit(1)
		}
		if err := webhookv1alpha1.SetupModelExportWebhookWithManager(mgr); err != nil {
			setupLog.Error(err, "Failed to create webhook", "webhook", "ModelExport")
			os.Exit(1)
		}
	}
	// +kubebuilder:scaffold:builder

	if harborWebhookAddr != "" && harborWebhookAddr != "0" {
		mux := http.NewServeMux()
		mux.Handle("/webhooks/harbor", &harborwebhook.Handler{
			Client:       mgr.GetClient(),
			SharedSecret: harborWebhookSecret,
		})
		if err := addHTTPServerRunnable(mgr, "harbor-webhook", harborWebhookAddr, mux); err != nil {
			setupLog.Error(err, "Failed to register harbor webhook server")
			os.Exit(1)
		}
	}

	if uiAddr != "" && uiAddr != "0" {
		uiSrv := &ui.Server{
			FallbackReader:     mgr.GetClient(),
			RestConfig:         restCfg,
			Scheme:             mgr.GetScheme(),
			RequireBearerToken: uiRequireBearerToken,
		}
		if err := addHTTPServerRunnable(mgr, "catalog-ui", uiAddr, uiSrv.Handler()); err != nil {
			setupLog.Error(err, "Failed to register catalog UI server")
			os.Exit(1)
		}
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("Starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "Failed to run manager")
		os.Exit(1)
	}
}

func addHTTPServerRunnable(mgr ctrl.Manager, name, addr string, handler http.Handler) error {
	srv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}
	return mgr.Add(manager.RunnableFunc(func(ctx context.Context) error {
		errCh := make(chan error, 1)
		go func() {
			setupLog.Info("Starting HTTP server", "name", name, "addr", addr)
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				errCh <- err
			}
			close(errCh)
		}()
		select {
		case <-ctx.Done():
			shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			return srv.Shutdown(shutCtx)
		case err := <-errCh:
			return err
		}
	}))
}
