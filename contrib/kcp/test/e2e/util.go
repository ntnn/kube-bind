/*
Copyright 2025 The Kube Bind Authors.

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

package e2e

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/headzoo/surf"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/yaml"

	"github.com/kube-bind/kube-bind/test/e2e/framework"
)

func init() {
	// avoid warnings from controller-runtime about a missing logger
	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))
	log.SetLogger(klog.NewKlogr())
}

func applyFile(t testing.TB, kubeconfig, file string) {
	t.Helper()
	t.Logf("Applying file %q", file)

	// TODO: built a dynamic client to apply the files instead of using
	// kubectl

	cmd := exec.CommandContext(t.Context(), "kubectl", "apply", "-f", file)
	cmd.Env = append(os.Environ(), "KUBECONFIG="+kubeconfig)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "failed to apply file %q: %s", file, output)
}

func performBindingWithBrowser(t *testing.T, backendAddr string, clusterID string, consumerCfg *rest.Config, consumerKubeconfig string) {
	bindURL := fmt.Sprintf("http://%s/clusters/%s/exports", backendAddr, clusterID)
	t.Logf("Bind URL: %s", bindURL)

	// Test binding dry run first (similar to happy-case test)
	t.Run("Service is bound dry run", func(t *testing.T) {
		authURLDryRunCh := make(chan string, 1)
		go simulateKCPBrowser(t, authURLDryRunCh, "cowboys")

		iostreams, _, bufOut, _ := genericclioptions.NewTestIOStreams()
		framework.Bind(t, iostreams, authURLDryRunCh, nil, bindURL, "--kubeconfig", consumerKubeconfig, "--dry-run")
		_, err := yaml.YAMLToJSON(bufOut.Bytes())
		require.NoError(t, err, "Generated output is not valid YAML")
	})

	// Perform actual binding (similar to happy-case test)
	t.Run("Service is bound", func(t *testing.T) {
		authURLCh := make(chan string, 1)
		go simulateKCPBrowser(t, authURLCh, "cowboys")

		iostreams, _, _, _ := genericclioptions.NewTestIOStreams()
		invocations := make(chan framework.SubCommandInvocation, 1)
		framework.Bind(t, iostreams, authURLCh, invocations, bindURL, "--kubeconfig", consumerKubeconfig)
		inv := <-invocations

		inv.Args = append(
			inv.Args,
			"--kubeconfig="+consumerKubeconfig,
			"--skip-konnector=true",
			"--no-banner",
			"-f=-", // api service export from stdin
		)

		framework.BindAPIService(t, inv.Stdin, "", inv.Args...)

		// Wait for CRD to be created on consumer side
		t.Logf("Waiting for cowboy CRD to be created on consumer side")
		crdClient := framework.ApiextensionsClient(t, consumerCfg).ApiextensionsV1().CustomResourceDefinitions()
		require.Eventually(t, func() bool {
			_, err := crdClient.Get(context.Background(), "cowboys.wildwest.dev", metav1.GetOptions{})
			return err == nil
		}, wait.ForeverTestTimeout, time.Millisecond*100)
	})
}

// simulateKCPBrowser simulates browser interaction for KCP binding.
func simulateKCPBrowser(t *testing.T, authURLCh chan string, resource string) {
	browser := surf.NewBrowser()
	authURL := <-authURLCh

	t.Logf("Browsing to auth URL: %s", authURL)
	err := browser.Open(authURL)
	require.NoError(t, err, "Failed to open auth URL")

	t.Logf("Waiting for browser to be at /resources")
	framework.BrowserEventuallyAtPath(t, browser, "/resources")

	t.Logf("Clicking %s resource", resource)
	err = browser.Click("a." + resource)
	require.NoError(t, err, "Failed to click resource link")

	t.Logf("Waiting for browser to be forwarded to client")
	framework.BrowserEventuallyAtPath(t, browser, "/callback")
}

// toUnstructured converts YAML manifest to unstructured object (from happy-case test).
func toUnstructured(t *testing.T, manifest string) *unstructured.Unstructured {
	t.Helper()

	obj := map[string]any{}
	err := yaml.Unmarshal([]byte(manifest), &obj)
	require.NoError(t, err, "Failed to unmarshal YAML manifest")

	return &unstructured.Unstructured{Object: obj}
}
