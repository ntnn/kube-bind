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
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"os/exec"
	"testing"
	"time"

	"github.com/gorilla/securecookie"
	kcpclientset "github.com/kcp-dev/kcp/sdk/client/clientset/versioned/cluster"
	kcptestinghelpers "github.com/kcp-dev/kcp/sdk/testing/helpers"
	kcptestingserver "github.com/kcp-dev/kcp/sdk/testing/server"
	"github.com/kcp-dev/logicalcluster/v3"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	"github.com/kube-bind/kube-bind/backend"
	"github.com/kube-bind/kube-bind/backend/options"
	"github.com/kube-bind/kube-bind/test/e2e/framework"
)

// This is just temporary as inprocess breaks probably due to dependency
// issues. Prebuilt works with the pinned forks.
const backendType = "prebuilt"

// startBackend is a copy of framework.StartBackend but skips the CRDs
// (which clashes with the APIResourceSchemas installed by kcp-init.
func startBackend(t *testing.T, args ...string) (string, *backend.Server) {
	signingKey := securecookie.GenerateRandomKey(32)
	require.NotEmpty(t, signingKey, "error creating signing key")
	encryptionKey := securecookie.GenerateRandomKey(32)
	require.NotEmpty(t, encryptionKey, "error creating encryption key")

	args = append(
		[]string{
			"--oidc-issuer-client-secret=ZXhhbXBsZS1hcHAtc2VjcmV0",
			"--oidc-issuer-client-id=kube-bind",
			"--oidc-issuer-url=http://127.0.0.1:5556/dex",
			"--cookie-signing-key=" + base64.StdEncoding.EncodeToString(signingKey),
			"--cookie-encryption-key=" + base64.StdEncoding.EncodeToString(encryptionKey),
		},
		args...,
	)

	switch backendType {
	case "prebuilt":
		addr := "127.0.0.1:8080"
		args = append(args,
			"--listen-address="+addr,
			"--oidc-callback-url=http://"+addr+"/callback",
		)
		backendCmd := exec.CommandContext(t.Context(),
			"../../../../bin/backend",
			args...,
		)
		backendCmd.Stdout = newLogWriter("[backend stdout] ", t)
		backendCmd.Stderr = newLogWriter("[backend stderr] ", t)
		require.NoError(t, backendCmd.Start())
		t.Cleanup(func() {
			if backendCmd.Process != nil {
				t.Logf("Stopping dex (PID: %d)", backendCmd.Process.Pid)
				assert.NoError(t, backendCmd.Process.Kill())
			}
		})
		return addr, nil
	case "inprocess":
		args = append(
			[]string{
				"--oidc-issuer-client-secret=ZXhhbXBsZS1hcHAtc2VjcmV0",
				"--oidc-issuer-client-id=kube-bind",
				"--oidc-issuer-url=http://127.0.0.1:5556/dex",
				"--cookie-signing-key=" + base64.StdEncoding.EncodeToString(signingKey),
				"--cookie-encryption-key=" + base64.StdEncoding.EncodeToString(encryptionKey),
			},
			args...,
		)

		fs := pflag.NewFlagSet("backend", pflag.ContinueOnError)
		opts := options.NewOptions()
		opts.AddFlags(fs)
		err := fs.Parse(args)
		require.NoError(t, err)

		t.Logf("starting backend with options: %#v", opts)

		// use a random port via an explicit listener. Then add a kube-bind-<port> client to dex
		// with the callback URL set to the listener's address.
		opts.Serve.Listener, err = net.Listen("tcp", "localhost:0")
		require.NoError(t, err)
		addr := opts.Serve.Listener.Addr()
		_, port, err := net.SplitHostPort(addr.String())
		require.NoError(t, err)

		opts.OIDC.IssuerClientID = "kube-bind-" + port
		framework.CreateDexClient(t, addr)

		opts.ExtraOptions.TestingSkipNameValidation = true
		opts.ExtraOptions.SchemaSource = options.CustomResourceDefinitionSource.String()

		completed, err := opts.Complete()
		require.NoError(t, err)

		config, err := backend.NewConfig(completed)
		require.NoError(t, err)

		server, err := backend.NewServer(t.Context(), config)
		require.NoError(t, err)

		err = server.Run(t.Context())
		require.NoError(t, err)
		t.Logf("backend listening on %s", addr)

		return addr.String(), server
	default:
		require.Fail(t, "unknown backend type %q", backendType)
		return "", nil
	}
}

func bootstrapBackend(t *testing.T, server kcptestingserver.RunningServer) string {
	t.Helper()
	t.Log("Bootstrapping backend")

	client, err := kcpclientset.NewForConfig(server.BaseConfig(t))
	require.NoError(t, err)

	exportUrl := ""
	kcptestinghelpers.Eventually(t, func() (bool, string) {
		exportES, err := client.Cluster(logicalcluster.NewPath("root").Join("kube-bind")).
			ApisV1alpha1().
			APIExportEndpointSlices().
			Get(t.Context(), "kube-bind.io", metav1.GetOptions{})
		if err != nil {
			return false, fmt.Sprintf("Error getting APIExportEndpointSlice: %v", err)
		}
		if len(exportES.Status.APIExportEndpoints) == 0 {
			return false, "APIExportEndpoints is empty"
		}
		exportUrl = exportES.Status.APIExportEndpoints[0].URL
		return true, ""
	}, wait.ForeverTestTimeout, time.Millisecond*100)
	require.NotEmpty(t, exportUrl, "APIExportEndpointSlice URL is empty")

	_, backendKubeconfig := wsConfig(t, server, logicalcluster.NewPath("root").Join("kube-bind"))

	t.Log("Starting kube-bind backend for KCP")
	addr, _ := startBackend(t,
		"--kubeconfig="+backendKubeconfig,
		"--multicluster-runtime-provider=kcp",
		"--server-url="+exportUrl,
		"--pretty-name=BigCorp.com",
		"--namespace-prefix=kube-bind-",
		"--schema-source=apiresourceschemas",
		"--consumer-scope=cluster", // TODO configure to test both modes
	)

	t.Log("Wait for backend to be ready")
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://"+addr+"/healthz", nil)
	require.NoError(t, err)
	kcptestinghelpers.Eventually(t, func() (bool, string) {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return false, ""
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusOK, ""
	}, wait.ForeverTestTimeout, time.Millisecond*100)
	t.Log("Backend is ready")

	return addr
}
