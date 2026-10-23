package main

import (
	"bytes"
	"context"
	"log"
	"net"
	"net/http"
	"strings"
	"testing"

	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

type diagnosticTransport func(*http.Request) (*http.Response, error)

func (f diagnosticTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestHashOnlyNetworkFailureKeepsSafeOperationalDiagnostic(t *testing.T) {
	rpc, err := ethereum.NewClient("https://user:secret@example.test/private-token", "")
	if err != nil {
		t.Fatal(err)
	}
	rpc.AuditFailures, rpc.Archive.HashOnly, rpc.RetryNetworkFailures = true, true, true
	rpc.Diagnostic = logRPCFailure
	rpc.HTTP.Transport = diagnosticTransport(func(*http.Request) (*http.Response, error) {
		return nil, &net.DNSError{Err: "secret resolver details", Name: "private-token", IsTemporary: true}
	})
	var output bytes.Buffer
	old := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(old) })
	result := rpc.One(context.Background(), "eth_chainId", nil)
	if result.Err == nil || result.Payload == (dex.Hash{}) || !ethereum.IsNetworkFailure(result.Err) {
		t.Fatal("failure lost classification or payload hash", result.Err)
	}
	text := output.String()
	if !strings.Contains(text, "source=example.test class=dns_error failure=rpc_transport_failure") {
		t.Fatal("hash-only source lost error category", text)
	}
	for _, secret := range []string{"secret", "user:", "private-token", "https://"} {
		if strings.Contains(text, secret) {
			t.Fatal("diagnostic exposed transport credentials")
		}
	}
}
