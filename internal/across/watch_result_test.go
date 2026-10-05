package across

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"
)

func TestWatchShutdownPreservesFatalAfterWorkerCancellation(t *testing.T) {
	for _, text := range []string{"finalized_hash_conflict", "across_write_frozen_capture_test: failed", "non_cancellation_worker_failure"} {
		t.Run(text, func(t *testing.T) {
			c := &Collector{}
			errs := make(chan error, 2)
			errs <- context.Canceled
			want := errors.New(text)
			errs <- want
			if got := c.finishWatchResult(context.Canceled, errs); got != want {
				t.Fatalf("worker cancellation hid substantive failure: %v", got)
			}
		})
	}
}

func TestWatchShutdownPreservesConfirmedFaultSharedByClones(t *testing.T) {
	c, _ := priorityProbeCollector(t)
	clone := c.Readers[8453].RPC.Clone()
	clone.AuditFailures = true
	clone.HTTP.Transport = protocolTransport(func(*http.Request) (*http.Response, error) {
		return nil, &net.DNSError{Err: "no such host", Name: "example.invalid", IsNotFound: true}
	})
	if result := clone.One(context.Background(), "eth_chainId", nil); result.Err == nil || !strings.Contains(result.Err.Error(), "network_issue_stop") {
		t.Fatal(result.Err)
	}
	errs := make(chan error, 2)
	errs <- context.Canceled
	errs <- clone.ConfirmedNetworkFault()
	got := c.finishWatchResult(context.Canceled, errs)
	if got == nil || !strings.Contains(got.Error(), "network_issue_stop") || !strings.Contains(got.Error(), "dns_not_found") {
		t.Fatalf("shared confirmed fault lost on shutdown: %v", got)
	}
}
