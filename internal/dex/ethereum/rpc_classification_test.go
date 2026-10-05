package ethereum

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestRPCErrorClassificationAndLaunchEvidence(t *testing.T) {
	for _, tc := range []struct {
		code    int
		message string
		rate    bool
		name    string
	}{{-32016, "over rate limit", true, "rpc_rate_limited(code=-32016)"}, {-32601, "method not found", false, "rpc_method_not_found"}, {-32000, "execution reverted", false, "rpc_remote_error(code=-32000)"}} {
		c := testClient(t, func(*http.Request) (*http.Response, error) {
			return response(`[{"jsonrpc":"2.0","id":1,"error":{"code":` + fmt.Sprint(tc.code) + `,"message":"` + tc.message + `"}}]`), nil
		})
		r := c.One(context.Background(), "x", nil)
		if r.Err.Error() != tc.name || IsRateLimited(r.Err) != tc.rate {
			t.Fatal(r.Err)
		}
		raw, e := c.Archive.Get(r.Payload)
		var envelope map[string]json.RawMessage
		if e != nil || json.Unmarshal(raw, &envelope) != nil || envelope["StartedAt"] == nil || r.StartedAt.IsZero() {
			t.Fatal("missing real launch time")
		}
	}
	c := testClient(t, func(*http.Request) (*http.Response, error) {
		r := response("limited")
		r.StatusCode = 429
		r.Header.Set("Retry-After", "7")
		return r, nil
	})
	r := c.One(context.Background(), "x", nil)
	var h *HTTPError
	if !errors.As(r.Err, &h) || h.RetryAfter.Seconds() != 7 || !IsRateLimited(r.Err) {
		t.Fatal(r.Err)
	}
}
func TestRPCOperationBudgetIsDistinctFromTransportTimeout(t *testing.T) {
	c := testClient(t, func(req *http.Request) (*http.Response, error) {
		<-req.Context().Done()
		return nil, req.Context().Err()
	})
	c.AuditFailures = true
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	r := c.One(ctx, "x", nil)
	if r.Err == nil || r.Err.Error() != "rpc_operation_budget_exhausted" {
		t.Fatal(r.Err)
	}
	if c.ConfirmedNetworkFault() != nil {
		t.Fatal("local deadline incorrectly proved a network issue")
	}
	raw, err := c.Archive.Get(r.Payload)
	if err != nil || !strings.Contains(string(raw), "rpc_operation_budget_exhausted") {
		t.Fatal("missing distinct budget evidence", err)
	}
}
func TestAuditedTransportFailureDoesNotLeakURL(t *testing.T) {
	c := testClient(t, func(*http.Request) (*http.Response, error) {
		return nil, errors.New("URL credentials secret must not leak")
	})
	c.AuditFailures = true
	r := c.One(context.Background(), "x", nil)
	if r.Err.Error() != "rpc_transport_failure" {
		t.Fatal(r.Err)
	}
	raw, e := c.Archive.Get(r.Payload)
	if e != nil || strings.Contains(string(raw), "secret") || !strings.Contains(string(raw), "rpc_transport_failure") {
		t.Fatal("unsafe or missing failure evidence", e)
	}
}
