package ethereum

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/vphoenix/crypto-market-info/internal/dex"
)

func headerResult(number string) Result {
	raw, _ := json.Marshal(map[string]string{
		"number": number, "hash": dex.Digest([]byte("block")).String(),
		"parentHash": dex.Digest([]byte("parent")).String(), "timestamp": "0x1",
		"baseFeePerGas": "0x1", "miner": "0x1111111111111111111111111111111111111111",
	})
	return Result{Raw: raw}
}

func TestNullHeaderIsUnavailableAndNeverSuppliesAnAnchor(t *testing.T) {
	for _, method := range []string{"eth_getBlockByNumber", "eth_getBlockByHash"} {
		b, err := parseHeader(Result{Raw: json.RawMessage(" \nnull\t")}, method, "latest")
		if !errors.Is(err, ErrBlockUnavailable) || b.Hash != (dex.Hash{}) || b.Canonical {
			t.Fatalf("null supplied a usable block: %+v %v", b, err)
		}
	}
	for _, raw := range []string{`{}`, `{"number":"bad"}`, `[]`, `"null"`} {
		if _, err := parseHeader(Result{Raw: json.RawMessage(raw)}, "eth_getBlockByNumber", "latest"); err == nil || errors.Is(err, ErrBlockUnavailable) {
			t.Fatalf("malformed header classified as temporary absence: %s %v", raw, err)
		}
	}
	for _, field := range []string{"number", "timestamp", "baseFeePerGas"} {
		r := headerResult("0x7")
		var wire map[string]string
		_ = json.Unmarshal(r.Raw, &wire)
		wire[field] = ""
		r.Raw, _ = json.Marshal(wire)
		if _, err := parseHeader(r, "eth_getBlockByNumber", "latest"); err == nil || errors.Is(err, ErrBlockUnavailable) || !strings.Contains(err.Error(), "header_") {
			t.Fatalf("bad %s lost stage or became retryable: %v", field, err)
		}
	}
}

func TestHeaderRetryAndCanonicalChangeKeepDistinctIdentities(t *testing.T) {
	attempt := 0
	c := testClient(t, func(req *http.Request) (*http.Response, error) {
		var calls []struct{ ID uint64 }
		if err := json.NewDecoder(req.Body).Decode(&calls); err != nil {
			return nil, err
		}
		attempt++
		raw := "null"
		if attempt > 1 {
			raw = string(headerResult("0x7").Raw)
		}
		return response(fmt.Sprintf(`[{"jsonrpc":"2.0","id":%d,"result":%s}]`, calls[0].ID, raw)), nil
	})
	if _, err := c.Header(context.Background(), "0x7"); !errors.Is(err, ErrBlockUnavailable) {
		t.Fatal("null result not retryable", err)
	}
	b, err := c.Header(context.Background(), "0x7")
	if err != nil || b.Number != 7 || b.Payload == (dex.Hash{}) {
		t.Fatal("fresh header failed or lost source evidence", b, err)
	}
	if err := c.Canonical(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	b.Hash = dex.Digest([]byte("orphan"))
	if err := c.Canonical(context.Background(), b); !errors.Is(err, ErrBlockNoLongerCanonical) || errors.Is(err, ErrBlockUnavailable) {
		t.Fatal("reorg not distinguished from absent block", err)
	}
}
