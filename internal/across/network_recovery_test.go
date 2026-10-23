package across

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestWatchDNSFailurePreservesGapAndRecoversWithoutRestart(t *testing.T) {
	m := runnerManifest(t)
	readers := map[uint64]*Reader{}
	for _, chain := range m.Chains {
		readers[chain.ChainID] = protocolChainReader(t, chain, nil)
	}
	r := readers[8453]
	r.RPC.AuditFailures = true
	transport := r.RPC.HTTP.Transport
	r.RPC.HTTP.Transport = protocolTransport(func(*http.Request) (*http.Response, error) {
		return nil, &net.DNSError{Err: "temporary resolver failure", IsTemporary: true}
	})
	store := &runnerProtocolStore{}
	c := &Collector{Manifest: m, Readers: readers, Store: store, Archive: r.RPC.Archive}
	if err := c.Watch(context.Background(), true, nil); err != nil {
		t.Fatalf("one source DNS failure stopped the healthy peer: %v", err)
	}
	if len(store.batches) == 0 {
		t.Fatal("healthy peer did not commit")
	}
	for _, b := range store.batches {
		if b.Capture.ChainId != 42161 || b.Capture.CompletedTasks != 1 {
			t.Fatal("DNS failure fabricated successful source coverage")
		}
	}
	// Reuse the same client and collector after the source recovers.
	r.RPC.HTTP.Transport = transport
	if err := c.Watch(context.Background(), true, nil); err != nil {
		t.Fatal(err)
	}
	if next := firstUncovered(100, 100, logIntervals(c.captures, 8453)); next != 101 {
		t.Fatal("recovered source could not complete its original range", next)
	}
}

func TestSourceQuotaNetworkBackoffPersistsAcrossRestart(t *testing.T) {
	m := runnerManifest(t)
	r := protocolChainReader(t, m.Chains[0], nil)
	r.RPC.AuditFailures = true
	r.RPC.HTTP.Transport = protocolTransport(func(*http.Request) (*http.Response, error) {
		return nil, &net.DNSError{Err: "temporary resolver failure", IsTemporary: true}
	})
	dir := t.TempDir()
	q, err := NewSourceQuota(dir, r.RPC.SourceID, time.Millisecond, 3)
	if err != nil {
		t.Fatal(err)
	}
	r.RPC.AfterRequest = q.After
	result := r.RPC.One(context.Background(), "eth_chainId", nil)
	if result.Err == nil {
		t.Fatal("DNS failure appeared successful")
	}
	for level := int64(1); level <= 5; level++ {
		if level > 1 {
			q.After(1, result.Err)
		}
		state := priorityQuotaState(t, q, nil)
		wantLevel := min(level, int64(4))
		if state[3] != wantLevel {
			t.Fatalf("network backoff level=%d, want %d", state[3], wantLevel)
		}
		want := time.Duration(1<<uint(wantLevel-1)) * 5 * time.Second
		if remaining := time.Until(time.Unix(0, state[1])); remaining < want-time.Second || remaining > want {
			t.Fatalf("backoff=%s, want approximately %s", remaining, want)
		}
		q, err = NewSourceQuota(dir, r.RPC.SourceID, time.Millisecond, 3)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		err = q.Before(ctx, 1)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) || priorityQuotaState(t, q, nil) != state {
			t.Fatal("restart bypassed cooldown or consumed unavailable quota", err)
		}
		q.After(1, nil)
		if priorityQuotaState(t, q, nil) != state {
			t.Fatal("an in-flight success erased network backoff")
		}
	}
	priorityQuotaState(t, q, func(s *[4]int64) { s[1] = time.Now().Add(-61 * time.Second).UnixNano() })
	q.After(1, nil)
	if priorityQuotaState(t, q, nil)[3] != 0 {
		t.Fatal("healthy source did not reset network backoff")
	}
}
