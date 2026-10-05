package reserve

import (
	"context"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
	"strconv"
	"strings"
	"sync"
)

// Headers may include hundreds of transaction hashes each. Five members keeps
// public-node response bodies below the timeout-prone twenty-header batches.
// The underlying client still permits at most two concurrent HTTP requests.
func (c *Collector) Headers(ctx context.Context, numbers []uint64) (map[uint64]dex.Block, error) {
	out := map[uint64]dex.Block{}
	var first error
	var mu sync.Mutex
	var wg sync.WaitGroup
	for start := 0; start < len(numbers); start += 5 {
		end := min(start+5, len(numbers))
		wg.Add(1)
		go func(part []uint64) {
			defer wg.Done()
			rows, e := c.RPC.Headers(ctx, part)
			mu.Lock()
			defer mu.Unlock()
			if e != nil {
				if first == nil {
					first = e
				}
				return
			}
			for n, b := range rows {
				b.Manifest = c.Manifest.Hash
				out[n] = b
			}
		}(numbers[start:end])
	}
	wg.Wait()
	if first != nil {
		return nil, first
	}
	return out, nil
}
func TransientRPC(e error) bool {
	if e == nil {
		return false
	}
	if ethereum.IsRateLimited(e) {
		return true
	}
	s := e.Error()
	switch s {
	case "rpc_transport_or_timeout", "rpc_transport_timeout", "rpc_transport_failure", "rpc_rate_limited", "rpc_source_wait_exceeds_deadline", "rpc_response_read_timeout", "rpc_response_read_failed", "rpc_response_truncated", "deadline_exceeded":
		return true
	}
	if strings.HasPrefix(s, "rpc_http_") {
		n, err := strconv.Atoi(strings.TrimPrefix(s, "rpc_http_"))
		return err == nil && (n == 429 || n >= 500 && n <= 599)
	}
	return false
}
