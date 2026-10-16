package testutil

import (
	"encoding/json"
	"runtime"
	"sort"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

// ScaleResult describes a local-files/in-memory-source experiment, not database
// throughput or production capacity. Compilation and conversion are outside the
// steady samples; all filesystem persistence settings remain unchanged.
type ScaleResult struct {
	Domain                  string
	HistoryRows             int
	Active                  uint64
	ConversionNS, RestartNS int64
	HotBytes                int64
	MedianNS, MinNS, MaxNS  int64
	Metrics                 runtimecheckpoint.Metrics
}

func MeasureScale(t testing.TB, r ScaleResult, query func() error) ScaleResult {
	t.Helper()
	// Exclude garbage from the synthetic in-memory history construction.
	runtime.GC()
	for i := 0; i < 100; i++ {
		if err := query(); err != nil {
			t.Fatal(err)
		}
	}
	const iterations = 200
	samples := make([]int64, 21)
	for i := range samples {
		start := time.Now()
		for j := 0; j < iterations; j++ {
			if err := query(); err != nil {
				t.Fatal(err)
			}
		}
		samples[i] = time.Since(start).Nanoseconds() / iterations
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	r.MinNS, r.MedianNS, r.MaxNS = samples[0], samples[len(samples)/2], samples[len(samples)-1]
	raw, _ := json.Marshal(r)
	t.Logf("RUNTIME_SCALE %s", raw)
	return r
}
func CompareScale(t testing.TB, a, b ScaleResult) {
	t.Helper()
	if a.Active != b.Active {
		t.Fatal("different active workload", a.Active, b.Active)
	}
	if b.HotBytes > 16<<20 || b.HotBytes > a.HotBytes+a.HotBytes/5+4096 {
		t.Fatal("hot state grew with completed history", a.HotBytes, b.HotBytes)
	}
	ratio := float64(b.MedianNS) / float64(a.MedianNS)
	// Timing is reported independently from deterministic correctness checks.
	// A noisy or failed performance target must not be presented as acceptance.
	t.Logf("RUNTIME_SCALE_TARGET domain=%s ratio=%.4f target_met=%t absolute_delta_ns=%d", a.Domain, ratio, ratio <= 1.2, b.MedianNS-a.MedianNS)
}
