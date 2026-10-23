package justlendkeeper

import (
	"fmt"
	"runtime"
	"testing"
	"time"
)

func BenchmarkDynamicSelection(b *testing.B) {
	for _, n := range []int{1226, 10000, 100000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			now := UTC(time.Now())
			cfg := DefaultConfig()
			s := NewState("benchmark", cfg)
			c := NewCollector(cfg, &s, "", Archive{}, nil)
			c.State.CallersValid = []string{"A01234567890123456789"}
			c.State.Implementation, _ = HexAddress(ImplementationHex)
			c.State.CodeHash = mustHex(ImplementationSHA)
			c.State.IdentityAt = now.Add(-time.Minute)
			c.State.Dynamic = &dynamicWork{Version: 1, Ready: true}
			c.State.Dynamic.Rate = dynamicRateFixture(c, now)
			for i := 0; i < n; i++ {
				addDynamicFixture(c, i, now, true)
			}
			runtime.GC()
			var mem runtime.MemStats
			runtime.ReadMemStats(&mem)
			b.ReportMetric(float64(mem.HeapAlloc), "heap_bytes")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, ok := c.chooseDynamic("focus", now); !ok {
					b.Fatal("no focus")
				}
				if _, ok := c.chooseDynamic("explore", now); !ok {
					b.Fatal("no explorer")
				}
			}
		})
	}
}
func TestDynamicCheckpointWritesOnlyChangedCheckPages(t *testing.T) {
	c, now := dynamicFixture(t)
	w := c.State.Dynamic
	c.State.Dynamic = nil
	keeperRuntimeFixture(t, c)
	c.State.Dynamic = w
	if err := w.open(c.runtime.files); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 1226; i++ {
		addDynamicFixture(c, i, *now, true)
	}
	if err := c.Save(); err != nil {
		t.Fatal(err)
	}
	orderRoot := w.OrderRoot
	cold := c.runtime.last.Candidates
	before := c.runtime.files.Metrics()
	o, _ := w.getOrder(dynamicPair(opportunityCandidate(1, *now, true)))
	key := dynamicPair(o.Candidate)
	for i := 0; i < 10; i++ {
		e := w.getCheck(key, o)
		e.LastAttempt = now.Add(time.Duration(i) * time.Second)
		e.FairSince = e.LastAttempt
		w.setCheck(key, e)
		if err := c.Save(); err != nil {
			t.Fatal(err)
		}
	}
	after := c.runtime.files.Metrics()
	if c.runtime.last.Candidates != cold {
		t.Fatal("hot checks rewrote legacy cold candidate object")
	}
	for i, p := range orderRoot.Pages {
		if w.OrderRoot.Pages[i].Object != p.Object {
			t.Fatal("checks rewrote order summary pages")
		}
	}
	t.Logf("1226 orders, 10 check updates: bytes=%d file_fsync=%d dir_fsync=%d; cold/order rewrites=0", after.WrittenBytes-before.WrittenBytes, after.FileSyncs-before.FileSyncs, after.DirectorySyncs-before.DirectorySyncs)
	if err := c.Save(); err != nil {
		t.Fatal(err)
	}
	idle := c.runtime.files.Metrics()
	if idle.WrittenBytes != after.WrittenBytes {
		t.Fatal("idle write")
	}
}
