package lst

import (
	"bytes"
	"math/big"
	"testing"
)

func TestPendingPreservesKnownZeroAndNull(t *testing.T) {
	b := lstBatchFixture()
	p := &b.Protocols[0]
	p.QueuePaused = Ptr(false)
	p.BunkerActive = Ptr(false)
	p.LastFinalizedRequestId = big.NewInt(0)
	p.LockedEthWei = big.NewInt(0)
	p.BaseFeePerGasWei = nil
	q := &b.Quotes[0]
	q.HedgeDepthLastUpdateId = Ptr(uint64(0))
	q.HedgeQuantityLot = Ptr(int64(0))
	if e := b.Seal(); e != nil {
		t.Fatal(e)
	}
	var buf bytes.Buffer
	if e := encodePersistent(&buf, b); e != nil {
		t.Fatal(e)
	}
	var actual Batch
	if e := decodePersistent(&buf, &actual); e != nil {
		t.Fatal(e)
	}
	if CanonicalHash(actual) != CanonicalHash(b) {
		t.Fatal("known zero, NULL or frozen identity changed")
	}
}
