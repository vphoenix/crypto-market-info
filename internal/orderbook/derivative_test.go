package orderbook

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

func TestDerivativeSequenceRecoveryAndResourceCap(t *testing.T) {
	b, _ := NewDerivative(1, false, 10)
	epoch := uuid.New()
	_ = b.Reset(epoch)
	at := time.Now().UTC()
	u := DerivativeUpdate{InstrumentID: 1, Snapshot: true, Epoch: epoch, ChangeID: 100, SourceTime: at, ReceivedAt: at,
		Bids: []DerivativeChangeLevel{{Action: DerivativeNew, Level: model.Level{PriceTick: 100, QtyLot: 1}}}, Asks: []DerivativeChangeLevel{{Action: DerivativeNew, Level: model.Level{PriceTick: 101, QtyLot: 2}}}}
	if err := b.Apply(u); err != nil {
		t.Fatal(err)
	}
	if err := b.Apply(u); err != nil {
		t.Fatal("identical duplicate", err)
	}
	conflict := u
	conflict.Bids = append([]DerivativeChangeLevel(nil), u.Bids...)
	conflict.Bids[0].QtyLot = 3
	if err := b.Apply(conflict); err == nil {
		t.Fatal("conflicting duplicate accepted")
	}
	if _, q := b.Current(); q.StreamValid {
		t.Fatal("invalid book remained usable")
	}
	if err := b.Apply(u); err == nil {
		t.Fatal("reused failed epoch")
	}
	old := epoch
	epoch = uuid.New()
	_ = b.Reset(epoch)
	u.Epoch = epoch
	if err := b.Apply(u); err != nil {
		t.Fatal(err)
	}
	stale := u
	stale.Epoch = old
	if err := b.Apply(stale); err == nil {
		t.Fatal("stale epoch accepted")
	}
	if _, q := b.Current(); !q.StreamValid {
		t.Fatal("retired queue poisoned replacement")
	}
	u.Snapshot = false
	u.PrevChangeID = 99
	u.ChangeID = 105
	if err := b.Apply(u); err == nil {
		t.Fatal("sequence gap accepted")
	}
	epoch = uuid.New()
	_ = b.Reset(epoch)
	u.Epoch = epoch
	u.Snapshot = true
	u.Bids = nil
	u.Asks = nil
	for p := int64(1); p <= 11; p++ {
		u.Bids = append(u.Bids, DerivativeChangeLevel{Action: DerivativeNew, Level: model.Level{PriceTick: p, QtyLot: 1}})
	}
	if err := b.Apply(u); err == nil {
		t.Fatal("over-depth snapshot silently trimmed")
	}
	if _, q := b.Current(); q.Reason != model.DerivativeResourceLimit {
		t.Fatal("resource failure not recorded")
	}
}
