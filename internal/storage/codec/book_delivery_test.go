package codec

import (
	"github.com/vphoenix/crypto-market-info/internal/model"
	"testing"
	"time"
)

func TestBookDeliveryKnowledgeVersionAndBounds(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	id, e := model.MinuteID(1, at)
	if e != nil {
		t.Fatal(e)
	}
	c := model.CompletedMinute{MinuteTime: at, Batches: []model.MinuteBatch{{Minute: model.MinuteBook{ID: id, InstrumentID: 1, MinuteTime: at, StoredDepth: 5, ValidBitmap: 1}}}}
	c.Batches[0].Minute.Bids[0] = model.Level{PriceTick: 100, QtyLot: 1}
	c.Batches[0].Minute.Asks[0] = model.Level{PriceTick: 101, QtyLot: 2}
	raw, e := EncodeBookDelivery(c, 1)
	if e != nil {
		t.Fatal(e)
	}
	got, e := DecodeBookDelivery(at, raw, 1)
	if e != nil || got.KnownFromMS != 0 {
		t.Fatal("v1 compatibility", e)
	}
	c.KnownFromMS = at.Add(65 * time.Second).UnixMilli()
	raw, e = EncodeBookDelivery(c, 2)
	if e != nil {
		t.Fatal(e)
	}
	got, e = DecodeBookDelivery(at, raw, 2)
	if e != nil || got.KnownFromMS != c.KnownFromMS {
		t.Fatal("v2 lost knowledge", e)
	}
	if _, e = DecodeBookDelivery(at, raw, 1); e == nil {
		t.Fatal("v2 decoded as v1")
	}
	if _, e = EncodeBookDelivery(c, 1); e == nil {
		t.Fatal("v1 silently dropped knowledge")
	}
	for _, known := range []int64{0, at.Add(59 * time.Second).UnixMilli(), time.Now().Add(time.Hour).UnixMilli()} {
		c.KnownFromMS = known
		if _, e = EncodeBookDelivery(c, 2); e == nil {
			t.Fatal("invalid knowledge accepted", known)
		}
	}
	if _, e = DecodeBookDelivery(at, raw[:len(raw)-1], 2); e == nil {
		t.Fatal("truncated v2 accepted")
	}
	if _, e = DecodeBookDelivery(at, raw, 3); e == nil {
		t.Fatal("unknown version accepted")
	}
}
