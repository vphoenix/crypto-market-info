package okx

import (
	"testing"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/orderbook"
)

func TestParseSwapMetadataAndContractMultiplier(t *testing.T) {
	payload := []byte(`{"code":"0","msg":"","data":[{"instType":"SWAP","instId":"BTC-USDT-SWAP","state":"live","baseCcy":"","quoteCcy":"","settleCcy":"USDT","ctType":"linear","ctVal":"0.01","ctMult":"1","ctValCcy":"BTC","tickSz":"0.1","lotSz":"1"}]}`)
	got, err := ParseInstruments(payload, model.MarketPerpetual)
	if err != nil || len(got) != 1 {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	if !got[0].ContractMultiplier.Equal(decimal.RequireFromString("0.01")) {
		t.Fatalf("multiplier=%s", got[0].ContractMultiplier)
	}
}

func TestOKXSequenceGapFailClosedAndHeartbeat(t *testing.T) {
	instrument := okxInstrument()
	book, _ := orderbook.New(2, 400)
	collector, _ := NewCollector(book)
	snapshotPayload := []byte(`{"arg":{"channel":"books","instId":"BTC-USDT-SWAP"},"action":"snapshot","data":[{"asks":[["101","1","0","1"]],"bids":[["100","2","0","1"]],"ts":"1000","prevSeqId":-1,"seqId":10,"checksum":0}]}`)
	snapshot, err := ParseDepth(snapshotPayload, instrument)
	if err != nil {
		t.Fatal(err)
	}
	if err = collector.Push(snapshot); err != nil {
		t.Fatal(err)
	}
	heartbeat, _ := ParseDepth([]byte(`{"arg":{"channel":"books","instId":"BTC-USDT-SWAP"},"action":"update","data":[{"asks":[],"bids":[],"ts":"2000","prevSeqId":10,"seqId":10,"checksum":0}]}`), instrument)
	if err = collector.Push(heartbeat); err != nil {
		t.Fatal(err)
	}
	gap, _ := ParseDepth([]byte(`{"arg":{"channel":"books","instId":"BTC-USDT-SWAP"},"action":"update","data":[{"asks":[],"bids":[["100","3","0","1"]],"ts":"3000","prevSeqId":11,"seqId":12,"checksum":0}]}`), instrument)
	if err = collector.Push(gap); err == nil {
		t.Fatal("gap accepted")
	}
	if _, valid := book.Snapshot(50); valid {
		t.Fatal("gap left book valid")
	}
}

func TestOKXMaintenanceSequenceResetIsNotIgnoredAsStale(t *testing.T) {
	instrument := okxInstrument()
	book, _ := orderbook.New(instrument.ID, 400)
	collector, _ := NewCollector(book)
	snapshot, err := ParseDepth([]byte(`{"arg":{"channel":"books","instId":"BTC-USDT-SWAP"},"action":"snapshot","data":[{"asks":[["101","1","0","1"]],"bids":[["100","2","0","1"]],"ts":"1000","prevSeqId":-1,"seqId":100,"checksum":0}]}`), instrument)
	if err != nil {
		t.Fatal(err)
	}
	if err = collector.Push(snapshot); err != nil {
		t.Fatal(err)
	}
	reset, err := ParseDepth([]byte(`{"arg":{"channel":"books","instId":"BTC-USDT-SWAP"},"action":"update","data":[{"asks":[],"bids":[["100","3","0","1"]],"ts":"2000","prevSeqId":100,"seqId":1,"checksum":0}]}`), instrument)
	if err != nil {
		t.Fatal(err)
	}
	if err = collector.Push(reset); err == nil {
		t.Fatal("maintenance sequence reset was ignored")
	}
	if _, valid := book.Snapshot(50); valid {
		t.Fatal("sequence reset left stale book sampleable")
	}
	followup := reset
	followup.PreviousSeqID, followup.SeqID = 1, 2
	if err = collector.Push(followup); err == nil {
		t.Fatal("post-reset update applied without a new snapshot")
	}
}

func okxInstrument() model.Instrument {
	settle := "USDT"
	return model.Instrument{ID: 2, Exchange: "OKX", MarketType: model.MarketPerpetual, ExchangeSymbol: "BTC-USDT-SWAP", BaseAsset: "BTC", QuoteAsset: "USDT", SettleAsset: &settle, ContractMultiplier: decimal.RequireFromString("0.01"), PriceTickSize: decimal.RequireFromString("0.1"), QuantityStepSize: decimal.NewFromInt(1)}
}
