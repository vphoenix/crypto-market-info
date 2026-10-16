package mysql

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/replay"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

func wideCEXMinute(t *testing.T) model.CompletedMinute {
	t.Helper()
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	out := model.CompletedMinute{MinuteTime: at}
	for id := uint32(1); id <= 20; id++ {
		minuteID, err := model.MinuteID(id, at)
		if err != nil {
			t.Fatal(err)
		}
		b := model.MinuteBatch{Minute: model.MinuteBook{ID: minuteID, InstrumentID: id, MinuteTime: at, StoredDepth: 50, ValidBitmap: (1<<60 - 1) &^ (1 << 17)}}
		for n := range 50 {
			b.Minute.Bids[n] = model.Level{PriceTick: 100000 - int64(n), QtyLot: uint64(n + 1)}
			b.Minute.Asks[n] = model.Level{PriceTick: 100001 + int64(n), QtyLot: uint64(n + 1)}
		}
		previous := 0
		for sec := 1; sec < 60; sec++ {
			if sec == 17 {
				continue
			}
			d := model.BookDelta{MinuteID: minuteID, SecondOffset: uint8(sec)}
			for n := range 50 {
				d.BidChangePrice = append(d.BidChangePrice, 100000+int64(previous)*1000-int64(n), 100000+int64(sec)*1000-int64(n))
				d.AskChangePrice = append(d.AskChangePrice, 100001+int64(previous)*1000+int64(n), 100001+int64(sec)*1000+int64(n))
				d.BidChangeQty = append(d.BidChangeQty, 0, uint64(n+sec+1))
				d.AskChangeQty = append(d.AskChangeQty, 0, uint64(n+sec+1))
			}
			b.Deltas = append(b.Deltas, d)
			previous = sec
		}
		out.Batches = append(out.Batches, b)
	}

	return out
}

func TestLargeCEXPreparationUsesBoundedUnlinkedChunks(t *testing.T) {
	c := &Client{database: "crypto_market_info", producer: Producer{Service: "collector", Generation: 1}}
	ctx := context.Background()
	ds, err := c.PrepareBookDeliveries(ctx, wideCEXMinute(t))
	if err != nil {
		t.Fatal(err)
	}
	d := ds[0]
	original := bytes.Clone(d.Payload)
	p, err := c.PrepareDeliveryBounded(ctx, d, 16<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if p.cex != nil || p.cexSpill == nil || p.cexSpill.chunks < 2 || len(p.Streams()) != 20 {
		t.Fatal("large batch not chunked")
	}
	if p.MemoryBytes() > 16<<20 {
		t.Fatal("working memory omitted", p.MemoryBytes())
	}
	if _, err := os.Stat(p.cexSpill.file.Name()); !os.IsNotExist(err) {
		t.Fatal("temporary artifact survives process crash", err)
	}
	if !bytes.Equal(original, d.Payload) {
		t.Fatal("durable payload modified")
	}
	t.Logf("wire=%d prepared_working_bytes=%d chunks=%d", len(d.Payload), p.MemoryBytes(), p.cexSpill.chunks)
	f := p.cexSpill.file
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Stat(); !errors.Is(err, os.ErrClosed) {
		t.Fatal("preparation handle not closed", err)
	}
}

func TestCEXPreparedChunksShareAtomicCommitAndReplay(t *testing.T) {
	c := integrationClient(t)
	c.prefetchMinuteIndexes = true
	ctx := context.Background()
	if err := c.InitDiscoverySchema(ctx); err != nil {
		t.Fatal(err)
	}
	all := wideCEXMinute(t)
	ds, err := c.PrepareBookDeliveries(ctx, all)
	if err != nil {
		t.Fatal(err)
	}
	d := ds[0]
	p, err := c.PrepareDeliveryBounded(ctx, d, 16<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	chunks := 0
	fault := context.WithValue(ctx, minuteFaultKey{}, func(stage string, _ *sql.Tx) error {
		if stage == "prepared_chunk" {
			chunks++
			if chunks == 2 {
				return context.DeadlineExceeded
			}
		}
		return nil
	})
	if err := c.CommitPrepared(fault, p); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("chunk rollback", err)
	}
	checkAbsent := func() {
		t.Helper()
		if ok, err := c.Committed(ctx, d); err != nil || ok {
			t.Fatal("partial commit visible", ok, err)
		}
		for _, name := range []string{"order_book_minute", "order_book_second_delta", "source_batch_status", "source_publication_intents"} {
			var count int
			if err := c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table(name)).Scan(&count); err != nil || count != 0 {
				t.Fatal("partial facts", name, count, err)
			}
		}
	}
	checkAbsent()
	// Damage only the disposable preparation. The original delivery is still
	// available and can be prepared again with its unchanged identity/hash.
	if _, err := p.cexSpill.file.WriteAt([]byte{0xff}, 40); err != nil {
		t.Fatal(err)
	}
	if err := c.CommitPrepared(ctx, p); !errors.Is(err, contracts.ErrConflict) {
		t.Fatal("corrupt preparation accepted", err)
	}
	checkAbsent()
	p.Close()
	p, err = c.PrepareDeliveryBounded(ctx, d, 16<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err = c.CommitPrepared(ctx, p); err != nil {
		t.Fatal(err)
	}
	if err = c.CommitPrepared(ctx, p); err != nil {
		t.Fatal("same identity retry", err)
	}
	for _, source := range all.Batches {
		stored, err := c.loadCommittedBook(ctx, source.Minute.InstrumentID, all.MinuteTime)
		if err != nil {
			t.Fatal(err)
		}
		for sec := uint8(0); sec < 60; sec++ {
			want, valid, err := replay.AtSecond(source.Minute, source.Deltas, sec)
			if err != nil {
				t.Fatal(err)
			}
			got, storedValid, err := replay.AtSecond(stored.Minute, stored.Deltas, sec)
			if err != nil || valid != storedValid || !reflect.DeepEqual(want, got) {
				t.Fatal("chunk changed replay", source.Minute.ID, sec, err)
			}
		}
	}
}
