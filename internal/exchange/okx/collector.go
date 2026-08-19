package okx

import (
	"fmt"

	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/orderbook"
)

type Collector struct {
	book        *orderbook.Book
	hasSnapshot bool
	lastSeqID   int64
}

func NewCollector(book *orderbook.Book) (*Collector, error) {
	if book == nil {
		return nil, fmt.Errorf("OKX collector requires an order book")
	}
	return &Collector{book: book}, nil
}

func (c *Collector) Reset(reason string) {
	c.hasSnapshot = false
	c.lastSeqID = 0
	c.book.MarkResyncing(reason)
}

func (c *Collector) Push(update DepthUpdate) error {
	if update.Snapshot {
		snapshot := model.BookSnapshot{InstrumentID: c.bookInstrumentID(), SourceTime: update.SourceTime, Sequence: update.SeqID, Bids: update.Bids, Asks: update.Asks}
		if err := c.book.ApplySnapshot(snapshot); err != nil {
			return err
		}
		c.hasSnapshot = true
		c.lastSeqID = update.SeqID
		return nil
	}
	if !c.hasSnapshot {
		return fmt.Errorf("OKX collector is awaiting snapshot")
	}
	if update.SeqID < update.PreviousSeqID {
		c.hasSnapshot = false
		c.book.MarkInvalid("OKX sequence reset")
		return fmt.Errorf("OKX sequence reset: have=%d prev=%d seq=%d", c.lastSeqID, update.PreviousSeqID, update.SeqID)
	}
	if len(update.Bids) == 0 && len(update.Asks) == 0 && update.PreviousSeqID == c.lastSeqID && update.SeqID == c.lastSeqID {
		return nil
	}
	if update.SeqID <= c.lastSeqID {
		return nil
	}
	if update.PreviousSeqID != c.lastSeqID || update.SeqID <= update.PreviousSeqID {
		c.hasSnapshot = false
		c.book.MarkInvalid("OKX prevSeqId/seqId gap")
		return fmt.Errorf("OKX sequence gap: have=%d prev=%d seq=%d", c.lastSeqID, update.PreviousSeqID, update.SeqID)
	}
	if err := c.book.ApplyChanges(c.lastSeqID, orderbook.ChangeSet{SourceTime: update.SourceTime, Sequence: update.SeqID, Bids: update.Bids, Asks: update.Asks}); err != nil {
		c.book.MarkInvalid(err.Error())
		return err
	}
	c.lastSeqID = update.SeqID
	if c.book.View().State != orderbook.StateValid {
		return fmt.Errorf("OKX book lost retained depth")
	}
	return nil
}

func (c *Collector) bookInstrumentID() uint32 {
	snapshot, ok := c.book.Snapshot(1)
	if ok {
		return snapshot.InstrumentID
	}
	// Before a snapshot the ID is only held internally; expose it through the lightweight view helper.
	return c.book.InstrumentID()
}
