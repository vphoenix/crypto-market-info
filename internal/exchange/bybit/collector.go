package bybit

import (
	"fmt"

	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/orderbook"
)

type Collector struct {
	book        *orderbook.Book
	hasSnapshot bool
	lastUpdate  int64
	lastCross   int64
}

func NewCollector(book *orderbook.Book) (*Collector, error) {
	if book == nil {
		return nil, fmt.Errorf("Bybit collector requires an order book")
	}
	return &Collector{book: book}, nil
}

func (c *Collector) Reset(reason string) {
	c.hasSnapshot = false
	c.lastUpdate = 0
	c.lastCross = 0
	c.book.MarkResyncing(reason)
}

func (c *Collector) Push(update DepthUpdate) error {
	if update.Snapshot {
		snapshot := model.BookSnapshot{InstrumentID: c.book.InstrumentID(), SourceTime: update.SourceTime, Sequence: update.UpdateID, Bids: update.Bids, Asks: update.Asks}
		if err := c.book.ApplySnapshot(snapshot); err != nil {
			c.book.MarkInvalid(err.Error())
			return err
		}
		c.hasSnapshot = true
		c.lastUpdate = update.UpdateID
		c.lastCross = update.CrossSequence
		return nil
	}
	if !c.hasSnapshot {
		c.book.MarkInvalid("Bybit delta arrived before snapshot")
		return fmt.Errorf("Bybit collector is awaiting snapshot")
	}
	if update.UpdateID != c.lastUpdate+1 {
		c.hasSnapshot = false
		c.book.MarkInvalid("Bybit u sequence gap")
		return fmt.Errorf("Bybit u sequence gap: have=%d update=%d", c.lastUpdate, update.UpdateID)
	}
	if update.CrossSequence < c.lastCross {
		c.hasSnapshot = false
		c.book.MarkInvalid("Bybit seq moved backwards")
		return fmt.Errorf("Bybit seq moved backwards: have=%d update=%d", c.lastCross, update.CrossSequence)
	}
	if err := c.book.ApplyChanges(c.lastUpdate, orderbook.ChangeSet{SourceTime: update.SourceTime, Sequence: update.UpdateID, Bids: update.Bids, Asks: update.Asks}); err != nil {
		c.hasSnapshot = false
		c.book.MarkInvalid(err.Error())
		return err
	}
	c.lastUpdate = update.UpdateID
	c.lastCross = update.CrossSequence
	if c.book.View().State != orderbook.StateValid {
		return fmt.Errorf("Bybit book lost retained depth")
	}
	return nil
}
