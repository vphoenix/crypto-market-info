package binance

import (
	"fmt"

	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/orderbook"
)

type Collector struct {
	book          *orderbook.Book
	futures       bool
	hasSnapshot   bool
	firstAccepted bool
	lastUpdateID  int64
}

func NewCollector(book *orderbook.Book, futures bool) (*Collector, error) {
	if book == nil {
		return nil, fmt.Errorf("Binance collector requires an order book")
	}
	return &Collector{book: book, futures: futures}, nil
}

func (c *Collector) Reset(reason string) {
	c.hasSnapshot, c.firstAccepted, c.lastUpdateID = false, false, 0
	c.book.MarkResyncing(reason)
}

func (c *Collector) ApplySnapshot(snapshot model.BookSnapshot) error {
	if err := c.book.ApplySnapshot(snapshot); err != nil {
		return err
	}
	c.hasSnapshot = true
	c.firstAccepted = false
	c.lastUpdateID = snapshot.Sequence
	return nil
}

func (c *Collector) Push(update DepthUpdate) error {
	if !c.hasSnapshot {
		return fmt.Errorf("Binance collector is awaiting snapshot")
	}
	if update.FinalUpdateID <= c.lastUpdateID {
		return nil
	}
	if !c.firstAccepted {
		bridgeID := c.lastUpdateID
		if !c.futures {
			bridgeID++
		}
		if update.FirstUpdateID > bridgeID || update.FinalUpdateID < bridgeID {
			c.book.MarkInvalid("Binance snapshot/diff bridge gap")
			return fmt.Errorf("Binance snapshot/diff bridge gap: snapshot=%d update=[%d,%d]", c.lastUpdateID, update.FirstUpdateID, update.FinalUpdateID)
		}
		c.firstAccepted = true
	} else if c.futures {
		if update.PreviousUpdateID != c.lastUpdateID {
			c.book.MarkInvalid("Binance pu sequence gap")
			return fmt.Errorf("Binance pu gap: have=%d update.pu=%d", c.lastUpdateID, update.PreviousUpdateID)
		}
	} else if update.FirstUpdateID > c.lastUpdateID+1 {
		c.book.MarkInvalid("Binance U/u sequence gap")
		return fmt.Errorf("Binance U/u gap: have=%d update.U=%d", c.lastUpdateID, update.FirstUpdateID)
	}
	if err := c.book.ApplyChanges(c.lastUpdateID, orderbook.ChangeSet{SourceTime: update.SourceTime, Sequence: update.FinalUpdateID, Bids: update.Bids, Asks: update.Asks}); err != nil {
		c.book.MarkInvalid(err.Error())
		return err
	}
	c.lastUpdateID = update.FinalUpdateID
	if c.book.View().State != orderbook.StateValid {
		return fmt.Errorf("Binance book lost retained depth")
	}
	return nil
}
