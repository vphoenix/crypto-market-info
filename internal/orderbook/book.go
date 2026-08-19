package orderbook

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
)

type State string

const (
	StateConnecting State = "connecting"
	StateValid      State = "valid"
	StateInvalid    State = "invalid"
	StateResyncing  State = "resyncing"
)

type ChangeSet struct {
	SourceTime time.Time
	Sequence   int64
	Bids       []model.Level
	Asks       []model.Level
}

type View struct {
	State      State
	Reason     string
	Sequence   int64
	SourceTime time.Time
	BidLevels  int
	AskLevels  int
}

type Book struct {
	mu              sync.RWMutex
	instrumentID    uint32
	retainedDepth   int
	state           State
	reason          string
	sequence        int64
	sourceTime      time.Time
	bids            map[int64]uint64
	asks            map[int64]uint64
	bidSideComplete bool
	askSideComplete bool
}

func New(instrumentID uint32, retainedDepth int) (*Book, error) {
	if instrumentID == 0 || retainedDepth < model.BookDepth {
		return nil, fmt.Errorf("order book requires an instrument and retained depth >= %d", model.BookDepth)
	}
	return &Book{
		instrumentID:  instrumentID,
		retainedDepth: retainedDepth,
		state:         StateConnecting,
		bids:          make(map[int64]uint64),
		asks:          make(map[int64]uint64),
	}, nil
}

func (b *Book) ApplySnapshot(snapshot model.BookSnapshot) error {
	if b == nil {
		return fmt.Errorf("order book is nil")
	}
	if snapshot.InstrumentID != b.instrumentID {
		return fmt.Errorf("snapshot instrument mismatch")
	}
	if err := snapshot.Validate(b.retainedDepth); err != nil {
		return err
	}
	bids := levelsToMap(snapshot.Bids)
	asks := levelsToMap(snapshot.Asks)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.bids, b.asks = bids, asks
	b.bidSideComplete = len(snapshot.Bids) < b.retainedDepth
	b.askSideComplete = len(snapshot.Asks) < b.retainedDepth
	b.sequence = snapshot.Sequence
	b.sourceTime = snapshot.SourceTime.UTC()
	b.state = StateValid
	b.reason = ""
	return nil
}

// ApplyChanges atomically applies absolute quantities. Quantity zero deletes a price.
// expectedSequence makes the exchange adapter's continuity decision race-free.
func (b *Book) ApplyChanges(expectedSequence int64, changes ChangeSet) error {
	if b == nil {
		return fmt.Errorf("order book is nil")
	}
	if changes.SourceTime.IsZero() || changes.Sequence <= expectedSequence {
		return fmt.Errorf("changes require source time and an increasing sequence")
	}
	if err := validateChanges(changes.Bids, "bids"); err != nil {
		return err
	}
	if err := validateChanges(changes.Asks, "asks"); err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.state != StateValid {
		return fmt.Errorf("order book is not valid")
	}
	if b.sequence != expectedSequence {
		return fmt.Errorf("order book sequence changed: have %d want %d", b.sequence, expectedSequence)
	}
	oldBids := capture(b.bids, changes.Bids)
	oldAsks := capture(b.asks, changes.Asks)
	apply(b.bids, changes.Bids)
	apply(b.asks, changes.Asks)
	if crossed(b.bids, b.asks) {
		restore(b.bids, oldBids)
		restore(b.asks, oldAsks)
		return fmt.Errorf("changes would lock or cross the order book")
	}
	if trim(b.bids, true, b.retainedDepth) {
		b.bidSideComplete = false
	}
	if trim(b.asks, false, b.retainedDepth) {
		b.askSideComplete = false
	}
	if (len(b.bids) < model.BookDepth && !b.bidSideComplete) || (len(b.asks) < model.BookDepth && !b.askSideComplete) {
		b.state = StateInvalid
		b.reason = "insufficient retained depth"
		return nil
	}
	b.sequence = changes.Sequence
	b.sourceTime = changes.SourceTime.UTC()
	return nil
}

func (b *Book) MarkInvalid(reason string) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state = StateInvalid
	b.reason = reason
}

func (b *Book) MarkResyncing(reason string) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.state = StateResyncing
	b.reason = reason
}

func (b *Book) Snapshot(depth int) (model.BookSnapshot, bool) {
	if b == nil || depth <= 0 || depth > b.retainedDepth {
		return model.BookSnapshot{}, false
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.state != StateValid || len(b.bids) == 0 || len(b.asks) == 0 {
		return model.BookSnapshot{}, false
	}
	return model.BookSnapshot{
		InstrumentID: b.instrumentID,
		SourceTime:   b.sourceTime,
		Sequence:     b.sequence,
		Bids:         sorted(b.bids, true, depth),
		Asks:         sorted(b.asks, false, depth),
	}, true
}

func (b *Book) View() View {
	if b == nil {
		return View{}
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	return View{State: b.state, Reason: b.reason, Sequence: b.sequence, SourceTime: b.sourceTime, BidLevels: len(b.bids), AskLevels: len(b.asks)}
}

func (b *Book) InstrumentID() uint32 {
	if b == nil {
		return 0
	}
	return b.instrumentID
}

func levelsToMap(levels []model.Level) map[int64]uint64 {
	out := make(map[int64]uint64, len(levels))
	for _, level := range levels {
		out[level.PriceTick] = level.QtyLot
	}
	return out
}

func validateChanges(levels []model.Level, side string) error {
	seen := make(map[int64]struct{}, len(levels))
	for index, level := range levels {
		if level.PriceTick <= 0 {
			return fmt.Errorf("%s change %d has non-positive price", side, index)
		}
		if _, exists := seen[level.PriceTick]; exists {
			return fmt.Errorf("%s contains duplicate price %d", side, level.PriceTick)
		}
		seen[level.PriceTick] = struct{}{}
	}
	return nil
}

type previousValue struct {
	price  int64
	qty    uint64
	exists bool
}

func capture(side map[int64]uint64, levels []model.Level) []previousValue {
	out := make([]previousValue, len(levels))
	for index, level := range levels {
		qty, exists := side[level.PriceTick]
		out[index] = previousValue{price: level.PriceTick, qty: qty, exists: exists}
	}
	return out
}

func restore(side map[int64]uint64, values []previousValue) {
	for _, value := range values {
		if value.exists {
			side[value.price] = value.qty
		} else {
			delete(side, value.price)
		}
	}
}

func apply(side map[int64]uint64, levels []model.Level) {
	for _, level := range levels {
		if level.QtyLot == 0 {
			delete(side, level.PriceTick)
		} else {
			side[level.PriceTick] = level.QtyLot
		}
	}
}

func crossed(bids, asks map[int64]uint64) bool {
	if len(bids) == 0 || len(asks) == 0 {
		return false
	}
	bestBid, bestAsk := int64(0), int64(^uint64(0)>>1)
	for price := range bids {
		if price > bestBid {
			bestBid = price
		}
	}
	for price := range asks {
		if price < bestAsk {
			bestAsk = price
		}
	}
	return bestBid >= bestAsk
}

func trim(side map[int64]uint64, bids bool, limit int) bool {
	if len(side) <= limit {
		return false
	}
	levels := sorted(side, bids, len(side))
	for _, level := range levels[limit:] {
		delete(side, level.PriceTick)
	}
	return true
}

func sorted(side map[int64]uint64, bids bool, limit int) []model.Level {
	prices := make([]int64, 0, len(side))
	for price := range side {
		prices = append(prices, price)
	}
	sort.Slice(prices, func(i, j int) bool {
		if bids {
			return prices[i] > prices[j]
		}
		return prices[i] < prices[j]
	})
	if len(prices) > limit {
		prices = prices[:limit]
	}
	levels := make([]model.Level, len(prices))
	for index, price := range prices {
		levels[index] = model.Level{PriceTick: price, QtyLot: side[price]}
	}
	return levels
}
