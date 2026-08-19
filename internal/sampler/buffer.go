package sampler

import (
	"fmt"
	"sort"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
)

type MinuteBuffer struct {
	instrumentID uint32
	minuteTime   time.Time
	minuteID     uint64
	anchored     bool
	sampled      uint64
	valid        uint64
	initial      model.BookSnapshot
	lastValid    model.BookSnapshot
	deltas       []model.BookDelta
}

func NewMinuteBuffer(instrumentID uint32, minuteTime time.Time) (*MinuteBuffer, error) {
	minuteTime = minuteTime.UTC()
	if !minuteTime.Equal(minuteTime.Truncate(time.Minute)) {
		return nil, fmt.Errorf("minute buffer time must be truncated to UTC minute")
	}
	id, err := model.MinuteID(instrumentID, minuteTime)
	if err != nil {
		return nil, err
	}
	return &MinuteBuffer{instrumentID: instrumentID, minuteTime: minuteTime, minuteID: id}, nil
}

func (b *MinuteBuffer) Sample(sampleTime time.Time, snapshot model.BookSnapshot, valid bool) error {
	if b == nil {
		return fmt.Errorf("minute buffer is nil")
	}
	sampleTime = sampleTime.UTC().Truncate(time.Second)
	if !sampleTime.Truncate(time.Minute).Equal(b.minuteTime) {
		return fmt.Errorf("sample is outside minute buffer")
	}
	offset := int(sampleTime.Second())
	bit := uint64(1) << offset
	if b.sampled&bit != 0 {
		return fmt.Errorf("second %d was already sampled", offset)
	}
	b.sampled |= bit
	if !valid {
		return nil
	}
	if snapshot.InstrumentID != b.instrumentID {
		return fmt.Errorf("sample instrument mismatch")
	}
	if err := snapshot.Validate(model.BookDepth); err != nil {
		return err
	}
	if offset == 0 {
		b.anchored = true
		b.initial = cloneSnapshot(snapshot)
		b.lastValid = cloneSnapshot(snapshot)
		b.valid |= bit
		return nil
	}
	if !b.anchored {
		return nil
	}
	delta := diff(b.minuteID, uint8(offset), b.lastValid, snapshot)
	if len(delta.BidChangePrice) > 0 || len(delta.AskChangePrice) > 0 {
		b.deltas = append(b.deltas, delta)
	}
	b.lastValid = cloneSnapshot(snapshot)
	b.valid |= bit
	return nil
}

func (b *MinuteBuffer) Batch() (model.MinuteBatch, bool) {
	if b == nil || !b.anchored {
		return model.MinuteBatch{}, false
	}
	minute := model.MinuteBook{ID: b.minuteID, InstrumentID: b.instrumentID, MinuteTime: b.minuteTime, ValidBitmap: b.valid}
	copy(minute.Bids[:], b.initial.Bids)
	copy(minute.Asks[:], b.initial.Asks)
	deltas := make([]model.BookDelta, len(b.deltas))
	copy(deltas, b.deltas)
	return model.MinuteBatch{Minute: minute, Deltas: deltas}, true
}

func diff(minuteID uint64, second uint8, previous, current model.BookSnapshot) model.BookDelta {
	return model.BookDelta{
		MinuteID: minuteID, SecondOffset: second,
		BidChangePrice: changedPrices(previous.Bids, current.Bids, true),
		AskChangePrice: changedPrices(previous.Asks, current.Asks, false),
		BidChangeQty:   changedQty(previous.Bids, current.Bids, true),
		AskChangeQty:   changedQty(previous.Asks, current.Asks, false),
	}
}

func changedPrices(previous, current []model.Level, bids bool) []int64 {
	prices, _ := changes(previous, current, bids)
	return prices
}

func changedQty(previous, current []model.Level, bids bool) []uint64 {
	_, quantities := changes(previous, current, bids)
	return quantities
}

func changes(previous, current []model.Level, bids bool) ([]int64, []uint64) {
	old := make(map[int64]uint64, len(previous))
	now := make(map[int64]uint64, len(current))
	for _, level := range previous {
		old[level.PriceTick] = level.QtyLot
	}
	for _, level := range current {
		now[level.PriceTick] = level.QtyLot
	}
	changed := make(map[int64]uint64)
	for price, qty := range now {
		if oldQty, exists := old[price]; !exists || oldQty != qty {
			changed[price] = qty
		}
	}
	for price := range old {
		if _, exists := now[price]; !exists {
			changed[price] = 0
		}
	}
	prices := make([]int64, 0, len(changed))
	for price := range changed {
		prices = append(prices, price)
	}
	sort.Slice(prices, func(i, j int) bool {
		if bids {
			return prices[i] > prices[j]
		}
		return prices[i] < prices[j]
	})
	qtys := make([]uint64, len(prices))
	for index, price := range prices {
		qtys[index] = changed[price]
	}
	return prices, qtys
}

func cloneSnapshot(in model.BookSnapshot) model.BookSnapshot {
	out := in
	out.Bids = append([]model.Level(nil), in.Bids...)
	out.Asks = append([]model.Level(nil), in.Asks...)
	return out
}
