package replay

import (
	"fmt"
	"sort"

	"github.com/vphoenix/crypto-market-info/internal/model"
)

func AtSecond(minute model.MinuteBook, deltas []model.BookDelta, second uint8) (model.BookSnapshot, bool, error) {
	if second > 59 {
		return model.BookSnapshot{}, false, fmt.Errorf("second offset must be between 0 and 59")
	}
	if minute.ValidBitmap&(uint64(1)<<second) == 0 {
		return model.BookSnapshot{}, false, nil
	}
	bids, asks := fixedSide(minute.Bids), fixedSide(minute.Asks)
	lastSecond := uint8(0)
	for _, delta := range deltas {
		if delta.MinuteID != minute.ID {
			return model.BookSnapshot{}, false, fmt.Errorf("delta minute_id mismatch")
		}
		if delta.SecondOffset == 0 || delta.SecondOffset > 59 || delta.SecondOffset <= lastSecond {
			return model.BookSnapshot{}, false, fmt.Errorf("deltas are not ordered by a valid second_offset")
		}
		lastSecond = delta.SecondOffset
		if delta.SecondOffset > second {
			break
		}
		if minute.ValidBitmap&(uint64(1)<<delta.SecondOffset) == 0 {
			return model.BookSnapshot{}, false, fmt.Errorf("delta exists for invalid second %d", delta.SecondOffset)
		}
		if len(delta.BidChangePrice) != len(delta.BidChangeQty) || len(delta.AskChangePrice) != len(delta.AskChangeQty) {
			return model.BookSnapshot{}, false, fmt.Errorf("delta price/quantity array length mismatch")
		}
		apply(bids, delta.BidChangePrice, delta.BidChangeQty)
		apply(asks, delta.AskChangePrice, delta.AskChangeQty)
	}
	snapshot := model.BookSnapshot{InstrumentID: minute.InstrumentID, Bids: top(bids, true), Asks: top(asks, false)}
	return snapshot, true, nil
}

func fixedSide(levels [model.BookDepth]model.Level) map[int64]uint64 {
	out := make(map[int64]uint64, model.BookDepth)
	for _, level := range levels {
		if level.PriceTick != 0 {
			out[level.PriceTick] = level.QtyLot
		}
	}
	return out
}

func apply(side map[int64]uint64, prices []int64, quantities []uint64) {
	for index, price := range prices {
		if quantities[index] == 0 {
			delete(side, price)
		} else {
			side[price] = quantities[index]
		}
	}
}

func top(side map[int64]uint64, bids bool) []model.Level {
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
	if len(prices) > model.BookDepth {
		prices = prices[:model.BookDepth]
	}
	out := make([]model.Level, len(prices))
	for index, price := range prices {
		out[index] = model.Level{PriceTick: price, QtyLot: side[price]}
	}
	return out
}
