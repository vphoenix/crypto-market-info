package contracts

import (
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

func ValidateMinuteBatch(batch model.MinuteBatch) error {
	if err := batch.Minute.ValidateDepth(); err != nil {
		return err
	}
	wantID, err := model.MinuteID(batch.Minute.InstrumentID, batch.Minute.MinuteTime)
	if err != nil {
		return err
	}
	if batch.Minute.ID != wantID {
		return fmt.Errorf("minute id does not match instrument/time")
	}
	if batch.Minute.ValidBitmap&1 == 0 {
		return fmt.Errorf("minute batch has no valid second-zero anchor")
	}
	if batch.Minute.ValidBitmap>>60 != 0 {
		return fmt.Errorf("minute valid bitmap exceeds 60 seconds")
	}
	bids, err := unpadded(batch.Minute.Bids)
	if err != nil {
		return fmt.Errorf("minute bids: %w", err)
	}
	asks, err := unpadded(batch.Minute.Asks)
	if err != nil {
		return fmt.Errorf("minute asks: %w", err)
	}
	if err = (model.BookSnapshot{InstrumentID: batch.Minute.InstrumentID, SourceTime: batch.Minute.MinuteTime, Bids: bids, Asks: asks}).Validate(int(batch.Minute.StoredDepth)); err != nil {
		return fmt.Errorf("minute snapshot: %w", err)
	}
	lastSecond := uint8(0)
	bidPrices, askPrices := make(map[int64]struct{}, len(bids)), make(map[int64]struct{}, len(asks))
	for _, level := range bids {
		bidPrices[level.PriceTick] = struct{}{}
	}
	for _, level := range asks {
		askPrices[level.PriceTick] = struct{}{}
	}
	for _, delta := range batch.Deltas {
		if delta.MinuteID != batch.Minute.ID || delta.SecondOffset == 0 || delta.SecondOffset > 59 {
			return fmt.Errorf("invalid delta identity/second")
		}
		if len(delta.BidChangePrice) != len(delta.BidChangeQty) || len(delta.AskChangePrice) != len(delta.AskChangeQty) {
			return fmt.Errorf("delta array length mismatch")
		}
		if batch.Minute.ValidBitmap&(uint64(1)<<delta.SecondOffset) == 0 {
			return fmt.Errorf("delta exists for invalid second %d", delta.SecondOffset)
		}
		if delta.SecondOffset <= lastSecond {
			return fmt.Errorf("deltas must have unique increasing second offsets")
		}
		lastSecond = delta.SecondOffset
		if err = validateDeltaSide(delta.BidChangePrice, "bid"); err != nil {
			return err
		}
		if err = validateDeltaSide(delta.AskChangePrice, "ask"); err != nil {
			return err
		}
		applyPriceKeys(bidPrices, delta.BidChangePrice, delta.BidChangeQty)
		applyPriceKeys(askPrices, delta.AskChangePrice, delta.AskChangeQty)
		if len(bidPrices) > int(batch.Minute.StoredDepth) || len(askPrices) > int(batch.Minute.StoredDepth) {
			return fmt.Errorf("second %d exceeds stored depth %d", delta.SecondOffset, batch.Minute.StoredDepth)
		}
	}
	return nil
}

func applyPriceKeys(side map[int64]struct{}, prices []int64, quantities []uint64) {
	for index, price := range prices {
		if quantities[index] == 0 {
			delete(side, price)
		} else {
			side[price] = struct{}{}
		}
	}
}

func validateDeltaSide(prices []int64, side string) error {
	seen := make(map[int64]struct{}, len(prices))
	for _, price := range prices {
		if price <= 0 {
			return fmt.Errorf("%s delta has non-positive price", side)
		}
		if _, exists := seen[price]; exists {
			return fmt.Errorf("%s delta repeats price %d", side, price)
		}
		seen[price] = struct{}{}
	}
	return nil
}

func unpadded(levels [model.LegacyBookDepth]model.Level) ([]model.Level, error) {
	out := make([]model.Level, 0, model.LegacyBookDepth)
	padding := false
	for index, level := range levels {
		if level.PriceTick == 0 && level.QtyLot == 0 {
			padding = true
			continue
		}
		if padding {
			return nil, fmt.Errorf("non-zero level %d follows zero padding", index+1)
		}
		if level.PriceTick <= 0 || level.QtyLot == 0 {
			return nil, fmt.Errorf("level %d has invalid price/quantity", index+1)
		}
		out = append(out, level)
	}
	return out, nil
}
