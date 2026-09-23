package options

import (
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

// ValidateSelection prevents a fixed run from silently dropping a required leg.
func ValidateSelection(specs []ContractSpec) error {
	if len(specs) == 0 || len(specs) > MaxLiveBooks {
		return fmt.Errorf("selection requires 1..%d instruments", MaxLiveBooks)
	}
	type group struct {
		futures int
		pairs   map[string]uint8
	}
	groups := map[string]*group{}
	seen := map[string]bool{}
	for _, s := range specs {
		if err := s.Validate(); err != nil {
			return err
		}
		i := s.Instrument
		if i.MarketType == model.MarketOptionCombo || i.ExpiryTime == nil || !ValidIndex(s.IndexID) || seen[i.ExchangeSymbol] {
			return fmt.Errorf("unsupported/duplicate selection member")
		}
		seen[i.ExchangeSymbol] = true
		key := fmt.Sprintf("%s/%s/%s/%s/%d", i.BaseAsset, *i.SettleAsset, s.SourceInstrumentType, s.IndexID, i.ExpiryTime.UnixMilli())
		g := groups[key]
		if g == nil {
			g = &group{pairs: map[string]uint8{}}
			groups[key] = g
		}
		if i.MarketType == model.MarketDelivery {
			g.futures++
			continue
		}
		bit := uint8(1)
		if s.OptionType == "put" {
			bit = 2
		}
		k := s.StrikeCurrency + "/" + s.Strike.String()
		if g.pairs[k]&bit != 0 {
			return fmt.Errorf("duplicate option leg")
		}
		g.pairs[k] |= bit
	}
	for key, g := range groups {
		if g.futures != 1 || len(g.pairs) == 0 {
			return fmt.Errorf("selection group %s requires options and one matching future", key)
		}
		for _, pair := range g.pairs {
			if pair != 3 {
				return fmt.Errorf("selection group %s lacks C/P leg", key)
			}
		}
	}
	return nil
}
