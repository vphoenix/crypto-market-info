package justlend

import (
	"context"
	"testing"
	"time"
)

type fakeSource struct {
	snapshot Snapshot
	err      error
}

func (f fakeSource) Fetch(context.Context) (Snapshot, error) { return f.snapshot, f.err }

func TestCollectorBuildsFourRoutesAndDerivedComponents(t *testing.T) {
	snapshot := validSnapshot()
	collector := &Collector{Client: fakeSource{snapshot: snapshot}, Now: func() time.Time { return time.UnixMilli(2000) }}
	batch, err := collector.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = batch.NormalizeAndValidate(); err != nil {
		t.Fatal(err)
	}
	if len(batch.Items) != 4 {
		t.Fatalf("items=%d", len(batch.Items))
	}
	byCode := map[string]int{}
	for i, item := range batch.Items {
		byCode[item.Route.ProductCode] = i
	}
	jtrx := batch.Items[byCode["jtrx"]].Observation
	if jtrx.Rate.String() != "0.03" || len(jtrx.RewardAssetKeys) != 2 || jtrx.RulePrincipalLossMode != "variable" || jtrx.RuleEligibility != "rejected" {
		t.Fatalf("jtrx=%+v", jtrx)
	}
	combo := batch.Items[byCode["strx-jstrx"]].Observation
	if combo.Rate.String() != "0.07" || combo.ExposureRatio.String() != "3" || combo.TVL.String() != "30" || combo.UnbondingSeconds != 14*86400 {
		t.Fatalf("combo=%+v", combo)
	}
	vault := batch.Items[byCode["trx-v2-vault"]].Observation
	if vault.PerformanceFeeRate.String() != "0.1" || vault.ObservationTime.UnixMilli() != 1000 {
		t.Fatalf("vault=%+v", vault)
	}
}

func TestCollectorRejectsScientificDecimalAndUnknownMiningReward(t *testing.T) {
	for name, mutate := range map[string]func(*Snapshot){
		"scientific":        func(s *Snapshot) { s.JTokens[0].SupplyRate = "1e-3" },
		"unknown reward":    func(s *Snapshot) { s.Mining[jtrxAddress]["NEW"] = "0" },
		"duplicate address": func(s *Snapshot) { s.JTokens = append(s.JTokens, s.JTokens[0]) },
	} {
		t.Run(name, func(t *testing.T) {
			snapshot := validSnapshot()
			mutate(&snapshot)
			_, err := (&Collector{Client: fakeSource{snapshot: snapshot}}).Collect(context.Background())
			if err == nil {
				t.Fatal("invalid response accepted")
			}
		})
	}
}

func TestMissingFixedMarketProducesUnavailableObservation(t *testing.T) {
	s := validSnapshot()
	s.JTokens = s.JTokens[1:]
	b, err := (&Collector{Client: fakeSource{snapshot: s}}).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if b.Items[1].Observation.Availability != "unavailable" || b.Items[1].Observation.Rate != nil {
		t.Fatalf("observation=%+v", b.Items[1].Observation)
	}
}

func TestMiningZeroAndMissingMarketKeepOnlyBaseYield(t *testing.T) {
	for name, mutate := range map[string]func(*Snapshot){
		"zero":    func(s *Snapshot) { s.Mining[jtrxAddress]["USDD"] = "0.00000000" },
		"missing": func(s *Snapshot) { delete(s.Mining, jtrxAddress) },
	} {
		t.Run(name, func(t *testing.T) {
			snapshot := validSnapshot()
			mutate(&snapshot)
			batch, err := (&Collector{Client: fakeSource{snapshot: snapshot}}).Collect(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			observation := batch.Items[1].Observation
			if observation.Rate.String() != "0.02" || len(observation.RewardAssetKeys) != 1 || len(observation.RewardComponentRates) != 1 || observation.RewardComponentRates[0].String() != "0.02" {
				t.Fatalf("observation=%+v", observation)
			}
		})
	}
}

func TestCollectorRejectsFixedProductIdentityChangesAndMissingRaw(t *testing.T) {
	for name, mutate := range map[string]func(*Snapshot){
		"strx address":            func(s *Snapshot) { s.STRX.StakeInfo.Address = nativePlaceholder },
		"strx symbol":             func(s *Snapshot) { s.STRX.StakeInfo.Symbol = "STRX" },
		"strx decimals":           func(s *Snapshot) { s.STRX.StakeInfo.Decimals = "6" },
		"jtrx symbol":             func(s *Snapshot) { s.JTokens[0].Symbol = "TRX" },
		"jtrx underlying":         func(s *Snapshot) { s.JTokens[0].UnderlyingAddress = strxAddress },
		"jtrx decimals":           func(s *Snapshot) { s.JTokens[0].UnderlyingDecimal = 18 },
		"jstrx underlying symbol": func(s *Snapshot) { s.JTokens[1].UnderlyingSymbol = "TRX" },
		"vault asset":             func(s *Snapshot) { s.Vaults[0].AssetAddress = nativePlaceholder },
		"vault decimals":          func(s *Snapshot) { s.Vaults[0].AssetDecimals = 18 },
		"missing raw":             func(s *Snapshot) { s.RawMining = nil },
	} {
		t.Run(name, func(t *testing.T) {
			snapshot := validSnapshot()
			mutate(&snapshot)
			if _, err := (&Collector{Client: fakeSource{snapshot: snapshot}}).Collect(context.Background()); err == nil {
				t.Fatal("identity/raw change accepted")
			}
		})
	}
}

func validSnapshot() Snapshot {
	return Snapshot{STRX: STRXData{StakeInfo: STRXStakeInfo{Address: strxAddress, Symbol: "sTRX", Decimals: "18", UnderlyingDecimal: "6", SupplyRate: "0.01", ExchangeRate: "1.5", TotalUnderlying: "150", TotalSupply: "100"}},
		JTokens: []JToken{{Address: jtrxAddress, Symbol: "jTRX", UnderlyingSymbol: "TRX", UnderlyingAddress: nativePlaceholder, UnderlyingDecimal: 6, SupplyRate: "0.02", ExchangeRate: "2", TotalSupply: "10"}, {Address: jstrxAddress, Symbol: "jsTRX", UnderlyingSymbol: "sTRX", UnderlyingAddress: strxAddress, UnderlyingDecimal: 18, SupplyRate: "0.03", ExchangeRate: "2", TotalSupply: "10"}},
		Mining:  map[string]map[string]string{jtrxAddress: {"USDD": "0.01"}, jstrxAddress: {"USDD": "0.03"}}, Vaults: []Vault{{Chain: "tron", Address: vaultAddress, Name: "TRX", Symbol: "jTRXv2", AssetAddress: wtrxAddress, AssetSymbol: "WTRX", AssetDecimals: 6, TotalSupplyAmount: "40", APY: "0.04", PerformanceFee: "10.00"}}, V2Time: time.UnixMilli(1000), RawSTRX: []byte("s"), RawJToken: []byte("j"), RawMining: []byte("m"), RawVault: []byte("v")}
}
