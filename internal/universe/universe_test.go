package universe

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
)

func universeInstrument(venue, base string) model.Instrument {
	settle := "USDT"
	return model.Instrument{Exchange: venue, MarketType: model.MarketPerpetual, ExchangeSymbol: base + "USDT", VenueContractVersion: "1", BaseAsset: base, QuoteAsset: "USDT", SettleAsset: &settle, ContractMultiplier: decimal.NewFromInt(1), PriceTickSize: decimal.RequireFromString("0.001"), QuantityStepSize: decimal.RequireFromString("0.001")}
}
func universeAliases(t *testing.T, entries ...Alias) *Aliases {
	t.Helper()
	if entries == nil {
		entries = []Alias{}
	}
	b, err := json.Marshal(AliasFile{SchemaVersion: 1, Aliases: entries})
	if err != nil {
		t.Fatal(err)
	}
	a, err := ParseAliases(b)
	if err != nil {
		t.Fatal(err)
	}
	return a
}
func universeAlias(venue, symbol, base, canonical, factor, version string) Alias {
	return Alias{Exchange: venue, MarketType: model.MarketPerpetual, ExchangeSymbol: symbol, VenueContractVersion: version, ExpectedBaseAsset: base, ExpectedQuoteAsset: "USDT", ExpectedSettleAsset: "USDT", CanonicalBaseAsset: canonical, CanonicalBaseUnitsPerVenueBaseUnit: factor, Reason: "verified unit mapping"}
}
func autoSelection(venues ...string) SelectionConfig {
	c := SelectionConfig{}
	for _, venue := range venues {
		c.Venues = append(c.Venues, VenueSelectionConfig{Venue: venue, Mode: Auto, TopicsPerConnection: 20, MaxInstruments: 500})
	}
	return c
}

func TestCommonUniverseCountsDistinctVenuesAndFutureFourth(t *testing.T) {
	catalogs := []VenueCatalog{
		{Venue: "Binance", Instruments: []model.Instrument{universeInstrument("Binance", "BTC"), universeInstrument("Binance", "DOGE")}},
		{Venue: "OKX", Instruments: []model.Instrument{universeInstrument("OKX", "BTC")}},
		{Venue: "Bybit", Instruments: []model.Instrument{universeInstrument("Bybit", "BTC"), universeInstrument("Bybit", "DOGE"), universeInstrument("Bybit", "ONLY")}},
	}
	a := universeAliases(t)
	r, err := BuildCommonPerpetualUniverse(catalogs, autoSelection("Binance", "OKX", "Bybit"), a)
	if err != nil || r.GroupCount != 2 || len(r.Selected) != 5 || r.IgnoredSingleVenueGroups != 1 {
		t.Fatalf("result=%+v err=%v", r, err)
	}
	catalogs = append(catalogs, VenueCatalog{Venue: "Fourth", Instruments: []model.Instrument{universeInstrument("Fourth", "ONLY")}})
	r, err = BuildCommonPerpetualUniverse(catalogs, autoSelection("Binance", "OKX", "Bybit", "Fourth"), a)
	if err != nil || r.GroupCount != 3 || len(r.Selected) != 7 {
		t.Fatalf("fourth venue result=%+v err=%v", r, err)
	}
}

func TestCommonUniverseAliasesHaveExactVersionAndFactor(t *testing.T) {
	entry := universeAlias("Binance", "1000PEPEUSDT", "1000PEPE", "PEPE", "1000", "1")
	other := entry
	other.VenueContractVersion = "2"
	other.CanonicalBaseUnitsPerVenueBaseUnit = "100"
	a := universeAliases(t, other, entry)
	i := universeInstrument("Binance", "1000PEPE")
	resolved, err := a.Resolve(i)
	if err != nil || resolved.MarketKey.String() != "PEPE-USDT-PERP" || !resolved.CanonicalBaseUnitsPerVenueBaseUnit.Equal(decimal.NewFromInt(1000)) {
		t.Fatalf("mapping=%+v err=%v", resolved, err)
	}
	i.VenueContractVersion = "2"
	resolved, err = a.Resolve(i)
	if err != nil || !resolved.CanonicalBaseUnitsPerVenueBaseUnit.Equal(decimal.NewFromInt(100)) {
		t.Fatal("contract version factor not exact")
	}
	i.VenueContractVersion = "3"
	resolved, err = a.Resolve(i)
	if err != nil || resolved.MappingKind != "identity" || resolved.MarketKey.String() != "1000PEPE-USDT-PERP" {
		t.Fatal("historical alias applied to current version")
	}
	i.VenueContractVersion = "1"
	i.BaseAsset = "OTHER"
	if _, err = a.Resolve(i); err == nil {
		t.Fatal("mismatched expected base accepted")
	}
	catalogs := []VenueCatalog{{"Binance", []model.Instrument{universeInstrument("Binance", "1000PEPE")}}, {"Bybit", []model.Instrument{universeInstrument("Bybit", "PEPE")}}}
	r, err := BuildCommonPerpetualUniverse(catalogs, autoSelection("Binance", "Bybit"), universeAliases(t))
	if err != nil || len(r.Selected) != 0 || len(r.PotentialAliases) != 1 {
		t.Fatalf("unconfigured alias was inferred: %+v err=%v", r, err)
	}
	r, err = BuildCommonPerpetualUniverse(catalogs, autoSelection("Binance", "Bybit"), a)
	if err != nil || len(r.Selected) != 2 || r.AliasCount != 1 || len(r.Warnings) != 1 {
		t.Fatalf("explicit alias selection=%+v err=%v", r, err)
	}
	quantity := decimal.NewFromInt(10).Mul(decimal.RequireFromString("0.1")).Mul(r.Selected[0].CanonicalBaseUnitsPerVenueBaseUnit)
	price := decimal.RequireFromString("0.02").Div(r.Selected[0].CanonicalBaseUnitsPerVenueBaseUnit)
	if !quantity.Equal(decimal.NewFromInt(1000)) || !price.Equal(decimal.RequireFromString("0.00002")) {
		t.Fatal("price/quantity multiplier direction wrong")
	}
}

func TestAliasesStrictJSONAndRevisionRollback(t *testing.T) {
	if got := universeAliases(t).Revision; got != "4074aefd9e106bbe3d8dbc39695ac5a6bc3748419321c5a6e90a8e395ccd3ab6" {
		t.Fatalf("canonical JSON/prefix fixture changed: %s", got)
	}
	entry := universeAlias("Binance", "1000PEPEUSDT", "1000PEPE", "PEPE", "1000", "1")
	a := universeAliases(t, entry)
	canonical, _ := json.Marshal(a.File)
	for _, bad := range []string{
		`{"schema_version":1,"aliases":[],"extra":1}`,
		`{"schema_version":1,"schema_version":1,"aliases":[]}`,
		`{"SCHEMA_VERSION":1,"aliases":[]}`,
		`{"schema_version":1,"aliases":null}`,
		string(canonical) + ` {}`,
		strings.Replace(string(canonical), `"exchange":"Binance"`, `"exchange":"Binance","Exchange":"Bybit"`, 1),
		strings.Replace(string(canonical), `"1000"`, `"0"`, 1),
		strings.Replace(string(canonical), `"1000"`, `"100000000000000000000"`, 1),
		strings.Replace(string(canonical), `"1000"`, `"0.0000000000000000001"`, 1),
		strings.Replace(string(canonical), `"venue_contract_version":"1"`, `"venue_contract_version":""`, 1),
	} {
		if _, err := ParseAliases([]byte(bad)); err == nil {
			t.Fatalf("accepted strict JSON violation %s", bad)
		}
	}
	if _, err := ParseAliases([]byte(strings.Replace(string(canonical), `"aliases":[`, `"aliases":[`+string(mustJSON(t, entry))+`,`, 1))); err == nil {
		t.Fatal("duplicate tuple accepted")
	}
	bEntry := entry
	bEntry.CanonicalBaseUnitsPerVenueBaseUnit = "100"
	b := universeAliases(t, bEntry)
	aAgain, err := ParseAliases(canonical)
	if err != nil || a.Revision != aAgain.Revision || a.Revision == b.Revision {
		t.Fatal("A -> B -> A revision rollback unstable")
	}
	other := universeAlias("Bybit", "1000PEPEUSDT", "1000PEPE", "PEPE", "1000", "1")
	if universeAliases(t, entry, other).Revision != universeAliases(t, other, entry).Revision {
		t.Fatal("alias input ordering changed revision")
	}
}

func TestCommonUniverseRejectsPartialDuplicateAndCollision(t *testing.T) {
	a := universeAliases(t)
	btc := universeInstrument("Binance", "BTC")
	base := []VenueCatalog{{"Binance", []model.Instrument{btc}}, {"OKX", []model.Instrument{universeInstrument("OKX", "BTC")}}}
	if _, err := BuildCommonPerpetualUniverse(base[:1], autoSelection("Binance", "OKX"), a); err == nil {
		t.Fatal("missing enabled catalog accepted")
	}
	duplicate := append([]VenueCatalog(nil), base...)
	duplicate[0].Instruments = []model.Instrument{btc, btc}
	if _, err := BuildCommonPerpetualUniverse(duplicate, autoSelection("Binance", "OKX"), a); err == nil {
		t.Fatal("duplicate live symbol accepted")
	}
	other := btc
	other.ExchangeSymbol = "BTCOTHERUSDT"
	duplicate[0].Instruments = []model.Instrument{btc, other}
	if _, err := BuildCommonPerpetualUniverse(duplicate, autoSelection("Binance", "OKX"), a); err == nil {
		t.Fatal("same-venue canonical collision accepted")
	}
	cfg := autoSelection("Binance", "OKX")
	cfg.Venues[0].Exclude = []string{"BTCOTHERUSDT"}
	r, err := BuildCommonPerpetualUniverse(duplicate, cfg, a)
	if err != nil || len(r.Selected) != 2 {
		t.Fatalf("raw exclusion did not resolve collision: %+v %v", r, err)
	}
}

func TestCommonUniverseIncludesExcludesAndOrder(t *testing.T) {
	a := universeAliases(t)
	catalogs := []VenueCatalog{{"Binance", []model.Instrument{universeInstrument("Binance", "ETH"), universeInstrument("Binance", "BTC")}}, {"OKX", []model.Instrument{universeInstrument("OKX", "BTC"), universeInstrument("OKX", "ETH")}}, {"Bybit", []model.Instrument{universeInstrument("Bybit", "BTC")}}}
	cfg := autoSelection("Bybit", "OKX", "Binance")
	first, err := BuildCommonPerpetualUniverse(catalogs, cfg, a)
	if err != nil {
		t.Fatal(err)
	}
	catalogs[0], catalogs[2] = catalogs[2], catalogs[0]
	catalogs[2].Instruments[0], catalogs[2].Instruments[1] = catalogs[2].Instruments[1], catalogs[2].Instruments[0]
	second, err := BuildCommonPerpetualUniverse(catalogs, cfg, a)
	if err != nil || !reflect.DeepEqual(first, second) {
		t.Fatal("API input order changed universe")
	}
	cfg.Venues[0].Exclude = []string{"BTCUSDT", "OLDUSDT"}
	r, err := BuildCommonPerpetualUniverse(catalogs, cfg, a)
	if err != nil || len(r.Selected) != 4 || len(r.Warnings) != 1 {
		t.Fatalf("two remaining venues not retained %+v %v", r, err)
	}
	cfg.Include = []string{"MISSING-USDT-PERP"}
	if _, err = BuildCommonPerpetualUniverse(catalogs, cfg, a); err == nil {
		t.Fatal("missing canonical include accepted")
	}
	cfg.Include = []string{"BTC-USDT-PERP"}
	cfg.Exclude = []string{"BTC-USDT-PERP"}
	if _, err = BuildCommonPerpetualUniverse(catalogs, cfg, a); err == nil {
		t.Fatal("canonical include/exclude overlap accepted")
	}
	cfg = autoSelection("Binance", "OKX")
	cfg.Venues[0].Mode = Explicit
	cfg.Venues[0].Include = []string{"BTCUSDT"}
	cfg.Venues[0].Exclude = []string{"BTCUSDT"}
	if _, err = BuildCommonPerpetualUniverse(catalogs, cfg, a); err == nil {
		t.Fatal("raw include/exclude overlap accepted")
	}
}

func TestSelectionRevisionIncludesFourthVenueAndStableInputs(t *testing.T) {
	cfg := autoSelection("OKX", "Binance")
	a := universeAliases(t)
	one, configJSON, err := SelectionRevision(cfg, a.Revision, nil, struct{ Limit int }{1000})
	if err != nil {
		t.Fatal(err)
	}
	cfg.Venues[0], cfg.Venues[1] = cfg.Venues[1], cfg.Venues[0]
	two, _, err := SelectionRevision(cfg, a.Revision, nil, struct{ Limit int }{1000})
	if err != nil || one != two {
		t.Fatal("selection config input order changed revision")
	}
	cfg.Venues = append(cfg.Venues, VenueSelectionConfig{Venue: "Fourth", Mode: Disabled})
	third, newJSON, err := SelectionRevision(cfg, a.Revision, nil, struct{ Limit int }{1000})
	if err != nil || third == one || newJSON == configJSON || !strings.Contains(newJSON, "Fourth") {
		t.Fatal("fourth venue absent from persisted selection revision")
	}
}

func TestPerpetualMarketKeyAndVenueModes(t *testing.T) {
	for _, bad := range []string{"btc-USDT-PERP", "BTC-USD-PERP", "BTC-USDT-PERP-USDT-PERP", "BTC-USDT-PERP ", "-USDT-PERP", "BTC-USDC-PERP"} {
		if _, err := ParsePerpetualMarketKey(bad); err == nil {
			t.Fatalf("accepted %q", bad)
		}
	}
	for raw, want := range map[string]SelectionMode{"auto": Auto, "-": Disabled, "BTCUSDT,ETHUSDT": Explicit} {
		got, err := ParseVenueSelection("Binance", raw)
		if err != nil || got.Mode != want {
			t.Fatalf("raw=%q got=%+v err=%v", raw, got, err)
		}
	}
	for _, raw := range []string{"", "BTCUSDT,", "BTCUSDT,BTCUSDT", "auto,BTCUSDT", "btcusdt"} {
		if _, err := ParseVenueSelection("Binance", raw); err == nil {
			t.Fatalf("accepted raw selection %q", raw)
		}
	}
	if _, err := NormalizeConfig(autoSelection("Binance")); err == nil {
		t.Fatal("single enabled venue accepted")
	}
	if _, err := NormalizeConfig(SelectionConfig{Venues: []VenueSelectionConfig{{Venue: "Binance", Mode: Disabled}}}); err != nil {
		t.Fatal("fully disabled perpetual selection rejected")
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
