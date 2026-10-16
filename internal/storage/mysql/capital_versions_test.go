package mysql

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/shopspring/decimal"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
)

func capitalFixture() model.CapitalParameters {
	d := decimal.RequireFromString
	sources := []model.PublicSource{}
	for _, endpoint := range []string{"discount-rate-interest-free-quota", "position-tiers?instType=SWAP&tdMode=cross&instFamily=BTC-USDT", "position-tiers?instType=MARGIN&tdMode=cross&ccy=BTC"} {
		sources = append(sources, model.PublicSource{URL: "https://www.okx.com/api/v5/public/" + endpoint, PayloadHash: model.ReferenceHash(endpoint), TimeBasis: "request_without_source_clock", SourceMS: 1000, ObservedMS: 1001})
	}
	return model.CapitalParameters{ID: "capital-one", InstrumentID: 17, Base: "BTC", SourceMS: 1000, ObservedMS: 1001, EffectiveMS: 1000, ValidUntilMS: 3601000, BorrowLeverage: d("5"), ModelHash: model.ReferenceHash("model"),
		QuoteDiscount: []model.DiscountTier{{Maximum: d("1000000"), Rate: d("1"), Penalty: d("0.01")}},
		BaseDiscount:  []model.DiscountTier{{Maximum: d("10000"), Rate: d("0.987654321098765432"), Penalty: d("0.02")}},
		PerpTiers:     []model.RiskTier{{Maximum: d("1000"), Initial: d("0.1"), Maintenance: d("0.05"), MaximumLeverage: d("10")}},
		BorrowTiers:   []model.RiskTier{{Maximum: d("500"), Initial: d("0.2"), Maintenance: d("0.1"), MaximumLeverage: d("5")}}, Sources: sources}
}
func sqlRoundTripCapitalRow(t *testing.T, name string, r record) record {
	t.Helper()
	mapping, err := Mapping("crypto_market_info", name)
	if err != nil {
		t.Fatal(err)
	}
	out := record{}
	for _, col := range mapping.Columns {
		native, err := codec.SQLValue(col.Type, r[col.Name])
		if err != nil {
			t.Fatal(name, col.Name, err)
		}
		got, err := codec.FromSQL(col.Type, native)
		if err != nil {
			t.Fatal(name, col.Name, err)
		}
		out[col.Name] = got
	}
	before, err := PackMigrationRow(mapping, MigrationRow(r))
	if err != nil {
		t.Fatal(err)
	}
	after, err := PackMigrationRow(mapping, MigrationRow(out))
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("typed SQL representation changed", err)
	}
	return out
}
func TestCapitalVersionTypedStorageReconstructsOriginalPublication(t *testing.T) {
	p := capitalFixture()
	groups, err := capitalVersionRecords(p)
	if err != nil {
		t.Fatal(err)
	}
	var v model.CapitalVersions
	v.Observation, err = decodeCapitalObservation(sqlRoundTripCapitalRow(t, "okx_capital_observation_v2", groups["okx_capital_observation_v2"][0]))
	if err != nil {
		t.Fatal(err)
	}
	versions := groups["okx_discount_rule_version"]
	v.Quote, err = decodeDiscountVersion(sqlRoundTripCapitalRow(t, "okx_discount_rule_version", versions[0]))
	if err != nil {
		t.Fatal(err)
	}
	v.Base, err = decodeDiscountVersion(sqlRoundTripCapitalRow(t, "okx_discount_rule_version", versions[1]))
	if err != nil {
		t.Fatal(err)
	}
	versions = groups["okx_risk_rule_version"]
	v.Perp, err = decodeRiskVersion(sqlRoundTripCapitalRow(t, "okx_risk_rule_version", versions[0]))
	if err != nil {
		t.Fatal(err)
	}
	v.Borrow, err = decodeRiskVersion(sqlRoundTripCapitalRow(t, "okx_risk_rule_version", versions[1]))
	if err != nil {
		t.Fatal(err)
	}
	got, err := v.Rebuild(p.Sources)
	if err != nil || model.DiscoveryHash(got.Body()) != model.DiscoveryHash(p.Body()) {
		t.Fatal("old logical publication hash changed", err)
	}
	old, err := Mapping("crypto_market_info", "okx_public_capital_parameters")
	if err != nil {
		t.Fatal(err)
	}
	oldRow := record(p.Body())
	oldRow["row_hash"] = model.DiscoveryHash(p.Body())
	if _, err = PackMigrationRow(old, MigrationRow(oldRow)); err != nil {
		t.Fatal("legacy representation no longer supported", err)
	}
	// Same quote and base currency can resolve the same immutable curve.
	p.Base = "USDT"
	p.BaseDiscount = p.QuoteDiscount
	same, err := capitalVersionRecords(p)
	if err != nil {
		t.Fatal(err)
	}
	if same["okx_discount_rule_version"][0]["version_hash"] != same["okx_discount_rule_version"][1]["version_hash"] {
		t.Fatal("identical currency curve duplicated")
	}
}
func TestCapitalExtensionsDoNotChangeFrozenMigrationCatalog(t *testing.T) {
	c, err := Mappings()
	if err != nil || len(c.Tables) != 125 {
		t.Fatal("frozen migration changed", err)
	}
	for _, name := range CapitalVersionTableNames() {
		for _, old := range c.Tables {
			if old.Name == name {
				t.Fatal("runtime extension leaked into history migration")
			}
		}
		tab, err := Mapping("crypto_market_info", name)
		if err != nil || tab.Multiplicity || tab.HashedKey {
			t.Fatal("incorrect runtime identity", err)
		}
		if _, err = Mapping("crypto_market_info_across", name); err == nil {
			t.Fatal("extension leaks into another domain")
		}
	}
	if !supportedDiscoveryCodec("okx_public_capital_parameters", model.CapitalVersionsCodec) || supportedDiscoveryCodec("funding_forecast_observation", model.CapitalVersionsCodec) {
		t.Fatal("codec accepted outside capital domain")
	}
}
func TestCapitalVersionRowsRejectDamagedScopeAndTypedArray(t *testing.T) {
	groups, err := capitalVersionRecords(capitalFixture())
	if err != nil {
		t.Fatal(err)
	}
	r := sqlRoundTripCapitalRow(t, "okx_risk_rule_version", groups["okx_risk_rule_version"][0])
	r["role"] = "MARGIN"
	if _, err = decodeRiskVersion(r); err == nil {
		t.Fatal("wrong role accepted")
	}
	r = sqlRoundTripCapitalRow(t, "okx_discount_rule_version", groups["okx_discount_rule_version"][0])
	r["amount_unit"] = "USDC"
	if _, err = decodeDiscountVersion(r); err == nil {
		t.Fatal("wrong units accepted")
	}
	tab, _ := Mapping("crypto_market_info", "okx_risk_rule_version")
	for _, col := range tab.Columns {
		if col.Name == "tiers" {
			raw, err := codec.Encode(col.Type, groups[tab.Name][0]["tiers"])
			if err != nil {
				t.Fatal(err)
			}
			if _, err = codec.Decode(col.Type, raw[:len(raw)-1]); err == nil {
				t.Fatal("truncated typed curve accepted")
			}
		}
	}
	r = sqlRoundTripCapitalRow(t, "okx_capital_observation_v2", groups["okx_capital_observation_v2"][0])
	r["row_hash"] = "short"
	if _, err = decodeCapitalObservation(r); err == nil {
		t.Fatal("invalid digest accepted")
	}
}
func TestCapitalVersionSyntheticRepeatedObservationSize(t *testing.T) {
	p := capitalFixture()
	// Representative multi-tier shape, not measured exchange data or disk bytes.
	p.PerpTiers = nil
	p.BorrowTiers = nil
	for n := int64(0); n < 90; n++ {
		tier := model.RiskTier{Minimum: decimal.NewFromInt(n * 100), Maximum: decimal.NewFromInt((n + 1) * 100), Initial: decimal.RequireFromString("0.2"), Maintenance: decimal.RequireFromString("0.1"), MaximumLeverage: decimal.NewFromInt(5)}
		p.PerpTiers = append(p.PerpTiers, tier)
		p.BorrowTiers = append(p.BorrowTiers, tier)
	}
	old, _ := Mapping("crypto_market_info", "okx_public_capital_parameters")
	oldBytes, newBytes := 0, 0
	unique := map[string]bool{}
	for n := 0; n < 48; n++ {
		p.ID = hex.EncodeToString([]byte{byte(n)})
		p.SourceMS += 1000
		p.ObservedMS += 1000
		p.EffectiveMS += 1000
		p.ValidUntilMS += 1000
		r := record(p.Body())
		r["row_hash"] = model.DiscoveryHash(p.Body())
		raw, e := PackMigrationRow(old, MigrationRow(r))
		if e != nil {
			t.Fatal(e)
		}
		oldBytes += len(raw)
		groups, e := capitalVersionRecords(p)
		if e != nil {
			t.Fatal(e)
		}
		for name, rows := range groups {
			tab, _ := Mapping("crypto_market_info", name)
			for _, r := range rows {
				key, e := keyHash(tab, r)
				if e != nil {
					t.Fatal(e)
				}
				id := name + string(key[:])
				if unique[id] {
					continue
				}
				unique[id] = true
				raw, e := PackMigrationRow(tab, MigrationRow(r))
				if e != nil {
					t.Fatal(e)
				}
				newBytes += len(raw)
			}
		}
	}
	if newBytes >= oldBytes {
		t.Fatal("repeated rule payload did not decrease", oldBytes, newBytes)
	}
	t.Logf("48 synthetic observations, 90 risk tiers: typed row payload old=%d new=%d bytes (excludes provenance, DB pages/indexes/compression)", oldBytes, newBytes)
}
