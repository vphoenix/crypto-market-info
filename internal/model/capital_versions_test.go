package model

import (
	"github.com/shopspring/decimal"
	"testing"
)

func capitalVersionFixture() CapitalParameters {
	d := decimal.RequireFromString
	source := PublicSource{URL: "https://www.okx.com/api/v5/public/discount-rate-interest-free-quota", PayloadHash: ReferenceHash("payload"), TimeBasis: "request_without_source_clock", SourceMS: 1000, ObservedMS: 1001}
	return CapitalParameters{ID: "observation-one", InstrumentID: 17, ObservedMS: 1001, SourceMS: 1000, EffectiveMS: 1000, ValidUntilMS: 3601000, Base: "BTC", BorrowLeverage: d("5"), ModelHash: ReferenceHash("model"),
		QuoteDiscount: []DiscountTier{{decimal.Zero, d("1000000"), d("1"), d("0.01")}}, BaseDiscount: []DiscountTier{{decimal.Zero, d("10000"), d("0.987654321098765432"), d("0.02")}},
		PerpTiers: []RiskTier{{decimal.Zero, d("10000"), d("0.1"), d("0.05"), d("10")}}, BorrowTiers: []RiskTier{{decimal.Zero, d("500"), d("0.2"), d("0.1"), d("5")}}, Sources: []PublicSource{source}}
}
func TestCapitalVersionsShareContentWithoutMergingObservations(t *testing.T) {
	p := capitalVersionFixture()
	v, err := SplitCapitalVersions(p)
	if err != nil {
		t.Fatal(err)
	}
	got, err := v.Rebuild(p.Sources)
	if err != nil || DiscoveryHash(got.Body()) != DiscoveryHash(p.Body()) {
		t.Fatal("logical fact changed", err)
	}
	next := p
	next.ID = "later"
	next.SourceMS += 1000
	next.ObservedMS += 1000
	next.EffectiveMS += 1000
	next.ValidUntilMS += 1000
	w, err := SplitCapitalVersions(next)
	if err != nil {
		t.Fatal(err)
	}
	if v.Quote.ID != w.Quote.ID || v.Base.ID != w.Base.ID || v.Perp.ID != w.Perp.ID || v.Borrow.ID != w.Borrow.ID {
		t.Fatal("observation clock leaked into rule identity")
	}
	if v.Observation.Hash == w.Observation.Hash || w.Observation.Parameters.SourceMS != next.SourceMS {
		t.Fatal("new observation time lost")
	}
	next.Base = "ETH"
	next.InstrumentID++
	w, err = SplitCapitalVersions(next)
	if err != nil {
		t.Fatal(err)
	}
	if v.Quote.ID != w.Quote.ID || v.Base.ID == w.Base.ID || v.Perp.ID == w.Perp.ID || v.Borrow.ID == w.Borrow.ID || v.Perp.ID == v.Borrow.ID {
		t.Fatal("scope sharing mismatch")
	}
	next = p
	next.InstrumentID++
	w, err = SplitCapitalVersions(next)
	if err != nil {
		t.Fatal(err)
	}
	if v.Base.ID != w.Base.ID || v.Perp.ID == w.Perp.ID {
		t.Fatal("same currency/new contract version not distinguished")
	}
}
func TestCapitalVersionCorruptionAndPrecisionFailClosed(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*CapitalVersions)
	}{
		{"missing", func(v *CapitalVersions) { v.Perp = RiskRuleVersion{} }},
		{"changed_tier", func(v *CapitalVersions) { v.Quote.Tiers[0].Rate = decimal.RequireFromString("0.9") }},
		{"swapped_roles", func(v *CapitalVersions) { v.Perp, v.Borrow = v.Borrow, v.Perp }},
		{"future_observation", func(v *CapitalVersions) { v.Observation.Parameters.ObservedMS++ }},
		{"changed_reference", func(v *CapitalVersions) { v.Observation.QuoteVersion = v.Base.ID }},
		{"wrong_currency_valid_hash", func(v *CapitalVersions) {
			v.Quote.Currency = "USDC"
			v.Quote.ID = DiscoveryHash(v.Quote.Body())
			v.Observation.QuoteVersion = v.Quote.ID
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			p := capitalVersionFixture()
			v, e := SplitCapitalVersions(p)
			if e != nil {
				t.Fatal(e)
			}
			test.mutate(&v)
			if _, e = v.Rebuild(p.Sources); e == nil {
				t.Fatal("corrupt reference accepted")
			}
		})
	}
	for _, value := range []string{"0.9876543210987654321", "100000000000000000000"} {
		p := capitalVersionFixture()
		p.BaseDiscount[0].Maximum = decimal.RequireFromString(value)
		if _, err := SplitCapitalVersions(p); err == nil {
			t.Fatal("nonrepresentable tier accepted", value)
		}
	}
	p := capitalVersionFixture()
	p.Sources[0].PayloadHash = ""
	if _, err := SplitCapitalVersions(p); err == nil {
		t.Fatal("source hash omitted")
	}
}
