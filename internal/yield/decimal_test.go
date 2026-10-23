package yield

import (
	"testing"
	"time"

	"github.com/shopspring/decimal"
)

func TestBatchDecimalQuantumPreservesEvidenceAndSourceValues(t *testing.T) {
	b := validRunnerBatch(time.Now())
	d := decimal.RequireFromString("1.123456789012345678987654321")
	original := d.String()
	o := &b.Items[0].Observation
	hash := *o.SourcePayloadHash
	o.Rate, o.ExposureRatio, o.TVL = &d, &d, &d
	o.RewardComponentRates = []*decimal.Decimal{&d}
	if err := b.NormalizeAndValidateForLiveCollection(); err != nil {
		t.Fatal(err)
	}
	for _, got := range []*decimal.Decimal{o.Rate, o.ExposureRatio, o.TVL, o.RewardComponentRates[0]} {
		if got.String() != "1.123456789012345678" {
			t.Fatal(got)
		}
	}
	if d.String() != original || *o.SourcePayloadHash != hash {
		t.Fatal("changed source value or evidence")
	}
	if err := b.NormalizeAndValidateForLiveCollection(); err != nil {
		t.Fatal("normalization is not idempotent", err)
	}
}

func TestBatchDecimalQuantumDoesNotHideInvalidSourceValues(t *testing.T) {
	for name, change := range map[string]func(*YieldObservation){
		"negative rate": func(o *YieldObservation) { d := decimal.RequireFromString("-0.0000000000000000001"); o.Rate = &d },
		"fee above one": func(o *YieldObservation) {
			d := decimal.RequireFromString("1.0000000000000000001")
			o.EntryFeeRate = &d
		},
		"positive ratio underflow": func(o *YieldObservation) {
			d := decimal.RequireFromString("0.0000000000000000001")
			o.ExposureRatio = &d
		},
		"overflow": func(o *YieldObservation) { d := decimal.RequireFromString("100000000000000000000"); o.TVL = &d },
	} {
		t.Run(name, func(t *testing.T) {
			b := validRunnerBatch(time.Now())
			change(&b.Items[0].Observation)
			if b.NormalizeAndValidate() == nil {
				t.Fatal("invalid value accepted")
			}
		})
	}
	b := validRunnerBatch(time.Now())
	d := decimal.RequireFromString("99999999999999999999.999999999999999999")
	b.Items[0].Observation.TVL = &d
	if err := b.NormalizeAndValidate(); err != nil {
		t.Fatal("maximum representable value rejected", err)
	}
}
