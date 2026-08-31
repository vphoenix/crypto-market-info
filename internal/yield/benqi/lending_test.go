package benqi

import (
	"context"
	"strings"
	"testing"
)

func lendingValues() map[string]string {
	argument := strings.Repeat("0", 24) + QiAVAX[2:]
	return map[string]string{
		callKey(QiAVAX, "0x313ce567"):              abi("8"),
		callKey(QiAVAX, "0x840bbeac"):              abi("1"),
		callKey(QiAVAX, "0x5fe3b567"):              "0x" + strings.Repeat("0", 24) + Controller[2:],
		callKey(QiAVAX, "0xd3bd2c72"):              abi("267399646"),
		callKey(QiAVAX, "0xbd6d894d"):              abi("233222643389531741806027720"),
		callKey(QiAVAX, "0x18160ddd"):              abi("1234567890123456"),
		callKey(QiAVAX, "0x3b1d21a2"):              abi("1000000000000000001"),
		callKey(Controller, "0x8e8f294b"+argument): abi("1") + abi("0")[2:] + abi("0")[2:],
		callKey(Controller, "0x731f0c2b"+argument): abi("0"),
	}
}

func TestLendingUsesCurrentExchangeRatePerSecondAPRAndRawTVL(t *testing.T) {
	client, fixture := fixtureClient(t, lendingValues())
	batch, err := NewLendingCollector(client).Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assertChainBatch(t, batch, "avalanche-qiavax-supply", QiAVAX)
	o := batch.Items[0].Observation
	if o.Rate.String() != "0.008432715236256" || o.RateKind != "apr" || o.RateOrigin != "derived" || o.ExposureRatio.String() != "0.023322264338953174" ||
		o.PoolCash.String() != "1.000000000000000001" || o.TVL.String() != "287929.18677842938523179" || o.Capacity != nil || o.RemainingCapacity != nil ||
		o.Availability != "available" || *o.UnbondingSeconds != 0 || o.RedemptionWindowSeconds != nil || o.PerformanceFeeRate != nil || o.EntryFeeRate != nil || o.ExitFeeRate != nil ||
		!o.RewardComponentRates[0].Equal(*o.Rate) {
		t.Fatalf("lending observation=%+v", o)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if len(fixture.seen) != 9 {
		t.Fatalf("lending read count=%d", len(fixture.seen))
	}
	for _, key := range fixture.seen {
		if strings.Contains(key, "0x182df0f5") {
			t.Fatal("exchangeRateStored must not be used")
		}
	}
}

func TestLendingZeroRateAndCashDoNotMeanClosedOrNoCapacity(t *testing.T) {
	for _, state := range []struct{ listed, paused, availability string }{
		{"1", "0", "available"}, {"1", "1", "paused"}, {"0", "1", "closed"},
	} {
		values := lendingValues()
		argument := strings.Repeat("0", 24) + QiAVAX[2:]
		values[callKey(Controller, "0x8e8f294b"+argument)] = abi(state.listed) + abi("0")[2:] + abi("1")[2:]
		values[callKey(Controller, "0x731f0c2b"+argument)] = abi(state.paused)
		values[callKey(QiAVAX, "0xd3bd2c72")], values[callKey(QiAVAX, "0x3b1d21a2")] = abi("0"), abi("0")
		client, _ := fixtureClient(t, values)
		batch, err := NewLendingCollector(client).Collect(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		o := batch.Items[0].Observation
		if o.Rate == nil || !o.Rate.IsZero() || !o.PoolCash.IsZero() || o.Availability != state.availability || o.RemainingCapacity != nil || o.Capacity != nil {
			t.Fatalf("zero rate/cash mapped incorrectly: %+v", o)
		}
	}
}

func TestLendingRejectsWrongIdentityMalformedABIAndOverflow(t *testing.T) {
	argument := strings.Repeat("0", 24) + QiAVAX[2:]
	for _, test := range []struct{ address, selector, value string }{
		{QiAVAX, "0x313ce567", abi("18")}, {QiAVAX, "0x840bbeac", abi("0")},
		{QiAVAX, "0x5fe3b567", "0x" + strings.Repeat("0", 24) + SAVAX[2:]},
		{QiAVAX, "0x5fe3b567", "0x1" + strings.Repeat("0", 23) + Controller[2:]},
		{QiAVAX, "0xbd6d894d", abi("0")}, {QiAVAX, "0xbd6d894d", abi("1")},
		{QiAVAX, "0xd3bd2c72", "0x"}, {QiAVAX, "0xd3bd2c72", "0x" + strings.Repeat("f", 64)},
		{QiAVAX, "0x18160ddd", "0x" + strings.Repeat("f", 64)},
		{QiAVAX, "0x3b1d21a2", "0x" + strings.Repeat("f", 64)},
		{Controller, "0x8e8f294b" + argument, abi("1")},
		{Controller, "0x8e8f294b" + argument, abi("1") + abi("0")[2:] + abi("2")[2:]},
		{Controller, "0x731f0c2b" + argument, abi("2")},
	} {
		t.Run(test.selector+"/"+test.value, func(t *testing.T) {
			values := lendingValues()
			values[callKey(test.address, test.selector)] = test.value
			client, _ := fixtureClient(t, values)
			if batch, err := NewLendingCollector(client).Collect(context.Background()); err == nil || len(batch.Items) != 0 {
				t.Fatalf("invalid lending state returned batch=%+v err=%v", batch, err)
			}
		})
	}
}
