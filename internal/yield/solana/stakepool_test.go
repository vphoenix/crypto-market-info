package solana

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	marketyield "github.com/vphoenix/crypto-market-info/internal/yield"
)

func stakePoolFixture(t *testing.T, mint string) []byte {
	return stakePoolFixtureVariant(t, mint, false)
}

func stakePoolFixtureVariant(t *testing.T, mint string, allOptionalFields bool) []byte {
	t.Helper()
	mintKey, err := DecodePubkey(mint)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte{1}
	data = append(data, make([]byte, 3*32)...)
	data = append(data, 1)
	data = append(data, make([]byte, 2*32)...)
	data = append(data, mintKey[:]...)
	data = append(data, make([]byte, 2*32)...)
	put64 := func(value uint64) {
		var encoded [8]byte
		binary.LittleEndian.PutUint64(encoded[:], value)
		data = append(data, encoded[:]...)
	}
	fee := func(denominator, numerator uint64) { put64(denominator); put64(numerator) }
	optionalPubkey := func() {
		if allOptionalFields {
			data = append(data, 1)
			data = append(data, make([]byte, 32)...)
			return
		}
		data = append(data, 0)
	}
	futureFee := func() {
		if allOptionalFields {
			data = append(data, 1)
			fee(1000, 1)
			return
		}
		data = append(data, 0)
	}
	put64(100_044)
	put64(100_000)
	put64(9)
	data = append(data, make([]byte, 8+8+32)...)
	fee(10, 1)
	futureFee()      // next epoch fee
	optionalPubkey() // preferred deposit validator
	optionalPubkey() // preferred withdraw validator
	fee(1000, 2)
	fee(1000, 3)
	futureFee()            // next withdrawal fee
	data = append(data, 0) // referral
	optionalPubkey()       // SOL deposit authority
	fee(1000, 4)
	data = append(data, 0) // SOL referral
	optionalPubkey()       // SOL withdrawal authority
	fee(1000, 5)
	futureFee() // next SOL withdrawal fee
	put64(100_000)
	put64(100_000)
	data = append(data, make([]byte, 611-len(data))...)
	return data
}

func TestParseStakePoolStrictlyReadsRequiredFields(t *testing.T) {
	fixture := stakePoolFixture(t, BSOLMintAddress)
	state, err := ParseStakePool(fixture)
	if err != nil {
		t.Fatal(err)
	}
	if state.TotalLamports != 100_044 || state.PoolTokenSupply != 100_000 || state.LastUpdateEpoch != 9 || state.EpochFee.Numerator != 1 || state.StakeWithdrawalFee.Numerator != 3 || state.SOLDepositFee.Numerator != 4 {
		t.Fatalf("state=%+v", state)
	}
	if _, err = ParseStakePool(stakePoolFixtureVariant(t, BSOLMintAddress, true)); err != nil {
		t.Fatalf("maximum-length Borsh prefix rejected: %v", err)
	}
	for name, data := range map[string][]byte{"truncated": fixture[:len(fixture)-1], "trailing": append(append([]byte(nil), fixture...), 0), "wrong type": append([]byte{2}, fixture[1:]...)} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseStakePool(data); err == nil {
				t.Fatal("invalid pool accepted")
			}
		})
	}
	badPadding := append([]byte(nil), fixture...)
	badPadding[len(badPadding)-1] = 1
	if _, err := ParseStakePool(badPadding); err != nil {
		t.Fatalf("unused preallocated tail rejected: %v", err)
	}
	badFee := append([]byte(nil), fixture...)
	// epoch_fee begins after account header, totals and Lockup.
	offset := 1 + 3*32 + 1 + 2*32 + 32 + 2*32 + 3*8 + 8 + 8 + 32
	binary.LittleEndian.PutUint64(badFee[offset:offset+8], 1)
	binary.LittleEndian.PutUint64(badFee[offset+8:offset+16], 2)
	if _, err := ParseStakePool(badFee); err == nil {
		t.Fatal("fee above one accepted")
	}
	zeroDenominator := append([]byte(nil), fixture...)
	binary.LittleEndian.PutUint64(zeroDenominator[offset:offset+8], 0)
	binary.LittleEndian.PutUint64(zeroDenominator[offset+8:offset+16], 1)
	if _, err := ParseStakePool(zeroDenominator); err == nil {
		t.Fatal("nonzero fee numerator with zero denominator accepted")
	}
	badTag := append([]byte(nil), fixture...)
	badTag[offset+16] = 3
	if _, err := ParseStakePool(badTag); err == nil {
		t.Fatal("invalid FutureEpoch tag accepted")
	}
}

func TestReaderChecksOwnerMintEpochAndFinalizedAnchor(t *testing.T) {
	fixture := stakePoolFixture(t, BSOLMintAddress)
	owner := StakePoolProgram
	epoch := uint64(9)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Method string `json:"method"`
		}
		if err := marketDecode(r, &body); err != nil {
			t.Fatal(err)
		}
		switch body.Method {
		case "getAccountInfo":
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":1,"result":{"context":{"slot":77},"value":{"data":[%q,"base64"],"executable":false,"owner":%q}}}`, base64.StdEncoding.EncodeToString(fixture), owner)
		case "getEpochInfo":
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":1,"result":{"epoch":%d}}`, epoch)
		case "getBlock":
			fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":{"blockHeight":70,"blockhash":"hash77","blockTime":1787500000}}`)
		default:
			t.Fatalf("method=%s", body.Method)
		}
	}))
	defer server.Close()
	reader := &Reader{Client: NewClient(server.URL)}
	snapshot, err := reader.Read(context.Background(), PoolConfig{Program: StakePoolProgram, Address: BSOLPoolAddress, Mint: BSOLMintAddress})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Slot != 77 || snapshot.BlockHeight != 70 || snapshot.BlockHash != "hash77" || len(snapshot.Payloads) != 3 {
		t.Fatalf("snapshot=%+v", snapshot)
	}
	owner = "wrong-owner"
	if _, err = reader.Read(context.Background(), PoolConfig{Program: StakePoolProgram, Address: BSOLPoolAddress, Mint: BSOLMintAddress}); err == nil || !strings.Contains(err.Error(), "owner") {
		t.Fatalf("wrong owner error=%v", err)
	}
	owner = StakePoolProgram
	epoch = 10
	if _, err = reader.Read(context.Background(), PoolConfig{Program: StakePoolProgram, Address: BSOLPoolAddress, Mint: BSOLMintAddress}); err == nil || !strings.Contains(err.Error(), "last update epoch") {
		t.Fatalf("stale epoch error=%v", err)
	}
	epoch = 9
	if _, err = reader.Read(context.Background(), PoolConfig{Program: StakePoolProgram, Address: BSOLPoolAddress, Mint: JitoMintAddress}); err == nil || !strings.Contains(err.Error(), "mint") {
		t.Fatalf("wrong mint error=%v", err)
	}
	if _, err = reader.Read(context.Background(), PoolConfig{Program: "not-a-program", Address: BSOLPoolAddress, Mint: BSOLMintAddress}); err == nil || !strings.Contains(err.Error(), "program") {
		t.Fatalf("invalid program error=%v", err)
	}
}

func marketDecode(r *http.Request, target any) error {
	defer r.Body.Close()
	return json.NewDecoder(r.Body).Decode(target)
}

func TestBSOLCollectorUsesDecimalMathAndChainEvidence(t *testing.T) {
	reader := fakePoolReader{snapshot: PoolSnapshot{State: mustPoolState(t), BlockHeight: 70, BlockHash: "hash77", BlockTime: 1787500000,
		Payloads: []marketyield.Payload{{Name: "a", Body: []byte("one")}}}}
	collector := &BSOLCollector{Reader: reader, Now: func() time.Time { return time.Unix(1787500100, 0).UTC() }}
	batch, err := collector.Collect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	o := batch.Items[0].Observation
	if o.Rate.String() != "0.080355" || o.ExposureRatio.String() != "1.00044" || o.TVL.String() != "0.000100044" || o.EntryFeeRate.String() != "0.004" || o.ExitFeeRate.String() != "0.003" || o.PerformanceFeeRate.String() != "0.1" {
		t.Fatalf("observation=%+v", o)
	}
	if o.UnbondingSeconds != nil || o.BlockHeight == nil || *o.BlockHeight != 70 || o.Finality == nil || *o.Finality != "finalized" || o.SourcePayloadHash == nil {
		t.Fatalf("evidence=%+v", o)
	}
}

func TestStakePoolProductsKeepFixedIdentities(t *testing.T) {
	tests := []struct {
		name, program, address, mint, provider, productCode, productName, positionAsset string
		product                                                                         StakePoolProduct
	}{
		{name: "bSOL", product: BSOLProduct, program: "SPoo1Ku8WFXoNDMHPsrGSTSG1Y47rzgn41SLUNakuHy", address: "stk9ApL5HeVAwPLr3TLhDXdZS8ptVu7zp6ov8HFDuMi", mint: "bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1", provider: "BlazeStake", productCode: "bsol", productName: "BlazeStake bSOL", positionAsset: "solana:mainnet:spl:bSo13r4TkiE4KumL71LsHTPpL2euBYLFx6h9HP3piy1"},
		{name: "laineSOL", product: LaineSOLProduct, program: "SPoo1Ku8WFXoNDMHPsrGSTSG1Y47rzgn41SLUNakuHy", address: "2qyEeSAWKfU18AFthrF7JA8z8ZCi1yt76Tqs917vwQTV", mint: "LAinEtNLgpmCP9Rvsf5Hn8W6EhNiKLZQti1xfWMLy6X", provider: "Laine", productCode: "lainesol", productName: "Laine laineSOL", positionAsset: "solana:mainnet:spl:LAinEtNLgpmCP9Rvsf5Hn8W6EhNiKLZQti1xfWMLy6X"},
		{name: "JupSOL", product: JupSOLProduct, program: "SPMBzsVUuoHA4Jm6KunbsotaahvVikZs1JyTW6iJvbn", address: "8VpRhuxa7sUUepdY3kQiTmX9rS5vx4WgaXiAnXq4KCtr", mint: "jupSoLaHXQiZZTSfEWMTRRgpnyFm8f6sZdosWBjx93v", provider: "Jupiter", productCode: "jupsol", productName: "Jupiter JupSOL", positionAsset: "solana:mainnet:spl:jupSoLaHXQiZZTSfEWMTRRgpnyFm8f6sZdosWBjx93v"},
		{name: "hSOL", product: HSOLProduct, program: "SP12tWFxD9oJsVWNavTTBZvMbA6gkAmxtVgxdqvyvhY", address: "3wK2g8ZdzAH8FJ7PKr2RcvGh7V9VYson5hrVsJM5Lmws", mint: "he1iusmfkpAdwvxLNGV8Y1iSbj4rUy6yMhEA3fotn9A", provider: "Helius", productCode: "hsol", productName: "Helius hSOL", positionAsset: "solana:mainnet:spl:he1iusmfkpAdwvxLNGV8Y1iSbj4rUy6yMhEA3fotn9A"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			product := test.product
			if product.Program != test.program || product.Address != test.address || product.Mint != test.mint || product.Provider != test.provider || product.ProductCode != test.productCode || product.ProductName != test.productName || product.PositionAssetKey != test.positionAsset {
				t.Fatalf("fixed product=%+v", product)
			}
			reader := &capturingPoolReader{snapshot: PoolSnapshot{State: mustPoolState(t), BlockHeight: 70, BlockHash: "hash77", BlockTime: 1787500000,
				Payloads: []marketyield.Payload{{Name: "a", Body: []byte("one")}}}}
			collector := &StakePoolCollector{Reader: reader, Product: product, Now: func() time.Time { return time.Unix(1787500100, 0).UTC() }}
			batch, err := collector.Collect(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			expectedConfig := PoolConfig{Program: test.program, Address: test.address, Mint: test.mint}
			if reader.config != expectedConfig || batch.Source != "solana-stakepool-"+test.productCode || len(batch.Items) != 1 {
				t.Fatalf("config=%+v batch=%+v", reader.config, batch)
			}
			item := batch.Items[0]
			if item.Route.Provider != test.provider || item.Route.ProductCode != test.productCode || item.Route.ProductName != test.productName || item.Route.PositionAssetKey != test.positionAsset || item.Route.ContractAddress == nil || *item.Route.ContractAddress != test.address {
				t.Fatalf("route=%+v", item.Route)
			}
			if err = batch.NormalizeAndValidateForLiveCollection(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

type fakePoolReader struct {
	snapshot PoolSnapshot
	err      error
}

type capturingPoolReader struct {
	config   PoolConfig
	snapshot PoolSnapshot
}

func (f *capturingPoolReader) Read(_ context.Context, config PoolConfig) (PoolSnapshot, error) {
	f.config = config
	return f.snapshot, nil
}

func (f fakePoolReader) Read(context.Context, PoolConfig) (PoolSnapshot, error) {
	return f.snapshot, f.err
}
func mustPoolState(t *testing.T) StakePoolState {
	state, err := ParseStakePool(stakePoolFixture(t, BSOLMintAddress))
	if err != nil {
		t.Fatal(err)
	}
	return state
}
