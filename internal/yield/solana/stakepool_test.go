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
	put64(100_044)
	put64(100_000)
	put64(9)
	data = append(data, make([]byte, 8+8+32)...)
	fee(10, 1)
	data = append(data, 0)    // next epoch fee
	data = append(data, 0, 0) // preferred validators
	fee(1000, 2)
	fee(1000, 3)
	data = append(data, 0) // next withdrawal fee
	data = append(data, 0) // referral
	data = append(data, 0) // SOL deposit authority
	fee(1000, 4)
	data = append(data, 0) // SOL referral
	data = append(data, 0) // SOL withdrawal authority
	fee(1000, 5)
	data = append(data, 0) // next SOL withdrawal fee
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
		t.Fatalf("unused fixed-account tail rejected: %v", err)
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
	snapshot, err := reader.Read(context.Background(), PoolConfig{Address: BSOLPoolAddress, Mint: BSOLMintAddress})
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Slot != 77 || snapshot.BlockHeight != 70 || snapshot.BlockHash != "hash77" || len(snapshot.Payloads) != 3 {
		t.Fatalf("snapshot=%+v", snapshot)
	}
	owner = "wrong-owner"
	if _, err = reader.Read(context.Background(), PoolConfig{Address: BSOLPoolAddress, Mint: BSOLMintAddress}); err == nil || !strings.Contains(err.Error(), "owner") {
		t.Fatalf("wrong owner error=%v", err)
	}
	owner = StakePoolProgram
	epoch = 10
	if _, err = reader.Read(context.Background(), PoolConfig{Address: BSOLPoolAddress, Mint: BSOLMintAddress}); err == nil || !strings.Contains(err.Error(), "last update epoch") {
		t.Fatalf("stale epoch error=%v", err)
	}
	epoch = 9
	if _, err = reader.Read(context.Background(), PoolConfig{Address: BSOLPoolAddress, Mint: JitoMintAddress}); err == nil || !strings.Contains(err.Error(), "mint") {
		t.Fatalf("wrong mint error=%v", err)
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

type fakePoolReader struct {
	snapshot PoolSnapshot
	err      error
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
