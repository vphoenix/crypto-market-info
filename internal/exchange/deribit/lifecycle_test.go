package deribit

import (
	"encoding/json"
	"github.com/google/uuid"
	"os"
	"testing"
	"time"
)

func TestLifecycleActualProtocolAndStrictFailure(t *testing.T) {
	at := time.Now().UTC().Truncate(time.Microsecond)
	epoch := uuid.New()
	for _, tt := range []struct {
		kind, ch, raw string
		valid         bool
	}{
		{"status", "public/status", `{"jsonrpc":"2.0","id":3,"result":{"locked":"false"}}`, true},
		{"status", "public/status", `{"jsonrpc":"2.0","id":3,"result":{"locked":false}}`, false},
		{"status", "public/status", `{"jsonrpc":"2.0","id":3,"result":{"locked":"partial","locked_indices":["btc_usd"]}}`, true},
		{"status", "public/status", `{"jsonrpc":"2.0","id":3,"result":{"locked":"partial"}}`, false},
		{"message", "instrument.state.option.BTC", `{"jsonrpc":"2.0","method":"subscription","params":{"channel":"instrument.state.option.BTC","data":{"instrument_name":"BTC-30OCT26-80000-C","state":"halted","timestamp":1791000000000}}}`, true},
		{"message", "platform_state", `{"jsonrpc":"2.0","method":"subscription","params":{"channel":"platform_state","data":{"price_index":"sol_usdc","locked":true}}}`, true},
		{"message", "platform_state", `{"jsonrpc":"2.0","method":"subscription","params":{"channel":"platform_state","data":{}}}`, false},
	} {
		t.Run(tt.raw, func(t *testing.T) {
			o, err := DecodeLifecycle(StreamEvent{Kind: tt.kind, Channel: tt.ch, Raw: []byte(tt.raw), Epoch: epoch, Sequence: 1}, at)
			if (err == nil) != tt.valid {
				t.Fatalf("err=%v", err)
			}
			if err == nil && o.PayloadHash == "" {
				t.Fatal("missing evidence")
			}
		})
	}
}
func TestCreationPreservesOriginalPayloadAndUnpairedDefinition(t *testing.T) {
	raw, err := os.ReadFile("testdata/metadata-BTC-option.json")
	if err != nil {
		t.Fatal(err)
	}
	var frame struct {
		Result []json.RawMessage `json:"result"`
	}
	if err = json.Unmarshal(raw, &frame); err != nil {
		t.Fatal(err)
	}
	wire := append([]byte(`{"jsonrpc":"2.0","method":"subscription","params":{"channel":"instrument.creation.option.BTC","data":`), frame.Result[0]...)
	wire = append(wire, []byte(`}}`)...)
	r, err := DecodeCreation(StreamEvent{Kind: "message", Channel: "instrument.creation.option.BTC", Raw: wire}, time.Now().UTC().Truncate(time.Microsecond))
	if err != nil || len(r.Instruments) != 1 {
		t.Fatal(err)
	}
	p := r.Instruments[0]
	if p.Spec.EvidenceHash != r.PayloadHash || p.Rule.PayloadHash != r.PayloadHash {
		t.Fatal("wrapper used as source hash")
	}
	p.Rule.InstrumentID = 1
	p.Rule.SourceURL = "wss://www.deribit.com/ws/api/v2#instrument.creation.option.BTC"
	if err = p.Rule.Validate(); err != nil {
		t.Fatal(err)
	}
}
