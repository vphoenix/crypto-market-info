package deribit

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"github.com/vphoenix/crypto-market-info/internal/orderbook"
)

func fixtureMetadata(t *testing.T) map[string]ParsedInstrument {
	t.Helper()
	out := map[string]ParsedInstrument{}
	for _, currency := range []string{"BTC", "ETH", "USDC"} {
		for _, kind := range []string{"option", "future"} {
			raw, err := os.ReadFile(fmt.Sprintf("testdata/metadata-%s-%s.json", currency, kind))
			if err != nil {
				t.Fatal(err)
			}
			items, excluded, err := DecodeInstruments(raw, currency, kind, time.Date(2026, 9, 20, 14, 14, 0, 0, time.UTC))
			if err != nil {
				t.Fatal(err)
			}
			if len(excluded) != 0 {
				t.Fatalf("unexpected exclusions %+v", excluded)
			}
			for _, p := range items {
				p.Spec.Instrument.ID = uint32(len(out) + 1)
				p.Rule.InstrumentID = p.Spec.Instrument.ID
				if err := p.Spec.Validate(); err != nil {
					t.Fatal(err)
				}
				if err := p.Rule.Validate(); err != nil {
					t.Fatal(err)
				}
				out[p.Spec.Instrument.ExchangeSymbol] = p
			}
		}
	}
	return out
}

func TestPublicFixturesFourFamiliesAndSequence(t *testing.T) {
	metadata := fixtureMetadata(t)
	if len(metadata) != 12 {
		t.Fatalf("fixture instruments=%d", len(metadata))
	}
	file, err := os.Open("testdata/books.jsonl")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	books := map[string]*orderbook.DerivativeBook{}
	epoch := uuid.New()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 4096), MaxMessageBytes)
	count := 0
	for scanner.Scan() {
		var row struct {
			Received time.Time `json:"received_at"`
			Payload  string    `json:"payload"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			t.Fatal(err)
		}
		var envelope struct {
			Params struct {
				Data struct {
					Name string `json:"instrument_name"`
				} `json:"data"`
			} `json:"params"`
		}
		if err := json.Unmarshal([]byte(row.Payload), &envelope); err != nil {
			t.Fatal(err)
		}
		name := envelope.Params.Data.Name
		p, ok := metadata[name]
		if !ok {
			t.Fatalf("missing fixture metadata %s", name)
		}
		book := books[name]
		if book == nil {
			book, _ = orderbook.NewDerivative(p.Spec.Instrument.ID, false, 20000)
			_ = book.Reset(epoch)
			books[name] = book
		}
		u, err := DecodeBook([]byte(row.Payload), p.Spec.Instrument, epoch, row.Received)
		if err != nil {
			t.Fatal(err)
		}
		if err := book.Apply(u); err != nil {
			t.Fatal(err)
		}
		s, q := book.Current()
		if !q.StreamValid || len(s.Bids) > 10 || len(s.Asks) > 10 {
			t.Fatal("invalid fixture book")
		}
		count++
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if count != 24 || len(books) != 12 {
		t.Fatalf("messages=%d books=%d", count, len(books))
	}
	for _, p := range metadata {
		i := p.Spec.Instrument
		if !i.ContractMultiplier.Equal(options.StorageUnit().Shift(8)) {
			t.Fatal("native amount multiplier changed")
		}
		if i.MarketType == model.MarketDelivery && p.Spec.SourceInstrumentType == "reversed" && p.Spec.NativeAmountKind != "usd" {
			t.Fatal("inverse USD face lost")
		}
		if i.MarketType == model.MarketDelivery && p.Spec.SourceInstrumentType == "linear" && p.Spec.NativeAmountKind != "base" {
			t.Fatal("linear base amount lost")
		}
	}
}

func TestTradingChangesDoNotChangeEconomicIdentity(t *testing.T) {
	raw, err := os.ReadFile("testdata/metadata-BTC-option.json")
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 9, 20, 14, 0, 0, 0, time.UTC)
	old, _, err := DecodeInstruments(raw, "BTC", "option", at)
	if err != nil {
		t.Fatal(err)
	}
	var e struct {
		JSONRPC string                       `json:"jsonrpc"`
		Result  []map[string]json.RawMessage `json:"result"`
	}
	_ = json.Unmarshal(raw, &e)
	e.Result[0]["tick_size"] = json.RawMessage("1e-5")
	e.Result[0]["min_trade_amount"] = json.RawMessage("0.01")
	changed, _ := json.Marshal(e)
	now, _, err := DecodeInstruments(changed, "BTC", "option", at.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if old[0].Spec.Version() != now[0].Spec.Version() || old[0].Spec.EvidenceHash == now[0].Spec.EvidenceHash {
		t.Fatal("identity must exclude refreshed evidence and tick rules")
	}
	old[0].Rule.InstrumentID = 1
	now[0].Rule.InstrumentID = 1
	if old[0].Rule.ID() == now[0].Rule.ID() {
		t.Fatal("changed rule needs new revision")
	}
	e.Result[0]["strike"] = json.RawMessage("99999")
	changed, _ = json.Marshal(e)
	economic, _, err := DecodeInstruments(changed, "BTC", "option", at)
	if err != nil {
		t.Fatal(err)
	}
	if economic[0].Spec.Version() == old[0].Spec.Version() {
		t.Fatal("payoff change did not change identity")
	}
}

func TestMetadataRequiredClassificationAndInactiveState(t *testing.T) {
	raw, err := os.ReadFile("testdata/metadata-BTC-future.json")
	if err != nil {
		t.Fatal(err)
	}
	var e struct {
		JSONRPC string                       `json:"jsonrpc"`
		Result  []map[string]json.RawMessage `json:"result"`
	}
	_ = json.Unmarshal(raw, &e)
	at := time.Date(2026, 9, 20, 14, 0, 0, 0, time.UTC)
	for _, test := range []struct{ field, value string }{{"settlement_period", "null"}, {"settlement_period", `"unknown"`}, {"state", `"invented"`}, {"is_active", "null"}} {
		_ = json.Unmarshal(raw, &e)
		e.Result[0][test.field] = json.RawMessage(test.value)
		body, _ := json.Marshal(e)
		if _, _, err := DecodeInstruments(body, "BTC", "future", at); err == nil {
			t.Fatalf("accepted %s=%s", test.field, test.value)
		}
	}
	_ = json.Unmarshal(raw, &e)
	e.Result[0]["is_active"] = json.RawMessage("false")
	body, _ := json.Marshal(e)
	items, _, err := DecodeInstruments(body, "BTC", "future", at)
	if err != nil || items[0].Active {
		t.Fatalf("inactive state lost: %v", err)
	}
	for _, duplicate := range []bool{false, true} {
		_ = json.Unmarshal(raw, &e)
		e.Result[0]["Instrument_Name"] = e.Result[0]["instrument_name"]
		if !duplicate {
			delete(e.Result[0], "instrument_name")
		}
		body, _ := json.Marshal(e)
		if _, _, err := DecodeInstruments(body, "BTC", "future", at); err == nil {
			t.Fatal("accepted case-insensitive metadata alias")
		}
	}
}

func TestStrictEnvelopeAndNumbers(t *testing.T) {
	for _, raw := range []string{`{"x":1,"x":2}`, `{"x":1,"X":2}`, `{"k":1,"K":2}`, `{"x":{"a":0,"a":1}}`, `[] true`, strings.Repeat("[", 34) + strings.Repeat("]", 34)} {
		var v any
		if err := decode([]byte(raw), &v); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
	i := fixtureMetadata(t)["BTC-23SEP26-80500-C"].Spec.Instrument
	epoch := uuid.New()
	at := time.Now().UTC()
	base := `{"jsonrpc":"2.0","method":"subscription","params":{"channel":"book.BTC-23SEP26-80500-C.100ms","data":{"type":"change","instrument_name":"BTC-23SEP26-80500-C","timestamp":1789913662000,"change_id":12,"prev_change_id":10,"bids":[["change",1e-4,1e-1]],"asks":[]}}}`
	if _, err := DecodeBook([]byte(base), i, epoch, at); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{strings.Replace(base, `,"prev_change_id":10`, "", 1), strings.Replace(base, `"asks":[]`, `"asks":null`, 1), strings.Replace(base, `1e-4`, `1e-9`, 1), strings.Replace(base, `"change",1e-4`, `"mystery",1e-4`, 1), strings.Replace(base, `.100ms`, `.none.10.100ms`, 1)} {
		if _, err := DecodeBook([]byte(bad), i, epoch, at); err == nil {
			t.Fatal("accepted malformed book", bad)
		}
	}
	if _, err := DecodeBook([]byte(strings.Replace(base, `"instrument_name"`, `"Instrument_Name"`, 1)), i, epoch, at); err == nil {
		t.Fatal("accepted single wrong-case required field")
	}
}
