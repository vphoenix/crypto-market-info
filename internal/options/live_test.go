package options

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestMetadataObservationScopeStatus(t *testing.T) {
	at := time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)
	hash := PayloadHash([]byte("source"))
	base := MetadataObservation{AttemptID: uuid.New(), RunID: uuid.New(), InstrumentID: 1, Symbol: "BTC-30OCT26", Scope: "BTC:future", SourceURL: "https://www.deribit.com/api/v2/public/get_instruments", RequestedAt: at, ObservedAt: at, PayloadHash: hash}
	for _, status := range []string{"request_error", "parse_error", "missing", "definition_changed", "complete"} {
		for _, complete := range []bool{false, true} {
			o := base
			o.Status, o.ScopeComplete = status, complete
			if complete {
				o.ScopeRawCount, o.ScopeAcceptedCount = 1, 1
			}
			if status == "complete" {
				o.DefinitionHash, o.TradingRuleID, o.State, o.Active = hash, hash, "open", true
			}
			want := complete == (status != "request_error" && status != "parse_error")
			if (o.Validate() == nil) != want {
				t.Fatalf("status=%s scope_complete=%v", status, complete)
			}
		}
	}
	o := base
	o.Status, o.ScopeComplete, o.DefinitionHash, o.TradingRuleID, o.State = "complete", true, hash, hash, "open"
	if o.Validate() == nil {
		t.Fatal("member accepted from empty response")
	}
}
