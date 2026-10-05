package lst

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex"
)

// Admission tests use the real wall clock and mocked HTTP, retaining the
// production three-second poll delay and original request/evidence gate.
func mvp6AdmissionFixture(t *testing.T, reply func(uint64, uint64) (*http.Response, error)) (*Collector, *int, *[]time.Time) {
	t.Helper()
	calls := 0
	var sent []time.Time
	tr, _ := transportFixture(t, func(req *http.Request) (*http.Response, error) {
		calls++
		sent = append(sent, Now())
		var call struct {
			Id     uint64
			Method string
			Params []json.RawMessage
		}
		if e := json.NewDecoder(req.Body).Decode(&call); e != nil {
			t.Fatal(e)
		}
		var tag string
		if call.Method != "eth_getBlockByNumber" || len(call.Params) != 2 || json.Unmarshal(call.Params[0], &tag) != nil || tag != "latest" {
			t.Fatal("unexpected source request during admission", call.Method, tag)
		}
		return reply(call.Id, uint64(calls))
	})
	tr.cfg.Clock = realClock{}
	tr.started = Now().Add(-2 * time.Minute)
	tr.cfg.RPCRequestsPerMinute = 32
	tr.MarkInitialized()
	m, e := LoadManifest("../../config/lst-lido-ethereum.json")
	if e != nil {
		t.Fatal(e)
	}
	c := &Collector{RPC: &RPC{Transport: tr, URL: "https://rpc.example"}, Manifest: m, Archive: tr.archive, Store: &runnerMemoryStore{}, StateDir: t.TempDir(), EntryRoutesPerRound: 1}
	c.Metadata.Instrument.ID = 1
	return c, &calls, &sent
}

func mvp6HeaderReply(id, attempt uint64, at time.Time) *http.Response {
	return transportReply(200, fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":{"number":"0x%x","hash":"0x%064x","parentHash":"0x%064x","timestamp":"0x%x","transactions":[]}}`, id, 100+attempt, 100+attempt, 99+attempt, at.Unix()))
}

func TestHeadAdmissionOldOldFreshPreservesThreeAttempts(t *testing.T) {
	c, calls, sent := mvp6AdmissionFixture(t, func(id, attempt uint64) (*http.Response, error) {
		at := Now().Add(-time.Minute)
		if attempt == 3 {
			at = Now()
		}
		return mvp6HeaderReply(id, attempt, at), nil
	})
	head, responses, err := c.marketHead(context.Background())
	if err != nil || *calls != 3 || len(responses) != 3 || head.Number != 103 || head.Hash != strings.Repeat("\x00", 31)+"g" {
		t.Fatal("fresh third head not selected", *calls, len(responses), head.Number, err)
	}
	for i := 1; i < len(*sent); i++ {
		if (*sent)[i].Sub((*sent)[i-1]) < 3*time.Second {
			t.Fatal("head polls burst", *sent)
		}
	}
	seen := map[string]bool{}
	for _, response := range responses {
		if len(response.PayloadHash) != 32 || response.RequestedAt.IsZero() || response.ReceivedAt.Before(response.RequestedAt) || response.AvailableAt.Before(response.ReceivedAt) {
			t.Fatal("attempt lost evidence or clock", response)
		}
		seen[response.PayloadHash] = true
	}
	if len(seen) != 3 || Now().Sub(head.Time) > 10*time.Second || !head.Response.AvailableAt.Equal(responses[2].AvailableAt) {
		t.Fatal("poll responses were collapsed or selected head changed")
	}
}

func TestHeadAdmissionFailuresKeepUnknownBatchAndEvidence(t *testing.T) {
	for _, name := range []string{"three_old", "three_future", "429", "timeout", "malformed_timestamp"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var payloads []string
			c, calls, _ := mvp6AdmissionFixture(t, func(id, attempt uint64) (*http.Response, error) {
				if name == "timeout" {
					return nil, context.DeadlineExceeded
				}
				var response *http.Response
				switch name {
				case "429":
					response = transportReply(429, fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"error":{"code":15,"message":"rate limit"}}`, id))
				case "malformed_timestamp":
					response = transportReply(200, fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":{"number":"0x65","hash":"0x%064x","parentHash":"0x%064x","timestamp":"not-a-quantity","transactions":[]}}`, id, 101, 100))
				default:
					at := Now().Add(-time.Minute)
					if name == "three_future" {
						at = Now().Add(time.Minute)
					}
					response = mvp6HeaderReply(id, attempt, at)
				}
				// Retain the exact mocked body without consuming the returned response.
				raw, e := io.ReadAll(response.Body)
				if e != nil {
					t.Fatal(e)
				}
				response.Body = io.NopCloser(strings.NewReader(string(raw)))
				payloads = append(payloads, dex.Digest(raw).String())
				return response, nil
			})
			b, err := c.Market(context.Background(), nil)
			wantCalls := 1
			if name == "three_old" || name == "three_future" {
				wantCalls = 3
			}
			if err != nil || *calls != wantCalls || b.Capture.Status != "partial" || b.Capture.Canonical || len(b.Quotes) != 8 || len(b.Protocols) != 1 || b.Protocols[0].StateStatus != "unknown" {
				t.Fatal("admission failure lost its batch", name, *calls, len(b.Quotes), b.Capture, err)
			}
			if err = Validate(b); err != nil {
				t.Fatal("failure cannot be stored", name, err)
			}
			if name == "three_old" || name == "three_future" {
				if b.Capture.ToBlock == nil || *b.Capture.ToBlock != 103 || b.Capture.ToBlockTime == nil || !strings.Contains(b.Capture.Reason, "head_admission_not_fresh") {
					t.Fatal("parsed stale anchor or refusal missing", b.Capture)
				}
			} else if b.Capture.ToBlock != nil || b.Capture.ToBlockHash != nil || b.Capture.ToBlockTime != nil {
				t.Fatal("unparsed failure invented an anchor", b.Capture)
			}
			if name == "timeout" && !strings.Contains(b.Capture.Reason, "transport_http_timeout") || name == "429" && (!strings.Contains(b.Capture.Reason, "http=429") || !strings.Contains(b.Capture.Reason, "rpc=15")) {
				t.Fatal("source cause lost", b.Capture.Reason)
			}
			notScheduled := 0
			for _, q := range b.Quotes {
				if q.BuyStatus != "unknown" || q.ConversionStatus != "unknown" || q.ExitStatus != "unknown" || q.HedgeStatus != "unknown" || q.BuyLstOutRaw != nil || q.MarkPriceTickE8 != nil || len(q.ChainPayloadHashes) != 0 {
					t.Fatal("failure reused an earlier quote", q)
				}
				if q.HedgeDepthLastUpdateId != nil || q.HedgeDepthEventTime != nil || q.HedgeDepthPayloadHash != nil || q.HedgeDepthRequestedAt != nil || q.MarkSourceTime != nil || q.MarkPayloadHash != nil || q.MarkRequestedAt != nil {
					t.Fatal("unrequested CEX invented source evidence", q)
				}
				if q.TimingStatus == "not_scheduled" {
					notScheduled++
				} else if q.TimingStatus != "stale" || !strings.Contains(q.Reason, b.Capture.Reason) {
					t.Fatal("failed planned member was hidden", q)
				}
			}
			if notScheduled != 7 {
				t.Fatal("eight member identity changed", notScheduled)
			}
			var root dex.Hash
			copy(root[:], b.Capture.EvidenceRootHash)
			raw, e := c.Archive.Get(root)
			if e != nil {
				t.Fatal(e)
			}
			var proof struct{ Responses []string }
			if e = json.Unmarshal(raw, &proof); e != nil || len(proof.Responses) != wantCalls {
				t.Fatal("poll response proof omitted", string(raw), e)
			}
			seenBodies := map[string]bool{}
			for _, h := range proof.Responses {
				var hash dex.Hash
				encoded, e := ParseHex(h, 32)
				if e != nil {
					t.Fatal(e)
				}
				copy(hash[:], encoded)
				archived, e := c.Archive.Get(hash)
				if e != nil {
					t.Fatal(e)
				}
				var response struct {
					Response                []byte
					RequestedAt, ReceivedAt time.Time
				}
				if e = json.Unmarshal(archived, &response); e != nil || response.RequestedAt.IsZero() || response.ReceivedAt.Before(response.RequestedAt) {
					t.Fatal("archived source clocks missing", string(archived), e)
				}
				seenBodies[dex.Digest(response.Response).String()] = true
			}
			for _, h := range payloads {
				if !seenBodies[h] {
					t.Fatal("successful stale response proof omitted", h)
				}
			}
			if name == "timeout" {
				var hash dex.Hash
				encoded, e := ParseHex(proof.Responses[0], 32)
				if e != nil {
					t.Fatal(e)
				}
				copy(hash[:], encoded)
				failure, e := c.Archive.Get(hash)
				if e != nil || !strings.Contains(string(failure), "transport_http_timeout") {
					t.Fatal("network attempt proof missing", e)
				}
			}
			if err = c.Commit(context.Background(), b); err != nil || len(c.Store.(*runnerMemoryStore).batches) != 1 {
				t.Fatal("unknown observation was not committed", err)
			}
		})
	}
}
