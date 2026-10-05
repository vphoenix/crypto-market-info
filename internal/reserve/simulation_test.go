package reserve

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
)

func simulationFixture(t *testing.T) (Manifest, Quote, dex.Block) {
	t.Helper()
	m, e := LoadManifest("../../config/reserve-ethereum.json")
	if e != nil {
		t.Fatal(e)
	}
	token := Address(m.Paths[0].Tokens[1])
	folio := Address(m.Folios[0].Address)
	entry := NewLeg(0, "entry", "exact_output", Address(USDC), token, Uint(10))
	exit := NewLeg(1, "exit", "exact_input", folio, Address(USDC), Uint(90))
	fill := func(l *Leg, p Path) {
		l.Status = "ok"
		l.AmountInRaw = Uint(10)
		l.AmountOutRaw = Uint(11)
		for _, a := range p.Tokens {
			l.PathTokens = append(l.PathTokens, Address(a))
		}
		for _, a := range p.Pools {
			l.PathPools = append(l.PathPools, Address(a))
		}
		l.PoolFees = append([]uint32{}, p.Fees...)
	}
	fill(&entry, m.Paths[0])
	fill(&exit, Reverse(m.Paths[12]))
	b := dex.Block{Anchor: dex.Anchor{ChainID: 1, Number: 100, Hash: dex.Digest([]byte("block")), Time: dex.Now(), Manifest: m.Hash}}
	q := Quote{ManifestHash: Hash(m.Hash), QuoteId: ID("quote"), PayloadHash: ID("proof"), Folio: folio, BlockNumber: b.Number, BlockHash: Hash(b.Hash), BlockTime: b.Time, RequestedBudgetRaw: Uint(100), GrossSharesRaw: Uint(100), NetSharesRaw: Uint(90), RouteKind: "mint", Status: "ok", Quality: "indicative_overlap", BasketAmounts: []Amount{{token, Uint(10)}}, DexLegs: []Leg{entry, exit}}
	return m, q, b
}
func TestSimulationKeepsCanonicalFailureAndReorgAsFailed(t *testing.T) {
	for _, mode := range []string{"changed", "unavailable", "canonical"} {
		t.Run(mode, func(t *testing.T) {
			m, q, b := simulationFixture(t)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var calls []struct {
					ID     uint64
					Method string
					Params []json.RawMessage
				}
				json.NewDecoder(r.Body).Decode(&calls)
				out := []map[string]any{}
				for _, c := range calls {
					var result any
					switch c.Method {
					case "eth_getCode":
						var address string
						json.Unmarshal(c.Params[0], &address)
						result = "0x01"
						if address == probeActor.String() {
							result = "0x"
						}
					case "eth_getStorageAt":
						var slot string
						json.Unmarshal(c.Params[1], &slot)
						result = fmt.Sprintf("0x%064x", 10)
						if slot == balanceSlot(probeActor) {
							result = fmt.Sprintf("0x%064x", 0)
						}
					case "eth_call":
						var obj map[string]string
						json.Unmarshal(c.Params[0], &obj)
						var raw []byte
						switch obj["data"][:10] {
						case "0x70a08231":
							raw = common.LeftPadBytes(Uint(10).Bytes(), 32)
						case "0xc45a0155":
							raw = common.LeftPadBytes(m.Factory[:], 32)
						case "0x4aa4a4fc":
							raw = common.LeftPadBytes(WETH[:], 32)
						default:
							raw, _ = probeABI.Methods["execute"].Outputs.Pack(Uint(10), Uint(11), Uint(90), Uint(100000))
						}
						result = "0x" + hex.EncodeToString(raw)
					case "eth_getBlockByNumber":
						if mode == "unavailable" {
							out = append(out, map[string]any{"jsonrpc": "2.0", "id": c.ID, "error": map[string]any{"code": -32016, "message": "Rate limit exceeded"}})
							continue
						}
						hash := b.Hash
						if mode == "changed" {
							hash = dex.Digest([]byte("other"))
						}
						result = map[string]string{"number": "0x64", "hash": hash.String(), "parentHash": b.Hash.String(), "timestamp": "0x1", "baseFeePerGas": "0x1", "miner": USDC.String()}
					}
					out = append(out, map[string]any{"jsonrpc": "2.0", "id": c.ID, "result": result})
				}
				json.NewEncoder(w).Encode(out)
			}))
			defer server.Close()
			rpc, e := ethereum.NewClient(server.URL, t.TempDir())
			if e != nil {
				t.Fatal(e)
			}
			c := Collector{RPC: rpc, Manifest: m}
			s, e := c.SimulateRoute(context.Background(), b, q)
			if e != nil {
				t.Fatal(e)
			}
			if mode == "canonical" {
				if s.Status != "joint_call_simulated" || s.AmountOutRaw.Cmp(big.NewInt(11)) != 0 {
					t.Fatal(s.Status, s.Reason)
				}
			} else if s.Status != "failed" || s.Reason == "" {
				t.Fatal("bad canonical result marked successful", s.Status, s.Reason)
			}
		})
	}
}
func TestSimulationRejectsUnlistedAndReversedPathIdentity(t *testing.T) {
	m, q, _ := simulationFixture(t)
	if _, e := planFor(m, q); e != nil {
		t.Fatal(e)
	}
	q.DexLegs[0].PathPools[0] = Address(USDT)
	if _, e := planFor(m, q); e == nil {
		t.Fatal("unlisted route simulated")
	}
}

func TestSimulationValidationRejectsCorruptionAndQuoteMismatch(t *testing.T) {
	_, q, b := simulationFixture(t)
	s := Simulation{ChainId: 1, ManifestHash: q.ManifestHash, QuoteId: q.QuoteId, BlockNumber: b.Number, BlockHash: q.BlockHash, BlockTime: b.Time, AvailableAt: dex.Now(), Folio: q.Folio, RouteKind: q.RouteKind, BudgetRaw: Uint(100), FundedRaw: Uint(1000), AmountInRaw: Uint(10), AmountOutRaw: Uint(11), SharesRaw: Uint(90), GasInternal: Ptr(uint64(100000)), WithinBudget: Ptr(true), Status: "joint_call_simulated", SimulatorHash: ID("runtime"), PayloadHash: ID("simulation-proof")}
	s.SimulationId = ID(struct{ Quote, Payload string }{s.QuoteId, s.PayloadHash})
	if e := ValidateSimulation(s); e != nil || !SimulationMatchesQuote(s, q) {
		t.Fatal(e)
	}
	for _, corrupt := range []func(*Simulation){
		func(s *Simulation) { s.SimulationId = ID("unbound") },
		func(s *Simulation) { s.Status = "unknown_success" },
		func(s *Simulation) { s.SharesRaw = nil },
		func(s *Simulation) { s.AmountOutRaw = new(big.Int).Lsh(Uint(1), 256) },
		func(s *Simulation) { s.AmountInRaw = big.NewInt(-1) },
		func(s *Simulation) { s.WithinBudget = Ptr(false) },
		func(s *Simulation) { s.FundedRaw = Uint(9) },
	} {
		bad := s
		corrupt(&bad)
		if ValidateSimulation(bad) == nil {
			t.Fatal("corrupt simulation accepted")
		}
	}
	q.RequestedBudgetRaw = Uint(101)
	if SimulationMatchesQuote(s, q) {
		t.Fatal("different budget joined")
	}
	s.Status, s.Reason = "failed", "simulation_canonical_check_failed"
	s.AmountInRaw, s.AmountOutRaw, s.SharesRaw = nil, nil, nil
	s.GasInternal, s.WithinBudget = nil, nil
	if e := ValidateSimulation(s); e != nil {
		t.Fatal("valid failed nullable record rejected", e)
	}
}
