package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/config"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/model"
	marketyield "github.com/vphoenix/crypto-market-info/internal/yield"
	"github.com/vphoenix/crypto-market-info/internal/yield/aave"
	"github.com/vphoenix/crypto-market-info/internal/yield/ankr"
	"github.com/vphoenix/crypto-market-info/internal/yield/avalanche"
	"github.com/vphoenix/crypto-market-info/internal/yield/benqi"
	"github.com/vphoenix/crypto-market-info/internal/yield/okxearn"
	"github.com/vphoenix/crypto-market-info/internal/yield/solana"
)

type failingYieldCollector struct {
	mu    sync.Mutex
	calls int
}

func (c *failingYieldCollector) Collect(context.Context) (marketyield.Batch, error) {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	return marketyield.Batch{}, errors.New("source unavailable")
}

type unusedYieldSink struct{}

func (unusedYieldSink) WriteYieldBatch(context.Context, marketyield.Batch) error { return nil }

func TestSelectSymbolsIsExactOrderedAndRejectsMissing(t *testing.T) {
	available := []model.Instrument{{ExchangeSymbol: "A"}, {ExchangeSymbol: "B"}}
	selected, err := selectSymbols("X", available, []string{"B", "A"})
	if err != nil {
		t.Fatal(err)
	}
	if selected[0].ExchangeSymbol != "B" || selected[1].ExchangeSymbol != "A" {
		t.Fatalf("selected=%+v", selected)
	}
	if _, err = selectSymbols("X", available, []string{"a"}); err == nil {
		t.Fatal("case-folded symbol was accepted")
	}
}

func TestFailingYieldSourceDoesNotStopOtherComponent(t *testing.T) {
	collector := &failingYieldCollector{}
	runner := &marketyield.Runner{Source: "failing-yield", Collector: collector, Sink: unusedYieldSink{}, Interval: time.Hour, RetryInterval: time.Millisecond, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	progress := make(chan struct{}, 3)
	marketComponent := func(ctx context.Context) error {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				select {
				case progress <- struct{}{}:
				default:
				}
			}
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	finished := make(chan error, 1)
	go func() {
		finished <- runComponents(ctx, []component{{name: "yield", run: runner.Run}, {name: "market", run: marketComponent}})
	}()
	for range 3 {
		select {
		case <-progress:
		case <-time.After(time.Second):
			t.Fatal("market component stopped while yield source was failing")
		}
	}
	deadline := time.Now().Add(time.Second)
	for {
		collector.mu.Lock()
		calls := collector.calls
		collector.mu.Unlock()
		if calls >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("yield runner did not retry continuous failures")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
}

func TestSOLYieldCollectorsRegisterSecondPhaseAsSeparateSources(t *testing.T) {
	rpc := solana.NewClient("https://solana.test")
	reader := &solana.Reader{Client: rpc}
	specs := solYieldCollectors(config.Config{JitoSOLBaseURL: "https://jito.test", MarinadeAPYBaseURL: "https://marinade.test", KaminoBaseURL: "https://kamino.test", SaveBaseURL: "https://save.test"}, rpc, reader)
	if len(specs) != 9 {
		t.Fatalf("fixed SOL collectors=%d", len(specs))
	}
	seen := make(map[string]int, len(specs))
	for _, spec := range specs {
		seen[spec.source]++
		if spec.collector == nil {
			t.Fatalf("source %q has nil collector", spec.source)
		}
	}
	for _, source := range []string{"solana-stakepool-lainesol", "solana-stakepool-jupsol", "solana-stakepool-hsol", "kamino-main-sol", "save-main-sol"} {
		if seen[source] != 1 {
			t.Fatalf("source %q registered %d times", source, seen[source])
		}
	}
}

func TestAVAXOnlyEnablesRegistryAndSixUniqueCollectors(t *testing.T) {
	if yieldEnabled(config.Config{}) || !yieldEnabled(config.Config{AVAXYieldEnabled: true}) {
		t.Fatal("AVAX-only configuration must enable the same yield startup/registry path")
	}
	const rpcEndpoint = "https://avalanche.test/ext/bc/C/rpc"
	specs := avaxYieldCollectors(config.Config{AVAXYieldEnabled: true, OKXREST: "https://okx.test", AvalancheRPCURL: rpcEndpoint})
	if len(specs) != 6 {
		t.Fatalf("AVAX collector count=%d", len(specs))
	}
	seen := make(map[string]bool, len(specs))
	for i, source := range []string{"okx-avax-flexible", "aave-v3-avax", "aave-v4-avax", "benqi-savax", "ankr-ankravax", "benqi-avax-lending"} {
		if specs[i].source != source || specs[i].name == "" || specs[i].collector == nil {
			t.Fatalf("AVAX source[%d]=%+v", i, specs[i])
		}
		if seen[specs[i].source] {
			t.Fatalf("duplicate AVAX source %q", specs[i].source)
		}
		seen[specs[i].source] = true
	}
	okxCollector := specs[0].collector.(*okxearn.Collector)
	v3 := specs[1].collector.(*aave.V3Collector)
	v4 := specs[2].collector.(*aave.V4Collector)
	staking := specs[3].collector.(*benqi.StakingCollector)
	ankrCollector := specs[4].collector.(*ankr.Collector)
	lending := specs[5].collector.(*benqi.LendingCollector)
	if okxCollector.BaseURL != "https://okx.test" || v3.Endpoint != aave.DefaultV3Endpoint || v4.Endpoint != aave.DefaultV4Endpoint {
		t.Fatal("phase-one HTTP endpoints changed")
	}
	rpc := staking.Client
	if rpc == nil || ankrCollector.Client != rpc || lending.Client != rpc || rpc.Endpoint != rpcEndpoint {
		t.Fatal("the three new sources must share the configured RPC client and gate")
	}
	clients := []*http.Client{okxCollector.HTTPClient, v3.HTTPClient, v4.HTTPClient, rpc.HTTPClient}
	gates := []*exchange.RequestGate{okxCollector.Retry.Cooldown, v3.Retry.Cooldown, v4.Retry.Cooldown, rpc.Retry.Cooldown}
	for i, client := range clients {
		if client == nil || client.Timeout != 20*time.Second || gates[i] == nil {
			t.Fatalf("client group %d must have a gate and 20s timeout", i)
		}
		for previous := 0; previous < i; previous++ {
			if client == clients[previous] || gates[i] == gates[previous] {
				t.Fatalf("client groups %d and %d share HTTP state or a cooldown gate", previous, i)
			}
		}
	}
	defaultSpecs := avaxYieldCollectors(config.Config{})
	if defaultSpecs[3].collector.(*benqi.StakingCollector).Client.Endpoint != avalanche.DefaultRPCURL {
		t.Fatal("empty RPC configuration did not use the public default")
	}
}

func TestAVAXProtocolFailureLeavesSharedRPCUsable(t *testing.T) {
	at := time.Now().UTC().Truncate(time.Millisecond)
	var requestsSeen atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestsSeen.Add(1)
		var requests []struct {
			ID     int
			Method string
			Params []json.RawMessage
		}
		if err := json.NewDecoder(r.Body).Decode(&requests); err != nil {
			t.Errorf("decode RPC batch: %v", err)
			http.Error(w, "invalid fixture request", http.StatusBadRequest)
			return
		}
		responses := make([]map[string]any, 0, len(requests))
		for _, request := range requests {
			response := map[string]any{"jsonrpc": "2.0", "id": request.ID}
			var result any
			switch request.Method {
			case "eth_chainId":
				result = "0xa86a"
			case "eth_getBlockByNumber":
				result = map[string]string{"number": "0x5966630", "hash": "0x" + strings.Repeat("1", 64), "timestamp": fmt.Sprintf("0x%x", at.Unix())}
			case "eth_getBalance":
				result = "0x0"
			case "eth_call":
				var call struct{ To, Data string }
				if len(request.Params) < 1 || json.Unmarshal(request.Params[0], &call) != nil {
					t.Error("invalid contract call")
					http.Error(w, "invalid fixture call", http.StatusBadRequest)
					return
				}
				if call.To == benqi.SAVAX {
					// A contract-specific RPC error must not poison the shared
					// client or stop a different route from collecting.
					response["error"] = map[string]any{"code": -32000, "message": "BENQI fixture failure"}
					responses = append(responses, response)
					continue
				}
				if call.To != ankr.Token {
					t.Errorf("unexpected contract %q", call.To)
				}
				switch call.Data {
				case "0x313ce567":
					result = fmt.Sprintf("0x%064x", 18)
				case "0x71ca337d":
					result = fmt.Sprintf("0x%064x", uint64(1_000_000_000_000_000_000))
				default:
					t.Errorf("unexpected Ankr selector %q", call.Data)
				}
			default:
				t.Errorf("unexpected RPC method %q", request.Method)
			}
			response["result"] = result
			responses = append(responses, response)
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(responses); err != nil {
			t.Errorf("encode RPC fixture: %v", err)
		}
	}))
	defer server.Close()
	specs := avaxYieldCollectors(config.Config{AVAXYieldEnabled: true, AvalancheRPCURL: server.URL})
	if requestsSeen.Load() != 0 {
		t.Fatal("collector construction performed an RPC startup probe")
	}
	rpc := specs[3].collector.(*benqi.StakingCollector).Client
	rpc.HTTPClient = server.Client()
	rpc.Retry = exchange.HTTPRetryConfig{MaxAttempts: 1, Cooldown: exchange.NewRequestGate(0)}
	rpc.Now = func() time.Time { return at }
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	failed, err := specs[3].collector.Collect(ctx)
	if err == nil || len(failed.Items) != 0 {
		t.Fatalf("failing BENQI source produced batch=%+v err=%v", failed, err)
	}
	success, err := specs[4].collector.Collect(ctx)
	if err != nil || len(success.Items) != 1 || success.Source != "ankr-ankravax" {
		t.Fatalf("BENQI failure affected Ankr: batch=%+v err=%v", success, err)
	}
	if err := success.NormalizeAndValidateForLiveCollection(); err != nil {
		t.Fatalf("Ankr must still produce a complete live batch: %v", err)
	}
	if requestsSeen.Load() != 4 {
		t.Fatalf("RPC requests=%d, want an anchor/state pair per route", requestsSeen.Load())
	}
}
