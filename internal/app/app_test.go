package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/config"
	"github.com/vphoenix/crypto-market-info/internal/model"
	marketyield "github.com/vphoenix/crypto-market-info/internal/yield"
	"github.com/vphoenix/crypto-market-info/internal/yield/aave"
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

func TestAVAXOnlyEnablesRegistryAndThreeIndependentCollectors(t *testing.T) {
	if yieldEnabled(config.Config{}) || !yieldEnabled(config.Config{AVAXYieldEnabled: true}) {
		t.Fatal("AVAX-only configuration must enable the same yield startup/registry path")
	}
	specs := avaxYieldCollectors(config.Config{AVAXYieldEnabled: true, OKXREST: "https://okx.test"})
	if len(specs) != 3 {
		t.Fatalf("AVAX collector count=%d", len(specs))
	}
	for i, source := range []string{"okx-avax-flexible", "aave-v3-avax", "aave-v4-avax"} {
		if specs[i].source != source || specs[i].name == "" || specs[i].collector == nil {
			t.Fatalf("AVAX source[%d]=%+v", i, specs[i])
		}
	}
	okxCollector := specs[0].collector.(*okxearn.Collector)
	v3 := specs[1].collector.(*aave.V3Collector)
	v4 := specs[2].collector.(*aave.V4Collector)
	if okxCollector.BaseURL != "https://okx.test" || v3.Endpoint != aave.DefaultV3Endpoint || v4.Endpoint != aave.DefaultV4Endpoint ||
		okxCollector.Retry.Cooldown == v3.Retry.Cooldown || v3.Retry.Cooldown == v4.Retry.Cooldown || okxCollector.Retry.Cooldown == v4.Retry.Cooldown {
		t.Fatal("wrong AVAX endpoints or shared source cooldown gates")
	}
	if okxCollector.HTTPClient.Timeout != 20*time.Second || v3.HTTPClient.Timeout != 20*time.Second || v4.HTTPClient.Timeout != 20*time.Second {
		t.Fatal("AVAX HTTP timeouts must be 20s")
	}
}
