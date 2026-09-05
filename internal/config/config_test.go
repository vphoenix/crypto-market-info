package config

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
)

func TestLoadYieldConfiguration(t *testing.T) {
	t.Setenv("JUSTLEND_YIELD_ENABLED", "true")
	t.Setenv("JUSTLEND_BASE_URL", "https://justlend.test")
	t.Setenv("TRON_STAKING_YIELD_ENABLED", "true")
	t.Setenv("TRON_HTTP_URL", "https://tron.test")
	t.Setenv("SOL_YIELD_ENABLED", "true")
	t.Setenv("SOLANA_RPC_URL", "https://solana.test")
	t.Setenv("SOL_VALIDATOR_VOTE_ACCOUNTS", "CcaHc2L43ZWjwCHART3oZoJvHLAe9hzT2DJNUpBzoTN1")
	t.Setenv("JITO_SOL_BASE_URL", "https://jito.test")
	t.Setenv("MARINADE_APY_BASE_URL", "https://apy.test")
	t.Setenv("MARINADE_VALIDATORS_BASE_URL", "https://validators.test")
	t.Setenv("KAMINO_BASE_URL", "https://kamino.test")
	t.Setenv("SAVE_BASE_URL", "https://save.test")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.JustLendYieldEnabled || cfg.JustLendBaseURL != "https://justlend.test" || !cfg.TRONStakingYieldEnabled || cfg.TRONHTTPURL != "https://tron.test" || !cfg.SOLYieldEnabled || cfg.SolanaRPCURL != "https://solana.test" || len(cfg.SOLValidatorVoteAccounts) != 1 || cfg.JitoSOLBaseURL != "https://jito.test" || cfg.MarinadeAPYBaseURL != "https://apy.test" || cfg.MarinadeValidatorsBaseURL != "https://validators.test" || cfg.KaminoBaseURL != "https://kamino.test" || cfg.SaveBaseURL != "https://save.test" {
		t.Fatalf("yield config=%+v", cfg)
	}
}

func TestBybitConfigurationIsOptIn(t *testing.T) {
	for _, key := range []string{"BYBIT_PERP_SYMBOLS", "BYBIT_REST_URL", "BYBIT_WS_URL"} {
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
	}
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.BybitPerpSymbols) != 0 || cfg.BybitREST != "https://api.bybit.com" || cfg.BybitWS != "wss://stream.bybit.com/v5/public/linear" {
		t.Fatalf("Bybit defaults=%+v", cfg)
	}
	t.Setenv("BYBIT_PERP_SYMBOLS", "BTCUSDT,ETHUSDT")
	t.Setenv("BYBIT_REST_URL", "https://bybit.test")
	t.Setenv("BYBIT_WS_URL", "wss://bybit.test/v5/public/linear")
	cfg, err = Load()
	if err != nil || len(cfg.BybitPerpSymbols) != 2 || cfg.BybitPerpSymbols[1] != "ETHUSDT" || cfg.BybitREST != "https://bybit.test" || cfg.BybitWS != "wss://bybit.test/v5/public/linear" {
		t.Fatalf("Bybit overrides=%+v err=%v", cfg, err)
	}
}

func TestLoadRejectsInvalidOrDuplicateSOLVoteAccounts(t *testing.T) {
	t.Setenv("SOL_VALIDATOR_VOTE_ACCOUNTS", "bad")
	if _, err := Load(); err == nil {
		t.Fatal("invalid Solana vote account accepted")
	}
	t.Setenv("SOL_VALIDATOR_VOTE_ACCOUNTS", "CcaHc2L43ZWjwCHART3oZoJvHLAe9hzT2DJNUpBzoTN1,CcaHc2L43ZWjwCHART3oZoJvHLAe9hzT2DJNUpBzoTN1")
	if _, err := Load(); err == nil {
		t.Fatal("duplicate Solana vote account accepted")
	}
}

func TestLoadRejectsInvalidYieldBoolean(t *testing.T) {
	t.Setenv("JUSTLEND_YIELD_ENABLED", "sometimes")
	if _, err := Load(); err == nil {
		t.Fatal("invalid yield boolean accepted")
	}
}

func TestAVAXYieldConfigurationIsOptIn(t *testing.T) {
	for _, key := range []string{"AVAX_YIELD_ENABLED", "AVALANCHE_RPC_URL"} {
		t.Setenv(key, "")
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
	}
	cfg, err := Load()
	if err != nil || cfg.AVAXYieldEnabled || cfg.AvalancheRPCURL != "https://api.avax.network/ext/bc/C/rpc" {
		t.Fatalf("AVAX defaults: enabled=%t rpc=%q err=%v", cfg.AVAXYieldEnabled, cfg.AvalancheRPCURL, err)
	}
	t.Setenv("AVAX_YIELD_ENABLED", "true")
	cfg, err = Load()
	if err != nil || !cfg.AVAXYieldEnabled {
		t.Fatalf("AVAX opt-in failed: enabled=%t err=%v", cfg.AVAXYieldEnabled, err)
	}
	t.Setenv("AVAX_YIELD_ENABLED", "sometimes")
	if _, err = Load(); err == nil {
		t.Fatal("invalid AVAX boolean accepted")
	}
}

func TestAvalancheRPCConfigurationDoesNotProbeOrBlockStartup(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, "RPC is unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	t.Setenv("AVAX_YIELD_ENABLED", "true")
	for _, endpoint := range []string{"  " + server.URL + "/ext/bc/C/rpc  ", "://invalid-rpc-url"} {
		t.Setenv("AVALANCHE_RPC_URL", endpoint)
		cfg, err := Load()
		if err != nil || !cfg.AVAXYieldEnabled || cfg.AvalancheRPCURL != strings.TrimSpace(endpoint) {
			t.Fatalf("RPC override must be loaded without probing: endpoint=%q err=%v", endpoint, err)
		}
	}
	if requests.Load() != 0 {
		t.Fatalf("configuration performed %d RPC startup probes", requests.Load())
	}
}
