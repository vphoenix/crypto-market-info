package config

import (
	"os"
	"testing"

	"github.com/vphoenix/crypto-market-info/internal/universe"
)

func clearPerpetualEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{"BINANCE_PERP_SYMBOLS", "OKX_PERP_SYMBOLS", "BYBIT_PERP_SYMBOLS", "MINUTE_QUEUE_CAPACITY", "PERP_UNIVERSE_INCLUDE", "PERP_UNIVERSE_EXCLUDE", "BINANCE_PERP_EXCLUDE_SYMBOLS", "OKX_PERP_EXCLUDE_SYMBOLS", "BYBIT_PERP_EXCLUDE_SYMBOLS"} {
		t.Setenv(key, "")
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPerpetualConfigurationThreeStatesAndUnsetDefaults(t *testing.T) {
	clearPerpetualEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	modes := map[string]universe.SelectionMode{}
	for _, v := range cfg.PerpetualSelection.Venues {
		modes[v.Venue] = v.Mode
	}
	if modes["Binance"] != universe.Explicit || modes["OKX"] != universe.Explicit || modes["Bybit"] != universe.Disabled || cfg.MinuteQueueCapacity != 0 {
		t.Fatalf("legacy defaults changed: %+v", cfg.PerpetualSelection)
	}
	for _, key := range []string{"BINANCE_PERP_SYMBOLS", "OKX_PERP_SYMBOLS", "BYBIT_PERP_SYMBOLS"} {
		t.Setenv(key, "auto")
	}
	cfg, err = Load()
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range cfg.PerpetualSelection.Venues {
		if v.Mode != universe.Auto {
			t.Fatal("auto state not preserved")
		}
	}
	for _, key := range []string{"BINANCE_PERP_SYMBOLS", "OKX_PERP_SYMBOLS", "BYBIT_PERP_SYMBOLS"} {
		t.Setenv(key, "-")
	}
	if _, err = Load(); err != nil {
		t.Fatal("all-disabled perpetual prevents spot/yield-only run")
	}
	t.Setenv("BINANCE_PERP_SYMBOLS", "auto")
	if _, err = Load(); err == nil {
		t.Fatal("single enabled perpetual venue accepted")
	}
}

func TestPerpetualConfigurationRejectsAmbiguousAndOverBudgetValues(t *testing.T) {
	for _, test := range []struct{ key, value string }{
		{"BINANCE_PERP_SYMBOLS", ""}, {"BINANCE_PERP_SYMBOLS", "BTCUSDT,"},
		{"MINUTE_QUEUE_CAPACITY", ""}, {"MINUTE_QUEUE_CAPACITY", "0"},
		{"BINANCE_PERP_BOOK_TOPICS_PER_CONNECTION", "201"}, {"OKX_PERP_BOOK_TOPICS_PER_CONNECTION", "21"}, {"BYBIT_PERP_BOOK_TOPICS_PER_CONNECTION", "0"},
		{"PERP_MAX_TOTAL_INSTRUMENTS", "0"}, {"PERP_MAX_TOTAL_BUFFERED_EVENTS", ""},
		{"PERP_UNIVERSE_INCLUDE", "btc-USDT-PERP"}, {"PERP_ASSET_ALIASES_FILE", ""},
	} {
		t.Run(test.key+"="+test.value, func(t *testing.T) {
			clearPerpetualEnv(t)
			t.Setenv(test.key, test.value)
			if _, err := Load(); err == nil {
				t.Fatal("invalid perpetual config accepted")
			}
		})
	}
}

func TestPerpetualConfigurationRejectsIncludeConflicts(t *testing.T) {
	clearPerpetualEnv(t)
	t.Setenv("BINANCE_PERP_EXCLUDE_SYMBOLS", "BTCUSDT")
	if _, err := Load(); err == nil {
		t.Fatal("raw include/exclude conflict accepted")
	}
	t.Setenv("BINANCE_PERP_EXCLUDE_SYMBOLS", "")
	t.Setenv("PERP_UNIVERSE_INCLUDE", "BTC-USDT-PERP")
	t.Setenv("PERP_UNIVERSE_EXCLUDE", "BTC-USDT-PERP")
	if _, err := Load(); err == nil {
		t.Fatal("canonical include/exclude conflict accepted")
	}
}
