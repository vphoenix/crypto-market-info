package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	chstore "github.com/vphoenix/crypto-market-info/internal/storage/clickhouse"
)

type Config struct {
	ClickHouse          chstore.Config
	BinanceSpotSymbols  []string
	BinancePerpSymbols  []string
	OKXSpotSymbols      []string
	OKXPerpSymbols      []string
	BinanceSpotREST     string
	BinanceFuturesREST  string
	BinanceSpotWS       string
	BinanceFuturesWS    string
	OKXREST             string
	OKXWS               string
	FundingEnabled      bool
	MinuteQueueCapacity int
}

func Load() (Config, error) {
	addresses := list("CLICKHOUSE_ADDRS", "127.0.0.1:9000")
	funding, err := boolean("FUNDING_ENABLED", true)
	if err != nil {
		return Config{}, err
	}
	queue, err := integer("MINUTE_QUEUE_CAPACITY", 128)
	if err != nil {
		return Config{}, err
	}
	return Config{
		ClickHouse:         chstore.Config{Addresses: addresses, Database: value("CLICKHOUSE_DATABASE", "crypto_market_info"), Username: value("CLICKHOUSE_USERNAME", "default"), Password: os.Getenv("CLICKHOUSE_PASSWORD"), DialTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, MaxAttempts: 3, RetryDelay: 250 * time.Millisecond},
		BinanceSpotSymbols: list("BINANCE_SPOT_SYMBOLS", "BTCUSDT"), BinancePerpSymbols: list("BINANCE_PERP_SYMBOLS", "BTCUSDT"),
		OKXSpotSymbols: list("OKX_SPOT_SYMBOLS", "BTC-USDT"), OKXPerpSymbols: list("OKX_PERP_SYMBOLS", "BTC-USDT-SWAP"),
		BinanceSpotREST: value("BINANCE_SPOT_REST_URL", "https://api.binance.com"), BinanceFuturesREST: value("BINANCE_FUTURES_REST_URL", "https://fapi.binance.com"),
		BinanceSpotWS: value("BINANCE_SPOT_WS_URL", "wss://stream.binance.com:9443/ws"), BinanceFuturesWS: value("BINANCE_FUTURES_WS_URL", "wss://fstream.binance.com/ws"),
		OKXREST: value("OKX_REST_URL", "https://www.okx.com"), OKXWS: value("OKX_WS_URL", "wss://ws.okx.com:8443/ws/v5/public"),
		FundingEnabled: funding, MinuteQueueCapacity: queue,
	}, nil
}

func value(key, fallback string) string {
	if raw, ok := os.LookupEnv(key); ok {
		return strings.TrimSpace(raw)
	}
	return fallback
}
func list(key, fallback string) []string {
	raw := value(key, fallback)
	if raw == "" || raw == "-" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}
func boolean(key string, fallback bool) (bool, error) {
	raw, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}
	parsed, err := strconv.ParseBool(strings.TrimSpace(raw))
	if err != nil {
		return false, fmt.Errorf("%s: %w", key, err)
	}
	return parsed, nil
}
func integer(key string, fallback int) (int, error) {
	raw, ok := os.LookupEnv(key)
	if !ok {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil || parsed <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", key)
	}
	return parsed, nil
}
