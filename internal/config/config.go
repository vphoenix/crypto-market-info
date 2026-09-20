package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	chstore "github.com/vphoenix/crypto-market-info/internal/storage/clickhouse"
	"github.com/vphoenix/crypto-market-info/internal/universe"
	"github.com/vphoenix/crypto-market-info/internal/yield/solana"
)

type Config struct {
	ClickHouse                chstore.Config
	BinanceSpotSymbols        []string
	BinancePerpSymbols        []string
	OKXSpotSymbols            []string
	OKXPerpSymbols            []string
	BybitPerpSymbols          []string
	BinanceSpotREST           string
	BinanceFuturesREST        string
	BinanceSpotWS             string
	BinanceFuturesWS          string // USDⓈ-M high-frequency public streams such as diff depth.
	BinanceMarketWS           string // USDⓈ-M regular market streams such as mark price.
	OKXREST                   string
	OKXWS                     string
	BybitREST                 string
	BybitWS                   string
	FundingEnabled            bool
	JustLendYieldEnabled      bool
	JustLendBaseURL           string
	TRONStakingYieldEnabled   bool
	TRONHTTPURL               string
	SOLYieldEnabled           bool
	AVAXYieldEnabled          bool
	AvalancheRPCURL           string
	SolanaRPCURL              string
	SOLValidatorVoteAccounts  []string
	JitoSOLBaseURL            string
	MarinadeAPYBaseURL        string
	MarinadeValidatorsBaseURL string
	KaminoBaseURL             string
	SaveBaseURL               string
	MinuteQueueCapacity       int
	PerpetualSelection        universe.SelectionConfig
	PerpAssetAliasesFile      string
	PerpMaxTotalInstruments   int
	PerpMaxTotalWSConnections int
	PerpMaxBufferedEvents     int
	MaxSampleSources          int
}

func Load() (Config, error) {
	addresses := list("CLICKHOUSE_ADDRS", "127.0.0.1:9000")
	funding, err := boolean("FUNDING_ENABLED", true)
	if err != nil {
		return Config{}, err
	}
	queue, err := integer("MINUTE_QUEUE_CAPACITY", 0)
	if err != nil {
		return Config{}, err
	}
	justLendYield, err := boolean("JUSTLEND_YIELD_ENABLED", false)
	if err != nil {
		return Config{}, err
	}
	tronYield, err := boolean("TRON_STAKING_YIELD_ENABLED", false)
	if err != nil {
		return Config{}, err
	}
	solYield, err := boolean("SOL_YIELD_ENABLED", false)
	if err != nil {
		return Config{}, err
	}
	avaxYield, err := boolean("AVAX_YIELD_ENABLED", false)
	if err != nil {
		return Config{}, err
	}
	voteAccounts := list("SOL_VALIDATOR_VOTE_ACCOUNTS", "-")
	seenVotes := make(map[string]struct{}, len(voteAccounts))
	for _, vote := range voteAccounts {
		if _, err = solana.DecodePubkey(vote); err != nil {
			return Config{}, fmt.Errorf("SOL_VALIDATOR_VOTE_ACCOUNTS contains invalid vote account %q: %w", vote, err)
		}
		if _, exists := seenVotes[vote]; exists {
			return Config{}, fmt.Errorf("SOL_VALIDATOR_VOTE_ACCOUNTS contains duplicate %q", vote)
		}
		seenVotes[vote] = struct{}{}
	}
	cfg := Config{
		ClickHouse:         chstore.Config{Addresses: addresses, Database: value("CLICKHOUSE_DATABASE", "crypto_market_info"), Username: value("CLICKHOUSE_USERNAME", "default"), Password: os.Getenv("CLICKHOUSE_PASSWORD"), DialTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, MaxAttempts: 3, RetryDelay: 250 * time.Millisecond},
		BinanceSpotSymbols: list("BINANCE_SPOT_SYMBOLS", "BTCUSDT"), BinancePerpSymbols: list("BINANCE_PERP_SYMBOLS", "BTCUSDT"),
		OKXSpotSymbols: list("OKX_SPOT_SYMBOLS", "BTC-USDT"), OKXPerpSymbols: list("OKX_PERP_SYMBOLS", "BTC-USDT-SWAP"),
		BybitPerpSymbols: list("BYBIT_PERP_SYMBOLS", "-"),
		BinanceSpotREST:  value("BINANCE_SPOT_REST_URL", "https://api.binance.com"), BinanceFuturesREST: value("BINANCE_FUTURES_REST_URL", "https://fapi.binance.com"),
		BinanceSpotWS: value("BINANCE_SPOT_WS_URL", "wss://stream.binance.com:443/ws"), BinanceFuturesWS: value("BINANCE_FUTURES_WS_URL", "wss://fstream.binance.com/public/ws"),
		BinanceMarketWS: value("BINANCE_FUTURES_MARKET_WS_URL", "wss://fstream.binance.com/market/ws"),
		OKXREST:         value("OKX_REST_URL", "https://www.okx.com"), OKXWS: value("OKX_WS_URL", "wss://ws.okx.com:8443/ws/v5/public"),
		BybitREST: value("BYBIT_REST_URL", "https://api.bybit.com"), BybitWS: value("BYBIT_WS_URL", "wss://stream.bybit.com/v5/public/linear"),
		FundingEnabled: funding, JustLendYieldEnabled: justLendYield, JustLendBaseURL: value("JUSTLEND_BASE_URL", "https://openapi.just.network"),
		TRONStakingYieldEnabled: tronYield, TRONHTTPURL: value("TRON_HTTP_URL", "https://api.trongrid.io"),
		SOLYieldEnabled: solYield, SolanaRPCURL: value("SOLANA_RPC_URL", "https://api.mainnet.solana.com"), SOLValidatorVoteAccounts: voteAccounts,
		AVAXYieldEnabled: avaxYield, AvalancheRPCURL: value("AVALANCHE_RPC_URL", "https://api.avax.network/ext/bc/C/rpc"),
		JitoSOLBaseURL: value("JITO_SOL_BASE_URL", "https://kobe.mainnet.jito.network"), MarinadeAPYBaseURL: value("MARINADE_APY_BASE_URL", "https://apy.marinade.finance"),
		MarinadeValidatorsBaseURL: value("MARINADE_VALIDATORS_BASE_URL", "https://validators-api.marinade.finance"), KaminoBaseURL: value("KAMINO_BASE_URL", "https://api.kamino.finance"),
		SaveBaseURL: value("SAVE_BASE_URL", "https://api.solend.fi"), MinuteQueueCapacity: queue,
	}
	if err = loadPerpetualConfig(&cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func loadPerpetualConfig(cfg *Config) error {
	for _, item := range []struct {
		venue, prefix, fallback string
		topics                  int
		legacy                  *[]string
	}{
		{"Binance", "BINANCE", "BTCUSDT", 200, &cfg.BinancePerpSymbols},
		{"OKX", "OKX", "BTC-USDT-SWAP", 20, &cfg.OKXPerpSymbols},
		{"Bybit", "BYBIT", "-", 20, &cfg.BybitPerpSymbols},
	} {
		v, err := universe.ParseVenueSelection(item.venue, value(item.prefix+"_PERP_SYMBOLS", item.fallback))
		if err != nil {
			return err
		}
		v.Exclude, err = universe.ParseList(value(item.prefix+"_PERP_EXCLUDE_SYMBOLS", ""))
		if err != nil {
			return err
		}
		v.TopicsPerConnection, err = integer(item.prefix+"_PERP_BOOK_TOPICS_PER_CONNECTION", item.topics)
		if err != nil {
			return err
		}
		if v.TopicsPerConnection > item.topics {
			return fmt.Errorf("%s topics per connection exceeds hard limit %d", item.venue, item.topics)
		}
		v.MaxInstruments, err = integer(item.prefix+"_PERP_MAX_INSTRUMENTS", 500)
		if err != nil {
			return err
		}
		*item.legacy = append([]string(nil), v.Include...)
		cfg.PerpetualSelection.Venues = append(cfg.PerpetualSelection.Venues, v)
	}
	var err error
	cfg.PerpetualSelection.Include, err = universe.ParseList(value("PERP_UNIVERSE_INCLUDE", ""))
	if err != nil {
		return err
	}
	cfg.PerpetualSelection.Exclude, err = universe.ParseList(value("PERP_UNIVERSE_EXCLUDE", ""))
	if err != nil {
		return err
	}
	cfg.PerpetualSelection, err = universe.NormalizeConfig(cfg.PerpetualSelection)
	if err != nil {
		return err
	}
	cfg.PerpAssetAliasesFile = value("PERP_ASSET_ALIASES_FILE", "config/perpetual-asset-aliases.json")
	if cfg.PerpAssetAliasesFile == "" || cfg.PerpAssetAliasesFile == "-" {
		return fmt.Errorf("PERP_ASSET_ALIASES_FILE must name a readable dictionary")
	}
	for _, item := range []struct {
		key      string
		fallback int
		out      *int
	}{
		{"PERP_MAX_TOTAL_INSTRUMENTS", 1000, &cfg.PerpMaxTotalInstruments},
		{"PERP_MAX_TOTAL_WS_CONNECTIONS", 128, &cfg.PerpMaxTotalWSConnections},
		{"PERP_MAX_TOTAL_BUFFERED_EVENTS", 1000000, &cfg.PerpMaxBufferedEvents},
		{"MARKET_DATA_MAX_SAMPLE_SOURCES", 1100, &cfg.MaxSampleSources},
	} {
		*item.out, err = integer(item.key, item.fallback)
		if err != nil {
			return err
		}
	}
	if value("MINUTE_WRITE_BATCH_INSTRUMENTS", "100") != "100" {
		return fmt.Errorf("MINUTE_WRITE_BATCH_INSTRUMENTS is fixed at 100 in this version")
	}
	return nil
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
