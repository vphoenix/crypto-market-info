package justlend

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/exchange"
	marketyield "github.com/vphoenix/crypto-market-info/internal/yield"
)

const DefaultBaseURL = "https://openapi.just.network"

type Client struct {
	BaseURL string
	HTTP    *http.Client
	Retry   exchange.HTTPRetryConfig
}

type Snapshot struct {
	STRX      STRXData
	JTokens   []JToken
	Mining    map[string]map[string]string
	Vaults    []Vault
	V2Time    time.Time
	RawSTRX   []byte
	RawJToken []byte
	RawMining []byte
	RawVault  []byte
}

type STRXData struct {
	StakeInfo STRXStakeInfo `json:"stakeInfo"`
}
type STRXStakeInfo struct {
	Address           string `json:"strxAddress"`
	Symbol            string `json:"symbol"`
	Decimals          string `json:"decimal"`
	TotalSupply       string `json:"totalSupply"`
	ExchangeRate      string `json:"exchangeRate"`
	TotalUnderlying   string `json:"totalUnderlying"`
	UnderlyingDecimal string `json:"underlyingDecimal"`
	SupplyRate        string `json:"supplyRate"`
}
type JToken struct {
	Address           string `json:"address"`
	Symbol            string `json:"symbol"`
	UnderlyingSymbol  string `json:"underlyingSymbol"`
	UnderlyingAddress string `json:"underlyingAddress"`
	UnderlyingDecimal int    `json:"underlyingDecimal"`
	SupplyRate        string `json:"supplyRate"`
	ExchangeRate      string `json:"exchangeRate"`
	TotalSupply       string `json:"totalSupply"`
}
type Vault struct {
	Chain             string `json:"chain"`
	Address           string `json:"vaultAddress"`
	Name              string `json:"vaultName"`
	Symbol            string `json:"vaultSymbol"`
	AssetAddress      string `json:"assetAddress"`
	AssetSymbol       string `json:"assetSymbol"`
	AssetDecimals     int    `json:"assetDecimals"`
	TotalSupplyAmount string `json:"totalSupplyAmount"`
	APY               string `json:"apy"`
	PerformanceFee    string `json:"performanceFee"`
}

func NewClient(baseURL string) *Client {
	if strings.TrimSpace(baseURL) == "" {
		baseURL = DefaultBaseURL
	}
	return &Client{BaseURL: strings.TrimRight(baseURL, "/"), HTTP: &http.Client{Timeout: 20 * time.Second}, Retry: exchange.DefaultHTTPRetryConfig()}
}

func (c *Client) Fetch(ctx context.Context) (Snapshot, error) {
	if c == nil || c.BaseURL == "" {
		return Snapshot{}, fmt.Errorf("JustLend base URL is required")
	}
	paths := []string{"/lend/strx", "/lend/jtoken", "/mining/apy", "/v2/index/vault/list?deposit=TRX&allPage=1&allPageSize=20"}
	raw := make([][]byte, len(paths))
	for index, path := range paths {
		payload, err := exchange.Get(ctx, c.HTTP, c.BaseURL+path, c.Retry)
		if err != nil {
			return Snapshot{}, fmt.Errorf("JustLend GET %s: %w", path, err)
		}
		raw[index] = payload
	}
	var strx struct {
		Code    int      `json:"code"`
		Message string   `json:"message"`
		Data    STRXData `json:"data"`
	}
	var tokens struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		Data    struct {
			TokenList []JToken `json:"tokenList"`
		} `json:"data"`
	}
	var mining struct {
		Code    int                          `json:"code"`
		Message string                       `json:"message"`
		Data    map[string]map[string]string `json:"data"`
	}
	var vaults struct {
		Code      int          `json:"code"`
		Message   string       `json:"message"`
		Timestamp stringNumber `json:"timestamp"`
		Data      struct {
			AllVaults struct {
				List []Vault `json:"list"`
			} `json:"allVaults"`
		} `json:"data"`
	}
	for index, target := range []any{&strx, &tokens, &mining, &vaults} {
		if err := marketyield.DecodeJSON(raw[index], target); err != nil {
			return Snapshot{}, fmt.Errorf("decode %s: %w", paths[index], err)
		}
	}
	if strx.Code != 0 {
		return Snapshot{}, fmt.Errorf("%s business code %d: %s", paths[0], strx.Code, strx.Message)
	}
	if tokens.Code != 0 {
		return Snapshot{}, fmt.Errorf("%s business code %d: %s", paths[1], tokens.Code, tokens.Message)
	}
	if mining.Code != 0 {
		return Snapshot{}, fmt.Errorf("%s business code %d: %s", paths[2], mining.Code, mining.Message)
	}
	if vaults.Code != 200 {
		return Snapshot{}, fmt.Errorf("%s business code %d: %s", paths[3], vaults.Code, vaults.Message)
	}
	if tokens.Data.TokenList == nil {
		return Snapshot{}, fmt.Errorf("%s missing tokenList", paths[1])
	}
	if mining.Data == nil {
		return Snapshot{}, fmt.Errorf("%s missing data", paths[2])
	}
	if vaults.Data.AllVaults.List == nil {
		return Snapshot{}, fmt.Errorf("%s missing allVaults.list", paths[3])
	}
	millis, err := strconv.ParseInt(string(vaults.Timestamp), 10, 64)
	if err != nil || millis <= 0 {
		return Snapshot{}, fmt.Errorf("invalid V2 timestamp %q", vaults.Timestamp)
	}
	return Snapshot{STRX: strx.Data, JTokens: tokens.Data.TokenList, Mining: mining.Data, Vaults: vaults.Data.AllVaults.List,
		V2Time: time.UnixMilli(millis).UTC(), RawSTRX: raw[0], RawJToken: raw[1], RawMining: raw[2], RawVault: raw[3]}, nil
}

type stringNumber string

func (n *stringNumber) UnmarshalJSON(payload []byte) error {
	if len(payload) == 0 || payload[0] == '"' {
		return fmt.Errorf("expected JSON integer")
	}
	*n = stringNumber(payload)
	return nil
}

func sourceHash(payloads ...marketyield.Payload) *string {
	return marketyield.Ptr(marketyield.HashPayloads(payloads...))
}
