package tron

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/exchange"
	marketyield "github.com/vphoenix/crypto-market-info/internal/yield"
)

const DefaultHTTPURL = "https://api.trongrid.io"

type Client struct {
	BaseURL string
	HTTP    *http.Client
	Retry   exchange.HTTPRetryConfig
	Now     func() time.Time
}

type Witness struct {
	Address   string      `json:"address"`
	VoteCount json.Number `json:"voteCount"`
	URL       string      `json:"url"`
}
type Block struct {
	ID     string
	Number uint64
	Time   time.Time
}
type Snapshot struct {
	NextMaintenance     int64
	StartBlock          Block
	EndBlock            Block
	Witnesses           []Witness
	Brokerage           map[string]int64
	Parameters          map[string]int64
	RawMaintenanceStart []byte
	RawStartBlock       []byte
	RawWitnesses        []byte
	RawParameters       []byte
	RawBrokerage        map[string][]byte
	RawEndBlock         []byte
	RawMaintenanceEnd   []byte
}

func NewClient(baseURL string) *Client {
	if strings.TrimSpace(baseURL) == "" {
		baseURL = DefaultHTTPURL
	}
	retry := exchange.DefaultHTTPRetryConfig()
	// TronGrid's anonymous public endpoint currently allows three requests per
	// second. Stay below that limit because one snapshot makes 127 sequential
	// brokerage requests in addition to its common anchor requests.
	retry.Cooldown = exchange.NewRequestGate(500 * time.Millisecond)
	return &Client{BaseURL: strings.TrimRight(baseURL, "/"), HTTP: &http.Client{Timeout: 20 * time.Second}, Retry: retry, Now: time.Now}
}

func (c *Client) Fetch(ctx context.Context) (Snapshot, error) {
	if c == nil || c.BaseURL == "" {
		return Snapshot{}, fmt.Errorf("TRON HTTP URL is required")
	}
	if _, err := url.ParseRequestURI(c.BaseURL); err != nil {
		return Snapshot{}, fmt.Errorf("invalid TRON HTTP URL: %w", err)
	}
	result := Snapshot{Brokerage: make(map[string]int64, 127), Parameters: make(map[string]int64), RawBrokerage: make(map[string][]byte, 127)}
	var err error
	if result.RawMaintenanceStart, result.NextMaintenance, err = c.nextMaintenance(ctx); err != nil {
		return Snapshot{}, err
	}
	now := time.Now
	if c.Now != nil {
		now = c.Now
	}
	until := time.UnixMilli(result.NextMaintenance).Sub(now().UTC())
	if until <= 0 || until < 2*time.Minute {
		return Snapshot{}, fmt.Errorf("TRON maintenance boundary is %s away", until)
	}
	if result.RawStartBlock, result.StartBlock, err = c.block(ctx); err != nil {
		return Snapshot{}, err
	}
	if result.RawWitnesses, result.Witnesses, err = c.witnesses(ctx); err != nil {
		return Snapshot{}, err
	}
	if result.RawParameters, result.Parameters, err = c.chainParameters(ctx); err != nil {
		return Snapshot{}, err
	}
	for _, witness := range result.Witnesses {
		raw, brokerage, fetchErr := c.brokerage(ctx, witness.Address)
		if fetchErr != nil {
			return Snapshot{}, fetchErr
		}
		result.RawBrokerage[witness.Address], result.Brokerage[witness.Address] = raw, brokerage
	}
	if result.RawEndBlock, result.EndBlock, err = c.block(ctx); err != nil {
		return Snapshot{}, err
	}
	var ending int64
	if result.RawMaintenanceEnd, ending, err = c.nextMaintenance(ctx); err != nil {
		return Snapshot{}, err
	}
	if ending != result.NextMaintenance {
		return Snapshot{}, fmt.Errorf("TRON maintenance changed from %d to %d during collection", result.NextMaintenance, ending)
	}
	return result, nil
}

func (c *Client) get(ctx context.Context, path string) ([]byte, error) {
	return exchange.Get(ctx, c.HTTP, c.BaseURL+path, c.Retry)
}
func (c *Client) post(ctx context.Context, path, body string) ([]byte, error) {
	return exchange.PostJSON(ctx, c.HTTP, c.BaseURL+path, []byte(body), c.Retry)
}
func decode(raw []byte, target any) error {
	return marketyield.DecodeJSON(raw, target)
}
func (c *Client) nextMaintenance(ctx context.Context) ([]byte, int64, error) {
	raw, err := c.get(ctx, "/wallet/getnextmaintenancetime")
	if err != nil {
		return nil, 0, err
	}
	var response struct {
		Num json.Number `json:"num"`
	}
	if err = decode(raw, &response); err != nil {
		return nil, 0, fmt.Errorf("decode next maintenance: %w", err)
	}
	value, err := strconv.ParseInt(response.Num.String(), 10, 64)
	if err != nil || value <= 0 {
		return nil, 0, fmt.Errorf("invalid next maintenance %q", response.Num)
	}
	return raw, value, nil
}
func (c *Client) block(ctx context.Context) ([]byte, Block, error) {
	raw, err := c.post(ctx, "/walletsolidity/getblock?visible=true", `{"detail":false}`)
	if err != nil {
		return nil, Block{}, err
	}
	var response struct {
		BlockID string `json:"blockID"`
		Header  struct {
			Raw struct {
				Number    json.Number `json:"number"`
				Timestamp json.Number `json:"timestamp"`
			} `json:"raw_data"`
		} `json:"block_header"`
	}
	if err = decode(raw, &response); err != nil {
		return nil, Block{}, fmt.Errorf("decode solid block: %w", err)
	}
	number, err := strconv.ParseUint(response.Header.Raw.Number.String(), 10, 64)
	if err != nil || number == 0 {
		return nil, Block{}, fmt.Errorf("invalid solid block number %q", response.Header.Raw.Number)
	}
	millis, err := strconv.ParseInt(response.Header.Raw.Timestamp.String(), 10, 64)
	if err != nil || millis <= 0 {
		return nil, Block{}, fmt.Errorf("invalid solid block timestamp %q", response.Header.Raw.Timestamp)
	}
	if !validBlockID(response.BlockID) {
		return nil, Block{}, fmt.Errorf("invalid solid block id %q", response.BlockID)
	}
	return raw, Block{ID: response.BlockID, Number: number, Time: time.UnixMilli(millis).UTC()}, nil
}
func (c *Client) witnesses(ctx context.Context) ([]byte, []Witness, error) {
	raw, err := c.get(ctx, "/walletsolidity/getpaginatednowwitnesslist?offset=0&limit=127&visible=true")
	if err != nil {
		return nil, nil, err
	}
	var response struct {
		Witnesses []Witness `json:"witnesses"`
	}
	if err = decode(raw, &response); err != nil {
		return nil, nil, fmt.Errorf("decode witnesses: %w", err)
	}
	return raw, response.Witnesses, nil
}
func (c *Client) chainParameters(ctx context.Context) ([]byte, map[string]int64, error) {
	raw, err := c.get(ctx, "/wallet/getchainparameters")
	if err != nil {
		return nil, nil, err
	}
	var response struct {
		Parameters []struct {
			Key   string      `json:"key"`
			Value json.Number `json:"value"`
		} `json:"chainParameter"`
	}
	if err = decode(raw, &response); err != nil {
		return nil, nil, fmt.Errorf("decode chain parameters: %w", err)
	}
	out := make(map[string]int64, len(response.Parameters))
	for _, item := range response.Parameters {
		if !requiredYieldChainParameter(item.Key) {
			// TRON includes feature flags whose value is omitted when disabled.
			// They do not participate in the staking APR calculation.
			continue
		}
		if _, exists := out[item.Key]; exists {
			return nil, nil, fmt.Errorf("duplicate chain parameter %s", item.Key)
		}
		value, parseErr := strconv.ParseInt(item.Value.String(), 10, 64)
		if parseErr != nil {
			return nil, nil, fmt.Errorf("invalid chain parameter %s", item.Key)
		}
		out[item.Key] = value
	}
	return raw, out, nil
}

func requiredYieldChainParameter(key string) bool {
	switch key {
	case "getMaintenanceTimeInterval", "getWitnessPayPerBlock", "getWitness127PayPerBlock", "getUnfreezeDelayDays":
		return true
	default:
		return false
	}
}
func (c *Client) brokerage(ctx context.Context, address string) ([]byte, int64, error) {
	body, err := json.Marshal(struct {
		Address string `json:"address"`
		Visible bool   `json:"visible"`
	}{address, true})
	if err != nil {
		return nil, 0, err
	}
	raw, err := exchange.PostJSON(ctx, c.HTTP, c.BaseURL+"/walletsolidity/getBrokerage", body, c.Retry)
	if err != nil {
		return nil, 0, fmt.Errorf("brokerage %s: %w", address, err)
	}
	var response struct {
		Brokerage json.Number `json:"brokerage"`
	}
	if err = decode(raw, &response); err != nil {
		return nil, 0, fmt.Errorf("decode brokerage %s: %w", address, err)
	}
	value, err := strconv.ParseInt(response.Brokerage.String(), 10, 64)
	if err != nil {
		return nil, 0, fmt.Errorf("invalid brokerage for %s", address)
	}
	return raw, value, nil
}

func validBlockID(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, char := range value {
		if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f') || (char >= 'A' && char <= 'F')) {
			return false
		}
	}
	return true
}
