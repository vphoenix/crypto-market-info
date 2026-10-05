package lst

// A deliberately small, conservative HTTP gate. All methods, including failed
// requests, consume the same persisted per-host budget before they are sent.
import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

type Clock interface {
	Now() time.Time
	Sleep(context.Context, time.Duration) error
}
type realClock struct{}

func (realClock) Now() time.Time { return time.Now().UTC() }
func (realClock) Sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

type TransportConfig struct {
	StateDir, ArchiveDir      string
	HTTP                      *http.Client
	Clock                     Clock
	Backfill                  bool
	RPCRequestsPerMinute      int           // zero preserves the original 120/min cap
	LogRequestsPerFiveMinutes int           // zero preserves live 4 / backfill 30; shares persisted per-host history
	StartupRPCGap             time.Duration // zero preserves the original 2s preflight gap
	RPCGap                    time.Duration // zero preserves 500ms; production slows every response gap
	PruneCommittedResponses   bool
	// Tests can inject zero jitter. Production jitter only lengthens cooldowns.
	Jitter func() time.Duration
}
type Response struct {
	Raw                                  []byte
	RequestedAt, ReceivedAt, AvailableAt time.Time
	PayloadHash, RequestHash             string
	HTTPStatus                           int
	Headers                              http.Header
}
type sentRequest struct {
	At     time.Time `json:"at"`
	Class  string    `json:"class"`
	Weight int       `json:"weight"`
}
type hostState struct {
	Version               int    `json:"version"`
	Host                  string `json:"host"`
	Source                string `json:"source"`
	NextAt, CooldownUntil time.Time
	Disabled              bool
	Failures, LimitHits   int
	WeightLimit           int
	Recent                []sentRequest
}
type hostGate struct {
	mu              sync.Mutex
	state           hostState
	file            string
	broken          bool
	sentThisProcess bool
}
type HostStats struct {
	Requests, Weight, Waits, RateLimits uint64
	Disabled                            bool
}
type Transport struct {
	cfg         TransportConfig
	archive     ethereum.Archive
	started     time.Time
	mu          sync.Mutex
	initialized bool
	hosts       map[string]*hostGate
	stats       map[string]HostStats
	rawEvidence []string // requests/attempts since the previous frozen batch
}

func NewTransport(c TransportConfig) (*Transport, error) {
	if c.LogRequestsPerFiveMinutes < 0 || c.LogRequestsPerFiveMinutes > 30 {
		return nil, errors.New("transport_log_five_minute_limit_invalid")
	}
	if c.RPCRequestsPerMinute < 0 || c.RPCRequestsPerMinute > 120 {
		return nil, errors.New("transport_rpc_minute_limit_invalid")
	}
	if c.StartupRPCGap != 0 && (c.StartupRPCGap < 2*time.Second || c.StartupRPCGap > 30*time.Second) {
		return nil, errors.New("transport_startup_rpc_gap_invalid")
	}
	if c.RPCGap != 0 && (c.RPCGap < 500*time.Millisecond || c.RPCGap > 10*time.Second) {
		return nil, errors.New("transport_rpc_gap_invalid")
	}
	if c.StateDir == "" || c.ArchiveDir == "" {
		return nil, errors.New("transport_state_and_archive_required")
	}
	if err := os.MkdirAll(c.StateDir, 0700); err != nil {
		return nil, errors.New("transport_state_directory_unavailable")
	}
	if c.Clock == nil {
		c.Clock = realClock{}
	}
	if c.Jitter == nil {
		c.Jitter = func() time.Duration { return time.Duration(rand.Int64N(int64(time.Second))) }
	}
	if c.HTTP == nil {
		c.HTTP = &http.Client{}
	}
	client := *c.HTTP
	client.Timeout = 5 * time.Second
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	c.HTTP = &client
	return &Transport{cfg: c, archive: ethereum.Archive{Dir: c.ArchiveDir}, started: c.Clock.Now().UTC(), hosts: map[string]*hostGate{}, stats: map[string]HostStats{}}, nil
}
func (t *Transport) MarkInitialized() { t.mu.Lock(); t.initialized = true; t.mu.Unlock() }

// Wait before creating a timed market observation, so maintenance requests
// cannot consume its whole fresh-data window while the rolling quota expires.
// This reserves no requests: every actual send still passes acquire. The
// collector owns the RPC gate serially; Binance uses a different host gate.
func (t *Transport) WaitRPCSlots(ctx context.Context, endpoint string, slots int) error {
	cap := 120
	if t.cfg.RPCRequestsPerMinute > 0 {
		cap = t.cfg.RPCRequestsPerMinute
	}
	if t.cfg.Backfill {
		cap = min(cap, 60)
	}
	if slots < 1 || slots > cap {
		return errors.New("market_rpc_slots_exceed_limit")
	}
	g, err := t.gate("rpc", endpoint)
	if err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if g.broken {
			return errors.New("transport_state_unavailable")
		}
		now := t.cfg.Clock.Now().UTC()
		// Let Market record a disabled/cooling source as missing immediately.
		if g.state.Disabled || g.state.CooldownUntil.After(now) {
			return nil
		}
		recent := []sentRequest{}
		for _, r := range g.state.Recent {
			if r.At.After(now.Add(-time.Minute)) {
				recent = append(recent, r)
			}
		}
		ready := g.state.NextAt
		if len(recent) > cap-slots {
			ready = later(ready, recent[len(recent)-(cap-slots)-1].At.Add(time.Minute))
		}
		if !ready.After(now) {
			return nil
		}
		if err := t.cfg.Clock.Sleep(ctx, ready.Sub(now)); err != nil {
			return err
		}
	}
}

// Optional maintenance does not wait for quota or begin a range it cannot
// conservatively finish. Actual sends still acquire the persisted host gate.
func (t *Transport) AvailableRPCSlots(endpoint string) (int, error) {
	cap := 120
	if t.cfg.RPCRequestsPerMinute > 0 {
		cap = t.cfg.RPCRequestsPerMinute
	}
	if t.cfg.Backfill {
		cap = min(cap, 60)
	}
	g, err := t.gate("rpc", endpoint)
	if err != nil {
		return 0, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.broken {
		return 0, errors.New("transport_state_unavailable")
	}
	now := t.cfg.Clock.Now().UTC()
	if g.state.Disabled || g.state.CooldownUntil.After(now) {
		return 0, nil
	}
	for _, r := range g.state.Recent {
		if r.At.After(now.Add(-time.Minute)) {
			cap--
		}
	}
	return max(0, cap), nil
}

func (t *Transport) Stats() map[string]HostStats {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := map[string]HostStats{}
	for k, v := range t.stats {
		out[k] = v
	}
	return out
}
func (t *Transport) stat(host string, f func(*HostStats)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.stats[host]
	f(&s)
	t.stats[host] = s
}
func transportURL(endpoint string) (*url.URL, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Hostname() == "" || (u.Scheme != "https" && u.Scheme != "http") || u.Fragment != "" {
		return nil, errors.New("transport_invalid_endpoint")
	}
	return u, nil
}
func (t *Transport) gate(source, endpoint string) (*hostGate, error) {
	if source != "rpc" && source != "binance" {
		return nil, errors.New("transport_invalid_source")
	}
	u, err := transportURL(endpoint)
	if err != nil {
		return nil, err
	}
	key := strings.ToLower(u.Hostname())
	t.mu.Lock()
	defer t.mu.Unlock()
	if g := t.hosts[key]; g != nil {
		if g.state.Source != source {
			return nil, errors.New("transport_host_source_conflict")
		}
		return g, nil
	}
	h := sha256.Sum256([]byte(key))
	file := filepath.Join(t.cfg.StateDir, "http-"+hex.EncodeToString(h[:])+".json")
	g := &hostGate{state: hostState{Version: 1, Host: key, Source: source}, file: file}
	if raw, e := os.ReadFile(file); e == nil {
		if json.Unmarshal(raw, &g.state) != nil || g.state.Version != 1 || g.state.Host != key || g.state.Source != source {
			return nil, errors.New("transport_state_invalid")
		}
	} else if !os.IsNotExist(e) {
		return nil, errors.New("transport_state_unreadable")
	}
	t.hosts[key] = g
	return g, nil
}
func saveGate(g *hostGate) error {
	if g.broken {
		return errors.New("transport_state_unavailable")
	}
	raw, err := json.Marshal(g.state)
	if err != nil {
		g.broken = true
		return errors.New("transport_state_encoding")
	}
	f, err := os.CreateTemp(filepath.Dir(g.file), ".gate-")
	if err != nil {
		g.broken = true
		return errors.New("transport_state_unwritable")
	}
	name := f.Name()
	defer os.Remove(name)
	if _, err = f.Write(raw); err == nil {
		err = f.Sync()
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(name, g.file)
	}
	if err == nil {
		var d *os.File
		d, err = os.Open(filepath.Dir(g.file))
		if err == nil {
			err = d.Sync()
			d.Close()
		}
	}
	if err != nil {
		g.broken = true
		return errors.New("transport_state_unwritable")
	}
	return nil
}
func later(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}
func (t *Transport) spacing(source string, now time.Time) time.Duration {
	if source == "binance" {
		return 5 * time.Second
	}
	t.mu.Lock()
	initialized := t.initialized
	t.mu.Unlock()
	if !initialized || now.Before(t.started.Add(time.Minute)) {
		return max(2*time.Second, t.cfg.StartupRPCGap, t.cfg.RPCGap)
	}
	if t.cfg.Backfill {
		return max(time.Second, t.cfg.RPCGap)
	}
	return max(500*time.Millisecond, t.cfg.RPCGap)
}
func (t *Transport) acquire(ctx context.Context, g *hostGate, class string, weight int) error {
	for {
		if err := ctx.Err(); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return errors.New("round_budget_exhausted")
			}
			return err
		}
		if g.broken {
			return errors.New("transport_state_unavailable")
		}
		if g.state.Disabled {
			return errors.New("source_disabled")
		}
		now := t.cfg.Clock.Now().UTC()
		ready := later(g.state.NextAt, g.state.CooldownUntil)
		if !g.sentThisProcess {
			// A process may have crashed with the previous request still in
			// flight. Its durable reservation precedes the actual network send,
			// so the replacement process also waits a full startup gap.
			ready = later(ready, t.started.Add(t.spacing(g.state.Source, t.started)))
		}
		keep := g.state.Recent[:0]
		for _, r := range g.state.Recent {
			if r.At.After(now.Add(-5 * time.Minute)) {
				keep = append(keep, r)
			}
		}
		g.state.Recent = keep
		count, totalWeight, funding60, funding300, logs300 := 0, 0, 0, 0, 0
		for _, r := range g.state.Recent {
			if r.Class == "funding" {
				funding300++
			}
			if r.Class == "logs" {
				logs300++
			}
			if r.At.After(now.Add(-time.Minute)) {
				count++
				totalWeight += r.Weight
				if r.Class == "funding" {
					funding60++
				}
			}
			if class == "quoter" && r.Class == class {
				ready = later(ready, r.At.Add(time.Second))
			}
			if class == "logs" && r.Class == class {
				ready = later(ready, r.At.Add(10*time.Second))
			}
		}
		cap := 120
		if t.cfg.RPCRequestsPerMinute > 0 {
			cap = t.cfg.RPCRequestsPerMinute
		}
		if t.cfg.Backfill {
			cap = min(cap, 60)
		}
		if g.state.Source == "binance" {
			cap = 12
		}
		for _, r := range g.state.Recent {
			if r.At.After(now.Add(-time.Minute)) && (count >= cap || (g.state.Source == "binance" && totalWeight+weight > 60)) {
				ready = later(ready, r.At.Add(time.Minute))
				break
			}
		}
		if class == "funding" && (funding60 >= 2 || funding300 >= 10) {
			for _, r := range g.state.Recent {
				if r.Class != "funding" {
					continue
				}
				if funding300 >= 10 {
					ready = later(ready, r.At.Add(5*time.Minute))
					break
				}
				if r.At.After(now.Add(-time.Minute)) {
					ready = later(ready, r.At.Add(time.Minute))
					break
				}
			}
		}
		logCap := 4
		if t.cfg.Backfill {
			logCap = 30
		}
		if t.cfg.LogRequestsPerFiveMinutes != 0 {
			logCap = t.cfg.LogRequestsPerFiveMinutes
		}
		if class == "logs" && logs300 >= logCap {
			for _, r := range g.state.Recent {
				if r.Class == "logs" {
					ready = later(ready, r.At.Add(5*time.Minute))
					break
				}
			}
		}
		if ready.After(now) {
			if deadline, ok := ctx.Deadline(); ok && ready.Add(t.cfg.HTTP.Timeout).After(deadline) {
				cause := "local_gate_budget_exhausted"
				if g.state.CooldownUntil.After(now) {
					cause = "source_cooldown"
				}
				return &gateWaitError{Cause: cause, ReadyAt: ready}
			}
			t.stat(g.state.Host, func(s *HostStats) { s.Waits++ })
			if err := t.cfg.Clock.Sleep(ctx, ready.Sub(now)); err != nil {
				return err
			}
			continue
		}
		g.state.Recent = append(g.state.Recent, sentRequest{now, class, weight})
		gap := t.spacing(g.state.Source, now)
		g.state.NextAt = now.Add(gap)
		if err := saveGate(g); err != nil {
			return err
		}
		if t.cfg.Clock.Now().Sub(now) >= gap {
			// Do not send from an expired reservation after a slow fsync. The
			// consumed slot stays durable; a caller can try a later scheduled round.
			return errors.New("transport_send_reservation_expired")
		}
		g.sentThisProcess = true
		return nil
	}
}
func retryAfter(h http.Header, now time.Time) time.Time {
	v := h.Get("Retry-After")
	if n, e := strconv.ParseInt(v, 10, 64); e == nil && n > 0 && n < 31536000 {
		return now.Add(time.Duration(n) * time.Second)
	}
	if at, e := http.ParseTime(v); e == nil {
		return at.UTC()
	}
	return time.Time{}
}
func responseLimit(raw []byte) (limited, disabled bool) {
	var v struct {
		Code  int    `json:"code"`
		Msg   string `json:"msg"`
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(raw, &v) != nil {
		return false, false
	}
	msg := v.Msg
	code := v.Code
	if v.Error != nil {
		msg = v.Error.Message
		code = v.Error.Code
	}
	lower := strings.ToLower(msg)
	disabled = strings.Contains(lower, "ip banned") || strings.Contains(lower, "ip ban until")
	// -32005 is also used for oversized log ranges/results. Only an explicit
	// rate/throttle message belongs to the host cooldown; range splitting is a
	// caller concern and must not turn into a retry storm.
	limited = code == -1003 || strings.Contains(lower, "rate limit") || strings.Contains(lower, "too many requests") || strings.Contains(lower, "request limit exceeded") || strings.Contains(lower, "throttl")
	return
}
func (t *Transport) penalize(g *hostGate, status int, headers http.Header, raw []byte, network bool) error {
	now := t.cfg.Clock.Now().UTC()
	// Start a fresh gap after every completed response or transport failure.
	// This includes network/body latency and persists a conservative boundary
	// for restart; slow requests never earn a burst of catch-up sends.
	g.state.NextAt = later(g.state.NextAt, now.Add(t.spacing(g.state.Source, now)))
	limited, disabled := responseLimit(raw)
	if status == 403 || status == 418 || disabled {
		g.state.Disabled = true
		t.stat(g.state.Host, func(s *HostStats) { s.Disabled = true })
	} else if status == 429 || limited {
		g.state.LimitHits++
		minutes := 5
		for n := 1; n < g.state.LimitHits && minutes < 30; n++ {
			minutes *= 2
		}
		if minutes > 30 {
			minutes = 30
		}
		g.state.CooldownUntil = later(g.state.CooldownUntil, later(now.Add(time.Duration(minutes)*time.Minute+t.cfg.Jitter()), retryAfter(headers, now)))
		t.stat(g.state.Host, func(s *HostStats) { s.RateLimits++ })
	} else if network || status >= 500 {
		g.state.Failures++
		secs := 15
		for n := 1; n < g.state.Failures && secs < 60; n++ {
			secs *= 2
		}
		g.state.CooldownUntil = later(g.state.CooldownUntil, now.Add(time.Duration(secs)*time.Second+t.cfg.Jitter()))
	} else if status >= 200 && status < 300 {
		g.state.Failures = 0
	}
	// Binance uses a host/IP-wide budget. A 50% watermark leaves room for the
	// existing collectors; a later response is still checked against this header.
	weightThreshold := 30
	if g.state.WeightLimit > 0 {
		weightThreshold = (g.state.WeightLimit + 1) / 2
	}
	if n, e := strconv.Atoi(headers.Get("X-Mbx-Used-Weight-1m")); e == nil && n >= weightThreshold {
		g.state.CooldownUntil = later(g.state.CooldownUntil, now.Add(time.Minute))
	}
	return saveGate(g)
}
func (t *Transport) Cooldown(source, endpoint string, d time.Duration) error {
	g, err := t.gate(source, endpoint)
	if err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if d < 5*time.Minute {
		d = 5 * time.Minute
	}
	g.state.CooldownUntil = later(g.state.CooldownUntil, t.cfg.Clock.Now().Add(d))
	return saveGate(g)
}
func (t *Transport) ReadyAt(source, endpoint string) (time.Time, error) {
	g, err := t.gate(source, endpoint)
	if err != nil {
		return time.Time{}, err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.state.Disabled {
		return time.Time{}, errors.New("source_disabled")
	}
	return later(g.state.NextAt, g.state.CooldownUntil), nil
}

// SetBinanceWeightLimit installs the public exchangeInfo REQUEST_WEIGHT /
// MINUTE limit. Until discovery the gate uses half its own 60/min allowance.
func (t *Transport) SetBinanceWeightLimit(endpoint string, limit int) error {
	if limit <= 0 {
		return errors.New("binance_weight_limit_invalid")
	}
	g, err := t.gate("binance", endpoint)
	if err != nil {
		return err
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.state.WeightLimit = limit
	return saveGate(g)
}

func (t *Transport) Do(ctx context.Context, source, method, endpoint string, body []byte, class string) (Response, error) {
	var out Response
	if class != "normal" && class != "quoter" && class != "logs" && class != "funding" {
		return out, errors.New("transport_invalid_class")
	}
	u, err := transportURL(endpoint)
	if err != nil {
		return out, err
	}
	weight := 1
	path := u.Path
	if source == "rpc" {
		if method != http.MethodPost {
			return out, errors.New("rpc_requires_post")
		}
		var call struct {
			JSONRPC string          `json:"jsonrpc"`
			Method  string          `json:"method"`
			Params  json.RawMessage `json:"params"`
		}
		if json.Unmarshal(body, &call) != nil || call.JSONRPC != "2.0" || !strings.HasPrefix(call.Method, "eth_") {
			return out, errors.New("rpc_single_read_method_required")
		}
		switch call.Method {
		case "eth_call", "eth_chainId", "eth_getBlockByNumber", "eth_getBlockByHash", "eth_getCode", "eth_getStorageAt", "eth_getLogs", "eth_getBlockReceipts", "eth_getTransactionByHash", "eth_getTransactionReceipt", "eth_maxPriorityFeePerGas", "eth_blockNumber":
		default:
			return out, errors.New("rpc_method_not_allowed")
		}
		if call.Method == "eth_getLogs" && class != "logs" {
			return out, errors.New("rpc_logs_class_required")
		}
		path = "/" // Provider API keys frequently occur in URL path/query/userinfo.
	} else if source == "binance" {
		if method != http.MethodGet || len(body) != 0 || u.User != nil {
			return out, errors.New("binance_public_get_required")
		}
		for key, values := range u.Query() {
			if len(values) != 1 {
				return out, errors.New("binance_duplicate_query_parameter")
			}
			switch key {
			case "symbol", "limit", "startTime", "endTime":
			default:
				return out, errors.New("binance_query_parameter_not_allowed")
			}
		}
		switch u.Path {
		case "/fapi/v1/depth":
			if u.Query().Get("symbol") != "ETHUSDT" || u.Query().Get("limit") != "10" {
				return out, errors.New("binance_depth_limit10_required")
			}
			weight = 2
		case "/fapi/v1/premiumIndex":
			if u.Query().Get("symbol") != "ETHUSDT" {
				return out, errors.New("binance_symbol_required")
			}
		case "/fapi/v1/exchangeInfo":
		case "/fapi/v1/fundingRate", "/fapi/v1/fundingInfo":
			if class != "funding" {
				return out, errors.New("binance_funding_class_required")
			}
			weight = 0
		default:
			return out, errors.New("binance_endpoint_not_allowed")
		}
		path = u.RequestURI()
	}
	g, err := t.gate(source, endpoint)
	if err != nil {
		return out, err
	}
	// One in flight per host also makes post-response cooldown immediate.
	g.mu.Lock()
	defer g.mu.Unlock()
	requestHash, err := t.putEvidence(struct {
		Source, Host, Method, Path string
		Body                       []byte
	}{source, g.state.Host, method, path, body})
	if err != nil {
		return out, errors.New("request_evidence_write_failed")
	}
	out.RequestHash = requestHash.String()
	// Archive before reserving a send: disk latency must not consume the next
	// request's gap and allow two real network sends to bunch together.
	if err = t.acquire(ctx, g, class, weight); err != nil {
		return out, err
	}
	if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) < t.cfg.HTTP.Timeout {
		// A near-expired market round is not a network failure. Preserve the
		// consumed budget but leave the host available for its canonical check.
		return out, errors.New("round_budget_exhausted")
	}
	requestCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, method, endpoint, bytes.NewReader(body))
	if err != nil {
		return out, errors.New("transport_request_invalid")
	}
	req.Header.Set("Accept", "application/json")
	if source == "rpc" {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("User-Agent", "crypto-market-info/lst-data")
	out.RequestedAt = t.cfg.Clock.Now().UTC().Truncate(time.Microsecond)
	// Record the actual send boundary, after request construction/persistence.
	g.state.Recent[len(g.state.Recent)-1].At = out.RequestedAt
	g.state.NextAt = later(g.state.NextAt, out.RequestedAt.Add(t.spacing(source, out.RequestedAt)))
	t.stat(g.state.Host, func(s *HostStats) { s.Requests++; s.Weight += uint64(weight) })
	resp, err := t.cfg.HTTP.Do(req)
	if err != nil {
		if e := t.penalize(g, 0, nil, nil, true); e != nil {
			return out, e
		}
		cause := "transport_network_error"
		var dns *net.DNSError
		var ne net.Error
		if errors.Is(err, context.Canceled) {
			cause = "transport_request_cancelled"
		} else if errors.Is(err, context.DeadlineExceeded) || errors.As(err, &ne) && ne.Timeout() {
			cause = "transport_http_timeout"
		} else if errors.As(err, &dns) {
			cause = "transport_dns_error"
		} else if strings.Contains(err.Error(), "proxyconnect") {
			cause = "transport_proxy_error"
		} else {
			var op *net.OpError
			if errors.As(err, &op) && op.Op == "dial" {
				cause = "transport_connect_error"
			}
		}
		out.ReceivedAt = t.cfg.Clock.Now().UTC().Truncate(time.Microsecond)
		return out, errors.Join(errors.New(cause), t.archiveFailure(&out, source, g.state.Host, cause))
	}
	defer resp.Body.Close()
	out.HTTPStatus = resp.StatusCode
	out.Headers = resp.Header.Clone()
	out.Raw, err = io.ReadAll(io.LimitReader(resp.Body, (16<<20)+1))
	out.ReceivedAt = t.cfg.Clock.Now().UTC().Truncate(time.Microsecond)
	if err != nil || len(out.Raw) > 16<<20 {
		if e := t.penalize(g, resp.StatusCode, resp.Header, nil, true); e != nil {
			return out, e
		}
		cause := "transport_response_read_failed"
		var ne net.Error
		if len(out.Raw) > 16<<20 {
			cause = "transport_response_too_large"
			out.Raw = out.Raw[:16<<20]
		} else if errors.Is(err, context.DeadlineExceeded) || errors.As(err, &ne) && ne.Timeout() {
			cause = "transport_response_read_timeout"
		} else if errors.Is(err, io.ErrUnexpectedEOF) {
			cause = "transport_response_truncated"
		}
		return out, errors.Join(errors.New(cause), t.archiveFailure(&out, source, g.state.Host, cause))
	}
	// Cooldown must be durable even if evidence storage subsequently fails.
	if err = t.penalize(g, resp.StatusCode, resp.Header, out.Raw, false); err != nil {
		return out, err
	}
	selected := http.Header{}
	for _, key := range []string{"Retry-After", "X-Mbx-Used-Weight-1m", "Content-Type"} {
		if v := resp.Header.Get(key); v != "" {
			selected.Set(key, v)
		}
	}
	h, err := t.putEvidence(struct {
		Source, Host, RequestHash string
		RequestedAt, ReceivedAt   time.Time
		HTTPStatus                int
		Headers                   http.Header
		Response                  []byte
	}{source, g.state.Host, out.RequestHash, out.RequestedAt, out.ReceivedAt, resp.StatusCode, selected, out.Raw})
	if err != nil {
		return out, errors.New("response_evidence_write_failed")
	}
	out.PayloadHash = h.String()
	out.AvailableAt = t.cfg.Clock.Now().UTC().Truncate(time.Microsecond)
	limited, disabled := responseLimit(out.Raw)
	if g.state.Disabled || disabled {
		return out, errors.New("source_disabled")
	}
	if resp.StatusCode == 429 || limited {
		return out, errors.New("source_rate_limited")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return out, fmt.Errorf("source_http_%d", resp.StatusCode)
	}
	return out, nil
}

func (t *Transport) archiveFailure(out *Response, source, host, cause string) error {
	// For failures this is an attempt/partial-body proof, never a complete RPC
	// response. The cause and HTTP status remain explicit in the observation.
	h, err := t.putEvidence(struct {
		Source, Host, RequestHash, Failure string
		RequestedAt, ReceivedAt            time.Time
		HTTPStatus                         int
		Response                           []byte
	}{source, host, out.RequestHash, cause, out.RequestedAt, out.ReceivedAt, out.HTTPStatus, out.Raw})
	if err != nil {
		return errors.New("failure_evidence_write_failed")
	}
	out.PayloadHash, out.AvailableAt = h.String(), t.cfg.Clock.Now().UTC().Truncate(time.Microsecond)
	return nil
}
