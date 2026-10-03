package justlendkeeper

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Request struct {
	Source     string
	Path       string
	Body       []byte
	Role       string
	Key        string
	Background bool
	Attempts   uint8
	NotBefore  time.Time
}
type Evidence struct {
	Source       string     `json:"source_id"`
	Path         string     `json:"path"`
	Started      time.Time  `json:"started_at"`
	Received     *time.Time `json:"received_at"`
	Available    time.Time  `json:"available_at"`
	RequestHash  string     `json:"request_hash,omitempty"`
	ResponseHash string     `json:"response_hash,omitempty"`
	Status       int        `json:"http_status"`
	Error        string     `json:"error,omitempty"`
}
type Client struct {
	Config  Config
	HTTP    *http.Client
	Archive Archive
	Limiter *Limiter
	Now     func() time.Time
}

func NewClient(c Config, a Archive, l *Limiter) *Client {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.DisableKeepAlives = true
	tr.ForceAttemptHTTP2 = false
	tr.Protocols = new(http.Protocols)
	tr.Protocols.SetHTTP1(true)
	// A cloned default transport may already advertise h2 in its TLS config.
	// Disable both ALPN advertising and the inherited h2 protocol handler.
	if tr.TLSClientConfig == nil {
		tr.TLSClientConfig = &tls.Config{}
	} else {
		tr.TLSClientConfig = tr.TLSClientConfig.Clone()
	}
	tr.TLSClientConfig.NextProtos = []string{"http/1.1"}
	tr.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
	tr.MaxConnsPerHost = 1
	return &Client{c, &http.Client{Timeout: 20 * time.Second, Transport: tr, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, a, l, l.Now}
}
func Post(source, path, role, key string, body any, background bool) Request {
	b, _ := json.Marshal(body)
	return Request{Source: source, Path: path, Role: role, Key: key, Body: b, Background: background}
}
func permitted(r Request) bool {
	if r.Source == "publicnode" {
		switch strings.Split(r.Path, "?")[0] {
		case "/wallet/getblock", "/walletsolidity/getblock", "/wallet/gettransactionbyid", "/wallet/gettransactioninfobyid", "/wallet/getaccount", "/wallet/getchainparameters", "/wallet/getcontract", "/wallet/triggerconstantcontract":
			return len(r.Body) > 0
		}
		return false
	}
	if r.Source == "trongrid" {
		return strings.HasPrefix(r.Path, "/v1/contracts/"+ContractBase58+"/events?") && r.Body == nil
	}
	return r.Source == "binance" && r.Path == "/api/v3/ticker/bookTicker?symbol=TRXUSDT" && r.Body == nil
}
func (c *Client) Send(ctx context.Context, r Request) (Evidence, []byte, error) {
	ev := Evidence{Source: r.Source, Path: r.Path}
	if !permitted(r) {
		return ev, nil, errors.New("request_method_not_allowed")
	}
	if len(r.Body) > 0 {
		h, e := c.Archive.Put(r.Body)
		if e != nil {
			return ev, nil, e
		}
		ev.RequestHash = Hex(h)
	}
	method := http.MethodGet
	if len(r.Body) > 0 {
		method = http.MethodPost
	}
	req, e := http.NewRequestWithContext(ctx, method, strings.TrimRight(c.Config.Base(r.Source), "/")+r.Path, strings.NewReader(string(r.Body)))
	if e != nil {
		return ev, nil, e
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "crypto-market-info/justlend-keeper-data")
	if r.Source == "trongrid" {
		if key := os.Getenv("JUSTLEND_KEEPER_TRONGRID_API_KEY"); key != "" {
			req.Header.Set("TRON-PRO-API-KEY", key)
		}
	}
	if e = c.Limiter.Reserve(r.Source, r.Background); e != nil {
		return ev, nil, e
	}
	ev.Started = UTC(c.Now())
	res, e := c.HTTP.Do(req)
	if e != nil {
		ev.Available = UTC(c.Now())
		c.Limiter.ExtendGap(r.Source, r.Background, ev.Available)
		if se := c.Limiter.Save(); se != nil {
			return ev, nil, se
		}
		ev.Error = "transport_error"
		if errors.Is(e, context.DeadlineExceeded) {
			ev.Error = "timeout"
		}
		return ev, nil, nil
	}
	defer res.Body.Close()
	ev.Received = Ptr(UTC(c.Now()))
	ev.Status = res.StatusCode
	c.Limiter.ExtendGap(r.Source, r.Background, *ev.Received)
	if e = c.Limiter.Status(r.Source, res.StatusCode, RetryAfter(res.Header.Get("Retry-After"), c.Now())); e != nil {
		return ev, nil, e
	}
	b, e := io.ReadAll(io.LimitReader(res.Body, 16*1024*1024+1))
	if e != nil {
		ev.Error = "response_read_error"
		ev.Available = UTC(c.Now())
		return ev, nil, nil
	}
	if len(b) > 16*1024*1024 {
		return ev, nil, errors.New("response_exceeds_16MiB")
	}
	h, e := c.Archive.Put(b)
	if e != nil {
		return ev, nil, e
	}
	ev.ResponseHash = Hex(h)
	ev.Available = UTC(c.Now())
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		ev.Error = "http_" + strconv.Itoa(res.StatusCode)
	}
	return ev, b, nil
}
func RetryAfter(s string, now time.Time) time.Duration {
	if n, e := strconv.ParseUint(s, 10, 32); e == nil {
		return time.Duration(n) * time.Second
	}
	if t, e := http.ParseTime(s); e == nil && t.After(now) {
		return t.Sub(now)
	}
	return 0
}
func EventPath(kind string, from, to time.Time, fingerprint string) string {
	q := url.Values{"event_name": {kind}, "only_confirmed": {"true"}, "limit": {"200"}, "order_by": {"block_timestamp,asc"}, "min_block_timestamp": {strconv.FormatInt(from.UnixMilli(), 10)}, "max_block_timestamp": {strconv.FormatInt(to.UnixMilli()-1, 10)}}
	if fingerprint != "" {
		q.Set("fingerprint", fingerprint)
	}
	return "/v1/contracts/" + ContractBase58 + "/events?" + q.Encode()
}
