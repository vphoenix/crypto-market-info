package optionslive

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/exchange"
	"github.com/vphoenix/crypto-market-info/internal/exchange/deribit"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"log/slog"
	"strings"
	"sync"
	"time"
)

type bookRoute struct {
	id           uint32
	symbol       string
	worker       *catalogWorker
	physical     *bookPhysical
	lastRecovery time.Time
}
type bookPhysical struct {
	epoch                  uuid.UUID
	ready                  bool
	confirmed              time.Time
	routes                 map[string]*bookRoute
	used, requested, acked map[string]bool
	pendingUnsubscribe     map[string]bool
	unsubscribing          map[string]bool
	commands               chan deribit.SessionCommand
	cancel                 context.CancelFunc
	closing                bool
}
type bookHub struct {
	lastRotation time.Time
	recovery     map[uint32]bool
	ctx          context.Context
	c            *deribit.Client
	cfg          Config
	logger       *slog.Logger
	mu           sync.Mutex
	routes       map[uint32]*bookRoute
	physical     []*bookPhysical
	workers      map[uuid.UUID]*catalogWorker
	indexConn    connection
	indexes      map[string]options.IndexSample
	resetIndex   chan struct{}
	wg           sync.WaitGroup
}

func newBookHub(ctx context.Context, c *deribit.Client, cfg Config, logger *slog.Logger) *bookHub {
	h := &bookHub{ctx: ctx, c: c, cfg: cfg, logger: logger, routes: map[uint32]*bookRoute{}, recovery: map[uint32]bool{}, workers: map[uuid.UUID]*catalogWorker{}, indexes: map[string]options.IndexSample{}, resetIndex: make(chan struct{}, 1)}
	h.wg.Add(2)
	go h.maintain()
	go h.runIndex()
	return h
}
func (h *bookHub) register(w *catalogWorker) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.workers[w.id] = w
	for id, s := range h.indexes {
		x := s
		_ = w.q.offer(event{Index: &catalogIndex{ID: id, Sample: &x, Connection: h.indexConn}})
	}
}
func (h *bookHub) removeWorker(w *catalogWorker) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.workers, w.id)
	for id, r := range h.routes {
		if r.worker == w {
			h.removeLocked(id)
		}
	}
}
func (h *bookHub) add(id uint32, symbol string, w *catalogWorker) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if r := h.routes[id]; r != nil {
		if r.worker != w {
			h.detach(r)
			r.worker = w
			h.assign(r)
		}
		return
	}
	r := &bookRoute{id: id, symbol: symbol, worker: w}
	h.routes[id] = r
	h.assign(r)
}
func (h *bookHub) remove(id uint32) { h.mu.Lock(); defer h.mu.Unlock(); h.removeLocked(id) }
func (h *bookHub) removeLocked(id uint32) {
	r := h.routes[id]
	if r == nil {
		return
	}
	delete(h.routes, id)
	delete(h.recovery, id)
	h.detach(r)
}
func (h *bookHub) detach(r *bookRoute) {
	p := r.physical
	if p == nil {
		return
	}
	ch := "book." + r.symbol + ".100ms"
	delete(p.routes, ch)
	r.physical = nil
	_ = r.worker.q.offer(event{InstrumentID: r.id, Stream: deribit.StreamEvent{Kind: "disconnected", Epoch: p.epoch, ReceivedAt: time.Now().UTC()}})
	if len(p.routes) == 0 {
		p.closing = true
		p.cancel()
		return
	}
	if p.requested[ch] {
		h.queueUnsubscribe(p, ch)
	}
}
func (h *bookHub) recover(id uint32) {
	h.mu.Lock()
	defer h.mu.Unlock()
	r := h.routes[id]
	if r == nil {
		return
	}
	h.recovery[id] = true
}
func (h *bookHub) assign(r *bookRoute) {
	ch := "book." + r.symbol + ".100ms"
	for _, p := range h.physical {
		if !p.closing && !p.used[ch] && len(p.used) < 4096 && len(p.routes) < h.cfg.ChannelsPerConnection {
			p.used[ch] = true
			p.routes[ch] = r
			r.physical = p
			if p.epoch != uuid.Nil {
				_ = r.worker.q.offer(event{InstrumentID: r.id, Stream: deribit.StreamEvent{Kind: "connected", Epoch: p.epoch, ReceivedAt: p.confirmed}})
			}
			return
		}
	}
	if len(h.physical) >= h.cfg.MaxConnections-2 {
		// Historical channel use can exhaust every live epoch even though
		// route capacity remains. Rotate one sparse epoch, then retry after
		// its goroutine releases the connection slot. Do not reuse a channel.
		if time.Since(h.lastRotation) >= time.Second {
			var candidate *bookPhysical
			for _, p := range h.physical {
				if !p.closing && len(p.routes) < h.cfg.ChannelsPerConnection && (candidate == nil || len(p.routes) < len(candidate.routes)) {
					candidate = p
				}
			}
			if candidate != nil {
				h.lastRotation = time.Now()
				candidate.closing = true
				candidate.cancel()
				for _, route := range candidate.routes {
					_ = route.worker.q.offer(event{InstrumentID: route.id, Stream: deribit.StreamEvent{Kind: "disconnected", Epoch: candidate.epoch, ReceivedAt: time.Now().UTC()}})
				}
			}
		}
		return
	}
	ctx, cancel := context.WithCancel(h.ctx)
	p := &bookPhysical{routes: map[string]*bookRoute{ch: r}, used: map[string]bool{ch: true}, requested: map[string]bool{}, acked: map[string]bool{}, commands: make(chan deribit.SessionCommand, 64), cancel: cancel}
	h.physical = append(h.physical, p)
	r.physical = p
	h.wg.Add(1)
	go h.runPhysical(ctx, p)
}
func (h *bookHub) runPhysical(ctx context.Context, p *bookPhysical) {
	defer h.wg.Done()
	if exchange.Wait(ctx, 100*time.Millisecond) {
		h.mu.Lock()
		channels := make([]string, 0, len(p.routes))
		for ch := range p.routes {
			channels = append(channels, ch)
			p.requested[ch] = true
		}
		h.mu.Unlock()
		if len(channels) > 0 {
			err := h.c.Session(ctx, channels, p.commands, func(f deribit.StreamEvent) error { return h.dispatch(p, f) })
			if h.ctx.Err() == nil {
				h.logger.Warn("options physical connection retired", "error", err)
			}
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	p.closing = true
	for _, r := range p.routes {
		if r.physical == p {
			r.physical = nil
		}
	}
	for i, x := range h.physical {
		if x == p {
			h.physical = append(h.physical[:i], h.physical[i+1:]...)
			break
		}
	}
}
func (h *bookHub) dispatch(p *bookPhysical, f deribit.StreamEvent) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if p.closing && f.Kind != "disconnected" {
		return nil
	}
	if f.ReceivedAt.IsZero() {
		f.ReceivedAt = time.Now().UTC().Truncate(time.Microsecond)
	}
	if f.Kind == "connected" {
		p.epoch = f.Epoch
		p.confirmed = f.ReceivedAt
		p.ready = false
	}
	if f.Epoch != p.epoch {
		return nil
	}
	if f.Kind == "subscribed" {
		p.acked[f.Channel] = true
		if !p.ready {
			return nil
		}
		f.Kind = "ready"
	}
	if f.Kind == "unsubscribed" {
		delete(p.unsubscribing, f.Channel)
		delete(p.requested, f.Channel)
		delete(p.acked, f.Channel)
	}
	if f.Kind == "ready" {
		p.ready = true
	}
	if f.Kind == "disconnected" {
		p.ready = false
	}
	p.confirmed = f.ReceivedAt
	if f.Channel != "" {
		r := p.routes[f.Channel]
		if r == nil {
			return nil
		}
		if f.Kind == "message" && !strings.HasPrefix(f.Channel, "book.") {
			return fmt.Errorf("wrong book route")
		}
		_ = r.worker.q.offer(event{InstrumentID: r.id, Stream: f})
		return nil
	}
	for ch, r := range p.routes {
		x := f
		if f.Kind == "ready" && !p.acked[ch] {
			x.Kind = "confirm"
		}
		_ = r.worker.q.offer(event{InstrumentID: r.id, Stream: x})
	}
	return nil
}
func (h *bookHub) maintain() {
	defer h.wg.Done()
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-h.ctx.Done():
			return
		case <-t.C:
			h.mu.Lock()
			h.recoverPending()
			for _, r := range h.routes {
				if r.physical == nil {
					h.assign(r)
				}
			}
			for _, p := range h.physical {
				if p.closing || p.epoch == uuid.Nil || !p.ready {
					continue
				}
				// Accumulate new members while one subscription batch waits for its
				// ACK, rather than filling the shared rate gate with small batches.
				if p.controlPending() || len(p.pendingUnsubscribe) != 0 {
					continue
				}
				var channels []string
				for ch := range p.routes {
					if !p.requested[ch] {
						channels = append(channels, ch)
					}
				}
				if len(channels) > 0 {
					select {
					case p.commands <- deribit.SessionCommand{Method: "public/subscribe", Channels: channels}:
						for _, ch := range channels {
							p.requested[ch] = true
						}
					default:
					}
				}
			}
			h.mu.Unlock()
		}
	}
}
func (h *bookHub) runIndex() {
	defer h.wg.Done()
	channels := []string{"deribit_price_index.btc_usd", "deribit_price_index.eth_usd", "deribit_price_index.btc_usdc", "deribit_price_index.eth_usdc"}
	for h.ctx.Err() == nil {
		ctx, cancel := context.WithCancel(h.ctx)
		finished := make(chan struct{})
		go func() {
			select {
			case <-h.resetIndex:
				cancel()
			case <-finished:
			case <-ctx.Done():
			}
		}()
		err := h.c.Session(ctx, channels, nil, func(f deribit.StreamEvent) error {
			h.mu.Lock()
			defer h.mu.Unlock()
			at := f.ReceivedAt
			if at.IsZero() {
				at = time.Now().UTC().Truncate(time.Microsecond)
			}
			if f.Kind == "connected" {
				h.indexConn = connection{epoch: f.Epoch, started: at, confirmed: at}
				clear(h.indexes)
			}
			if f.Epoch != h.indexConn.epoch {
				return nil
			}
			h.indexConn.confirmed = at
			if f.Kind == "ready" {
				h.indexConn.ready = true
			}
			if f.Kind == "disconnected" {
				h.indexConn.ready = false
				clear(h.indexes)
			}
			var sample *options.IndexSample
			id := ""
			if f.Kind == "message" {
				id = strings.TrimPrefix(f.Channel, "deribit_price_index.")
				x, e := deribit.DecodeIndex(f.Raw, id, f.Epoch, at)
				if e != nil {
					return e
				}
				h.indexes[id] = x
				sample = &x
			}
			for _, w := range h.workers {
				_ = w.q.offer(event{Index: &catalogIndex{ID: id, Sample: sample, Connection: h.indexConn}})
			}
			return nil
		})
		close(finished)
		cancel()
		if h.ctx.Err() != nil {
			return
		}
		h.logger.Warn("options index reconnect", "error", err)
		if !exchange.Wait(h.ctx, time.Second) {
			return
		}
	}
}
func (h *bookHub) wait() { h.wg.Wait() }

// Coalesce a global invalidation into one fresh physical epoch per connection.
// Partial recovery preserves unrelated channels and migrates just affected books.
func (h *bookHub) recoverPending() {
	now := time.Now()
	for _, p := range h.physical {
		if p.closing {
			continue
		}
		all := len(p.routes) > 0
		for _, r := range p.routes {
			if !h.recovery[r.id] || now.Sub(r.lastRecovery) < 5*time.Second {
				all = false
				break
			}
		}
		if all {
			p.closing = true
			p.cancel()
			for _, r := range p.routes {
				r.lastRecovery = now
				delete(h.recovery, r.id)
				_ = r.worker.q.offer(event{InstrumentID: r.id, Stream: deribit.StreamEvent{Kind: "disconnected", Epoch: p.epoch, ReceivedAt: time.Now().UTC()}})
			}
		}
	}
	var partial []*bookRoute
	cancels := map[*bookPhysical][]string{}
	for id := range h.recovery {
		r := h.routes[id]
		if r != nil && now.Sub(r.lastRecovery) < 5*time.Second {
			continue
		}
		if r != nil {
			r.lastRecovery = now
			p := r.physical
			if p != nil {
				ch := "book." + r.symbol + ".100ms"
				delete(p.routes, ch)
				r.physical = nil
				_ = r.worker.q.offer(event{InstrumentID: r.id, Stream: deribit.StreamEvent{Kind: "disconnected", Epoch: p.epoch, ReceivedAt: time.Now().UTC()}})
				if p.requested[ch] {
					cancels[p] = append(cancels[p], ch)
				}
			}
			partial = append(partial, r)
		}
		delete(h.recovery, id)
	}
	for p, channels := range cancels {
		if len(p.routes) == 0 {
			p.closing = true
			p.cancel()
			continue
		}
		for _, ch := range channels {
			h.queueUnsubscribe(p, ch)
		}
	}
	for _, r := range partial {
		h.assign(r)
	}
	h.flushUnsubscribes()
}

func (h *bookHub) queueUnsubscribe(p *bookPhysical, ch string) {
	if p.pendingUnsubscribe == nil {
		p.pendingUnsubscribe = map[string]bool{}
	}
	p.pendingUnsubscribe[ch] = true
}

func (h *bookHub) flushUnsubscribes() {
	for _, p := range h.physical {
		if p.closing || len(p.pendingUnsubscribe) == 0 || p.controlPending() {
			continue
		}
		channels := make([]string, 0, len(p.pendingUnsubscribe))
		for ch := range p.pendingUnsubscribe {
			channels = append(channels, ch)
			if len(channels) == 512 {
				break
			}
		}
		select {
		case p.commands <- deribit.SessionCommand{Method: "public/unsubscribe", Channels: channels}:
			if p.unsubscribing == nil {
				p.unsubscribing = map[string]bool{}
			}
			for _, ch := range channels {
				p.unsubscribing[ch] = true
				delete(p.pendingUnsubscribe, ch)
			}
		default:
			// Keep the bounded intent until the session command queue drains.
		}
	}
}

func (p *bookPhysical) controlPending() bool {
	if len(p.unsubscribing) != 0 {
		return true
	}
	for ch := range p.requested {
		if p.requested[ch] && !p.acked[ch] {
			return true
		}
	}
	return false
}
