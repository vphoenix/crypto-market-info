package optionslive

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/exchange/deribit"
)

func TestBookHubRecoversAllChannelsInAcknowledgedBatches(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	p := &bookPhysical{epoch: uuid.New(), ready: true, routes: map[string]*bookRoute{}, requested: map[string]bool{}, acked: map[string]bool{}, commands: make(chan deribit.SessionCommand, 4)}
	h := &bookHub{ctx: ctx, routes: map[uint32]*bookRoute{}, physical: []*bookPhysical{p}, recovery: map[uint32]bool{}}
	for id := uint32(1); id <= 200; id++ {
		r := &bookRoute{id: id, physical: p}
		p.routes[fmt.Sprintf("book.%03d.100ms", id)] = r
		h.routes[id] = r
	}
	h.wg.Add(1)
	go h.maintain()
	defer func() { cancel(); h.wait() }()
	seen := map[string]bool{}
	for len(seen) < 200 {
		select {
		case cmd := <-p.commands:
			if cmd.Method != "public/subscribe" || len(cmd.Channels) == 0 || len(cmd.Channels) > 64 {
				t.Fatal("unbounded recovery subscription", cmd)
			}
			// The next batch must wait for every ACK, even while maintenance ticks.
			select {
			case next := <-p.commands:
				t.Fatal("next batch sent before ACK", next)
			case <-time.After(150 * time.Millisecond):
			}
			h.mu.Lock()
			for _, ch := range cmd.Channels {
				if seen[ch] {
					h.mu.Unlock()
					t.Fatal("recovery subscribed twice", ch)
				}
				seen[ch], p.acked[ch] = true, true
			}
			h.mu.Unlock()
		case <-ctx.Done():
			t.Fatal("recovery omitted channels", len(seen))
		}
	}
}
