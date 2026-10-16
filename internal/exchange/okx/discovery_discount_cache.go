package okx

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
)

const discountResponseMaxAge = time.Minute

type discountFlight struct {
	done     chan struct{}
	response PublicResponse
	err      error
}
type discountResponseCache struct {
	mu     sync.Mutex
	value  PublicResponse
	flight *discountFlight
}

func copyPublicResponse(r PublicResponse) PublicResponse {
	r.Data = append([]byte(nil), r.Data...)
	return r
}

// A cache hit is an explicit reference to the original observation, not a new
// successful source check. No clock or hash is refreshed on a hit. Failed or
// expired observations never serve as fallback during a new physical request.
func (c *Client) discoveryDiscount(ctx context.Context) (PublicResponse, error) {
	if err := ctx.Err(); err != nil {
		return PublicResponse{}, err
	}
	endpoint := strings.TrimRight(c.BaseURL, "/") + "/api/v5/public/discount-rate-interest-free-quota"
	cache := &c.discountResponses
	cache.mu.Lock()
	now := time.Now().UTC().UnixMilli()
	r := cache.value
	if r.Source.URL == endpoint && r.Source.SourceMS > 0 && r.Source.SourceMS <= now && now-r.Source.SourceMS < discountResponseMaxAge.Milliseconds() {
		cache.mu.Unlock()
		return copyPublicResponse(r), nil
	}
	if active := cache.flight; active != nil {
		cache.mu.Unlock()
		select {
		case <-ctx.Done():
			return PublicResponse{}, ctx.Err()
		case <-active.done:
			if err := ctx.Err(); err != nil {
				return PublicResponse{}, err
			}
			return copyPublicResponse(active.response), active.err
		}
	}
	flight := &discountFlight{done: make(chan struct{})}
	cache.flight = flight
	cache.mu.Unlock()
	r, err := c.DiscoveryPublic(ctx, "/api/v5/public/discount-rate-interest-free-quota", nil)
	if err == nil {
		// All capital observations require USDT. Decode and check its exact curve
		// before sharing; each caller separately validates its own currency.
		var tiers []model.DiscountTier
		tiers, err = ParseDiscoveryDiscount(r, "USDT")
		if err == nil {
			version := model.DiscountRuleVersion{Currency: "USDT", Tiers: tiers}
			version.ID = model.DiscoveryHash(version.Body())
			err = version.Validate()
		}
		if err == nil && (r.Source.SourceMS <= 0 || time.Now().UTC().UnixMilli()-r.Source.SourceMS >= discountResponseMaxAge.Milliseconds()) {
			err = fmt.Errorf("shared discount response expired while fetching")
		}
	}
	if err == nil {
		err = ctx.Err()
	}
	cache.mu.Lock()
	if err == nil {
		cache.value = copyPublicResponse(r)
		flight.response = copyPublicResponse(r)
	} else {
		cache.value = PublicResponse{}
	}
	flight.err = err
	cache.flight = nil
	close(flight.done)
	cache.mu.Unlock()
	return copyPublicResponse(flight.response), err
}
