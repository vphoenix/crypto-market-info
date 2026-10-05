package lst

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// Reasons retain a stable stage/cause plus the actual send and evidence identity.
// Never put an endpoint, credentials, or an unsanitized network error here.
func failureReason(stage string, err error, res Response) string {
	if err == nil {
		return ""
	}
	cause := strings.ReplaceAll(err.Error(), "\n", "+")
	s := fmt.Sprintf("stage=%s cause=%s http_attempted=%t http=%d", stage, cause, !res.RequestedAt.IsZero(), res.HTTPStatus)
	var rpcErr *RPCError
	if errors.As(err, &rpcErr) {
		s += fmt.Sprintf(" rpc=%d", rpcErr.Code)
	}
	if res.RequestHash != "" {
		s += " request=" + res.RequestHash
	}
	if len(res.PayloadHash) == 32 {
		s += " response=" + Hex(res.PayloadHash)
	} else if len(res.PayloadHash) == 66 {
		s += " response=" + res.PayloadHash
	}
	return s
}

type gateWaitError struct {
	Cause   string
	ReadyAt time.Time
}

func (e *gateWaitError) Error() string {
	return e.Cause + " until=" + e.ReadyAt.UTC().Format(time.RFC3339Nano)
}

func appendReason(existing, extra string) string {
	if existing == "" {
		return extra
	}
	if extra == "" {
		return existing
	}
	return existing + "; " + extra
}

// Preserve the original acceptance thresholds. Report every timing failure so
// a fresh-looking set of legs cannot hide cross-source misalignment.
func quoteTimingReason(q Quote, p ProtocolState, b Block, started, ended time.Time) string {
	reasons := []string{}
	add := func(cause string, value time.Duration) {
		reasons = append(reasons, fmt.Sprintf("stage=timing cause=%s value_ms=%d", cause, value.Milliseconds()))
	}
	if p.StateStatus != "ok" {
		reasons = append(reasons, "stage=timing cause=protocol_unavailable")
	}
	if elapsed := ended.Sub(started); elapsed > 30*time.Second {
		add("round_duration_exceeded", elapsed)
	}
	if b.Time.IsZero() {
		reasons = append(reasons, "stage=timing cause=block_time_missing")
	} else {
		if age := ended.Sub(b.Time); age > 60*time.Second {
			add("block_age_exceeded", age)
		}
		if b.Time.After(ended.Add(2 * time.Second)) {
			add("block_time_in_future", b.Time.Sub(ended))
		}
	}
	pairs := []struct {
		name              string
		source, available *time.Time
	}{
		{"hedge_depth", q.HedgeDepthEventTime, q.HedgeDepthAvailableAt},
		{"mark", q.MarkSourceTime, q.MarkAvailableAt},
	}
	for _, pair := range pairs {
		if pair.source == nil || pair.available == nil {
			reasons = append(reasons, "stage=timing cause="+pair.name+"_time_missing")
			continue
		}
		if age := ended.Sub(*pair.source); age > 15*time.Second {
			add(pair.name+"_source_age_exceeded", age)
		}
		lag := pair.available.Sub(*pair.source)
		if lag > 15*time.Second || lag < -2*time.Second {
			add(pair.name+"_availability_lag_invalid", lag)
		}
		diff := pair.source.Sub(b.Time)
		if diff > 30*time.Second || diff < -30*time.Second {
			add(pair.name+"_block_alignment_exceeded", diff)
		}
	}
	return strings.Join(reasons, "; ")
}
