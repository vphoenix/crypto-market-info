package justlendkeeper

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"sort"
	"strconv"
	"time"
)

func (c *Collector) AddBackfill(from, to time.Time) {
	c.State.BackfillActive = true
	c.State.BackfillFrom = from
	c.State.BackfillTo = to
	for at := from; at.Before(to); {
		end := at.Add(24 * time.Hour)
		if end.After(to) {
			end = to
		}
		id := "backfill:Liquidate:" + at.Format(time.RFC3339Nano) + ":" + end.Format(time.RFC3339Nano)
		exists := false
		for i, s := range c.State.Scans {
			if s.ID == id {
				if s.Failed {
					c.State.Scans[i].Done = false
					c.State.Scans[i].Failed = false
				}
				exists = true
			}
		}
		if !exists {
			c.State.Scans = append(c.State.Scans, Scan{ID: id, Kind: "Liquidate", Mode: "backfill", From: UTC(at), To: UTC(end)})
		}
		at = end
	}
}
func (c *Collector) SeedBootstrap() {
	end := UTC(c.Now().Add(-120 * time.Second))
	bounds := []time.Time{end, end.Add(-24 * time.Hour), end.Add(-7 * 24 * time.Hour), end.Add(-30 * 24 * time.Hour)}
	for i := 0; i < 3; i++ {
		for _, kind := range []string{"RentResource", "ReturnResource"} {
			id := "bootstrap:" + kind + ":" + bounds[i+1].Format(time.RFC3339Nano)
			c.State.Scans = append(c.State.Scans, Scan{ID: id, Kind: kind, Mode: "bootstrap", From: bounds[i+1], To: bounds[i], MaxPages: 10})
		}
	}
	for _, kind := range []string{"Liquidate", "RentResource", "ReturnResource"} {
		c.State.EventCursors[kind] = end
	}
	c.State.RescanDay = c.Now().UTC().Format("2006-01-02")
}
func age(at, now time.Time) int {
	d := now.Sub(at)
	if d < 24*time.Hour {
		return 0
	}
	if d < 7*24*time.Hour {
		return 1
	}
	return 2
}
func (c *Collector) SelectCohort() {
	limits := []int{15, 15, 20}
	pool := append([]Candidate(nil), c.State.Candidates...)
	sort.Slice(pool, func(i, j int) bool {
		return Hash([]byte("keeper-cohort-v1"+pool[i].Renter+pool[i].Receiver)) < Hash([]byte("keeper-cohort-v1"+pool[j].Renter+pool[j].Receiver))
	})
	c.State.CohortId = uuid.New()
	for _, can := range pool {
		row, e := EventRow(can.Origin)
		if e != nil || (row.EventKind == "return" && row.AmountSun.Sign() == 0) {
			continue
		}
		a := age(can.LastActivity, c.Now())
		if limits[a] == 0 {
			continue
		}
		limits[a]--
		can.Stratum = uint8(a)
		can.Lifecycle = Hex(row.TxId) + ":" + can.Origin.Name
		c.State.Cohort = append(c.State.Cohort, can)
		c.AddSeed(can)
	}
	c.State.BootstrapReady = true
}
func (c *Collector) AddSeed(can Candidate) {
	for i := range c.State.Cohort {
		member := &c.State.Cohort[i]
		if member.Renter == can.Renter && member.Receiver == can.Receiver {
			if member.SeedRevision != seedRevision {
				member.SeedRevision, member.SeedAttempts = seedRevision, 0
			}
			member.SeedAttempts++
			member.SeedNext = UTC(c.Now().Add(30 * time.Minute))
			can = *member
			break
		}
	}
	o := c.NewOperation("seed", "catchup", "publicnode")
	o.Batch.Capture.CaptureKind = "events"
	o.Batch.Capture.CoverageScope = "sampled_rental_events"
	o.Candidate = &can
	o.RawEvents = []RawEvent{can.Origin}
	if can.Ambiguous {
		o.Failed = true
		o.Batch.Capture.Status = "skipped"
		o.Batch.Capture.Reason = "same_block_candidate_ambiguous"
		o.RawEvents = nil
		c.State.Ops = append(c.State.Ops, o)
		return
	}
	o.Requests = []Request{headRequest(true, "solid", true)}
	o.AddHydration()
	c.State.Ops = append(c.State.Ops, o)
}

// One bounded re-verification at a time, without resetting the fixed cohort,
// source cooldowns or daily budget. A decoder revision can recover old failures.
func (c *Collector) RetrySeed() {
	if c.Active("seed") {
		return
	}
	for _, can := range c.State.Cohort {
		if can.Verified || can.Closed || can.Ambiguous {
			continue
		}
		if can.SeedRevision == seedRevision && (can.SeedAttempts >= 3 || c.Now().Before(can.SeedNext)) {
			continue
		}
		c.AddSeed(can)
		return
	}
}
func (c *Collector) AddProbe(can Candidate, scheduled time.Time) {
	caller := ""
	for _, a := range c.State.CallersValid {
		if a != can.Renter && a != can.Receiver {
			caller = a
			break
		}
	}
	o := c.NewOperation("probe", "live", "publicnode")
	o.Batch.Capture.CaptureKind = "probes"
	o.Candidate = &can
	if caller == "" || !can.Verified || can.Closed {
		o.Failed = true
		o.Batch.Capture.Status = "skipped"
		o.Batch.Capture.Reason = "caller_conflict_or_unverified_candidate"
		c.State.Ops = append(c.State.Ops, o)
		return
	}
	o.Scheduled = UTC(scheduled)
	o.Caller = caller
	param := Hex(can.Renter[1:])
	param = "000000000000000000000000" + param + "000000000000000000000000" + Hex(can.Receiver[1:]) + "0000000000000000000000000000000000000000000000000000000000000001"
	o.Requests = []Request{headRequest(false, "probe_before", false), Post("publicnode", "/wallet/triggerconstantcontract", "probe", "", map[string]any{"owner_address": Hex(caller), "contract_address": ContractHex, "function_selector": "liquidate(address,address,uint256)", "parameter": param, "call_value": 0}, false), headRequest(false, "probe_after", false)}
	c.State.Ops = append(c.State.Ops, o)
}
func (c *Collector) AddCosts() {
	o := c.NewOperation("resources", "live", "publicnode")
	o.Batch.Capture.CaptureKind = "costs"
	o.Requests = []Request{headRequest(false, "cost_before", false), Post("publicnode", "/wallet/getchainparameters", "parameters", "", map[string]any{}, false), Post("publicnode", "/wallet/getcontract", "proxy", "", map[string]any{"value": ContractHex}, false), headRequest(false, "cost_after", false)}
	c.State.Ops = append(c.State.Ops, o)
}
func (c *Collector) AddQuote() {
	o := c.NewOperation("quote", "live", "binance")
	o.Batch.Capture.CaptureKind = "costs"
	o.Requests = []Request{{Source: "binance", Path: "/api/v3/ticker/bookTicker?symbol=TRXUSDT", Role: "quote"}}
	c.State.Ops = append(c.State.Ops, o)
}
func (c *Collector) Active(kind string) bool {
	for _, o := range c.State.Ops {
		if o.Kind == kind {
			return true
		}
	}
	return false
}
func (c *Collector) Schedule(watch bool) {
	if watch && c.State.BootstrapReady && !c.Active("seed") {
		c.RefillCohort()
		c.RetrySeed()
	}
	now := c.Now().UTC()
	kept := c.State.Scans[:0]
	for _, sc := range c.State.Scans {
		if sc.Done && !sc.Failed && (sc.Mode == "live" || (sc.Mode == "catchup" && len(sc.ID) > 6 && sc.ID[:6] == "watch:")) {
			continue
		}
		kept = append(kept, sc)
	}
	c.State.Scans = kept
	if watch && !c.Active("identity") && !now.Before(c.State.NextIdentity) {
		c.AddIdentity()
		c.State.NextIdentity = now.Add(5 * time.Minute)
	}
	if watch && !c.Active("quote") && !now.Before(c.State.NextQuote) {
		c.AddQuote()
		c.State.NextQuote = now.Add(time.Minute)
	}
	if watch && !c.Active("resources") && !now.Before(c.State.NextCosts) {
		c.AddCosts()
		c.State.NextCosts = now.Add(5 * time.Minute)
	}
	if watch && !c.State.BootstrapReady {
		has := false
		done := true
		for _, s := range c.State.Scans {
			if s.Mode == "bootstrap" {
				has = true
				if !s.Done {
					done = false
				}
			}
		}
		if !has {
			c.SeedBootstrap()
		} else if done {
			c.SelectCohort()
		}
	}
	// At most one unfinished page operation per fixed scan; no page burst or fan-out.
	for _, s := range c.State.Scans {
		if s.Done {
			continue
		}
		if !watch && s.Mode != "backfill" {
			continue
		}
		if watch && s.Mode == "backfill" {
			continue
		}
		active := false
		for _, o := range c.State.Ops {
			if o.ScanID == s.ID {
				active = true
				break
			}
		}
		if !active {
			c.AddPage(s)
		}
	}
	if !watch || !c.State.BootstrapReady {
		return
	}
	if !now.Before(c.State.NextEvents) {
		end := UTC(now.Add(-120 * time.Second))
		for _, kind := range []string{"Liquidate", "RentResource", "ReturnResource"} {
			active := false
			for _, s := range c.State.Scans {
				if s.Kind == kind && (s.Mode == "live" || s.Mode == "catchup") && !s.Done {
					active = true
				}
			}
			if active {
				continue
			}
			from := c.State.EventCursors[kind]
			if from.IsZero() {
				from = end.Add(-30 * time.Second)
			}
			to := from.Add(30 * time.Second)
			if to.After(end) {
				to = end
			}
			if !from.Before(to) {
				continue
			}
			id := "watch:" + kind + ":" + from.Format(time.RFC3339Nano)
			mode := "live"
			if now.Sub(from) > 3*time.Minute {
				mode = "catchup"
				// Enlarge the time window, never the request rate, to drain a
				// bootstrap/restart backlog without replaying missed timer ticks.
				to = from.Add(5 * time.Minute)
				if to.After(end) {
					to = end
				}
			}
			sc := Scan{ID: id, Kind: kind, Mode: mode, From: from, To: to}
			found := -1
			for i, s := range c.State.Scans {
				if s.ID == id {
					found = i
					break
				}
			}
			if found >= 0 {
				c.State.Scans[found].Done = false
				c.State.Scans[found].Failed = false
				c.AddPage(c.State.Scans[found])
			} else {
				c.State.Scans = append(c.State.Scans, sc)
				c.AddPage(sc)
			}
		}
		c.State.NextEvents = now.Add(30 * time.Second)
	}
	if !c.Active("probe") && !now.Before(c.State.NextProbe) {
		n := len(c.State.Cohort)
		for i := 0; i < 5 && i < n; i++ {
			idx := (int(c.State.Rotation) + i) % n
			c.AddProbe(c.State.Cohort[idx], now)
		}
		if n > 0 {
			c.State.Rotation = (c.State.Rotation + 5) % uint32(n)
		}
		c.State.NextProbe = now.Add(30 * time.Second)
	}
	day := now.UTC().Format("2006-01-02")
	if c.State.RescanDay != "" && c.State.RescanDay != day {
		end := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
		s := Scan{ID: "rescan:" + day, Kind: "Liquidate", Mode: "catchup", From: end.Add(-24 * time.Hour), To: end}
		c.State.Scans = append(c.State.Scans, s)
		c.AddPage(s)
		c.State.RescanDay = day
	}
}
func (c *Collector) Run(ctx context.Context, watch bool, maxPages uint32) error {
	pages := uint32(0)
	for _, s := range c.State.Scans {
		pages += s.Pages
	}
	base := pages
	for {
		if e := ctx.Err(); e != nil {
			return e
		}
		c.Schedule(watch)
		if e := c.Save(); e != nil {
			return e
		}
		done := !watch
		pages = 0
		partial := false
		for _, s := range c.State.Scans {
			pages += s.Pages
			if s.Mode == "backfill" {
				if !s.Done {
					done = false
				}
				if s.Failed {
					partial = true
				}
			}
		}
		if done && len(c.State.Ops) == 0 {
			if !partial {
				c.State.BackfillActive = false
				if e := c.Save(); e != nil {
					return e
				}
			}
			if partial {
				return errors.New("backfill_incomplete")
			}
			return nil
		}
		if !watch && maxPages > 0 && pages-base >= maxPages {
			return nil
		}
		progress, next, e := c.Step(ctx)
		if e != nil {
			return e
		}
		if !progress {
			wait := next.Sub(c.Now())
			if wait > time.Second {
				wait = time.Second
			}
			if wait < time.Millisecond {
				wait = time.Millisecond
			}
			if e = c.Sleep(ctx, wait); e != nil {
				return e
			}
		}
	}
}

func (c *Collector) RefillCohort() {
	pool := append([]Candidate(nil), c.State.Candidates...)
	sort.Slice(pool, func(i, j int) bool {
		return Hash([]byte("keeper-cohort-v1"+pool[i].Renter+pool[i].Receiver)) < Hash([]byte("keeper-cohort-v1"+pool[j].Renter+pool[j].Receiver))
	})
	for slot, old := range c.State.Cohort {
		if !old.Closed || !old.Verified {
			continue
		}
		for _, can := range pool {
			if can.Closed || can.Ambiguous || age(can.LastActivity, c.Now()) != int(old.Stratum) {
				continue
			}
			row, e := EventRow(can.Origin)
			if e != nil || row.EventKind != "rent" {
				continue
			}
			used := false
			for _, member := range c.State.Cohort {
				if member.Renter == can.Renter && member.Receiver == can.Receiver {
					used = true
				}
			}
			if used {
				continue
			}
			can.Stratum = old.Stratum
			can.Verified = false
			can.Lifecycle = "seed:" + can.Origin.Transaction + ":" + strconv.Itoa(int(can.Origin.Index))
			c.State.Cohort[slot] = can
			c.State.CohortChanges = append(c.State.CohortChanges, CohortChange{UTC(c.Now()), slot, Snapshot(old), Snapshot(can), "solid_close_same_stratum_replacement"})
			if len(c.State.CohortChanges) > 100 {
				c.State.CohortChanges = c.State.CohortChanges[len(c.State.CohortChanges)-100:]
			}
			c.AddSeed(can)
			break
		}
	}
}
