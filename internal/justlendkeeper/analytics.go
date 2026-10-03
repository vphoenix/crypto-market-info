package justlendkeeper

import (
	"errors"
	"math/big"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/shopspring/decimal"
)

type completeWindow struct{ From, To time.Time }
type windowPages struct {
	From, To time.Time
	Edges    map[string]string
	Valid    bool
}

func windowComplete(p windowPages) bool {
	if !p.Valid {
		return false
	}
	seen := map[string]bool{}
	at := ""
	for {
		next, ok := p.Edges[at]
		if !ok || seen[at] {
			return false
		}
		seen[at] = true
		if next == "" {
			return true
		}
		at = next
	}
}
func covered(from, to time.Time, ws []completeWindow) bool {
	sort.Slice(ws, func(i, j int) bool { return ws[i].From.Before(ws[j].From) })
	at := from
	for _, w := range ws {
		if w.From.After(at) {
			break
		}
		if w.To.After(at) {
			at = w.To
		}
		if !at.Before(to) {
			return true
		}
	}
	return false
}
func saveStats(out string, events []RentalEvent, ws []completeWindow, from, to time.Time, s *Summary) error {
	amounts := map[string]*big.Int{}
	days := map[string]*big.Int{}
	counts := map[string]int{}
	for _, r := range events {
		a := Hex(*r.Liquidator)
		if amounts[a] == nil {
			amounts[a] = big.NewInt(0)
		}
		amounts[a].Add(amounts[a], r.RewardSun)
		d := r.BlockTime.Format("2006-01-02")
		if days[d] == nil {
			days[d] = big.NewInt(0)
		}
		days[d].Add(days[d], r.RewardSun)
		counts[d]++
	}
	type pair struct {
		Key    string
		Amount *big.Int
	}
	addresses := []pair{}
	total := big.NewInt(0)
	for k, n := range amounts {
		addresses = append(addresses, pair{k, n})
		total.Add(total, n)
	}
	sort.Slice(addresses, func(i, j int) bool { return addresses[i].Amount.Cmp(addresses[j].Amount) > 0 })
	rows := [][]string{}
	top := big.NewInt(0)
	for i, p := range addresses {
		share := decimal.Zero
		if total.Sign() > 0 {
			share = decimal.NewFromBigInt(p.Amount, 0).Div(decimal.NewFromBigInt(total, 0)).Mul(decimal.NewFromInt(100))
		}
		rows = append(rows, []string{p.Key, sun(p.Amount), share.String()})
		if i < 2 {
			top.Add(top, p.Amount)
		}
	}
	if total.Sign() > 0 {
		s.TopTwoAddressPercent = decimal.NewFromBigInt(top, 0).Div(decimal.NewFromBigInt(total, 0)).Mul(decimal.NewFromInt(100)).String()
	}
	if e := CSV(filepath.Join(out, "address_concentration.csv"), []string{"liquidator_hex", "observed_reward_trx", "share_percent"}, rows); e != nil {
		return e
	}
	at := time.Date(from.UTC().Year(), from.UTC().Month(), from.UTC().Day(), 0, 0, 0, 0, time.UTC)
	dayRows := [][]string{}
	fullAmounts := []decimal.Decimal{}
	observedDays := []*big.Int{}
	full := map[string]bool{}
	for day := at; day.Before(to); day = day.Add(24 * time.Hour) {
		key := day.Format("2006-01-02")
		amount := days[key]
		if amount == nil {
			amount = big.NewInt(0)
		}
		complete := !day.Before(from) && !day.Add(24*time.Hour).After(to) && covered(day, day.Add(24*time.Hour), ws)
		full[key] = complete
		label := "unknown"
		if complete {
			label = sun(amount)
			fullAmounts = append(fullAmounts, decimal.NewFromBigInt(amount, -6))
		}
		dayRows = append(dayRows, []string{key, strconv.Itoa(counts[key]), sun(amount), strconv.FormatBool(complete), label})
		observedDays = append(observedDays, amount)
	}
	s.CompleteUTCDays = len(fullAmounts)
	if len(fullAmounts) > 0 {
		sort.Slice(fullAmounts, func(i, j int) bool { return fullAmounts[i].LessThan(fullAmounts[j]) })
		m := fullAmounts[len(fullAmounts)/2]
		if len(fullAmounts)%2 == 0 {
			m = m.Add(fullAmounts[len(fullAmounts)/2-1]).Div(decimal.NewFromInt(2))
		}
		s.CompleteDayMedianTRX = Ptr(m.String())
	}
	var worst *big.Int
	for start := at; !start.Add(7 * 24 * time.Hour).After(to); start = start.Add(24 * time.Hour) {
		sum := big.NewInt(0)
		ok := true
		for i := 0; i < 7; i++ {
			k := start.Add(time.Duration(i) * 24 * time.Hour).Format("2006-01-02")
			if !full[k] {
				ok = false
				break
			}
			if days[k] != nil {
				sum.Add(sum, days[k])
			}
		}
		if ok && (worst == nil || sum.Cmp(worst) < 0) {
			worst = new(big.Int).Set(sum)
		}
	}
	if worst != nil {
		s.WorstCompleteSevenDayTRX = Ptr(sun(worst))
	}
	sort.Slice(observedDays, func(i, j int) bool { return observedDays[i].Cmp(observedDays[j]) > 0 })
	topDays := big.NewInt(0)
	for i, n := range observedDays {
		if i >= 5 {
			break
		}
		topDays.Add(topDays, n)
	}
	if total.Sign() > 0 {
		s.TopFiveObservedDayPercent = decimal.NewFromBigInt(topDays, 0).Div(decimal.NewFromBigInt(total, 0)).Mul(decimal.NewFromInt(100)).String()
	}
	return CSV(filepath.Join(out, "daily_rewards.csv"), []string{"utc_day", "observed_events", "observed_reward_trx", "vendor_window_complete", "complete_day_reward_trx"}, dayRows)
}
func costAt(costs []CostObservation, kind string, at time.Time, age time.Duration) *CostObservation {
	var selected *CostObservation
	for i := range costs {
		q := &costs[i]
		if q.ObservationKind != kind || q.Status != "ok" || q.AvailableAt.After(at) || q.SourceTime == nil || q.SourceTime.After(at) || at.Sub(*q.SourceTime) > age || at.Sub(q.AvailableAt) > age {
			continue
		}
		if selected == nil || q.AvailableAt.After(selected.AvailableAt) {
			selected = q
		}
	}
	return selected
}
func saveCosts(out string, costs []CostObservation, probes []Probe) error {
	rawRows := [][]string{}
	for _, q := range costs {
		bid, ask := "unknown", "unknown"
		if q.BidPriceUsdt != nil {
			bid = q.BidPriceUsdt.String()
		}
		if q.AskPriceUsdt != nil {
			ask = q.AskPriceUsdt.String()
		}
		rawRows = append(rawRows, []string{q.CaptureId.String(), strconv.Itoa(int(q.ObservationIndex)), q.ObservationKind, q.SourceId, q.AvailableAt.Format(time.RFC3339Nano), q.Status, q.Reason, fieldU(q.EnergyFeeSunPerUnit), fieldU(q.BandwidthFeeSunPerByte), bid, ask})
	}
	if e := CSV(filepath.Join(out, "cost_observations.csv"), []string{"capture_id", "index", "kind", "source", "effective_available_utc", "status", "reason", "sun_per_energy", "sun_per_bandwidth_byte", "trx_usdt_bid", "trx_usdt_ask"}, rawRows); e != nil {
		return e
	}
	rows := [][]string{}
	for _, p := range probes {
		maxPrice, margin, revalue := "unknown", "unknown", "unknown"
		status := "missing_reward_or_energy"
		price := costAt(costs, "trx_usdt_bbo", p.AvailableAt, 120*time.Second)
		resource := costAt(costs, "chain_resource", p.AvailableAt, 10*time.Minute)
		if p.RewardReturnSun != nil && p.EnergyUsed != nil && *p.EnergyUsed > 0 && resource != nil && resource.EnergyFeeSunPerUnit != nil && resource.BandwidthFeeSunPerByte != nil {
			r := decimal.NewFromBigInt(p.RewardReturnSun, 0)
			e := decimal.NewFromBigInt(new(big.Int).SetUint64(*p.EnergyUsed), 0)
			bw := decimal.NewFromInt(400).Mul(decimal.NewFromBigInt(new(big.Int).SetUint64(*resource.BandwidthFeeSunPerByte), 0))
			budget := r.Sub(bw)
			if budget.Sign() > 0 {
				maxPrice = budget.Div(e).String()
			} else {
				maxPrice = "0"
			}
			margin = budget.Sub(e.Mul(decimal.NewFromBigInt(new(big.Int).SetUint64(*resource.EnergyFeeSunPerUnit), 0))).Shift(-6).String()
			status = "uncertified_reward_burn_only_not_net_profit"
			if price != nil && price.BidPriceUsdt != nil && price.BidQtyTrx != nil {
				trx := decimal.NewFromBigInt(p.RewardReturnSun, -6)
				if trx.LessThanOrEqual(*price.BidQtyTrx) {
					revalue = trx.Mul(*price.BidPriceUsdt).String()
				} else {
					status += ";bbo_capacity_unknown"
				}
			}
		}
		rows = append(rows, []string{p.CaptureId.String(), strconv.Itoa(int(p.ProbeIndex)), p.AvailableAt.Format(time.RFC3339Nano), status, fieldBig(p.RewardReturnSun), fieldU(p.EnergyUsed), "400", maxPrice, margin, revalue, "unknown"})
	}
	return CSV(filepath.Join(out, "resource_break_even.csv"), []string{"capture_id", "probe_index", "available_utc", "status", "reward_return_sun", "energy_used", "assumed_bandwidth_bytes", "max_sun_per_energy_before_other_costs", "burn_only_margin_trx_before_other_costs", "bid_capacity_checked_gross_usdt", "all_in_net_profit"}, rows)
}
func verifyManifest(a Archive, cap Capture) (Manifest, error) {
	b, e := a.Get(cap.EvidenceManifestHash)
	if e != nil {
		return Manifest{}, e
	}
	var m Manifest
	if e = Decode(b, &m); e != nil {
		return m, e
	}
	if m.Version != "keeper-v1" || m.Capture != cap.CaptureId.String() {
		return m, errors.New("manifest_capture_mismatch")
	}
	if m.DigestEncoding != "jl-keeper-fact-v1" && (m.DigestEncoding != "" || cap.EventRows+cap.ReceiptRows+cap.ProbeRows+cap.CostRows != 0) {
		return m, errors.New("manifest_digest_encoding_unsupported")
	}
	for _, ev := range m.Requests {
		for _, h := range []string{ev.RequestHash, ev.ResponseHash} {
			if h != "" {
				x, e := BinaryHex(h, 32)
				if e != nil {
					return m, e
				}
				if _, e = a.Get(x); e != nil {
					return m, e
				}
			}
		}
	}
	return m, nil
}
