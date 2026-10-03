package justlendkeeper

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"github.com/shopspring/decimal"
	"math/big"
	"strings"
	"time"
)

func ParseConstant(b []byte) (string, error) {
	var s struct {
		Result struct {
			Result  *bool  `json:"result"`
			Message string `json:"message"`
		} `json:"result"`
		Constant    []string `json:"constant_result"`
		Transaction struct {
			Ret []map[string]json.RawMessage `json:"ret"`
		} `json:"transaction"`
	}
	if e := Decode(b, &s); e != nil {
		return "", e
	}
	if s.Result.Result == nil || !*s.Result.Result || len(s.Constant) != 1 || len(s.Transaction.Ret) != 1 {
		return "", errors.New("constant_output_incomplete")
	}
	msg, _ := hex.DecodeString(s.Result.Message)
	if strings.Contains(strings.ToUpper(string(msg)), "REVERT") {
		return "", errors.New("constant_revert")
	}
	if raw, ok := s.Transaction.Ret[0]["ret"]; ok && string(raw) != "\"SUCESS\"" && string(raw) != "0" {
		return "", errors.New("constant_tvm_failed")
	}
	if cr, ok := s.Transaction.Ret[0]["contractRet"]; ok && string(cr) != "1" && string(cr) != "\"SUCCESS\"" {
		return "", errors.New("constant_contract_result_not_success")
	}
	if len(s.Constant[0]) != 64 {
		return "", errors.New("constant_output_length")
	}
	return s.Constant[0], nil
}
func ParseProbe(b []byte, ev Evidence, can Candidate, caller string, scheduled time.Time, c *Collector) (Probe, error) {
	p := Probe{ResourceType: 1, Renter: can.Renter, Receiver: can.Receiver, CallerAddress: caller, ScheduledAt: UTC(scheduled), RequestStartedAt: Ptr(ev.Started), ResponseReceivedAt: ev.Received, AvailableAt: ev.Available, EndpointView: "wallet_latest", StateBinding: "node_latest_unpinned", IdentityStatus: c.State.IdentityStatus, Status: "unknown", RewardConsistency: "incomplete", CandidateOriginTx: Ptr(mustHex(can.Origin.Transaction)), CandidateOriginEventIndex: Ptr(can.Origin.Index)}
	p.ContractAddress, _ = HexAddress(ContractHex)
	if c.State.Implementation != "" {
		p.ImplementationAddress = Ptr(c.State.Implementation)
	}
	if c.State.CodeHash != "" {
		p.ImplementationCodeHash = Ptr(c.State.CodeHash)
	}
	if !c.State.IdentityAt.IsZero() {
		p.ImplementationCheckedAt = Ptr(c.State.IdentityAt)
	}
	if ev.Available.Sub(c.State.IdentityAt) > 10*time.Minute {
		p.IdentityStatus = "unknown"
	}
	if ev.RequestHash != "" {
		p.RequestPayloadHash = Ptr(mustHex(ev.RequestHash))
	}
	if ev.ResponseHash != "" {
		p.ResponsePayloadHash = Ptr(mustHex(ev.ResponseHash))
	}
	var s struct {
		Result struct {
			Result  *bool  `json:"result"`
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"result"`
		Energy      *uint64  `json:"energy_used"`
		Penalty     *uint64  `json:"energy_penalty"`
		Constant    []string `json:"constant_result"`
		Transaction struct {
			Ret []map[string]json.RawMessage `json:"ret"`
		} `json:"transaction"`
		Logs     []Log           `json:"logs"`
		Internal json.RawMessage `json:"internal_transactions"`
	}
	if e := Decode(b, &s); e != nil {
		return p, e
	}
	p.ApiSuccess = s.Result.Result
	p.EnergyUsed = s.Energy
	p.EnergyPenalty = s.Penalty
	p.ErrorCode = s.Result.Code
	msg, _ := hex.DecodeString(s.Result.Message)
	p.ErrorMessage = string(msg)
	if s.Result.Result == nil || !*s.Result.Result {
		p.Status = "rpc_error"
		return p, nil
	}
	if len(s.Transaction.Ret) != 1 {
		p.ErrorCode = "missing_tvm_result"
		return p, nil
	}
	if cr, ok := s.Transaction.Ret[0]["contractRet"]; ok && string(cr) != "1" && string(cr) != "\"SUCCESS\"" {
		p.TvmResult = "contractRet:" + string(cr)
		if string(cr) == "0" || string(cr) == "\"DEFAULT\"" {
			p.ErrorCode = "unknown_contract_result"
		} else {
			p.Status = "revert"
		}
		return p, nil
	}
	p.TvmResult = "SUCESS_default"
	if raw, ok := s.Transaction.Ret[0]["ret"]; ok {
		p.TvmResult = string(raw)
		if string(raw) == "\"FAILED\"" || string(raw) == "1" {
			p.Status = "revert"
			return p, nil
		}
		if string(raw) != "\"SUCESS\"" && string(raw) != "0" {
			p.ErrorCode = "unknown_tvm_result"
			return p, nil
		}
	}
	if strings.Contains(strings.ToUpper(p.ErrorMessage), "REVERT") || (len(s.Constant) > 0 && strings.HasPrefix(s.Constant[0], "08c379a0")) {
		p.Status = "revert"
		return p, nil
	}
	if len(s.Constant) != 1 || len(s.Constant[0]) != 64 {
		p.ErrorCode = "unknown_success_abi"
		return p, nil
	}
	raw, e := hex.DecodeString(s.Constant[0])
	if e != nil {
		return p, e
	}
	p.RewardReturnSun = new(big.Int).SetBytes(raw)
	reward := big.NewInt(0)
	logCount := 0
	for _, l := range s.Logs {
		row, e := DecodeLog(l)
		if e == nil && row.EventKind == "liquidate" && row.Renter == can.Renter && row.Receiver == can.Receiver && row.Liquidator != nil && *row.Liquidator == caller {
			reward.Add(reward, row.RewardSun)
			logCount++
		}
	}
	if logCount > 0 {
		p.RewardLogSun = reward
	}
	if len(s.Internal) > 0 {
		ts, e := Transfers(s.Internal)
		if e != nil {
			return p, e
		}
		sum := big.NewInt(0)
		for _, t := range ts {
			if !t.Rejected && t.Sender == p.ContractAddress && t.Recipient == caller {
				sum.Add(sum, &t.AmountSun)
			}
		}
		p.RewardTransferSun = sum
	}
	if p.RewardLogSun != nil && p.RewardTransferSun != nil {
		if p.RewardLogSun.Cmp(p.RewardReturnSun) == 0 && p.RewardTransferSun.Cmp(p.RewardReturnSun) == 0 {
			p.RewardConsistency = "matched"
		} else {
			p.RewardConsistency = "mismatch"
		}
	}
	if p.RewardReturnSun.Sign() == 0 && p.IdentityStatus == "verified_manifest" && p.RewardConsistency != "mismatch" {
		p.Status = "success_zero"
		return p, nil
	}
	p.ErrorCode = "success_fixture_not_certified"
	return p, nil
}
func ParseQuote(b []byte, ev Evidence) (CostObservation, error) {
	r := CostObservation{ObservationKind: "trx_usdt_bbo", SourceId: "binance", RequestStartedAt: ev.Started, ReceivedAt: ev.Received, AvailableAt: ev.Available, SourceTime: ev.Received, SourceTimeKind: "received", StateBinding: "offchain", Symbol: "TRXUSDT", Status: "ok", PayloadHash: Ptr(mustHex(ev.ResponseHash))}
	var s struct {
		Symbol string `json:"symbol"`
		Bid    string `json:"bidPrice"`
		BidQty string `json:"bidQty"`
		Ask    string `json:"askPrice"`
		AskQty string `json:"askQty"`
	}
	if e := Decode(b, &s); e != nil {
		return r, e
	}
	if s.Symbol != "TRXUSDT" {
		return r, errors.New("quote_asset_mismatch")
	}
	for _, f := range []struct {
		Text string
		Dest **decimal.Decimal
	}{{s.Bid, &r.BidPriceUsdt}, {s.BidQty, &r.BidQtyTrx}, {s.Ask, &r.AskPriceUsdt}, {s.AskQty, &r.AskQtyTrx}} {
		d, e := decimal.NewFromString(f.Text)
		if e != nil || d.Sign() <= 0 || d.Exponent() < -18 || d.Coefficient().BitLen() > 128 || d.GreaterThanOrEqual(decimal.New(1, 20)) {
			return r, errors.New("invalid_bbo_decimal")
		}
		// Match ClickHouse Decimal(38,18)'s representation before hashing.
		fixed := decimal.NewFromBigInt(d.Shift(18).BigInt(), -18)
		if !fixed.Equal(d) {
			return r, errors.New("bbo_decimal_precision_loss")
		}
		*f.Dest = Ptr(fixed)
	}
	if r.BidPriceUsdt.GreaterThan(*r.AskPriceUsdt) {
		return r, errors.New("crossed_bbo")
	}
	return r, nil
}
func ParseParameters(b []byte, ev Evidence) (CostObservation, error) {
	r := CostObservation{ObservationKind: "chain_resource", SourceId: "publicnode", RequestStartedAt: ev.Started, ReceivedAt: ev.Received, AvailableAt: ev.Available, SourceTime: ev.Received, SourceTimeKind: "received", StateBinding: "node_latest_unpinned", Status: "ok", PayloadHash: Ptr(mustHex(ev.ResponseHash))}
	var s struct {
		Parameters []struct {
			Key   string          `json:"key"`
			Value json.RawMessage `json:"value"`
		} `json:"chainParameter"`
	}
	if e := Decode(b, &s); e != nil {
		return r, e
	}
	seen := map[string]bool{}
	for _, p := range s.Parameters {
		if seen[p.Key] {
			return r, errors.New("duplicate_chain_parameter")
		}
		seen[p.Key] = true
		// TRON ChainParameter.Value is signed int64; unrelated parameters
		// legitimately use -1. A missing value is a protobuf zero default,
		// but never supplies a missing required resource price.
		var value int64
		if len(p.Value) > 0 {
			if string(p.Value) == "null" || Decode(p.Value, &value) != nil {
				return r, errors.New("invalid_chain_parameter_integer")
			}
		}
		switch p.Key {
		case "getEnergyFee":
			if len(p.Value) == 0 || value <= 0 {
				return r, errors.New("invalid_energy_price")
			}
			r.EnergyFeeSunPerUnit = Ptr(uint64(value))
		case "getTransactionFee":
			if len(p.Value) == 0 || value <= 0 {
				return r, errors.New("invalid_bandwidth_price")
			}
			r.BandwidthFeeSunPerByte = Ptr(uint64(value))
		}
	}
	if r.EnergyFeeSunPerUnit == nil || r.BandwidthFeeSunPerByte == nil || *r.EnergyFeeSunPerUnit == 0 || *r.BandwidthFeeSunPerByte == 0 {
		return r, errors.New("missing_resource_price")
	}
	return r, nil
}

func (c *Collector) ErrorProbe(o *Operation, ev Evidence) Probe {
	p, _ := ParseProbe([]byte(`{"result":{"result":false}}`), ev, *o.Candidate, o.Caller, o.Scheduled, c)
	p.ApiSuccess = nil
	p.Status = "rpc_error"
	if ev.Error == "timeout" {
		p.Status = "timeout"
	}
	p.ErrorCode = ev.Error
	p.ProbeIndex = uint32(len(o.Batch.Probes))
	p.CaptureId = o.Batch.Capture.CaptureId
	p.CaptureStartedAt = o.Batch.Capture.CaptureStartedAt
	p.CohortId = c.State.CohortId
	return p
}
func (c *Collector) ErrorCost(o *Operation, r Request, ev Evidence) {
	q := CostObservation{SourceId: r.Source, RequestStartedAt: ev.Started, ReceivedAt: ev.Received, AvailableAt: ev.Available, Status: "error", Reason: ev.Error, SourceTimeKind: "received", SourceTime: ev.Received, CaptureId: o.Batch.Capture.CaptureId, CaptureStartedAt: o.Batch.Capture.CaptureStartedAt, ObservationIndex: uint32(len(o.Batch.Costs))}
	if ev.ResponseHash != "" {
		q.PayloadHash = Ptr(mustHex(ev.ResponseHash))
	}
	if r.Role == "quote" {
		q.ObservationKind = "trx_usdt_bbo"
		q.Symbol = "TRXUSDT"
		q.StateBinding = "offchain"
	} else {
		q.ObservationKind = "chain_resource"
		q.StateBinding = "node_latest_unpinned"
	}
	o.Batch.Costs = append(o.Batch.Costs, q)
}
