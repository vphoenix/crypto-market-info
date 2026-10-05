package justlendkeeper

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/crypto/sha3"
	"io"
	"math/big"
	"net/url"
	"regexp"
	"strings"
	"time"
)

var digits = regexp.MustCompile(`^(0|[1-9][0-9]*)$`)

func strictTokens(d *json.Decoder) error {
	t, e := d.Token()
	if e != nil {
		return e
	}
	switch x := t.(type) {
	case json.Delim:
		switch x {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				k, e := d.Token()
				if e != nil {
					return e
				}
				s, ok := k.(string)
				if !ok || seen[s] {
					return errors.New("duplicate_json_key")
				}
				seen[s] = true
				if e = strictTokens(d); e != nil {
					return e
				}
			}
			_, e = d.Token()
			return e
		case '[':
			for d.More() {
				if e = strictTokens(d); e != nil {
					return e
				}
			}
			_, e = d.Token()
			return e
		default:
			return errors.New("unexpected_json_delimiter")
		}
	}
	return nil
}
func Decode(b []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	if e := strictTokens(d); e != nil {
		return e
	}
	if _, e := d.Token(); e != io.EOF {
		return errors.New("trailing_json")
	}
	d = json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	return d.Decode(v)
}
func Uint(b json.RawMessage) (*big.Int, error) {
	s := string(b)
	if strings.HasPrefix(s, "\"") {
		if e := json.Unmarshal(b, &s); e != nil {
			return nil, e
		}
	}
	if !digits.MatchString(s) {
		return nil, errors.New("invalid_unsigned_integer")
	}
	n, ok := new(big.Int).SetString(s, 10)
	if !ok || n.BitLen() > 256 {
		return nil, errors.New("uint256_overflow")
	}
	return n, nil
}
func Num(m map[string]json.RawMessage, k string) (*big.Int, error) {
	b, ok := m[k]
	if !ok {
		return nil, fmt.Errorf("missing_%s", k)
	}
	return Uint(b)
}
func OptU(m map[string]json.RawMessage, k string) (*uint64, error) {
	b, ok := m[k]
	if !ok {
		return nil, nil
	}
	n, e := Uint(b)
	if e != nil || n.BitLen() > 64 {
		return nil, fmt.Errorf("invalid_%s", k)
	}
	return Ptr(n.Uint64()), nil
}
func ReqU(m map[string]json.RawMessage, k string) (uint64, error) {
	n, e := OptU(m, k)
	if e != nil || n == nil {
		return 0, fmt.Errorf("missing_or_invalid_%s", k)
	}
	return *n, nil
}
func text(m map[string]json.RawMessage, k string) (string, error) {
	var s string
	b, ok := m[k]
	if !ok {
		return "", fmt.Errorf("missing_%s", k)
	}
	if e := json.Unmarshal(b, &s); e != nil {
		return "", e
	}
	return s, nil
}
func WordAddress(s string) (string, error) {
	b, e := hex.DecodeString(s)
	if e != nil || len(b) != 32 || !bytes.Equal(b[:12], make([]byte, 12)) {
		return "", errors.New("invalid_abi_address")
	}
	return string(append([]byte{0x41}, b[12:]...)), nil
}
func Topic(signature string) string {
	h := sha3.NewLegacyKeccak256()
	h.Write([]byte(signature))
	return string(h.Sum(nil))
}

var signatures = map[string]string{"Liquidate": "Liquidate(address,address,address,uint256,uint256,uint256,uint256,uint256)", "RentResource": "RentResource(address,address,uint256,uint256,uint256,uint256)", "ReturnResource": "ReturnResource(address,address,uint256,uint256,uint256,uint256,uint256)"}

const seedRevision = "energy-market-events-v2"

var extendedSignatures = map[string]string{
	"RentResource":   "RentResource(address,address,uint256,uint256,uint256,uint256,uint256,uint256)",
	"ReturnResource": "ReturnResource(address,address,uint256,uint256,uint256,uint256,uint256,uint256,uint256)",
}

type RawEvent struct {
	BlockNumber     uint64                     `json:"block_number"`
	BlockTimestamp  int64                      `json:"block_timestamp"`
	ContractAddress string                     `json:"contract_address"`
	Index           uint32                     `json:"event_index"`
	Ordinal         uint32                     `json:"-"`
	Name            string                     `json:"event_name"`
	Result          map[string]json.RawMessage `json:"result"`
	Transaction     string                     `json:"transaction_id"`
	Evidence        Evidence                   `json:"-"`
}
type Page struct {
	ExcludedBoundary uint32     `json:"-"`
	ExcludedResource uint32     `json:"-"`
	Data             []RawEvent `json:"data"`
	Success          *bool      `json:"success"`
	Meta             struct {
		Links struct {
			Next string `json:"next"`
		} `json:"links"`
		Fingerprint string `json:"fingerprint"`
	} `json:"meta"`
}

func ParsePage(b []byte, ev Evidence, c Config, s Scan) (Page, string, error) {
	var p Page
	if e := Decode(b, &p); e != nil {
		return p, "", e
	}
	if p.Success == nil || !*p.Success || p.Data == nil || len(p.Data) > 200 {
		return p, "", errors.New("invalid_event_page")
	}
	// A source index missing from JSON must not silently become index zero.
	var required struct {
		Data []map[string]json.RawMessage `json:"data"`
	}
	if e := Decode(b, &required); e != nil {
		return p, "", e
	}
	for _, row := range required.Data {
		if n, e := ReqU(row, "event_index"); e != nil || n > uint64(^uint32(0)) {
			return p, "", errors.New("invalid_or_missing_event_index")
		}
	}
	filtered := make([]RawEvent, 0, len(p.Data))
	lo, hi := eventQueryBounds(s.From, s.To)
	for i := range p.Data {
		r := &p.Data[i]
		r.Ordinal = uint32(i)
		r.Evidence = ev
		at := time.UnixMilli(r.BlockTimestamp)
		if r.Name != s.Kind || r.ContractAddress != ContractBase58 || r.BlockNumber == 0 || r.BlockTimestamp <= 0 || at.Before(lo) || !at.Before(hi) {
			return p, "", errors.New("event_filter_mismatch")
		}
		if _, e := BinaryHex(r.Transaction, 32); e != nil {
			return p, "", e
		}
		resource, e := Num(r.Result, "resourceType")
		if e != nil {
			return p, "", e
		}
		if resource.Sign() == 0 {
			p.ExcludedResource++
			continue
		}
		if _, e := EventRow(*r); e != nil {
			return p, "", e
		}
		if at.Before(s.From) || !at.Before(s.To) {
			p.ExcludedBoundary++
			continue
		}
		filtered = append(filtered, *r)
	}
	p.Data = filtered
	next := ""
	if p.Meta.Links.Next != "" {
		u, e := url.Parse(p.Meta.Links.Next)
		base, _ := url.Parse(c.EventAPIURL)
		if e != nil || u.User != nil || u.Fragment != "" || (u.Host != "" && (u.Host != base.Host || u.Scheme != base.Scheme)) || u.Path != "/v1/contracts/"+ContractBase58+"/events" {
			return p, "", errors.New("unsafe_next_page")
		}
		q := u.Query()
		expected, _ := url.Parse(EventPath(s.Kind, s.From, s.To, ""))
		for k, vals := range expected.Query() {
			if q.Get(k) != vals[0] {
				return p, "", errors.New("next_page_filter_changed")
			}
		}
		for k, vals := range q {
			if len(vals) != 1 || (expected.Query().Get(k) == "" && k != "fingerprint") {
				return p, "", errors.New("unexpected_next_page_query")
			}
		}
		next = q.Get("fingerprint")
		if next == "" || next == s.Fingerprint {
			return p, "", errors.New("nonprogressing_pagination")
		}
	}
	if p.Meta.Fingerprint != "" && next != "" && p.Meta.Fingerprint != next {
		return p, "", errors.New("pagination_fingerprint_conflict")
	}
	return p, next, nil
}
func EventRow(r RawEvent) (RentalEvent, error) {
	var out RentalEvent
	out.ResourceType = 1
	out.Finality = "solid"
	out.ContractAddress, _ = HexAddress(ContractHex)
	out.BlockNumber = r.BlockNumber
	out.BlockTime = UTC(time.UnixMilli(r.BlockTimestamp))
	out.TxId, _ = BinaryHex(r.Transaction, 32)
	out.ProviderEventIndex = r.Index
	out.AbiRevision = "energy-market-events-v1"
	out.PositionStatus = "indexed_only"
	out.RequestStartedAt = r.Evidence.Started
	out.AvailableAt = r.Evidence.Available
	out.PayloadHash, _ = BinaryHex(r.Evidence.ResponseHash, 32)
	resource, e := Num(r.Result, "resourceType")
	if e != nil {
		return out, e
	}
	if resource.Cmp(big.NewInt(1)) != 0 {
		return out, errors.New("not_energy_event")
	}
	for _, pair := range []struct {
		Name string
		Dest *string
	}{{"renter", &out.Renter}, {"receiver", &out.Receiver}} {
		v, e := text(r.Result, pair.Name)
		if e != nil {
			return out, e
		}
		*pair.Dest, e = Address(v)
		if e != nil {
			return out, e
		}
	}
	out.AmountSun, e = Num(r.Result, "amount")
	if e != nil {
		return out, e
	}
	var fields []string
	switch r.Name {
	case "Liquidate":
		out.EventKind = "liquidate"
		v, e := text(r.Result, "liquidator")
		if e != nil {
			return out, e
		}
		a, e := Address(v)
		if e != nil {
			return out, e
		}
		out.Liquidator = Ptr(a)
		fields = []string{"usageRental", "liquidateFee", "sendBack"}
		out.UsageRentalSun, e = Num(r.Result, fields[0])
		if e == nil {
			out.RewardSun, e = Num(r.Result, fields[1])
		}
		if e == nil {
			out.SendBackSun, e = Num(r.Result, fields[2])
		}
	case "RentResource":
		out.EventKind = "rent"
		out.AddedAmountSun, e = Num(r.Result, "addedAmount")
		if e == nil {
			out.AddedDepositSun, e = Num(r.Result, "addedSecurityDeposit")
		}
	case "ReturnResource":
		out.EventKind = "return"
		out.ReturnedAmountSun, e = Num(r.Result, "subedAmount")
		if e == nil {
			out.ReturnedDepositSun, e = Num(r.Result, "subedSecurityDeposit")
		}
		if e == nil {
			out.UsageRentalSun, e = Num(r.Result, "usageRental")
		}
	default:
		return out, errors.New("unknown_event")
	}
	if e == nil && r.Name != "Liquidate" {
		_, deposit := r.Result["securityDeposit"]
		_, index := r.Result["rentIndex"]
		if deposit != index {
			return out, errors.New("incomplete_extended_event")
		}
		if deposit {
			out.AbiRevision = seedRevision
			out.SecurityDepositSun, e = Num(r.Result, "securityDeposit")
			if e == nil {
				out.RentIndex, e = Num(r.Result, "rentIndex")
			}
		}
	}
	return out, e
}

type Log struct {
	Address string   `json:"address"`
	Topics  []string `json:"topics"`
	Data    string   `json:"data"`
}

func DecodeLog(l Log) (RentalEvent, error) {
	var out RentalEvent
	addr, e := Address(l.Address)
	contract, _ := HexAddress(ContractHex)
	if e != nil || addr != contract || len(l.Topics) < 3 {
		return out, errors.New("other_log")
	}
	t, e := BinaryHex(l.Topics[0], 32)
	if e != nil {
		return out, e
	}
	name := ""
	extended := false
	for n, s := range signatures {
		if t == Topic(s) {
			name = n
		}
	}
	for n, s := range extendedSignatures {
		if t == Topic(s) {
			name, extended = n, true
		}
	}
	if name == "" {
		return out, errors.New("other_log")
	}
	wantTopics := 3
	if name == "Liquidate" {
		wantTopics = 4
	}
	if len(l.Topics) != wantTopics {
		return out, errors.New("bad_event_topics")
	}
	data, e := hex.DecodeString(l.Data)
	words := 4
	if name != "RentResource" {
		words = 5
	}
	if extended {
		words += 2
	}
	if e != nil || len(data) != 32*words {
		return out, errors.New("bad_event_data")
	}
	ints := make([]*big.Int, words)
	for i := range ints {
		ints[i] = new(big.Int).SetBytes(data[i*32 : (i+1)*32])
	}
	if ints[1].Cmp(big.NewInt(1)) != 0 {
		return out, errors.New("not_energy_event")
	}
	offset := 1
	if name == "Liquidate" {
		a, e := WordAddress(l.Topics[1])
		if e != nil {
			return out, e
		}
		out.Liquidator = Ptr(a)
		offset = 2
	}
	out.Renter, e = WordAddress(l.Topics[offset])
	if e != nil {
		return out, e
	}
	out.Receiver, e = WordAddress(l.Topics[offset+1])
	if e != nil {
		return out, e
	}
	out.ResourceType = 1
	out.ContractAddress = contract
	out.AbiRevision = "energy-market-events-v1"
	if extended {
		out.AbiRevision = seedRevision
		out.SecurityDepositSun = ints[words-2]
		out.RentIndex = ints[words-1]
	}
	switch name {
	case "Liquidate":
		out.EventKind = "liquidate"
		out.AmountSun = ints[0]
		out.UsageRentalSun = ints[2]
		out.RewardSun = ints[3]
		out.SendBackSun = ints[4]
	case "RentResource":
		out.EventKind = "rent"
		out.AddedAmountSun = ints[0]
		out.AddedDepositSun = ints[2]
		out.AmountSun = ints[3]
	case "ReturnResource":
		out.EventKind = "return"
		out.ReturnedAmountSun = ints[0]
		out.UsageRentalSun = ints[2]
		out.ReturnedDepositSun = ints[3]
		out.AmountSun = ints[4]
	}
	return out, nil
}
func EventEqual(a, b RentalEvent) bool {
	x := func(r RentalEvent) any {
		return struct {
			Kind, Renter, Receiver                                                         string
			Liquidator                                                                     *string
			Amount, Added, Deposit, Returned, Refund, Usage, Reward, Back, Security, Index *big.Int
		}{r.EventKind, r.Renter, r.Receiver, r.Liquidator, r.AmountSun, r.AddedAmountSun, r.AddedDepositSun, r.ReturnedAmountSun, r.ReturnedDepositSun, r.UsageRentalSun, r.RewardSun, r.SendBackSun, r.SecurityDepositSun, r.RentIndex}
	}
	aa, ea := FactBytes(x(a))
	bb, eb := FactBytes(x(b))
	return ea == nil && eb == nil && bytes.Equal(aa, bb)
}

type Header struct {
	Number       uint64
	Hash         string
	Time         time.Time
	Transactions []string
}

func ParseBlock(b []byte, full bool) (Header, error) {
	var r struct {
		ID     string `json:"blockID"`
		Header struct {
			Raw struct {
				Number    *uint64 `json:"number"`
				Timestamp *int64  `json:"timestamp"`
			} `json:"raw_data"`
		} `json:"block_header"`
		Transactions []struct {
			ID string `json:"txID"`
		} `json:"transactions"`
	}
	if e := Decode(b, &r); e != nil {
		return Header{}, e
	}
	if r.Header.Raw.Number == nil || r.Header.Raw.Timestamp == nil || *r.Header.Raw.Timestamp <= 0 {
		return Header{}, errors.New("missing_block_header")
	}
	h, e := BinaryHex(r.ID, 32)
	if e != nil {
		return Header{}, e
	}
	n := new(big.Int).SetBytes([]byte(h[:8]))
	if n.Uint64() != *r.Header.Raw.Number {
		return Header{}, errors.New("block_id_height_mismatch")
	}
	out := Header{Number: *r.Header.Raw.Number, Hash: h, Time: UTC(time.UnixMilli(*r.Header.Raw.Timestamp))}
	if full {
		for _, t := range r.Transactions {
			v, e := BinaryHex(t.ID, 32)
			if e != nil {
				return out, e
			}
			out.Transactions = append(out.Transactions, v)
		}
	}
	return out, nil
}
func ParseReceipt(b []byte, ev Evidence) (TxReceipt, []Log, error) {
	var out TxReceipt
	m := map[string]json.RawMessage{}
	if e := Decode(b, &m); e != nil {
		return out, nil, e
	}
	id, e := text(m, "id")
	if e != nil {
		return out, nil, e
	}
	out.TxId, e = BinaryHex(id, 32)
	if e != nil {
		return out, nil, e
	}
	out.BlockNumber, e = ReqU(m, "blockNumber")
	if e != nil {
		return out, nil, e
	}
	ms, e := ReqU(m, "blockTimeStamp")
	if e != nil || ms == 0 || ms > 1<<63-1 {
		return out, nil, errors.New("bad_receipt_time")
	}
	out.BlockTime = UTC(time.UnixMilli(int64(ms)))
	out.Finality = "solid"
	out.ReceiptPayloadHash, _ = BinaryHex(ev.ResponseHash, 32)
	out.RequestStartedAt = ev.Started
	out.AvailableAt = ev.Available
	sub := map[string]json.RawMessage{}
	if e = Decode(m["receipt"], &sub); e != nil {
		return out, nil, e
	}
	out.ExecutionResult, _ = text(sub, "result")
	if out.ExecutionResult == "" {
		out.ExecutionResult = "unknown"
	}
	var logs []Log
	if e = Decode(m["log"], &logs); e != nil {
		return out, nil, e
	}
	for _, f := range []struct {
		Map  map[string]json.RawMessage
		Key  string
		Dest **uint64
	}{{m, "fee", &out.FeeSun}, {sub, "energy_usage_total", &out.EnergyUsageTotal}, {sub, "energy_usage", &out.EnergyUsage}, {sub, "origin_energy_usage", &out.OriginEnergyUsage}, {sub, "energy_fee", &out.EnergyFeeSun}, {sub, "energy_penalty_total", &out.EnergyPenaltyTotal}, {sub, "net_usage", &out.NetUsage}, {sub, "net_fee", &out.NetFeeSun}, {m, "multi_sign_fee", &out.MultisignFeeSun}, {m, "memo_fee", &out.MemoFeeSun}} {
		*f.Dest, e = OptU(f.Map, f.Key)
		if e != nil {
			return out, nil, e
		}
	}
	out.NativeTransferStatus = "absent_unknown"
	if raw, ok := m["internal_transactions"]; ok {
		out.NativeTransfers, e = Transfers(raw)
		if e != nil {
			return out, nil, e
		}
		out.NativeTransferStatus = "present"
	}
	contract, _ := HexAddress(ContractHex)
	for _, l := range logs {
		if a, _ := Address(l.Address); a == contract && len(l.Topics) > 0 {
			t, _ := BinaryHex(l.Topics[0], 32)
			if t == Topic(signatures["Liquidate"]) {
				if _, e = DecodeLog(l); e != nil {
					return out, nil, e
				}
				out.RentalLiquidationLogCount++
				continue
			}
		}
		out.OtherLogCount++
	}
	out.ReceiptComplete = true
	out.CallClass = "unknown"
	return out, logs, nil
}
func Transfers(raw []byte) ([]NativeTransfer, error) {
	var rows []struct {
		From     string                       `json:"caller_address"`
		To       string                       `json:"transferTo_address"`
		Rejected bool                         `json:"rejected"`
		Values   []map[string]json.RawMessage `json:"callValueInfo"`
	}
	if e := Decode(raw, &rows); e != nil {
		return nil, e
	}
	out := []NativeTransfer{}
	for i, r := range rows {
		from, e := Address(r.From)
		if e != nil {
			return nil, e
		}
		to, e := Address(r.To)
		if e != nil {
			return nil, e
		}
		if r.Values == nil {
			return nil, errors.New("missing_call_value_info")
		}
		for j, v := range r.Values {
			if token, ok := v["tokenId"]; ok {
				var s string
				if e = json.Unmarshal(token, &s); e != nil {
					return nil, e
				}
				if s != "" {
					continue
				}
			}
			n := big.NewInt(0)
			if x, ok := v["callValue"]; ok {
				n, e = Uint(x)
				if e != nil {
					return nil, e
				}
			}
			out = append(out, NativeTransfer{uint32(i), uint32(j), from, to, *n, r.Rejected})
		}
	}
	return out, nil
}
func ApplyBody(out *TxReceipt, b []byte, ev Evidence) error {
	var r struct {
		ID     string `json:"txID"`
		RawHex string `json:"raw_data_hex"`
		Raw    struct {
			Contracts []struct {
				Type      string `json:"type"`
				Parameter struct {
					Value map[string]json.RawMessage `json:"value"`
				} `json:"parameter"`
			} `json:"contract"`
		} `json:"raw_data"`
	}
	if e := Decode(b, &r); e != nil {
		return e
	}
	id, e := BinaryHex(r.ID, 32)
	if e != nil || id != out.TxId {
		return errors.New("transaction_id_mismatch")
	}
	raw, e := hex.DecodeString(r.RawHex)
	if e != nil || len(raw) == 0 {
		return errors.New("missing_transaction_bytes")
	}
	sum := sha256.Sum256(raw)
	if string(sum[:]) != id {
		return errors.New("transaction_hash_mismatch")
	}
	if len(r.Raw.Contracts) != 1 || r.Raw.Contracts[0].Type != "TriggerSmartContract" {
		out.CallClass = "wrapper_or_mixed"
		out.BodyComplete = true
		out.BodyPayloadHash = Ptr(mustHex(ev.ResponseHash))
		return nil
	}
	v := r.Raw.Contracts[0].Parameter.Value
	from, e := text(v, "owner_address")
	if e != nil {
		return e
	}
	a, e := Address(from)
	if e != nil {
		return e
	}
	out.Sender = Ptr(a)
	target, e := text(v, "contract_address")
	if e != nil {
		return e
	}
	a, e = Address(target)
	if e != nil {
		return e
	}
	out.OuterTarget = Ptr(a)
	data, e := text(v, "data")
	if e != nil {
		return e
	}
	d, e := hex.DecodeString(data)
	if e != nil || len(d) < 4 {
		return errors.New("invalid_calldata")
	}
	out.OuterSelector = Ptr(string(d[:4]))
	out.OuterCallValueSun = big.NewInt(0)
	if x, ok := v["call_value"]; ok {
		out.OuterCallValueSun, e = Uint(x)
		if e != nil {
			return e
		}
	}
	out.CallClass = "wrapper_or_mixed"
	if target == ContractHex && data[:8] == "26c01303" && len(d) == 100 {
		out.CallClass = "direct_liquidate"
	}
	out.BodyComplete = true
	out.BodyPayloadHash = Ptr(mustHex(ev.ResponseHash))
	out.AvailableAt = ev.Available
	return nil
}
func mustHex(s string) string { v, _ := BinaryHex(s, 32); return v }

func RawFromRow(r RentalEvent) RawEvent {
	m := map[string]json.RawMessage{}
	set := func(k, s string) { b, _ := json.Marshal(s); m[k] = b }
	set("renter", "0x"+Hex(r.Renter[1:]))
	set("receiver", "0x"+Hex(r.Receiver[1:]))
	set("resourceType", "1")
	set("amount", r.AmountSun.String())
	if r.SecurityDepositSun != nil && r.RentIndex != nil {
		set("securityDeposit", r.SecurityDepositSun.String())
		set("rentIndex", r.RentIndex.String())
	}
	name := "RentResource"
	if r.EventKind == "rent" {
		set("addedAmount", r.AddedAmountSun.String())
		set("addedSecurityDeposit", r.AddedDepositSun.String())
	} else if r.EventKind == "return" {
		name = "ReturnResource"
		set("subedAmount", r.ReturnedAmountSun.String())
		set("subedSecurityDeposit", r.ReturnedDepositSun.String())
		set("usageRental", r.UsageRentalSun.String())
	} else {
		name = "Liquidate"
		set("liquidator", "0x"+Hex((*r.Liquidator)[1:]))
		set("usageRental", r.UsageRentalSun.String())
		set("liquidateFee", r.RewardSun.String())
		set("sendBack", r.SendBackSun.String())
	}
	return RawEvent{BlockNumber: r.BlockNumber, BlockTimestamp: r.BlockTime.UnixMilli(), ContractAddress: ContractBase58, Index: r.ProviderEventIndex, Name: name, Result: m, Transaction: Hex(r.TxId), Evidence: Evidence{Source: "trongrid", Started: r.RequestStartedAt, Available: r.AvailableAt, ResponseHash: Hex(r.PayloadHash)}}
}
