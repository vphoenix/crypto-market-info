package reserve

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/dex/ethereum"
)

// One typed row per transaction, including logs outside the Folio whitelist.
// Calldata and ABI log data are binary bytes in String columns, never raw JSON.
type ReceiptLog struct {
	LogIndex uint32   `json:"log_index"`
	Emitter  string   `json:"emitter"`
	Topics   []string `json:"topics"`
	Data     string   `json:"data"`
}
type ReceiptData struct {
	ChainId        uint64       `ch:"chain_id"`
	BlockNumber    uint64       `ch:"block_number"`
	BlockHash      string       `ch:"block_hash"`
	BlockTime      time.Time    `ch:"block_time"`
	TxHash         string       `ch:"tx_hash"`
	TxIndex        uint32       `ch:"tx_index"`
	ReceiptHash    string       `ch:"receipt_hash"`
	CalldataHash   string       `ch:"calldata_hash"`
	Calldata       string       `ch:"calldata"`
	LogCount       uint32       `ch:"log_count"`
	Logs           []ReceiptLog `ch:"logs"`
	DataHash       string       `ch:"data_hash"`
	MaterializedAt time.Time    `ch:"materialized_at"`
}
type ReceiptDataStore interface {
	ReserveReceiptData(context.Context, dex.Anchor, dex.Hash) (ReceiptData, bool, error)
	WriteReserveReceiptData(context.Context, ReceiptData, dex.Receipt) error
	ReserveReceipts(context.Context) ([]dex.Receipt, error)
}

func ReceiptDataHash(d ReceiptData) string {
	d.DataHash = ""
	d.MaterializedAt = time.Time{}
	return ID(d)
}

func ValidateReceiptData(d ReceiptData, r dex.Receipt) error {
	if len(d.BlockHash) != 32 || len(d.TxHash) != 32 || len(d.ReceiptHash) != 32 || len(d.CalldataHash) != 32 || len(d.DataHash) != 32 || d.BlockTime.IsZero() {
		return errors.New("receipt_data_invalid_hash_or_time")
	}
	if d.ChainId != 1 || r.ChainID != 1 || d.BlockNumber != r.Number || d.BlockHash != Hash(r.Hash) || d.TxHash != Hash(r.TxHash) || d.TxIndex != r.TxIndex || !d.BlockTime.Equal(r.Time) || d.ReceiptHash != Hash(r.ReceiptHash) || d.CalldataHash != Hash(r.CalldataHash) || d.LogCount != r.LogCount || d.MaterializedAt.IsZero() {
		return errors.New("receipt_data_anchor_mismatch")
	}
	if len(d.Logs) != int(d.LogCount) || d.DataHash != ReceiptDataHash(d) || dex.Digest([]byte(d.Calldata)) != r.CalldataHash {
		return errors.New("receipt_data_digest_or_count_mismatch")
	}
	if len(d.Calldata) >= 4 {
		if string(r.Selector) != d.Calldata[:4] {
			return errors.New("receipt_data_selector_mismatch")
		}
	} else if len(r.Selector) != 0 {
		return errors.New("receipt_data_selector_mismatch")
	}
	for i, l := range d.Logs {
		if len(l.Emitter) != 20 || len(l.Topics) > 4 || i > 0 && d.Logs[i-1].LogIndex >= l.LogIndex {
			return errors.New("receipt_data_log_identity")
		}
		for _, topic := range l.Topics {
			if len(topic) != 32 {
				return errors.New("receipt_data_log_topic")
			}
		}
	}
	return nil
}

// Validates the source while it is in memory. The HTTP response can then be
// discarded; later reuse reads only this typed record and its receipt summary.
func DecodeReceiptData(r dex.Receipt, raw, calldata []byte) (ReceiptData, error) {
	d := ReceiptData{ChainId: r.ChainID, BlockNumber: r.Number, BlockHash: Hash(r.Hash), BlockTime: r.Time, TxHash: Hash(r.TxHash), TxIndex: r.TxIndex, ReceiptHash: Hash(r.ReceiptHash), CalldataHash: Hash(r.CalldataHash), Calldata: string(calldata), LogCount: r.LogCount, Logs: []ReceiptLog{}, MaterializedAt: dex.Now()}
	if dex.Digest(raw) != r.ReceiptHash {
		return d, errors.New("receipt_source_hash_mismatch")
	}
	var v struct {
		TransactionHash, BlockHash, BlockNumber, TransactionIndex, From, To, Type, Status, GasUsed, EffectiveGasPrice string
		Logs                                                                                                          []rawLog
	}
	if json.Unmarshal(raw, &v) != nil || v.Logs == nil || v.TransactionHash != r.TxHash.String() || v.BlockHash != r.Hash.String() || v.From != r.From.String() || (r.HasTo && v.To != r.To.String()) || (!r.HasTo && v.To != "") {
		return d, errors.New("receipt_source_identity_mismatch")
	}
	for _, q := range []struct {
		raw      string
		bits     int
		expected uint64
	}{
		{v.BlockNumber, 64, r.Number}, {v.TransactionIndex, 32, uint64(r.TxIndex)}, {v.Type, 8, uint64(r.Type)}, {v.Status, 1, uint64(r.Status)}, {v.GasUsed, 64, r.GasUsed},
	} {
		n, e := ethereum.Quantity(q.raw, q.bits)
		if e != nil || n.Uint64() != q.expected {
			return d, errors.New("receipt_source_summary_mismatch")
		}
	}
	price, e := ethereum.Quantity(v.EffectiveGasPrice, 256)
	if e != nil || r.GasPrice == nil || price.Cmp(r.GasPrice) != 0 {
		return d, errors.New("receipt_source_gas_price_mismatch")
	}
	for _, rawLog := range v.Logs {
		emitter, e := dex.ParseAddress(rawLog.Address)
		if e != nil {
			return d, e
		}
		l, e := parseLog(rawLog, r.Anchor, map[dex.Address]bool{emitter: true})
		if e != nil || l.TxHash != r.TxHash || l.TxIndex != r.TxIndex {
			return d, errors.New("receipt_source_log_identity")
		}
		topics := []string{}
		for _, topic := range l.Topics {
			topics = append(topics, Hash(topic))
		}
		d.Logs = append(d.Logs, ReceiptLog{l.Index, Address(l.Emitter), topics, string(l.Data)})
	}
	sort.Slice(d.Logs, func(i, j int) bool { return d.Logs[i].LogIndex < d.Logs[j].LogIndex })
	d.DataHash = ReceiptDataHash(d)
	return d, ValidateReceiptData(d, r)
}

func ReceiptDataContains(d ReceiptData, r dex.Receipt, wanted []dex.Log) error {
	if e := ValidateReceiptData(d, r); e != nil {
		return e
	}
	facts := map[uint32]string{}
	for _, v := range d.Logs {
		l := dex.Log{Anchor: r.Anchor, TxHash: r.TxHash, TxIndex: r.TxIndex, Index: v.LogIndex, Data: []byte(v.Data)}
		copy(l.Emitter[:], v.Emitter)
		for _, s := range v.Topics {
			var h dex.Hash
			copy(h[:], s)
			l.Topics = append(l.Topics, h)
		}
		facts[l.Index] = LogFact(l)
	}
	for _, l := range wanted {
		if l.Hash != r.Hash || l.TxHash != r.TxHash || facts[l.Index] != LogFact(l) {
			return errors.New("receipt_selected_log_conflict")
		}
	}
	return nil
}

// Offline, idempotent migration. It covers all receipt identities, including
// old manifests and orphaned attempts, and never revises old capture facts.
func MigrateReceiptData(ctx context.Context, store ReceiptDataStore, archive ethereum.Archive) (int, error) {
	rows, e := store.ReserveReceipts(ctx)
	if e != nil {
		return 0, e
	}
	for i, r := range rows {
		if ctx.Err() != nil {
			return i, ctx.Err()
		}
		existing, ok, e := store.ReserveReceiptData(ctx, r.Anchor, r.TxHash)
		if e != nil {
			return i, e
		}
		if ok {
			if e = ValidateReceiptData(existing, r); e != nil {
				return i, e
			}
			continue
		}
		raw, e := archive.Get(r.ReceiptHash)
		if e != nil {
			return i, e
		}
		calldata, e := archive.Get(r.CalldataHash)
		if e != nil {
			return i, e
		}
		d, e := DecodeReceiptData(r, raw, calldata)
		if e != nil {
			return i, e
		}
		if e = store.WriteReserveReceiptData(ctx, d, r); e != nil {
			return i, e
		}
	}
	return len(rows), nil
}
