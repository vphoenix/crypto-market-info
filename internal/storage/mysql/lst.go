package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/lst"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

func (c *Client) LSTCaptures(ctx context.Context, manifest string) ([]lst.Capture, error) {
	if len(manifest) != 32 {
		return nil, fmt.Errorf("invalid LST manifest")
	}
	rows, e := readCaptureModels[lst.Capture](ctx, c, "lst_capture", "manifest_hash=? AND committed=1", []byte(manifest))
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].StartedAt.Equal(rows[j].StartedAt) {
			return rows[i].CaptureId.String() < rows[j].CaptureId.String()
		}
		return rows[i].StartedAt.Before(rows[j].StartedAt)
	})
	return rows, e
}

// LSTCapture confirms the actual revision after a possibly stale SQL no-op.
func (c *Client) LSTCapture(ctx context.Context, want lst.Capture) (lst.Capture, error) {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	cap, ok, err := c.lstCapture(ctx, want.CaptureId)
	if err != nil {
		return cap, err
	}
	if !ok || !cap.Committed || lst.CaptureIdentity(cap) != lst.CaptureIdentity(want) {
		return cap, contracts.ErrConflict
	}
	return cap, nil
}

// LSTCapturePage is reserved for explicit conversion, never normal polling.
func (c *Client) LSTCapturePage(ctx context.Context, manifest string, after []byte, limit int) ([]lst.Capture, []byte, error) {
	if len(manifest) != 32 || len(after) != 0 && len(after) != 16 || limit < 1 || limit > 500 {
		return nil, nil, fmt.Errorf("invalid LST conversion page")
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	where := "FORCE INDEX (PRIMARY) WHERE manifest_hash=? AND committed=1"
	args := []any{[]byte(manifest)}
	if len(after) > 0 {
		where += " AND capture_id>?"
		args = append(args, after)
	}
	where += fmt.Sprintf(" ORDER BY capture_id LIMIT %d", limit)
	rows, err := readModels[lst.Capture](ctx, c, c.db, "lst_capture", where, args...)
	if err != nil || len(rows) == 0 {
		return rows, nil, err
	}
	last := rows[len(rows)-1].CaptureId
	return rows, append([]byte(nil), last[:]...), nil
}
func (c *Client) lstBatch(ctx context.Context, q executor, cap lst.Capture) (lst.Batch, error) {
	b := lst.Batch{Capture: cap}
	var e error
	where := "WHERE capture_id=?"
	id := cap.CaptureId[:]
	if b.Protocols, e = readModels[lst.ProtocolState](ctx, c, q, "lst_protocol_state", where, id); e != nil {
		return b, e
	}
	if b.Quotes, e = readModels[lst.Quote](ctx, c, q, "lst_quote_observation", where, id); e != nil {
		return b, e
	}
	if b.Requests, e = readModels[lst.WithdrawalRequest](ctx, c, q, "lst_withdrawal_request", where, id); e != nil {
		return b, e
	}
	if b.Finalizations, e = readModels[lst.WithdrawalFinalization](ctx, c, q, "lst_withdrawal_finalization", where, id); e != nil {
		return b, e
	}
	if b.Claims, e = readModels[lst.WithdrawalClaim](ctx, c, q, "lst_withdrawal_claim", where, id); e != nil {
		return b, e
	}
	if b.Funding, e = readModels[lst.FundingSettlement](ctx, c, q, "lst_funding_settlement", where, id); e != nil {
		return b, e
	}
	return b, lst.Validate(b)
}
func (c *Client) LSTBatch(ctx context.Context, cap lst.Capture) (lst.Batch, error) {
	rows, e := readModels[lst.Capture](ctx, c, c.db, "lst_capture", "WHERE capture_id=?", cap.CaptureId[:])
	if e != nil {
		return lst.Batch{}, e
	}
	if len(rows) != 1 || !rows[0].Committed {
		return lst.Batch{}, fmt.Errorf("LST capture not committed")
	}
	if lst.CaptureIdentity(rows[0]) != lst.CaptureIdentity(cap) {
		return lst.Batch{}, contracts.ErrConflict
	}
	return c.lstBatch(ctx, c.db, rows[0])
}
func (c *Client) WriteLST(ctx context.Context, b lst.Batch) error {
	if e := lst.Validate(b); e != nil {
		return e
	}
	return c.transaction(ctx, func(tx *sql.Tx) error {
		rows, e := readModels[lst.Capture](ctx, c, tx, "lst_capture", "WHERE capture_id=? FOR UPDATE", b.Capture.CaptureId[:])
		if e != nil {
			return e
		}
		if len(rows) > 1 {
			return contracts.ErrConflict
		}
		if len(rows) == 1 {
			if lst.CaptureIdentity(rows[0]) != lst.CaptureIdentity(b.Capture) {
				return contracts.ErrConflict
			}
			_, e = c.lstBatch(ctx, tx, rows[0])
			return e
		}
		if e = lstCheckEvents(ctx, c, tx, "lst_withdrawal_request", b.Requests); e != nil {
			return e
		}
		if e = lstCheckEvents(ctx, c, tx, "lst_withdrawal_finalization", b.Finalizations); e != nil {
			return e
		}
		if e = lstCheckEvents(ctx, c, tx, "lst_withdrawal_claim", b.Claims); e != nil {
			return e
		}
		for _, r := range b.Funding {
			old, e := readModels[lst.FundingSettlement](ctx, c, tx, "lst_funding_settlement", "WHERE instrument_id=? AND funding_time=?", r.InstrumentId, r.FundingTime)
			if e != nil {
				return e
			}
			for _, v := range old {
				if !v.FundingRate.Equal(r.FundingRate) || (v.SettlementMarkPriceTickE8 != nil && r.SettlementMarkPriceTickE8 != nil && *v.SettlementMarkPriceTickE8 != *r.SettlementMarkPriceTickE8) {
					return contracts.ErrConflict
				}
			}
		}
		if e = putModels(ctx, c, tx, "lst_protocol_state", b.Protocols); e != nil {
			return e
		}
		if e = putModels(ctx, c, tx, "lst_quote_observation", b.Quotes); e != nil {
			return e
		}
		if e = putModels(ctx, c, tx, "lst_withdrawal_request", b.Requests); e != nil {
			return e
		}
		if e = putModels(ctx, c, tx, "lst_withdrawal_finalization", b.Finalizations); e != nil {
			return e
		}
		if e = putModels(ctx, c, tx, "lst_withdrawal_claim", b.Claims); e != nil {
			return e
		}
		if e = putModels(ctx, c, tx, "lst_funding_settlement", b.Funding); e != nil {
			return e
		}
		if _, e = c.lstBatch(ctx, tx, b.Capture); e != nil {
			return e
		}
		return putModels(ctx, c, tx, "lst_capture", []lst.Capture{b.Capture})
	})
}
func lstCheckEvents[T any](ctx context.Context, c *Client, q executor, name string, rows []T) error {
	for _, row := range rows {
		v := reflect.ValueOf(row)
		existing, e := readModels[T](ctx, c, q, name, "WHERE chain_id=? AND queue_address=? AND block_hash=? AND transaction_hash=? AND log_index=?", v.FieldByName("ChainId").Interface(), []byte(v.FieldByName("QueueAddress").String()), []byte(v.FieldByName("BlockHash").String()), []byte(v.FieldByName("TransactionHash").String()), v.FieldByName("LogIndex").Interface())
		if e != nil {
			return e
		}
		for _, old := range existing {
			if lstEventFact(old) != lstEventFact(row) || !lstReceiptCompatible(old, row) {
				return contracts.ErrConflict
			}
		}
	}
	return nil
}
func (c *Client) WriteLSTRevision(ctx context.Context, next lst.Capture) error {
	return c.transaction(ctx, func(tx *sql.Tx) error {
		rows, e := readModels[lst.Capture](ctx, c, tx, "lst_capture", "WHERE capture_id=? FOR UPDATE", next.CaptureId[:])
		if e != nil {
			return e
		}
		if len(rows) != 1 || !rows[0].Committed {
			return fmt.Errorf("LST revision capture missing")
		}
		old := rows[0]
		if lst.CaptureIdentity(old) != lst.CaptureIdentity(next) {
			return contracts.ErrConflict
		}
		if next.Revision < old.Revision {
			return nil
		}
		if next.Revision == old.Revision {
			if lst.CanonicalHash(old) == lst.CanonicalHash(next) {
				return nil
			}
			return contracts.ErrConflict
		}
		if old.Finality == "finalized" && (next.Finality != "finalized" || next.Canonical != old.Canonical) {
			return contracts.ErrConflict
		}
		if _, e = c.lstBatch(ctx, tx, next); e != nil {
			return e
		}
		return replaceModel(ctx, c, tx, "lst_capture", next)
	})
}
func (c *Client) lstCapture(ctx context.Context, id uuid.UUID) (lst.Capture, bool, error) {
	rows, e := readModels[lst.Capture](ctx, c, c.db, "lst_capture", "WHERE capture_id=?", id[:])
	if e != nil {
		return lst.Capture{}, false, e
	}
	if len(rows) > 1 {
		return lst.Capture{}, false, contracts.ErrConflict
	}
	if len(rows) == 0 {
		return lst.Capture{}, false, nil
	}
	return rows[0], true, nil
}

var _ lst.Store = (*Client)(nil)
