package mysql

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"time"
)

func (c *Client) Delivery(kind, scope string, minute time.Time, members uint32, payload []byte) contracts.Delivery {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s/%d/%s/%s/%s/%d", c.producer.Service, c.producer.Generation, c.producer.Scope, kind, scope, minute.Unix())))
	return contracts.Delivery{Database: c.database, Kind: kind, ID: hex.EncodeToString(h[:]), Service: c.producer.Service, Generation: c.producer.Generation, Minute: minute, Digest: sha256.Sum256(payload), Members: members, Codec: 1, Payload: payload}
}

// PrepareBookDeliveries performs no database IO. Retries reuse these sealed
// deliveries, or supply the same CompletedMinute.KnownFromMS on each attempt.
func (c *Client) PrepareBookDeliveries(ctx context.Context, all model.CompletedMinute) ([]contracts.Delivery, error) {
	if e := codec.ValidateCompletedMinute(all); e != nil {
		return nil, e
	}
	known := all.KnownFromMS
	if c.discoveryEnabled.Load() && known == 0 {
		known = model.CeilMilliseconds(time.Now().UTC())
	}
	if !c.discoveryEnabled.Load() {
		known = 0
	}
	version := uint16(1)
	if known != 0 {
		version = 2
	}
	out := []contracts.Delivery{}
	for offset := 0; offset < len(all.Batches); offset += 100 {
		part := model.CompletedMinute{MinuteTime: all.MinuteTime, Batches: all.Batches[offset:min(len(all.Batches), offset+100)]}
		part.KnownFromMS = known
		raw, e := codec.EncodeBookDelivery(part, version)
		if e != nil {
			return nil, e
		}
		scope := fmt.Sprintf("%d", offset)
		for _, b := range part.Batches {
			scope += fmt.Sprintf("/%d", b.Minute.InstrumentID)
		}
		delivery := c.Delivery("cex", scope, part.MinuteTime, uint32(len(part.Batches)), raw)
		delivery.Codec = version
		out = append(out, delivery)
	}
	return out, nil
}
func (c *Client) recordDelivery(ctx context.Context, tx *sql.Tx, d contracts.Delivery) error {
	_, e := tx.ExecContext(ctx, "INSERT INTO cmi_storage.ingest_batch(database_name,kind,batch_id,service,generation,logical_time,codec,content_hash,member_count,committed_at) VALUES(?,?,?,?,?,?,?,?,?,UTC_TIMESTAMP(6))", d.Database, d.Kind, d.ID, d.Service, d.Generation, d.Minute, d.Codec, d.Digest[:], d.Members)
	return e
}
func (c *Client) commitOptions(ctx context.Context, d contracts.Delivery) error {
	prepared, e := c.PrepareDelivery(ctx, d)
	if e != nil {
		return e
	}
	return c.CommitPrepared(ctx, prepared)
}
