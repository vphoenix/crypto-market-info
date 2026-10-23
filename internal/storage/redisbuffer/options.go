package redisbuffer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"github.com/vphoenix/crypto-market-info/internal/storage/spool"
	"strings"
)

var optionPartType = parsed("Tuple(kind String, id String, payload String)")

func (b *Buffer) acceptOptions(ctx context.Context, d contracts.Delivery) error {
	if d.Codec != 1 {
		return fmt.Errorf("unsupported option codec")
	}
	meta, parts, e := codec.OptionsParts(d.Kind, d.Payload)
	if e != nil {
		return e
	}
	if len(parts) > 1345 || d.Members == 0 || d.Members > 448 {
		return fmt.Errorf("option delivery quota")
	}
	used, e := b.memory(ctx)
	if e != nil {
		return e
	}
	if used+int64(len(d.Payload))*2 > b.soft {
		return contracts.ErrCapacity
	}
	conn := b.client.Conn()
	defer conn.Close()
	connectionID, e := conn.ClientID(ctx).Result()
	if e != nil {
		return e
	}
	keys := []string{b.batchKey(d.Database, d.ID), b.readyKey(d.Database)}
	digests := []any{}
	memberKeys := []string{}
	all := append([]codec.OptionPart{{Kind: "manifest", ID: d.Kind, Raw: meta}}, parts...)
	planned := []string{}
	for n := range all {
		planned = append(planned, fmt.Sprintf("%s:book:%s:%s:%d:%s:%d:%d", b.prefix, d.Database, d.Kind, d.Generation, d.ID, d.Minute.Unix()/60, n))
	}
	if e = b.begin(ctx, conn, d, planned); e != nil {
		return e
	}
	// Bound queued commands while amortizing the round trip. Sealing still
	// checks every member digest, then WAITAOF confirms this same connection.
	pipe := conn.Pipeline()
	for n, p := range all {
		key := fmt.Sprintf("%s:book:%s:%s:%d:%s:%d:%d", b.prefix, d.Database, d.Kind, d.Generation, d.ID, d.Minute.Unix()/60, n)
		raw, e := codec.Encode(optionPartType, map[string]any{"kind": p.Kind, "id": p.ID, "payload": string(p.Raw)})
		if e != nil {
			return e
		}
		h := sha256.Sum256(raw)
		hash := hex.EncodeToString(h[:])
		pipe.Eval(ctx, memberScript, []string{key, b.batchKey(d.Database, d.ID)}, hash, hex.EncodeToString(d.Digest[:]), "payload", raw)
		if pipe.Len() >= 32 {
			if _, e = pipe.Exec(ctx); e != nil {
				return conflict(e)
			}
		}
		keys = append(keys, key)
		memberKeys = append(memberKeys, key)
		digests = append(digests, hash)
	}
	if _, e = pipe.Exec(ctx); e != nil {
		return conflict(e)
	}
	header, e := codec.Encode(headerType, map[string]any{"database": d.Database, "kind": d.Kind, "id": d.ID, "service": d.Service, "generation": d.Generation, "minute": d.Minute, "digest": d.Digest[:], "members": d.Members, "codec": d.Codec, "keys": memberKeys})
	if e != nil {
		return e
	}
	args := append([]any{hex.EncodeToString(d.Digest[:]), header, d.Minute.Unix() / 60, d.ID, uuid.NewString()}, digests...)
	args = append(args, len(d.Payload))
	if e = conn.Eval(ctx, sealScript, keys, args...).Err(); e != nil {
		return conflict(e)
	}
	confirmed, e := conn.WaitAOF(ctx, 1, 0, b.wait).Result()
	if e != nil {
		return e
	}
	if len(confirmed) != 2 || confirmed[0] < 1 {
		return fmt.Errorf("option AOF confirmation timeout")
	}
	after, e := conn.ClientID(ctx).Result()
	if e != nil {
		return e
	}
	if after != connectionID {
		return fmt.Errorf("option barrier connection changed")
	}
	return nil
}
func (b *Buffer) loadOptions(ctx context.Context, d contracts.Delivery, h map[string]any) (contracts.Delivery, error) {
	keys := h["keys"].([]any)
	if len(keys) < 2 || len(keys) > 1346 {
		return d, fmt.Errorf("option manifest quota")
	}
	parts := []codec.OptionPart{}
	var meta []byte
	for n, v := range keys {
		key := v.(string)
		expected := fmt.Sprintf("%s:book:%s:%s:%d:%s:%d:%d", b.prefix, d.Database, d.Kind, d.Generation, d.ID, d.Minute.Unix()/60, n)
		if key != expected || !strings.HasPrefix(key, b.prefix+":book:") {
			return d, contracts.ErrConflict
		}
		fields, e := b.client.HGetAll(ctx, key).Result()
		if e != nil {
			return d, e
		}
		raw := []byte(fields["payload"])
		hash := sha256.Sum256(raw)
		if fields["digest"] != hex.EncodeToString(hash[:]) {
			return d, contracts.ErrConflict
		}
		value, e := codec.Decode(optionPartType, raw)
		if e != nil {
			return d, e
		}
		p := value.(map[string]any)
		if n == 0 {
			if p["kind"] != "manifest" || p["id"] != d.Kind {
				return d, contracts.ErrConflict
			}
			meta = []byte(p["payload"].(string))
			continue
		}
		parts = append(parts, codec.OptionPart{Kind: p["kind"].(string), ID: p["id"].(string), Raw: []byte(p["payload"].(string))})
	}
	raw, members, e := codec.JoinOptionsParts(d.Kind, meta, parts)
	if e != nil {
		return d, e
	}
	if members != d.Members {
		return d, contracts.ErrConflict
	}
	d.Payload = raw
	return d, spool.Validate(d)
}
