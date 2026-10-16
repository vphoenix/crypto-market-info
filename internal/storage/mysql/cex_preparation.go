package mysql

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/model"
	"github.com/vphoenix/crypto-market-info/internal/storage/codec"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"github.com/vphoenix/crypto-market-info/internal/storage/spool"
)

// Temporary preparation is unlinked immediately. A kill closes it in the
// kernel; recovery always starts from the unchanged Redis/spool delivery.
type cexPreparation struct {
	file         *os.File
	streams      []uint32
	chunks       int
	workingBytes int64
}

type cexPreparedChunk struct {
	Minute       model.CompletedMinute
	Hashes       map[uint64][]byte
	Publications []publicationRecord
}

func (p *PreparedDelivery) Close() error {
	if p != nil && p.cexSpill != nil && p.cexSpill.file != nil {
		err := p.cexSpill.file.Close()
		p.cexSpill.file = nil
		return err
	}
	return nil
}

// PrepareDeliveryBounded uses small independently prepared CEX chunks when a
// large frame would monopolize the preparation budget. The final transaction,
// generation fence and ingest_batch identity are still shared by every chunk.
func (c *Client) PrepareDeliveryBounded(ctx context.Context, d contracts.Delivery, budget int64) (*PreparedDelivery, error) {
	if d.Kind != "cex" || int64(len(d.Payload))*8 <= budget {
		return c.PrepareDelivery(ctx, d)
	}
	if err := spool.Validate(d); err != nil {
		return nil, err
	}
	if d.Database != c.database || d.Service != c.producer.Service || d.Generation != c.producer.Generation {
		return nil, contracts.ErrFenced
	}
	if d.Members == 0 || d.Members > 100 {
		return nil, fmt.Errorf("delivery member count mismatch")
	}
	f, err := os.CreateTemp("", "cmi-cex-preparation-*")
	if err != nil {
		return nil, err
	}
	if err = os.Remove(f.Name()); err != nil {
		f.Close()
		return nil, err
	}
	p := &PreparedDelivery{d: d, cexSpill: &cexPreparation{file: f}}
	p.d.Payload = nil
	complete := false
	defer func() {
		if !complete {
			p.Close()
		}
	}()
	chunk := cexPreparedChunk{Minute: model.CompletedMinute{MinuteTime: d.Minute}, Hashes: map[uint64][]byte{}}
	var chunkBytes int64
	flush := func() error {
		if len(chunk.Minute.Batches) == 0 {
			return nil
		}
		if err := c.prefetchCEXPublications(ctx, chunk.Publications); err != nil {
			return err
		}
		var encoded bytes.Buffer
		if err := gob.NewEncoder(&encoded).Encode(chunk); err != nil {
			return err
		}
		if encoded.Len() > 8<<20 {
			return fmt.Errorf("prepared chunk exceeds bounded scratch space: %w", contracts.ErrCapacity)
		}
		heap := (&PreparedDelivery{cex: &chunk.Minute, cexHashes: chunk.Hashes, cexPublications: chunk.Publications}).MemoryBytes()
		// Includes the encoded read buffer and bounded SQL row/parameter scratch.
		working := heap + int64(encoded.Len()) + 8<<20
		p.cexSpill.workingBytes = max(p.cexSpill.workingBytes, working)
		var header [36]byte
		binary.BigEndian.PutUint32(header[:4], uint32(encoded.Len()))
		hash := sha256.Sum256(encoded.Bytes())
		copy(header[4:], hash[:])
		if _, err := f.Write(header[:]); err != nil {
			return err
		}
		if _, err := f.Write(encoded.Bytes()); err != nil {
			return err
		}
		p.cexSpill.chunks++
		chunk.Minute.Batches = nil
		chunk.Hashes = map[uint64][]byte{}
		chunk.Publications = nil
		chunkBytes = 0
		return ctx.Err()
	}
	knownV1 := model.CeilMilliseconds(time.Now().UTC())
	err = codec.WalkBookDelivery(d.Minute, d.Payload, d.Codec, func(batch model.MinuteBatch, known int64) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(p.cexSpill.streams) >= int(d.Members) {
			return fmt.Errorf("delivery member count mismatch")
		}
		// Price arrays dominate wide legacy minutes. Flush before adding the
		// next book; no whole-batch maps or decoded model are retained.
		size := int64(4096 + len(batch.Deltas)*128)
		for _, delta := range batch.Deltas {
			size += int64(len(delta.BidChangePrice)+len(delta.BidChangeQty)+len(delta.AskChangePrice)+len(delta.AskChangeQty)) * 8
		}
		if chunkBytes+size > 1<<20 {
			if err := flush(); err != nil {
				return err
			}
		}
		chunk.Minute.KnownFromMS = known
		hash := model.DiscoveryHash(model.TradingMinuteBody(batch))
		chunk.Hashes[batch.Minute.ID] = digestBytes(hash)
		if c.discoveryEnabled.Load() || known != 0 {
			if known == 0 {
				known = knownV1
			}
			pub, err := minutePublicationHash(batch, known, hash)
			if err != nil {
				return err
			}
			chunk.Publications = append(chunk.Publications, pub)
		}
		chunk.Minute.Batches = append(chunk.Minute.Batches, batch)
		chunkBytes += size
		p.cexSpill.streams = append(p.cexSpill.streams, batch.Minute.InstrumentID)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if uint32(len(p.cexSpill.streams)) != d.Members {
		return nil, fmt.Errorf("delivery member count mismatch")
	}
	if err = flush(); err != nil {
		return nil, err
	}
	if p.MemoryBytes() > budget {
		return nil, fmt.Errorf("single prepared chunk exceeds budget: %w", contracts.ErrCapacity)
	}
	complete = true
	return p, nil
}

func (c *Client) commitCEXPreparation(ctx context.Context, tx *sql.Tx, p *PreparedDelivery) error {
	s := p.cexSpill
	if s.file == nil {
		return fmt.Errorf("closed CEX preparation")
	}
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	var members uint32
	for range s.chunks {
		if err := ctx.Err(); err != nil {
			return err
		}
		var header [36]byte
		if _, err := io.ReadFull(s.file, header[:]); err != nil {
			return err
		}
		n := binary.BigEndian.Uint32(header[:4])
		if n == 0 || n > 8<<20 {
			return fmt.Errorf("invalid prepared chunk size")
		}
		raw := make([]byte, n)
		if _, err := io.ReadFull(s.file, raw); err != nil {
			return err
		}
		hash := sha256.Sum256(raw)
		if !bytes.Equal(header[4:], hash[:]) {
			return contracts.ErrConflict
		}
		var chunk cexPreparedChunk
		if err := gob.NewDecoder(bytes.NewReader(raw)).Decode(&chunk); err != nil {
			return err
		}
		if err := c.commitCEXRows(ctx, tx, p.d, chunk.Minute, chunk.Hashes, chunk.Publications); err != nil {
			return err
		}
		members += uint32(len(chunk.Minute.Batches))
		if err := minuteFault(ctx, "prepared_chunk", tx); err != nil {
			return err
		}
	}
	if members != p.d.Members {
		return fmt.Errorf("prepared chunk member count mismatch")
	}
	var tail [1]byte
	if n, err := s.file.Read(tail[:]); n != 0 || err != io.EOF {
		return fmt.Errorf("prepared chunk trailing data")
	}
	return nil
}
