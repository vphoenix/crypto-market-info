package runtimecheckpoint

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

// InputInventory describes conversion provenance, never database completeness.
// The complete bounded metadata document is an immutable recovery reference;
// counts are only monitoring summaries and cannot erase unknown source ranges.
type InputInventory struct {
	Object                                              Reference
	Service                                             string
	CollectedAt                                         time.Time
	Plans, VerifiedRanges, PendingOrUnknown, Unreadable uint32
	UnlistedSourceRanges                                string
	FullHistoryCertified                                bool
}
type ConversionInput struct {
	Inventory InputInventory
	raw       []byte
	binding   Binding
}
type inventoryRow struct {
	Kind           string `json:"kind"`
	Table          string `json:"table"`
	State          int    `json:"state"`
	Classification string `json:"classification"`
}
type inventoryDocument struct {
	Version              uint32            `json:"version"`
	Service              string            `json:"service"`
	Database             string            `json:"database"`
	ServerUUID           string            `json:"server_uuid"`
	CollectedAt          time.Time         `json:"collected_at"`
	Plans                []inventoryRow    `json:"plans"`
	Coverage             []inventoryRow    `json:"coverage"`
	Unreadable           []json.RawMessage `json:"unreadable"`
	UnlistedSourceRanges string            `json:"unlisted_source_ranges"`
	DependencyProof      string            `json:"runtime_dependency_proof"`
	FullHistoryCertified *bool             `json:"full_history_certified"`
}

func ReadConversionInput(path string, binding Binding, service string) (ConversionInput, error) {
	var result ConversionInput
	if path == "" {
		return result, errors.New("conversion requires an explicit migrated/pending/unreadable input inventory")
	}
	file, err := os.Open(path)
	if err != nil {
		return result, err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, (8<<20)+1))
	if err != nil {
		return result, err
	}
	if len(raw) > 8<<20 {
		return result, ErrCapacity
	}
	return parseConversionInput(raw, binding, service)
}

func parseConversionInput(raw []byte, binding Binding, service string) (ConversionInput, error) {
	var result ConversionInput
	var doc inventoryDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		return result, err
	}
	if doc.Version != 1 || doc.Service != service || doc.Database != binding.Database || doc.ServerUUID != binding.ServerUUID || doc.CollectedAt.IsZero() || doc.UnlistedSourceRanges != "unknown" || doc.DependencyProof != "requires_frozen_scope_comparison" || doc.FullHistoryCertified == nil || *doc.FullHistoryCertified {
		return result, fmt.Errorf("conversion input target/provenance mismatch: %w", ErrConflict)
	}
	if len(doc.Plans) > 4096 || len(doc.Coverage) > 4096 || len(doc.Unreadable) > 4096 {
		return result, ErrCapacity
	}
	sum := sha256.Sum256(raw)
	out := InputInventory{Object: Reference{hex.EncodeToString(sum[:]), int64(len(raw))}, Service: service, CollectedAt: doc.CollectedAt, Plans: uint32(len(doc.Plans)), Unreadable: uint32(len(doc.Unreadable)), UnlistedSourceRanges: "unknown"}
	for _, row := range doc.Plans {
		want := "pending_or_unknown"
		if row.State == 2 {
			want = "verified_table"
		} else if row.State == 5 {
			want = "verified_rows_only"
		}
		if row.Kind != "plan" || row.Table == "" || row.Classification != want {
			return result, ErrConflict
		}
		if row.State != 2 {
			out.PendingOrUnknown++
		}
	}
	for _, row := range doc.Coverage {
		want := "pending_or_unknown"
		if row.State == 2 {
			want = "verified_range"
		}
		if row.Kind != "coverage" || row.Table == "" || row.Classification != want {
			return result, ErrConflict
		}
		if row.State == 2 {
			out.VerifiedRanges++
		} else {
			out.PendingOrUnknown++
		}
	}
	for _, row := range doc.Unreadable {
		var gap struct {
			Kind, Table string
			KeyHash     string `json:"key_hash"`
			Key         string `json:"key_hex"`
			Reason      string `json:"reason_hex"`
		}
		if json.Unmarshal(row, &gap) != nil || gap.Kind != "unreadable" || gap.Table == "" || gap.KeyHash == "" || gap.Key == "" || gap.Reason == "" {
			return result, ErrConflict
		}
	}
	return ConversionInput{Inventory: out, raw: raw, binding: binding}, nil
}
func (in ConversionInput) Persist(files *Store, previous InputInventory) (InputInventory, error) {
	b := files.Binding()
	a := in.binding
	a.StateVersion = b.StateVersion
	if a != b || len(in.raw) == 0 {
		return InputInventory{}, ErrConflict
	}
	if previous.Object.Hash != "" && previous != in.Inventory {
		return previous, errors.New("conversion input inventory changed during resume")
	}
	ref, err := files.PutObject(in.raw)
	if err != nil {
		return previous, err
	}
	if ref != in.Inventory.Object {
		return previous, ErrConflict
	}
	return in.Inventory, nil
}

// InputReferences keeps conversion metadata alongside every subsequent runtime
// checkpoint, including the previous checkpoint and exported recovery chains.
func (in InputInventory) References() []Reference {
	if in.Object.Hash == "" {
		return nil
	}
	return []Reference{in.Object}
}
func (in InputInventory) Verify(files *Store) error {
	if in.Object.Hash == "" {
		return errors.New("converted state has no input inventory")
	}
	raw, err := files.ReadObject(in.Object)
	if err != nil {
		return err
	}
	// ReadObject verifies the exact frozen bytes; completeness remains explicitly
	// false, including after normal recovery or a later database-history import.
	parsed, err := parseConversionInput(raw, files.Binding(), in.Service)
	if err != nil {
		return err
	}
	if parsed.Inventory != in {
		return ErrRecovery
	}
	return nil
}
