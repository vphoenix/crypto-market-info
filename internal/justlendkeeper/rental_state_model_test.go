package justlendkeeper

import (
	"bytes"
	"context"
	"encoding/csv"
	"github.com/google/uuid"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func rentalStateTestBatch() Batch {
	now := time.Date(2026, 10, 24, 0, 0, 0, 0, time.UTC)
	b := Batch{Capture: Capture{CaptureId: uuid.New(), CaptureStartedAt: now, ConfigHash: Hash([]byte("config")), Network: Network, ContractAddress: strings.Repeat("a", 21), CaptureKind: "rental_state", AvailableAt: now, Status: "partial", EvidenceManifestHash: Hash([]byte("manifest"))}}
	Seal(&b)
	return b
}

func TestRentalStateExportAuthenticatesProofAndPreservesUnknown(t *testing.T) {
	b := rentalStateTestBatch()
	cfg := DefaultConfig()
	b.Capture.ConfigHash = cfg.Hash()
	b.RentalStates = []RentalStateObservation{rentalStateUnknown(b)}
	Seal(&b)
	out := filepath.Join(t.TempDir(), "export")
	if e := Export(context.Background(), testReportReader{batches: []Batch{b}}, Archive{}, cfg, b.Capture.CaptureStartedAt.Add(-time.Second), b.Capture.AvailableAt.Add(time.Second), out); e != nil {
		t.Fatal(e)
	}
	for _, name := range []string{"rental_states", "rental_state_batches"} {
		f, e := os.Open(filepath.Join(out, name+".csv"))
		if e != nil {
			t.Fatal(e)
		}
		rows, e := csv.NewReader(f).ReadAll()
		f.Close()
		if e != nil || len(rows) != 2 {
			t.Fatal(name, e, rows)
		}
		if name == "rental_states" {
			for i, col := range rows[0] {
				if col == "liquidatable" || col == "security_deposit_sun" {
					if rows[1][i] != "unknown" {
						t.Fatal("unknown exported as value", rows[1][i])
					}
				}
			}
		}
		if name == "rental_state_batches" {
			for i, col := range rows[0] {
				if col == "digest" && rows[1][i] != Hex(b.RentalStateProof.Digest) {
					t.Fatal("proof digest was not exported as hex")
				}
			}
		}
	}
	b.RentalStateProof = nil
	if e := Export(context.Background(), testReportReader{batches: []Batch{b}}, Archive{}, cfg, b.Capture.CaptureStartedAt.Add(-time.Second), b.Capture.AvailableAt.Add(time.Second), filepath.Join(t.TempDir(), "bad")); e == nil {
		t.Fatal("export accepted missing proof")
	}
}

func TestRentalStateRejectsMissingProof(t *testing.T) {
	b := rentalStateTestBatch()
	b.RentalStateProof = nil
	// A committed declaration alone must not publish unproven state members.
	if err := Validate(b); err == nil {
		t.Fatal("rental state capture accepted without batch proof")
	}
}

func rentalStateUnknown(b Batch) RentalStateObservation {
	return RentalStateObservation{CaptureId: b.Capture.CaptureId, CaptureStartedAt: b.Capture.CaptureStartedAt, ContractAddress: b.Capture.ContractAddress, Renter: strings.Repeat("b", 21), Receiver: strings.Repeat("c", 21), CallerAddress: strings.Repeat("d", 21), ResourceType: 1, ScheduledAt: b.Capture.CaptureStartedAt, AvailableAt: b.Capture.AvailableAt, EndpointView: "wallet_latest", StateBinding: "node_latest_unpinned", Status: "unknown"}
}

func TestRentalStateProofEmptyUnknownAndFrozenRetry(t *testing.T) {
	b := rentalStateTestBatch()
	if b.RentalStateProof == nil {
		t.Fatal("Seal omitted empty-state proof")
	}
	if err := Validate(b); err != nil {
		t.Fatal(err)
	}
	b.RentalStates = []RentalStateObservation{rentalStateUnknown(b)}
	b.RentalStates[0].RentIndex = new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	Seal(&b)
	before, _ := Freeze(b)
	var restored Batch
	if err := Thaw(before, &restored); err != nil {
		t.Fatal(err)
	}
	if err := Validate(restored); err != nil {
		t.Fatal(err)
	}
	after, _ := Freeze(restored)
	if !bytes.Equal(before, after) || restored.RentalStates[0].SecurityDepositSun != nil || restored.RentalStates[0].Liquidatable != nil {
		t.Fatal("frozen retry changed facts or unknown became zero")
	}
}

func TestRentalStateRejectsTamperedMembersAndLegacyMix(t *testing.T) {
	base := rentalStateTestBatch()
	base.RentalStates = []RentalStateObservation{rentalStateUnknown(base)}
	Seal(&base)
	cases := map[string]func(*Batch){
		"missing_member":   func(b *Batch) { b.RentalStates = nil },
		"extra_member":     func(b *Batch) { b.RentalStates = append(b.RentalStates, b.RentalStates[0]) },
		"wrong_digest":     func(b *Batch) { b.RentalStateProof.Digest = Hash([]byte("changed")) },
		"wrong_capture":    func(b *Batch) { b.RentalStateProof.CaptureId = uuid.New() },
		"legacy_kind":      func(b *Batch) { b.Capture.CaptureKind = "probe" },
		"duplicate_index":  func(b *Batch) { b.RentalStates = append(b.RentalStates, b.RentalStates[0]); Seal(b) },
		"unproven_success": func(b *Batch) { b.RentalStates[0].Status = "ok"; Seal(b) },
		"overflow":         func(b *Batch) { b.RentalStates[0].RentIndex = new(big.Int).Lsh(big.NewInt(1), 256); Seal(b) },
	}
	raw, _ := Freeze(base)
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			var b Batch
			if e := Thaw(raw, &b); e != nil {
				t.Fatal(e)
			}
			mutate(&b)
			if e := Validate(b); e == nil {
				t.Fatal("accepted malformed membership")
			}
		})
	}
	legacy := rentalStateTestBatch()
	legacy.Capture.CaptureKind = "probe"
	legacy.RentalStateProof = nil
	Seal(&legacy)
	if e := Validate(legacy); e != nil {
		t.Fatal("legacy empty capture rejected", e)
	}
}

func rentalStateVerifiedTuple(b Batch) RentalStateObservation {
	r := rentalStateUnknown(b)
	r.ApiSuccess = Ptr(true)
	r.TvmResult = "SUCCESS"
	r.IdentityStatus = "verified_manifest"
	impl, _ := HexAddress(ImplementationHex)
	hash, _ := BinaryHex(ImplementationSHA, 32)
	r.ImplementationAddress = &impl
	r.ImplementationCodeHash = &hash
	r.ImplementationCheckedAt = Ptr(r.ScheduledAt)
	r.RequestStartedAt = Ptr(r.ScheduledAt)
	r.ResponseReceivedAt = Ptr(r.ScheduledAt)
	r.RequestPayloadHash = Ptr(Hash([]byte("request")))
	r.ResponsePayloadHash = Ptr(Hash([]byte("response")))
	r.SecurityDepositSun = big.NewInt(0)
	r.RentIndex = big.NewInt(1)
	r.Liquidatable = Ptr(true)
	return r
}

func TestRentalStatePreservesProbeAfterMissingTrailingHead(t *testing.T) {
	b := rentalStateTestBatch()
	r := rentalStateVerifiedTuple(b)
	r.ErrorCode = "missing_heads"
	b.RentalStates = []RentalStateObservation{r}
	b.Probes = []Probe{{CaptureId: b.Capture.CaptureId, CaptureStartedAt: b.Capture.CaptureStartedAt, ContractAddress: r.ContractAddress, Renter: r.Renter, Receiver: r.Receiver, CallerAddress: r.CallerAddress, ResourceType: 1, StateBinding: "node_latest_unpinned", Status: "unknown"}}
	Seal(&b)
	if e := Validate(b); e != nil {
		t.Fatal("lost performed probe after missing trailing head", e)
	}
	for _, field := range []string{"identity", "api", "tvm", "hash", "tuple", "false"} {
		t.Run(field, func(t *testing.T) {
			v := b
			v.RentalStates = append([]RentalStateObservation(nil), b.RentalStates...)
			r := &v.RentalStates[0]
			switch field {
			case "identity":
				r.ImplementationCodeHash = Ptr(Hash([]byte("wrong code")))
			case "api":
				r.ApiSuccess = Ptr(false)
			case "tvm":
				r.TvmResult = "FAILED"
			case "hash":
				r.ResponsePayloadHash = nil
			case "tuple":
				r.SecurityDepositSun = nil
			case "false":
				r.Liquidatable = Ptr(false)
			}
			Seal(&v)
			if e := Validate(v); e == nil {
				t.Fatal("probe allowed from untrusted conditional state")
			}
		})
	}
}

func TestRentalStateDelayedCompletionKeepsEligibleEvidence(t *testing.T) {
	b := rentalStateTestBatch()
	r := rentalStateVerifiedTuple(b)
	r.AvailableAt = r.AvailableAt.Add(20 * time.Minute)
	r.ErrorCode = "head_after_rate_limited"
	b.RentalStates = []RentalStateObservation{r}
	b.Probes = []Probe{{CaptureId: b.Capture.CaptureId, CaptureStartedAt: b.Capture.CaptureStartedAt, ContractAddress: r.ContractAddress, Renter: r.Renter, Receiver: r.Receiver, CallerAddress: r.CallerAddress, ResourceType: 1, StateBinding: "node_latest_unpinned", Status: "unknown"}}
	Seal(&b)
	if e := Validate(b); e != nil {
		t.Fatal("delayed final head erased an eligible observed tuple", e)
	}
	// Fresh at request time is insufficient if the response itself took too long.
	b.RentalStates[0].ResponseReceivedAt = Ptr(r.AvailableAt)
	Seal(&b)
	if e := Validate(b); e == nil {
		t.Fatal("accepted implementation identity stale at response time")
	}
}
