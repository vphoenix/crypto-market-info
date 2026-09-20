package model

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// CompletedMinute is the completion barrier between sampling and persistence.
// Batches is immutable after handoff, sorted by instrument ID, and may be empty.
type CompletedMinute struct {
	MinuteTime time.Time
	Batches    []MinuteBatch
}

type CanonicalMapping struct {
	InstrumentID                       uint32
	MappingRevision                    string
	CanonicalMarketKey                 string
	CanonicalBaseAsset                 string
	CanonicalQuoteAsset                string
	CanonicalSettleAsset               string
	CanonicalBaseUnitsPerVenueBaseUnit decimal.Decimal
	MappingKind                        string
	RecordedAt                         time.Time
}

type PerpetualUniverseRun struct {
	RunID               uuid.UUID
	SelectionRevision   string
	MappingRevision     string
	StartedAt           time.Time
	SelectionConfigJSON string
	EnabledVenues       []string
	CanonicalInclude    []string
	CanonicalExclude    []string
	CanonicalGroupCount uint32
	InstrumentCount     uint32
}

type PerpetualUniverseMember struct {
	RunID              uuid.UUID
	InstrumentID       uint32
	CanonicalMarketKey string
}

var canonicalBasePattern = regexp.MustCompile(`^[A-Z0-9][A-Z0-9._]{0,31}$`)
var revisionPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

func ValidateCanonicalMarketKey(key string) error {
	base, ok := strings.CutSuffix(key, "-USDT-PERP")
	if !ok || !canonicalBasePattern.MatchString(base) {
		return fmt.Errorf("invalid canonical market key %q", key)
	}
	return nil
}

func ValidateMappingRevision(revision string) error {
	if !revisionPattern.MatchString(revision) {
		return fmt.Errorf("revision must be a lowercase SHA-256 hex digest")
	}
	return nil
}

func (m CanonicalMapping) Validate() error {
	if m.InstrumentID == 0 {
		return fmt.Errorf("canonical mapping requires instrument_id")
	}
	if err := ValidateMappingRevision(m.MappingRevision); err != nil {
		return err
	}
	if err := ValidateCanonicalMarketKey(m.CanonicalMarketKey); err != nil {
		return err
	}
	if m.CanonicalMarketKey != m.CanonicalBaseAsset+"-USDT-PERP" || m.CanonicalQuoteAsset != "USDT" || m.CanonicalSettleAsset != "USDT" {
		return fmt.Errorf("canonical mapping fields do not match a USDT perpetual key")
	}
	factor := m.CanonicalBaseUnitsPerVenueBaseUnit
	if !factor.IsPositive() || factor.Exponent() < -18 || factor.GreaterThanOrEqual(decimal.New(1, 20)) {
		return fmt.Errorf("canonical unit factor must fit positive Decimal(38,18)")
	}
	if m.MappingKind != "identity" && m.MappingKind != "alias" {
		return fmt.Errorf("unknown canonical mapping kind %q", m.MappingKind)
	}
	if m.MappingKind == "identity" && !factor.Equal(decimal.NewFromInt(1)) {
		return fmt.Errorf("identity mapping must have factor 1")
	}
	return validateMetadataTime(m.RecordedAt, "recorded_at")
}

// SameMapping ignores the first-prepared timestamp, which is retained on reuse.
func (m CanonicalMapping) SameMapping(other CanonicalMapping) bool {
	return m.InstrumentID == other.InstrumentID && m.MappingRevision == other.MappingRevision &&
		m.CanonicalMarketKey == other.CanonicalMarketKey && m.CanonicalBaseAsset == other.CanonicalBaseAsset &&
		m.CanonicalQuoteAsset == other.CanonicalQuoteAsset && m.CanonicalSettleAsset == other.CanonicalSettleAsset &&
		m.CanonicalBaseUnitsPerVenueBaseUnit.Equal(other.CanonicalBaseUnitsPerVenueBaseUnit) && m.MappingKind == other.MappingKind
}

func (r PerpetualUniverseRun) Validate() error {
	if r.RunID == uuid.Nil {
		return fmt.Errorf("universe run requires run_id")
	}
	if err := ValidateMappingRevision(r.MappingRevision); err != nil {
		return err
	}
	if err := ValidateMappingRevision(r.SelectionRevision); err != nil {
		return err
	}
	if err := validateMetadataTime(r.StartedAt, "started_at"); err != nil {
		return err
	}
	if !json.Valid([]byte(r.SelectionConfigJSON)) || !strings.HasPrefix(r.SelectionConfigJSON, "{") {
		return fmt.Errorf("selection_config_json must be a JSON object")
	}
	for field, values := range map[string][]string{"enabled_venues": r.EnabledVenues, "canonical_include": r.CanonicalInclude, "canonical_exclude": r.CanonicalExclude} {
		for index, value := range values {
			if value == "" || strings.TrimSpace(value) != value || (index > 0 && values[index-1] >= value) {
				return fmt.Errorf("%s must contain exact sorted unique strings", field)
			}
			if field != "enabled_venues" {
				if err := ValidateCanonicalMarketKey(value); err != nil {
					return err
				}
			}
		}
	}
	if len(r.EnabledVenues) == 1 || (r.InstrumentCount > 0 && len(r.EnabledVenues) < 2) {
		return fmt.Errorf("universe requires zero or at least two enabled venues")
	}
	if r.CanonicalGroupCount > r.InstrumentCount/2 {
		return fmt.Errorf("universe group count exceeds two-venue instrument count")
	}
	return nil
}

func validateMetadataTime(at time.Time, field string) error {
	if at.IsZero() || !at.Equal(at.UTC().Truncate(time.Millisecond)) {
		return fmt.Errorf("%s requires nonzero millisecond-precision UTC time", field)
	}
	return nil
}
