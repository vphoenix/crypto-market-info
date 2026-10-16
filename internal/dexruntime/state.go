// Package dexruntime owns only DEX runtime coverage and recoverable block
// publications. Existing bounded fact/finality/receipt reads keep their roles.
package dexruntime

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
)

type interval struct{ From, To uint64 }
type blockState struct {
	Number, Revision uint64
	Hash             dex.Hash
	AvailableAt      time.Time
}
type state struct {
	Version                 uint32
	Binding                 runtimecheckpoint.Binding
	Manifest                dex.Hash
	Verified                bool
	Coverage, Finalized     []interval
	ActiveRoot, HeightsRoot runtimecheckpoint.PageRoot
	Conversion              ConversionReport
	active                  *runtimecheckpoint.PagedMap[blockState]
	heights                 *runtimecheckpoint.PagedMap[uint64]
}

func key(b dex.Block) string    { return fmt.Sprintf("%020d/%x", b.Number, b.Hash[:]) }
func heightKey(n uint64) string { return fmt.Sprintf("%020d", n) }
func contains(ranges []interval, n uint64) bool {
	i := sort.Search(len(ranges), func(i int) bool { return ranges[i].To >= n })
	return i < len(ranges) && ranges[i].From <= n
}
func add(ranges []interval, n uint64) []interval {
	i := sort.Search(len(ranges), func(i int) bool { return ranges[i].To >= n || ranges[i].To != math.MaxUint64 && ranges[i].To+1 == n })
	if i == len(ranges) {
		return append(ranges, interval{n, n})
	}
	if ranges[i].From > n && ranges[i].From-n > 1 {
		ranges = append(ranges, interval{})
		copy(ranges[i+1:], ranges[i:])
		ranges[i] = interval{n, n}
		return ranges
	}
	ranges[i].From = min(ranges[i].From, n)
	ranges[i].To = max(ranges[i].To, n)
	if i+1 < len(ranges) && ranges[i].To != math.MaxUint64 && ranges[i].To+1 >= ranges[i+1].From {
		ranges[i].To = max(ranges[i].To, ranges[i+1].To)
		ranges = append(ranges[:i+1], ranges[i+2:]...)
	}
	return ranges
}
func remove(ranges []interval, n uint64) []interval {
	i := sort.Search(len(ranges), func(i int) bool { return ranges[i].To >= n })
	if i == len(ranges) || ranges[i].From > n {
		return ranges
	}
	r := ranges[i]
	switch {
	case r.From == n && r.To == n:
		return append(ranges[:i], ranges[i+1:]...)
	case r.From == n:
		ranges[i].From++
	case r.To == n:
		ranges[i].To--
	default:
		ranges = append(ranges, interval{})
		copy(ranges[i+2:], ranges[i+1:])
		ranges[i] = interval{r.From, n - 1}
		ranges[i+1] = interval{n + 1, r.To}
	}
	return ranges
}
func (s *state) open(files *runtimecheckpoint.Store) error {
	var err error
	s.active, err = runtimecheckpoint.OpenPagedMap[blockState](files, s.ActiveRoot, 2<<20, func(blockState) bool { return true }, func(b blockState) time.Time { return b.AvailableAt })
	if err != nil {
		return err
	}
	s.heights, err = runtimecheckpoint.OpenPagedMap[uint64](files, s.HeightsRoot, 512<<10, func(uint64) bool { return true })
	return err
}
func (s *state) references() ([]runtimecheckpoint.Reference, error) {
	var refs, more []runtimecheckpoint.Reference
	var err error
	s.ActiveRoot, refs, err = s.active.Snapshot()
	if err != nil {
		return nil, err
	}
	s.HeightsRoot, more, err = s.heights.Snapshot()
	return append(append(refs, more...), s.Conversion.InputInventory.References()...), err
}
func (s *state) apply(b dex.Block) error {
	if b.Manifest != s.Manifest || b.ChainID != 1 || !b.Committed || b.Number == 0 || b.Revision == 0 || b.Hash == (dex.Hash{}) {
		return runtimecheckpoint.ErrConflict
	}
	id := key(b)
	old, exists, err := s.active.Get(id)
	if err != nil {
		return err
	}
	if exists && b.Revision < old.Revision {
		return errors.New("dex_runtime_actual_revision_regressed")
	}
	// SQL forbids downgrading an already-finalized key. The interval is only
	// presence coverage; it is not a substitute for the original hash/finality
	// conflict and member checks in the database and chain runner.
	active := b.Canonical && b.Finality != "finalized"
	n, have, err := s.heights.Get(heightKey(b.Number))
	if err != nil {
		return err
	}
	if exists && !have {
		return runtimecheckpoint.ErrRecovery
	}
	if active {
		if !exists {
			n++
			if err = s.heights.Set(heightKey(b.Number), n); err != nil {
				return err
			}
		}
		if err = s.active.Set(id, blockState{b.Number, b.Revision, b.Hash, b.AvailableAt}); err != nil {
			return err
		}
	} else if exists {
		if n == 0 {
			return runtimecheckpoint.ErrRecovery
		}
		n--
		if err = s.active.Delete(id); err != nil {
			return err
		}
		if n == 0 {
			err = s.heights.Delete(heightKey(b.Number))
		} else {
			err = s.heights.Set(heightKey(b.Number), n)
		}
		if err != nil {
			return err
		}
	}
	if b.Canonical && b.Finality == "finalized" {
		s.Finalized = add(s.Finalized, b.Number)
	}
	if b.Canonical || n > 0 || contains(s.Finalized, b.Number) {
		s.Coverage = add(s.Coverage, b.Number)
	} else {
		s.Coverage = remove(s.Coverage, b.Number)
	}
	return nil
}
func (s *state) gap() uint64 {
	if len(s.Coverage) < 2 {
		return 0
	}
	return s.Coverage[0].To + 1
}
func validIntervals(ranges []interval) bool {
	for i, r := range ranges {
		if r.From == 0 || r.From > r.To || i > 0 && (ranges[i-1].To == math.MaxUint64 || ranges[i-1].To+1 >= r.From) {
			return false
		}
	}
	return true
}
func (s *state) validate(b runtimecheckpoint.Binding, m dex.Hash) error {
	if s.Version != 1 || !s.Verified || s.Binding != b || s.Manifest != m || !validIntervals(s.Coverage) || !validIntervals(s.Finalized) {
		return runtimecheckpoint.ErrRecovery
	}
	for _, r := range s.Finalized {
		i := sort.Search(len(s.Coverage), func(i int) bool { return s.Coverage[i].To >= r.To })
		if i == len(s.Coverage) || s.Coverage[i].From > r.From {
			return runtimecheckpoint.ErrRecovery
		}
	}
	return nil
}
func (s *state) memoryBytes() int64 {
	return runtimecheckpoint.EstimateTypedBytes(*s) + s.active.MemoryBytes() + s.heights.MemoryBytes()
}
