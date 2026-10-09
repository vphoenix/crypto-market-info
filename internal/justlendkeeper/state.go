package justlendkeeper

import (
	"bytes"
	"errors"
	"github.com/google/uuid"
	"os"
	"path/filepath"
	"time"
)

type SourceLimit struct {
	Next     time.Time
	Cooldown time.Time
	Blocked  bool
	Limited  uint32
}
type Scan struct {
	ID          string
	Kind        string
	Mode        string
	From        time.Time
	To          time.Time
	Fingerprint string
	Pages       uint32
	MaxPages    uint32
	Done        bool
	Failed      bool
	Failures    uint32
	RetryAt     time.Time
}
type Candidate struct {
	Ambiguous  bool
	LastNumber uint64
	LastTx     uint32
	LastLog    uint32

	Renter       string
	Receiver     string
	LastActivity time.Time
	Origin       RawEvent
	Closed       bool
	Verified     bool
	Lifecycle    string
	Stratum      uint8
	SeedRevision string
	SeedAttempts uint8
	SeedNext     time.Time
}
type State struct {
	SolidAnchors map[uint64]string

	CohortChanges []CohortChange

	BackfillActive bool
	BackfillFrom   time.Time
	BackfillTo     time.Time
	HistoryFrom    time.Time
	HistoryTo      time.Time
	HistoryCursor  time.Time

	Version         uint32
	Target          string
	ConfigHash      string
	Day             string
	Used            uint32
	NextGlobal      time.Time
	NextBackground  time.Time
	Sources         map[string]SourceLimit
	Scans           []Scan
	Candidates      []Candidate
	Cohort          []Candidate
	CohortId        uuid.UUID
	Rotation        uint32
	BootstrapReady  bool
	Ops             []Operation
	NextEvents      time.Time
	NextProbe       time.Time
	NextCosts       time.Time
	NextQuote       time.Time
	NextIdentity    time.Time
	Implementation  string
	CodeHash        string
	IdentityAt      time.Time
	IdentityStatus  string
	CallersValid    []string
	EventCursors    map[string]time.Time
	RescanDay       string
	LastSolidNumber uint64
	LastSolidHash   string
	IndexingEnabled bool
	EvidenceCursors map[string]time.Time
	NextEnrichment  time.Time
}

func NewState(target string, c Config) State {
	return State{Version: 1, Target: target, ConfigHash: Hex(c.Hash()), SolidAnchors: map[uint64]string{}, Sources: map[string]SourceLimit{}, EventCursors: map[string]time.Time{}, IdentityStatus: "unknown"}
}
func SaveState(path string, s State) error {
	return (&stateWriter{}).Save(path, s)
}
func LoadState(path, target string, c Config) (State, error) {
	b, e := os.ReadFile(path)
	if errors.Is(e, os.ErrNotExist) {
		s := NewState(target, c)
		return s, SaveState(path, s)
	}
	if e != nil {
		return State{}, e
	}
	if len(b) < 32 || !bytes.Equal(b[:32], []byte(Hash(b[32:]))) {
		return State{}, errors.New("state_checksum_failed")
	}
	var s State
	if e = decodeState(path, b[32:], &s); e != nil {
		return s, e
	}
	if s.Version != 1 || s.Target != target || s.ConfigHash != Hex(c.Hash()) || s.Sources == nil {
		return s, errors.New("state_version_target_config_mismatch")
	}
	// Additive local-state migration preserves budgets, cooldowns and pending work.
	if s.SolidAnchors == nil {
		s.SolidAnchors = map[uint64]string{}
	}
	if s.EventCursors == nil {
		s.EventCursors = map[string]time.Time{}
	}
	return s, nil
}
func StateFile(dir string) string { return filepath.Join(dir, "state.gob") }

type Limiter struct {
	State   *State
	Save    func() error
	Now     func() time.Time
	Started time.Time
	Budget  uint32
	LastGap time.Duration // this single in-flight reservation's gap, including warmup
}

func (l *Limiter) Ready(source string, background bool) time.Time {
	now := l.Now().UTC()
	r := l.State.Sources[source]
	if r.Blocked {
		return now.Add(24 * time.Hour)
	}
	next := l.State.NextGlobal
	if next.Before(r.Next) {
		next = r.Next
	}
	if next.Before(r.Cooldown) {
		next = r.Cooldown
	}
	if background && next.Before(l.State.NextBackground) {
		next = l.State.NextBackground
	}
	if now.Before(l.Started.Add(5*time.Minute)) && next.Before(l.Started.Add(5*time.Second)) {
		next = l.Started.Add(5 * time.Second)
	}
	if l.State.Day == now.UTC().Format("2006-01-02") && l.State.Used >= l.Budget {
		tomorrow := time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, time.UTC)
		if next.Before(tomorrow) {
			next = tomorrow
		}
	}
	return next
}
func (l *Limiter) Reserve(source string, background bool) error {
	now := UTC(l.Now())
	if now.Before(l.Ready(source, background)) || l.State.Sources[source].Blocked {
		return errors.New("request_not_ready")
	}
	day := now.Format("2006-01-02")
	if day != l.State.Day {
		l.State.Day = day
		l.State.Used = 0
	}
	if l.State.Used >= l.Budget {
		return errors.New("request_budget_exhausted")
	}
	gap := time.Second
	if now.Before(l.Started.Add(5 * time.Minute)) {
		gap = 5 * time.Second
	}
	l.LastGap = gap
	l.State.NextGlobal = now.Add(gap)
	sg := time.Second
	if source == "trongrid" {
		sg = 5 * time.Second
	}
	r := l.State.Sources[source]
	r.Next = now.Add(sg)
	l.State.Sources[source] = r
	if background {
		l.State.NextBackground = now.Add(5 * time.Second)
	}
	l.State.Used++
	return l.Save()
}

// ExtendGap anchors the next attempt after an observed response (or transport
// completion). Disk fsync between Reserve and HTTP start cannot shorten spacing.
// The caller persists this with Status, or explicitly after a transport failure.
func (l *Limiter) ExtendGap(source string, background bool, at time.Time) {
	gap := l.LastGap
	if gap <= 0 {
		gap = 5 * time.Second
	}
	if next := at.Add(gap); l.State.NextGlobal.Before(next) {
		l.State.NextGlobal = next
	}
	r := l.State.Sources[source]
	sourceGap := time.Second
	if source == "trongrid" {
		sourceGap = 5 * time.Second
	}
	if next := at.Add(sourceGap); r.Next.Before(next) {
		r.Next = next
	}
	l.State.Sources[source] = r
	if background {
		if next := at.Add(5 * time.Second); l.State.NextBackground.Before(next) {
			l.State.NextBackground = next
		}
	}
}
func (l *Limiter) Status(source string, status int, retryAfter time.Duration) error {
	r := l.State.Sources[source]
	now := l.Now().UTC()
	if status == 401 || status == 403 {
		r.Blocked = true
	}
	if status == 429 || status == 418 {
		r.Limited++
		shift := r.Limited - 1
		if shift > 4 {
			shift = 4
		}
		d := 5 * time.Minute * time.Duration(1<<shift)
		if d > time.Hour {
			d = time.Hour
		}
		if retryAfter > d {
			d = retryAfter
		}
		r.Cooldown = now.Add(d)
	}
	if status >= 200 && status < 300 {
		r.Limited = 0
	}
	l.State.Sources[source] = r
	return l.Save()
}
