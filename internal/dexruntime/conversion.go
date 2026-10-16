package dexruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/vphoenix/crypto-market-info/internal/dex"
	"github.com/vphoenix/crypto-market-info/internal/runtimecheckpoint"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
)

type ConversionReport struct {
	InputInventory                 runtimecheckpoint.InputInventory
	Version                        uint32
	Binding                        runtimecheckpoint.Binding
	FrozenAt, VerifiedAt           time.Time
	Headers, Unfinalized, FirstGap uint64
	InputDigest                    string
	Verified                       bool
}
type pager interface {
	DEXRuntimePage(context.Context, dex.Hash, []byte, int) ([]dex.Block, []byte, error)
}
type conversion struct {
	Version           uint32
	Binding           runtimecheckpoint.Binding
	Cursor, HashState []byte
	State             state
	Report            ConversionReport
}

func appendDigest(previous []byte, rows []dex.Block) ([]byte, string, error) {
	h := sha256.New()
	if len(previous) > 0 {
		if err := h.(encoding.BinaryUnmarshaler).UnmarshalBinary(previous); err != nil {
			return nil, "", err
		}
	}
	for _, b := range rows {
		sum := dex.ObjectHash(b)
		h.Write(sum[:])
	}
	next, err := h.(encoding.BinaryMarshaler).MarshalBinary()
	return next, hex.EncodeToString(h.Sum(nil)), err
}
func Convert(ctx context.Context, dir string, binding runtimecheckpoint.Binding, inputPath string, manifest dex.Hash, db contracts.DEXStore, check func(context.Context) (runtimecheckpoint.Binding, error), progress func(string)) (ConversionReport, error) {
	var empty ConversionReport
	input, inputErr := runtimecheckpoint.ReadConversionInput(inputPath, binding, "dex")
	if inputErr != nil {
		return empty, inputErr
	}
	if check == nil || binding.ScopeHash != manifest.String() {
		return empty, runtimecheckpoint.ErrConflict
	}
	verify := func() error {
		actual, err := check(ctx)
		if err != nil {
			return err
		}
		if actual != binding {
			return runtimecheckpoint.ErrConflict
		}
		return nil
	}
	if err := verify(); err != nil {
		return empty, err
	}
	if _, err := os.Stat(dir); err == nil {
		return empty, errors.New("DEX runtime destination exists")
	} else if !os.IsNotExist(err) {
		return empty, err
	}
	source, ok := db.(pager)
	if !ok {
		return empty, errors.New("DEX conversion pager required")
	}
	stage := dir + ".conversion"
	stageBinding := binding
	stageBinding.StateVersion += 1000
	var files *runtimecheckpoint.Store
	var p conversion
	var err error
	if _, err = os.Stat(filepath.Join(stage, "checkpoint.current")); os.IsNotExist(err) {
		p = conversion{Version: 1, Binding: binding, State: state{Version: 1, Binding: binding, Manifest: manifest}, Report: ConversionReport{Version: 1, Binding: binding, FrozenAt: time.Now().UTC()}}
		raw, e := runtimecheckpoint.MarshalTyped(p)
		if e != nil {
			return empty, e
		}
		files, err = runtimecheckpoint.Create(stage, stageBinding, raw, nil, 64<<20)
	} else if err == nil {
		files, err = runtimecheckpoint.Open(stage, stageBinding, 64<<20)
		if err == nil {
			err = runtimecheckpoint.UnmarshalTyped(files.State(), &p)
		}
	}
	if err != nil {
		if files != nil {
			files.Close()
		}
		return empty, err
	}
	defer files.Close()
	p.Report.InputInventory, err = input.Persist(files, p.Report.InputInventory)
	if err != nil {
		return p.Report, err
	}
	if p.Version != 1 || p.Binding != binding || p.State.Manifest != manifest {
		return p.Report, runtimecheckpoint.ErrRecovery
	}
	if err = p.State.open(files); err != nil {
		return p.Report, err
	}
	save := func() error {
		refs, e := p.State.references()
		if e != nil {
			return e
		}
		raw, e := runtimecheckpoint.MarshalTyped(p)
		if e != nil {
			return e
		}
		return files.Checkpoint(raw, append(refs, p.Report.InputInventory.References()...))
	}
	if err = save(); err != nil {
		return p.Report, err
	}

	for !p.Report.Verified {
		if err = ctx.Err(); err != nil {
			return p.Report, err
		}
		rows, next, e := source.DEXRuntimePage(ctx, manifest, p.Cursor, 500)
		if e != nil {
			return p.Report, e
		}
		if len(rows) == 0 {
			break
		}
		if len(next) != 40 || bytes.Compare(next, p.Cursor) <= 0 {
			return p.Report, runtimecheckpoint.ErrRecovery
		}
		for _, b := range rows {
			if e = p.State.apply(b); e != nil {
				return p.Report, e
			}
			p.Report.Headers++
		}
		p.HashState, p.Report.InputDigest, err = appendDigest(p.HashState, rows)
		if err != nil {
			return p.Report, err
		}
		p.Cursor = append([]byte(nil), next...)
		if err = save(); err != nil {
			return p.Report, err
		}
		if progress != nil {
			progress(fmt.Sprintf("DEX runtime conversion headers=%d unfinalized=%d intervals=%d", p.Report.Headers, p.State.active.Len(), len(p.State.Coverage)))
		}
	}
	// A second complete traversal proves the input was frozen. Independently
	// reproduce the old sorted-height adjacency rule (including duplicate heights).
	var hashState []byte
	var digest string
	var count, active, previous, gap uint64
	havePrevious := false
	for after := []byte(nil); ; {
		rows, next, e := source.DEXRuntimePage(ctx, manifest, after, 500)
		if e != nil {
			return p.Report, e
		}
		if len(rows) == 0 {
			break
		}
		if len(next) != 40 || bytes.Compare(next, after) <= 0 {
			return p.Report, runtimecheckpoint.ErrRecovery
		}
		for _, b := range rows {
			count++
			if b.Canonical {
				if havePrevious && b.Number < previous {
					return p.Report, runtimecheckpoint.ErrConflict
				}
				if havePrevious && gap == 0 && b.Number > previous && b.Number-previous > 1 {
					gap = previous + 1
				}
				previous, havePrevious = b.Number, true
				if b.Finality != "finalized" {
					active++
					got, ok, e := p.State.active.Get(key(b))
					if e != nil {
						return p.Report, e
					}
					if !ok || got.Number != b.Number || got.Hash != b.Hash || got.Revision != b.Revision || !got.AvailableAt.Equal(b.AvailableAt) {
						return p.Report, runtimecheckpoint.ErrConflict
					}
				}
			}
		}
		hashState, digest, e = appendDigest(hashState, rows)
		if e != nil {
			return p.Report, e
		}
		after = next
	}
	if count == 0 {
		_, digest, err = appendDigest(nil, nil)
		if err != nil {
			return p.Report, err
		}
		if p.Report.InputDigest == "" {
			p.Report.InputDigest = digest
		}
	}
	if count != p.Report.Headers || digest != p.Report.InputDigest || gap != p.State.gap() || active != p.State.active.Len() {
		return p.Report, errors.New("DEX frozen coverage comparison failed")
	}
	p.Report.Verified = true
	p.Report.VerifiedAt = time.Now().UTC()
	p.Report.Unfinalized = active
	p.Report.FirstGap = gap
	p.State.Verified = true
	p.State.Conversion = p.Report
	if err = save(); err != nil {
		return p.Report, err
	}
	if err = verify(); err != nil {
		return p.Report, err
	}
	refs, err := p.State.references()
	if err != nil {
		return p.Report, err
	}
	ready := dir + ".ready"
	var target *runtimecheckpoint.Store
	if _, err = os.Stat(filepath.Join(ready, "checkpoint.current")); os.IsNotExist(err) {
		target, err = runtimecheckpoint.Create(ready, binding, []byte("unpublished conversion"), nil, 0)
	} else if err == nil {
		target, err = runtimecheckpoint.Open(ready, binding, 0)
	}
	if err != nil {
		return p.Report, err
	}
	defer target.Close()
	for _, ref := range refs {
		raw, e := files.ReadObject(ref)
		if e != nil {
			return p.Report, e
		}
		got, e := target.PutObject(raw)
		if e != nil {
			return p.Report, e
		}
		if got != ref {
			return p.Report, runtimecheckpoint.ErrConflict
		}
	}
	if err = p.State.open(target); err != nil {
		return p.Report, err
	}
	raw, err := runtimecheckpoint.MarshalTyped(p.State)
	if err != nil {
		return p.Report, err
	}
	if err = target.Checkpoint(raw, refs); err != nil {
		return p.Report, err
	}
	if err = p.State.validate(binding, manifest); err != nil {
		return p.Report, err
	}
	o := owner{state: p.State, files: target}
	if err = o.capacity(); err != nil {
		return p.Report, err
	}
	if err = target.SealConversion(); err != nil {
		return p.Report, err
	}
	if err = verify(); err != nil {
		return p.Report, err
	}
	if err = target.Close(); err != nil {
		return p.Report, err
	}
	if err = os.Rename(ready, dir); err != nil {
		return p.Report, err
	}
	parent, err := os.Open(filepath.Dir(dir))
	if err != nil {
		return p.Report, err
	}
	defer parent.Close()
	return p.Report, parent.Sync()
}

type Inspection struct {
	Files                 runtimecheckpoint.Inspection
	Conversion            ConversionReport
	FirstGap, Unfinalized uint64
}

func (s *Store) Inspect(ctx context.Context, exportDir string) (Inspection, error) {
	value, err := s.owner.call(ctx, func(o *owner) (any, error) {
		if len(o.inflight) > 0 || len(o.pending) > 0 {
			return nil, ErrBusy
		}
		if e := o.checkpoint(); e != nil {
			return nil, e
		}
		if exportDir != "" {
			if e := o.files.Export(exportDir); e != nil {
				return nil, e
			}
		}
		files, e := o.files.Inspect()
		if e != nil {
			return nil, e
		}
		return Inspection{files, o.state.Conversion, o.state.gap(), o.state.active.Len()}, nil
	})
	if err != nil {
		return Inspection{}, err
	}
	return value.(Inspection), nil
}
func (s *Store) Rollback(ctx context.Context, exportDir string) (Inspection, error) {
	if exportDir == "" {
		return Inspection{}, errors.New("DEX rollback requires current recovery export")
	}
	report, err := s.Inspect(ctx, exportDir)
	if err != nil {
		return report, err
	}
	// The legacy runner uses current database state. This explicit traversal is
	// permitted only during rollback, never as a restart/polling fallback.
	gap, err := s.DEXStore.DEXGapStart(ctx, s.owner.state.Manifest)
	if err != nil {
		return report, err
	}
	if gap != report.FirstGap {
		return report, errors.New("DEX current legacy coverage differs")
	}
	return report, nil
}
