package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/options"
	"github.com/vphoenix/crypto-market-info/internal/storage/contracts"
	"reflect"
	"slices"
	"time"
)

type catalogRow interface {
	Validate() error
	Hash() string
}

func writeCatalogRow[T catalogRow](ctx context.Context, c *Client, name string, row T, id uuid.UUID) error {
	if e := row.Validate(); e != nil {
		return e
	}
	reflect.ValueOf(&row).Elem().FieldByName("RowHash").SetString(row.Hash())
	return c.transaction(ctx, func(tx *sql.Tx) error { return putModels(ctx, c, tx, name, []T{row}) })
}
func (c *Client) WriteOptionsCatalog(ctx context.Context, o options.CatalogObservation) error {
	return writeCatalogRow(ctx, c, "options_catalog_scope_observation", o, o.ID)
}
func (c *Client) WriteOptionsLifecycle(ctx context.Context, o options.LifecycleObservation) error {
	return c.WriteOptionsLifecycles(ctx, []options.LifecycleObservation{o})
}
func (c *Client) WriteOptionsLifecycles(ctx context.Context, input []options.LifecycleObservation) error {
	rows := make([]options.LifecycleObservation, len(input))
	copy(rows, input)
	for n := range rows {
		if e := rows[n].Validate(); e != nil {
			return e
		}
		rows[n].RowHash = rows[n].Hash()
	}
	return c.transaction(ctx, func(tx *sql.Tx) error { return putModelsBulk(ctx, c, tx, "options_lifecycle_observation", rows) })
}
func loadCatalogRows[T catalogRow](ctx context.Context, c *Client, name string, ids []uuid.UUID) (map[uuid.UUID]T, error) {
	if len(ids) > 2048 {
		return nil, fmt.Errorf("evidence read quota exceeded")
	}
	out := map[uuid.UUID]T{}
	missing := []uuid.UUID{}
	seen := map[uuid.UUID]bool{}
	for _, id := range ids {
		if seen[id] {
			return nil, contracts.ErrConflict
		}
		seen[id] = true
		if v, ok := cachedValue[T](c, name+"/"+id.String()); ok {
			out[id] = v
		} else {
			missing = append(missing, id)
		}
	}
	if len(missing) == 0 {
		return out, nil
	}
	rows, e := loadCatalogRowsUncached[T](ctx, c, name, missing)
	if e != nil {
		return nil, e
	}
	for id, row := range rows {
		rememberValue(c, name+"/"+id.String(), row)
		out[id] = row
	}
	return out, nil
}
func loadCatalogRowsUncached[T catalogRow](ctx context.Context, c *Client, name string, ids []uuid.UUID) (map[uuid.UUID]T, error) {
	out := map[uuid.UUID]T{}
	if len(ids) == 0 {
		return out, nil
	}
	if len(ids) > 2048 {
		return nil, fmt.Errorf("evidence read quota exceeded")
	}
	args := make([]any, len(ids))
	for n := range ids {
		args[n] = ids[n][:]
	}
	rows, e := readModels[T](ctx, c, c.db, name, "WHERE observation_id IN ("+placeholders(len(ids))+")", args...)
	if e != nil {
		return nil, e
	}
	for _, r := range rows {
		v := reflect.ValueOf(r)
		id := v.FieldByName("ID").Interface().(uuid.UUID)
		if e = r.Validate(); e != nil {
			return nil, e
		}
		if v.FieldByName("RowHash").String() != r.Hash() {
			return nil, fmt.Errorf("evidence digest mismatch")
		}
		if _, ok := out[id]; ok {
			return nil, contracts.ErrConflict
		}
		out[id] = r
	}
	if len(out) != len(ids) {
		return nil, fmt.Errorf("missing catalog evidence")
	}
	return out, nil
}
func (c *Client) LoadOptionsPlan(ctx context.Context, id uuid.UUID) (options.CollectionPlan, error) {
	return cachedMetadata(ctx, c, "plan/"+id.String(), func(ctx context.Context) (options.CollectionPlan, error) { return c.loadOptionsPlan(ctx, id) })
}
func (c *Client) loadOptionsPlan(ctx context.Context, id uuid.UUID) (options.CollectionPlan, error) {
	rows, e := readModels[options.CollectionPlan](ctx, c, c.db, "options_collection_plan", "WHERE plan_id=?", id[:])
	if e != nil {
		return options.CollectionPlan{}, e
	}
	if len(rows) == 0 {
		return options.CollectionPlan{}, contracts.ErrNotFound
	}
	if len(rows) != 1 {
		return options.CollectionPlan{}, contracts.ErrConflict
	}
	p := rows[0]
	if e = p.Validate(); e != nil {
		return p, e
	}
	if p.RowHash != p.Hash() {
		return p, fmt.Errorf("plan digest mismatch")
	}
	return p, nil
}
func (c *Client) LatestOptionsPlan(ctx context.Context) (options.CollectionPlan, error) {
	return c.optionsPlanAt(ctx, time.Time{})
}
func (c *Client) OptionsPlanAt(ctx context.Context, at time.Time) (options.CollectionPlan, error) {
	return c.optionsPlanAt(ctx, at)
}

// Sort only the identifier before loading the selected plan's large arrays.
// Sorting complete historical plan rows can exceed the startup read timeout.
func (c *Client) latestOptionsPlanID(ctx context.Context, q executor, at time.Time, lock bool) (uuid.UUID, error) {
	where := "ORDER BY effective_minute DESC LIMIT 1"
	var args []any
	if !at.IsZero() {
		where = "WHERE effective_minute<=? " + where
		args = append(args, at.UTC())
	}
	if lock {
		where += " FOR UPDATE"
	}
	var raw []byte
	e := q.QueryRowContext(ctx, "SELECT plan_id FROM "+c.table("options_collection_plan")+" "+where, args...).Scan(&raw)
	if errors.Is(e, sql.ErrNoRows) {
		return uuid.Nil, contracts.ErrNotFound
	}
	if e != nil {
		return uuid.Nil, e
	}
	return uuid.FromBytes(raw)
}
func (c *Client) optionsPlanAt(ctx context.Context, at time.Time) (options.CollectionPlan, error) {
	id, e := c.latestOptionsPlanID(ctx, c.db, at, false)
	if e != nil {
		return options.CollectionPlan{}, e
	}
	p, e := c.LoadOptionsPlan(ctx, id)
	if e != nil {
		return p, e
	}
	var count uint64
	if e = c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+c.table("options_collection_plan")+" WHERE effective_minute=?", p.EffectiveMinute).Scan(&count); e != nil {
		return p, e
	}
	if count != 1 {
		return p, fmt.Errorf("ambiguous plan boundary")
	}
	if p.PreviousID != uuid.Nil {
		old, e := c.LoadOptionsPlan(ctx, p.PreviousID)
		if e != nil {
			return p, e
		}
		if e = p.ValidateSuccessor(&old); e != nil {
			return p, e
		}
	}
	return p, nil
}
func (c *Client) validateOptionsPlanReferences(ctx context.Context, p options.CollectionPlan) error {
	seen := map[uuid.UUID]bool{}
	runIDs := []uuid.UUID{}
	for _, id := range p.RunIDs {
		if seen[id] {
			continue
		}
		seen[id] = true
		runIDs = append(runIDs, id)
	}
	runs, err := c.loadOptionsRunGroup(ctx, runIDs)
	if err != nil {
		return err
	}
	for _, id := range runIDs {
		if e := p.ValidateRun(runs[id], p.EffectiveMinute); e != nil {
			return e
		}
	}
	ids := slices.Clone(p.ObservationIDs)
	slices.SortFunc(ids, func(a, b uuid.UUID) int { return slices.Compare(a[:], b[:]) })
	ids = slices.Compact(ids)
	proofs, e := loadCatalogRows[options.CatalogObservation](ctx, c, "options_catalog_scope_observation", ids)
	if e != nil {
		return e
	}
	covered := map[struct {
		id         uint32
		definition string
	}]bool{}
	for _, observation := range proofs {
		if observation.Status != "complete" || observation.ObservedAt.After(p.CreatedAt) {
			continue
		}
		for n, id := range observation.InstrumentIDs {
			covered[struct {
				id         uint32
				definition string
			}{id, observation.Definitions[n]}] = true
		}
	}
	for n, id := range p.InstrumentIDs {
		if !covered[struct {
			id         uint32
			definition string
		}{id, p.Definitions[n]}] {
			return fmt.Errorf("plan member without catalog evidence")
		}
	}
	return nil
}

func (c *Client) WriteOptionsPlan(ctx context.Context, p options.CollectionPlan) error {
	if e := p.Validate(); e != nil {
		return e
	}
	// A durable retry keeps the original fast path. New publications prepare
	// immutable run/spec/catalog references without holding the generation lock
	// or a transaction connection from this same two-connection pool.
	existing, err := readModels[options.CollectionPlan](ctx, c, c.db, "options_collection_plan", "WHERE plan_id=?", p.ID[:])
	if err != nil {
		return err
	}
	referencesPrepared := len(existing) == 0
	if referencesPrepared {
		if err = c.validateOptionsPlanReferences(ctx, p); err != nil {
			return err
		}
	}
	return c.transaction(ctx, func(tx *sql.Tx) error {
		old, e := readModels[options.CollectionPlan](ctx, c, tx, "options_collection_plan", "WHERE plan_id=? FOR UPDATE", p.ID[:])
		if e != nil {
			return e
		}
		if len(old) > 0 {
			if len(old) != 1 || old[0].Hash() != p.Hash() || old[0].RowHash != p.Hash() {
				return contracts.ErrConflict
			}
			return nil
		}
		if !referencesPrepared {
			return fmt.Errorf("plan disappeared during publication retry")
		}
		var prev *options.CollectionPlan
		id, e := c.latestOptionsPlanID(ctx, tx, time.Time{}, true)
		if e != nil && !errors.Is(e, contracts.ErrNotFound) {
			return e
		}
		if e == nil {
			previous, e := readModels[options.CollectionPlan](ctx, c, tx, "options_collection_plan", "WHERE plan_id=? FOR UPDATE", id[:])
			if e != nil {
				return e
			}
			if len(previous) != 1 {
				return contracts.ErrConflict
			}
			prev = &previous[0]
		}
		if e = p.ValidateSuccessor(prev); e != nil {
			return e
		}
		p.RowHash = p.Hash()
		return putModels(ctx, c, tx, "options_collection_plan", []options.CollectionPlan{p})
	})
}
func (c *Client) OptionsStoreIdentity() string { return "mysql:" + c.database }
func (c *Client) OptionsNotFound() error       { return contracts.ErrNotFound }
func (c *Client) HasOptionsCatalog(ctx context.Context) (bool, error) {
	var n int
	e := c.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=? AND table_name='options_collection_plan'", c.database).Scan(&n)
	return n == 1, e
}

type CatalogPlanSnapshot = contracts.CatalogPlanSnapshot

func (c *Client) LoadOptionsPlanSnapshot(ctx context.Context, at time.Time) (CatalogPlanSnapshot, error) {
	var out CatalogPlanSnapshot
	out.MinuteTime = at.UTC().Truncate(time.Minute)
	p, e := c.OptionsPlanAt(ctx, out.MinuteTime)
	if e != nil {
		return out, e
	}
	out.Plan = p
	out.ExpectedBooks = len(p.InstrumentIDs)
	seen := map[uuid.UUID]bool{}
	for _, id := range p.RunIDs {
		if seen[id] {
			continue
		}
		seen[id] = true
		v, e := c.LoadOptionsMinute(ctx, id, out.MinuteTime)
		if e != nil {
			if !errors.Is(e, contracts.ErrNotFound) {
				return out, e
			}
			out.MissingRuns = append(out.MissingRuns, id)
			for _, owner := range p.RunIDs {
				if owner == id {
					out.MissingBooks++
				}
			}
			continue
		}
		out.Runs = append(out.Runs, v)
		for _, b := range v.Books {
			if b.Quality[at.Second()].ReplayValid {
				out.ValidBooks++
			}
		}
	}
	return out, nil
}
