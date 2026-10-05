package clickhouse

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/vphoenix/crypto-market-info/internal/options"
)

//go:embed options_catalog_schema.sql
var optionsCatalogDDL string

func OptionsCatalogSchemaStatements(database string) ([]string, error) {
	if !identifierPattern.MatchString(database) {
		return nil, fmt.Errorf("invalid database")
	}
	ss := regexp.MustCompile(`(?s)CREATE TABLE IF NOT EXISTS (options_\w+).*?;`).FindAllString(optionsCatalogDDL, -1)
	if len(ss) != 5 {
		return nil, fmt.Errorf("expected five catalog tables")
	}
	for n, s := range ss {
		ss[n] = regexp.MustCompile(`CREATE TABLE IF NOT EXISTS (options_\w+)`).ReplaceAllString(s, "CREATE TABLE IF NOT EXISTS `"+database+"`.`$1`")
	}
	return ss, nil
}
func (c *Client) InitOptionsCatalogSchema(ctx context.Context) error {
	if c.readOnly {
		return fmt.Errorf("read only")
	}
	if err := c.InitOptionsLiveSchema(ctx); err != nil {
		return err
	}
	ss, err := OptionsCatalogSchemaStatements(c.database)
	if err != nil {
		return err
	}
	for _, s := range ss {
		if err = c.conn.Exec(ctx, s); err != nil {
			return err
		}
	}
	return nil
}

type catalogRow interface {
	Validate() error
	Hash() string
}

// Per-identity cancellable locks do not hold the global derivative lock across
// network retries. The owner keeps observation IDs stable on ambiguous writes.
func (c *Client) catalogLock(ctx context.Context, key string) (func(), error) {
	c.catalogLocksOnce.Do(func() {
		for i := range c.catalogLocks {
			c.catalogLocks[i] = make(chan struct{}, 1)
			c.catalogMinuteLocks[i] = make(chan struct{}, 1)
		}

		c.catalogPlanLock = make(chan struct{}, 1)
	})
	hash := uint32(2166136261)
	for _, b := range []byte(key) {
		hash = (hash ^ uint32(b)) * 16777619
	}
	ch := c.catalogLocks[hash%uint32(len(c.catalogLocks))]
	if strings.HasPrefix(key, "minute:") {
		ch = c.catalogMinuteLocks[hash%uint32(len(c.catalogMinuteLocks))]
	}
	if key == "plan-chain" {
		ch = c.catalogPlanLock
	}
	select {
	case ch <- struct{}{}:
		return func() { <-ch }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func writeCatalogRow[T catalogRow](ctx context.Context, c *Client, table string, row T, id uuid.UUID) error {
	if c.readOnly {
		return fmt.Errorf("read only")
	}
	if err := row.Validate(); err != nil {
		return err
	}
	unlock, err := c.catalogLock(ctx, table+id.String())
	if err != nil {
		return err
	}
	defer unlock()
	old, err := keeperRead[T](ctx, c, table, "WHERE observation_id=?", id)
	if err != nil {
		return err
	}
	if len(old) > 0 {
		if len(old) != 1 || old[0].Hash() != row.Hash() || reflect.ValueOf(old[0]).FieldByName("RowHash").String() != old[0].Hash() {
			return fmt.Errorf("immutable observation conflict")
		}
		return nil
	}
	v := reflect.ValueOf(&row).Elem()
	v.FieldByName("RowHash").SetString(row.Hash())
	return c.retryWrite(ctx, func(ctx context.Context) error {
		return c.dexInsert(ctx, table, keeperColumns(reflect.TypeOf(row)), keeperValues([]T{row}))
	})
}
func (c *Client) WriteOptionsCatalog(ctx context.Context, o options.CatalogObservation) error {
	return writeCatalogRow(ctx, c, "options_catalog_scope_observation", o, o.ID)
}
func (c *Client) WriteOptionsLifecycle(ctx context.Context, o options.LifecycleObservation) error {
	return c.WriteOptionsLifecycles(ctx, []options.LifecycleObservation{o})
}

// Each observation retains its immutable identity across a batch retry. Validate
// the entire input and all existing identities before writing any new rows.
func (c *Client) WriteOptionsLifecycles(ctx context.Context, input []options.LifecycleObservation) error {
	if c.readOnly {
		return fmt.Errorf("read only")
	}
	if len(input) == 0 {
		return nil
	}
	wanted := make(map[uuid.UUID]options.LifecycleObservation, len(input))
	ids := make([]uuid.UUID, 0, len(input))
	for _, o := range input {
		if err := o.Validate(); err != nil {
			return err
		}
		if old, ok := wanted[o.ID]; ok {
			if old.Hash() != o.Hash() {
				return fmt.Errorf("immutable lifecycle batch conflict")
			}
			continue
		}
		o.RowHash = o.Hash()
		wanted[o.ID] = o
		ids = append(ids, o.ID)
	}
	unlock, err := c.catalogLock(ctx, "lifecycle-batch")
	if err != nil {
		return err
	}
	defer unlock()
	old, err := keeperRead[options.LifecycleObservation](ctx, c, "options_lifecycle_observation", "WHERE observation_id IN (?)", ids)
	if err != nil {
		return err
	}
	seen := map[uuid.UUID]bool{}
	for _, o := range old {
		w, ok := wanted[o.ID]
		if !ok || seen[o.ID] || o.Validate() != nil || o.RowHash != o.Hash() || o.Hash() != w.Hash() {
			return fmt.Errorf("immutable lifecycle observation conflict")
		}
		seen[o.ID] = true
	}
	var fresh []options.LifecycleObservation
	for _, id := range ids {
		if !seen[id] {
			fresh = append(fresh, wanted[id])
		}
	}
	if len(fresh) == 0 {
		return nil
	}
	err = c.retryWrite(ctx, func(ctx context.Context) error {
		return c.dexInsert(ctx, "options_lifecycle_observation", keeperColumns(reflect.TypeOf(options.LifecycleObservation{})), keeperValues(fresh))
	})
	if err != nil {
		return err
	}
	return c.derivativeInserted("lifecycle_observations")
}
func loadCatalogRows[T catalogRow](ctx context.Context, c *Client, table string, ids []uuid.UUID) (map[uuid.UUID]T, error) {
	out := map[uuid.UUID]T{}
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := keeperRead[T](ctx, c, table, "WHERE observation_id IN (?)", ids)
	if err != nil {
		return out, err
	}
	for _, r := range rows {
		v := reflect.ValueOf(r)
		id := v.FieldByName("ID").Interface().(uuid.UUID)
		if err = r.Validate(); err != nil {
			return nil, err
		}
		if v.FieldByName("RowHash").String() != r.Hash() {
			return nil, fmt.Errorf("observation digest mismatch")
		}
		if _, exists := out[id]; exists {
			return nil, fmt.Errorf("duplicate observation")
		}
		out[id] = r
	}
	if len(out) != len(ids) {
		return nil, fmt.Errorf("missing catalog evidence")
	}
	return out, nil
}
func (c *Client) LoadOptionsPlan(ctx context.Context, id uuid.UUID) (options.CollectionPlan, error) {
	rows, err := keeperRead[options.CollectionPlan](ctx, c, "options_collection_plan", "WHERE plan_id=?", id)
	if err != nil {
		return options.CollectionPlan{}, err
	}
	if len(rows) == 0 {
		return options.CollectionPlan{}, ErrNotFound
	}
	if len(rows) != 1 {
		return options.CollectionPlan{}, fmt.Errorf("ambiguous plan")
	}
	p := rows[0]
	if err = p.Validate(); err != nil {
		return p, err
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
func (c *Client) optionsPlanAt(ctx context.Context, at time.Time) (options.CollectionPlan, error) {
	where := "ORDER BY effective_minute DESC LIMIT 1"
	var args []any
	if !at.IsZero() {
		where = "WHERE effective_minute<=? " + where
		args = append(args, at.UTC())
	}
	rows, err := keeperRead[options.CollectionPlan](ctx, c, "options_collection_plan", where, args...)
	if err != nil {
		return options.CollectionPlan{}, err
	}
	if len(rows) == 0 {
		return options.CollectionPlan{}, ErrNotFound
	}
	p, err := c.LoadOptionsPlan(ctx, rows[0].ID)
	if err != nil {
		return p, err
	}
	var count uint64
	if err = c.conn.QueryRow(ctx, "SELECT count() FROM "+c.table("options_collection_plan")+" FINAL WHERE effective_minute=?", p.EffectiveMinute).Scan(&count); err != nil {
		return p, err
	}
	if count != 1 {
		return p, fmt.Errorf("ambiguous plan boundary")
	}
	if p.PreviousID != uuid.Nil {
		previous, err := c.LoadOptionsPlan(ctx, p.PreviousID)
		if err != nil {
			return p, err
		}
		if err = p.ValidateSuccessor(&previous); err != nil {
			return p, err
		}
	}
	return p, nil
}
func (c *Client) WriteOptionsPlan(ctx context.Context, p options.CollectionPlan) error {
	if c.readOnly {
		return fmt.Errorf("read only")
	}
	if err := p.Validate(); err != nil {
		return err
	}
	unlock, err := c.catalogLock(ctx, "plan-chain")
	if err != nil {
		return err
	}
	defer unlock()
	old, err := c.LoadOptionsPlan(ctx, p.ID)
	if err == nil {
		if old.Hash() != p.Hash() {
			return fmt.Errorf("immutable plan conflict")
		}
		return nil
	}
	if !errors.Is(err, ErrNotFound) {
		return err
	}
	previous, err := c.LatestOptionsPlan(ctx)
	var prev *options.CollectionPlan
	if err == nil {
		prev = &previous
	} else if !errors.Is(err, ErrNotFound) {
		return err
	}
	if err = p.ValidateSuccessor(prev); err != nil {
		return err
	}
	seen := map[uuid.UUID]bool{}
	for _, id := range p.RunIDs {
		seen[id] = true
	}
	for id := range seen {
		r, err := c.LoadOptionsRun(ctx, id)
		if err != nil {
			return err
		}
		if err = p.ValidateRun(r, p.EffectiveMinute); err != nil {
			return err
		}
	}
	ids := slices.Clone(p.ObservationIDs)
	slices.SortFunc(ids, func(a, b uuid.UUID) int { return slices.Compare(a[:], b[:]) })
	ids = slices.Compact(ids)
	proofs, err := loadCatalogRows[options.CatalogObservation](ctx, c, "options_catalog_scope_observation", ids)
	if err != nil {
		return err
	}
	for n, id := range p.InstrumentIDs {
		backed := false
		for _, o := range proofs {
			if o.Status != "complete" || o.ObservedAt.After(p.CreatedAt) {
				continue
			}
			for k, x := range o.InstrumentIDs {
				if x == id && o.Definitions[k] == p.Definitions[n] {
					backed = true
				}
			}
		}
		if !backed {
			return fmt.Errorf("plan member without catalog definition evidence")
		}
	}
	p.RowHash = p.Hash()
	return c.retryWrite(ctx, func(ctx context.Context) error {
		return c.dexInsert(ctx, "options_collection_plan", keeperColumns(reflect.TypeOf(p)), keeperValues([]options.CollectionPlan{p}))
	})
}

func (c *Client) OptionsStoreIdentity() string { return c.storeIdentity + ":" + c.database }
func (c *Client) OptionsNotFound() error       { return ErrNotFound }
