package lst

import (
	"encoding/gob"
	"errors"
	"io"
	"reflect"
)

// Gob flattens pointers and omits zero values: *bool(false) or *uint64(0)
// otherwise comes back nil. Persist explicit presence alongside the raw binary
// batch so database retry preserves every NULL and every known zero exactly.
type pendingEnvelope struct {
	Version uint32
	Batch   Batch
	Present [][]int
}

func persistentRows(b *Batch) []reflect.Value {
	out := []reflect.Value{reflect.ValueOf(&b.Capture).Elem()}
	for _, group := range batchRows(*b) {
		for i := 0; i < group.rows.Len(); i++ {
			out = append(out, group.rows.Index(i))
		}
	}
	return out
}

func encodePersistent(w io.Writer, v any) error {
	b, ok := v.(Batch)
	if !ok {
		return gob.NewEncoder(w).Encode(v)
	}
	env := pendingEnvelope{Version: 1, Batch: b}
	for _, row := range persistentRows(&env.Batch) {
		fields := []int{}
		for i := 0; i < row.NumField(); i++ {
			if row.Field(i).Kind() == reflect.Pointer && !row.Field(i).IsNil() {
				fields = append(fields, i)
			}
		}
		env.Present = append(env.Present, fields)
	}
	return gob.NewEncoder(w).Encode(env)
}

func decodePersistent(r io.Reader, v any) error {
	b, ok := v.(*Batch)
	if !ok {
		return gob.NewDecoder(r).Decode(v)
	}
	var env pendingEnvelope
	if e := gob.NewDecoder(r).Decode(&env); e != nil {
		return e
	}
	if env.Version != 1 {
		return errors.New("pending_encoding_version")
	}
	rows := persistentRows(&env.Batch)
	if len(rows) != len(env.Present) {
		return errors.New("pending_presence_membership")
	}
	for i, row := range rows {
		previous := -1
		for _, field := range env.Present[i] {
			if field <= previous || field >= row.NumField() || row.Field(field).Kind() != reflect.Pointer {
				return errors.New("pending_presence_field")
			}
			previous = field
			if row.Field(field).IsNil() {
				row.Field(field).Set(reflect.New(row.Field(field).Type().Elem()))
			}
		}
	}
	if e := Validate(env.Batch); e != nil {
		return e
	}
	*b = env.Batch
	return nil
}
