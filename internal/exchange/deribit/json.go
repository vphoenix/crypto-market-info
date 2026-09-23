// Package deribit implements strict public-protocol decoding and public REST/WS
// adapters. Fixed contract selection and sampling live in optionslive.
package deribit

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"strings"
	"unicode"
)

const MaxMessageBytes = 8 << 20

// Reject duplicate keys before unmarshalling: otherwise a repeated identity or
// timestamp can silently replace the first value. Unknown extension fields are
// allowed; known fields are decoded into strict concrete types.
func decode(data []byte, out any) error {
	if len(data) == 0 || len(data) > MaxMessageBytes {
		return fmt.Errorf("JSON payload outside size budget")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	canonical := make(map[string]string)
	jsonFieldNames(reflect.TypeOf(out), canonical, make(map[reflect.Type]bool))
	if err := jsonValue(d, 0, canonical); err != nil {
		return err
	}
	if _, err := d.Token(); err != io.EOF {
		return fmt.Errorf("trailing JSON data")
	}
	return json.Unmarshal(data, out)
}
func jsonValue(d *json.Decoder, depth int, canonical map[string]string) error {
	if depth > 32 {
		return fmt.Errorf("JSON nesting budget exceeded")
	}
	t, err := d.Token()
	if err != nil {
		return err
	}
	if delim, ok := t.(json.Delim); ok {
		switch delim {
		case '{':
			seen := make(map[string]bool)
			for d.More() {
				k, err := d.Token()
				if err != nil {
					return err
				}
				s, ok := k.(string)
				folded := foldJSONKey(s)
				if !ok || seen[folded] {
					return fmt.Errorf("duplicate/invalid JSON key")
				}
				if want, known := canonical[folded]; known && s != want {
					return fmt.Errorf("noncanonical JSON key %q", s)
				}
				seen[folded] = true
				if err := jsonValue(d, depth+1, canonical); err != nil {
					return err
				}
			}
			end, err := d.Token()
			if err != nil || end != json.Delim('}') {
				return fmt.Errorf("invalid object")
			}
		case '[':
			for d.More() {
				if err := jsonValue(d, depth+1, canonical); err != nil {
					return err
				}
			}
			end, err := d.Token()
			if err != nil || end != json.Delim(']') {
				return fmt.Errorf("invalid array")
			}
		default:
			return fmt.Errorf("unexpected JSON delimiter")
		}
	}
	return nil
}

// encoding/json matches keys case-insensitively, including Unicode simple-fold
// aliases (for example the Kelvin sign). Normalize the complete fold cycle.
func foldJSONKey(s string) string {
	return strings.Map(func(r rune) rune {
		lowest := r
		for next := unicode.SimpleFold(r); next != r; next = unicode.SimpleFold(next) {
			if next < lowest {
				lowest = next
			}
		}
		return lowest
	}, s)
}
func jsonFieldNames(t reflect.Type, names map[string]string, seen map[reflect.Type]bool) {
	if t == nil || seen[t] {
		return
	}
	seen[t] = true
	switch t.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Array:
		jsonFieldNames(t.Elem(), names, seen)
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			f := t.Field(i)
			if f.PkgPath != "" {
				continue
			}
			name := strings.Split(f.Tag.Get("json"), ",")[0]
			if name == "-" {
				continue
			}
			if name == "" {
				name = f.Name
			}
			names[foldJSONKey(name)] = name
			jsonFieldNames(f.Type, names, seen)
		}
	}
}
