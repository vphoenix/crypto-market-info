package mysql

import (
	"encoding/hex"
	"fmt"
	"math"
	"time"
)

// Source SQL expressions are never evaluated or translated at runtime. These
// are the existing option model's reviewed compact representations.
func expandAliases(t Table, r record) error {
	for _, f := range optionCompactFields(t.Name) {
		names := f.physicalNames()
		v, ok := r[names[0]]
		if !ok {
			return fmt.Errorf("compact physical column missing")
		}
		if f.kind == "hex" {
			s, ok := v.(string)
			if !ok || len(s) != 32 {
				return fmt.Errorf("compact hash width")
			}
			r[f.name] = hex.EncodeToString([]byte(s))
			continue
		}
		if f.kind == "hexarray" {
			arr, ok := v.([]any)
			if !ok {
				return fmt.Errorf("compact hash array")
			}
			out := make([]any, len(arr))
			for i, x := range arr {
				s, ok := x.(string)
				if !ok || len(s) != 32 {
					return fmt.Errorf("compact hash width")
				}
				out[i] = hex.EncodeToString([]byte(s))
			}
			r[f.name] = out
			continue
		}
		offsets, ok := v.([]any)
		if !ok {
			return fmt.Errorf("compact offsets type")
		}
		values, ok := r[names[1]].([]any)
		if !ok || len(offsets) != len(values) || len(offsets) == 0 || len(offsets) > 60 {
			return fmt.Errorf("compact array lengths")
		}
		out := make([]any, 60)
		for i, o := range offsets {
			start, ok := o.(uint8)
			if !ok || start > 59 || (i == 0 && start != 0) {
				return fmt.Errorf("compact offset")
			}
			end := uint8(60)
			if i+1 < len(offsets) {
				var ok bool
				end, ok = offsets[i+1].(uint8)
				if !ok || end <= start || end > 59 {
					return fmt.Errorf("compact offsets not increasing")
				}
			}
			x := values[i]
			if x != nil && f.kind == "time" {
				base := r["minute_time"].(time.Time).UnixMicro()
				offset, ok := x.(int64)
				if !ok || (offset > 0 && base > math.MaxInt64-offset) || (offset < 0 && base < math.MinInt64-offset) {
					return fmt.Errorf("compact time overflow")
				}
				x = time.UnixMicro(base + offset).UTC()
			}
			if x != nil && f.kind == "rlehex" {
				s, ok := x.(string)
				if !ok || len(s) != 32 {
					return fmt.Errorf("compact nullable hash width")
				}
				x = hex.EncodeToString([]byte(s))
			}
			for second := start; second < end; second++ {
				out[second] = x
			}
		}
		r[f.name] = out
	}
	return nil
}
