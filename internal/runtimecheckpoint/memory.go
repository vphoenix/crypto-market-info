package runtimecheckpoint

import "reflect"

// EstimateTypedBytes conservatively counts a concrete object graph, including
// map capacity overhead and reachable strings/slices. It is an estimate, not
// process RSS. The owner reports it separately from runtime.MemStats and RSS;
// frozen payload files are accounted as disk bytes, not resident domain state.
func EstimateTypedBytes(value any) int64 {
	seen := map[uintptr]bool{}
	var visit func(reflect.Value, bool) int64
	visit = func(v reflect.Value, inline bool) int64 {
		if !v.IsValid() {
			return 0
		}
		var n int64
		if !inline {
			n = int64(v.Type().Size())
		}
		switch v.Kind() {
		case reflect.Interface:
			if !v.IsNil() {
				n += visit(v.Elem(), false)
			}
		case reflect.Pointer:
			if !v.IsNil() && !seen[v.Pointer()] {
				seen[v.Pointer()] = true
				n += visit(v.Elem(), false)
			}
		case reflect.String:
			n += int64(v.Len())
		case reflect.Slice:
			if !v.IsNil() && !seen[v.Pointer()] {
				seen[v.Pointer()] = true
				n += int64(v.Cap()) * int64(v.Type().Elem().Size())
				for i := 0; i < v.Len(); i++ {
					n += visit(v.Index(i), true)
				}
			}
		case reflect.Array:
			for i := 0; i < v.Len(); i++ {
				n += visit(v.Index(i), true)
			}
		case reflect.Struct:
			for i := 0; i < v.NumField(); i++ {
				if v.Type().Field(i).IsExported() {
					n += visit(v.Field(i), true)
				}
			}
		case reflect.Map:
			if !v.IsNil() && !seen[v.Pointer()] {
				seen[v.Pointer()] = true
				n += 64 + int64(v.Len())*(32+2*int64(v.Type().Key().Size()+v.Type().Elem().Size()))
				iter := v.MapRange()
				for iter.Next() {
					n += visit(iter.Key(), true) + visit(iter.Value(), true)
				}
			}
		}
		return n
	}
	return visit(reflect.ValueOf(value), false)
}
