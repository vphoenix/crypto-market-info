package across

import "context"

// LoadProgress contains operational timings only, never fact rows or credentials.
// A report can persist these before completion; collectors need no callback.
type LoadProgress struct {
	Phase         string `json:"phase"`
	Table         string `json:"table,omitempty"`
	Completed     uint64 `json:"completed,omitempty"`
	Total         uint64 `json:"total,omitempty"`
	Queries       uint64 `json:"queries,omitempty"`
	Payloads      uint64 `json:"unique_rpc_payloads_checked,omitempty"`
	SQLMicros     int64  `json:"sql_microseconds,omitempty"`
	ElapsedMicros int64  `json:"elapsed_microseconds"`
}

type loadProgressKey struct{}

func WithLoadProgress(ctx context.Context, progress func(LoadProgress)) context.Context {
	return context.WithValue(ctx, loadProgressKey{}, progress)
}

func RecordLoadProgress(ctx context.Context, progress LoadProgress) {
	if callback, ok := ctx.Value(loadProgressKey{}).(func(LoadProgress)); ok {
		callback(progress)
	}
}
