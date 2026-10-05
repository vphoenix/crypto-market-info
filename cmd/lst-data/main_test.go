package main

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func TestPauseLiveLogsRejectsInvalidOrNonWatchConfigBeforeCollection(t *testing.T) {
	for _, tc := range []struct{ command, value, want string }{
		{"watch", "not-a-bool", "invalid_pause_live_logs"},
		{"backfill", "true", "pause_live_logs_only_for_watch"},
		{"probe", "true", "pause_live_logs_only_for_watch"},
	} {
		t.Run(tc.command+"_"+tc.value, func(t *testing.T) {
			t.Setenv("LST_PAUSE_LIVE_LOGS", tc.value)
			dir := t.TempDir()
			err := run(context.Background(), []string{tc.command, "--manifest", "../../config/lst-lido-ethereum.json", "--state-dir", filepath.Join(dir, "state"), "--evidence", filepath.Join(dir, "evidence"), "--clickhouse", "127.0.0.1:1"})
			if err == nil || err.Error() != tc.want {
				t.Fatal("invalid collection mode reached database or source", err)
			}
		})
	}
}

func TestWatchLogSegmentRejectsInvalidConfigBeforeSourceOrDatabase(t *testing.T) {
	for _, tc := range []struct {
		name, pause, want string
		flags             []string
	}{
		{"negative_start", "false", "invalid value", []string{"--from-block", "-1"}},
		{"noninteger_start", "false", "invalid value", []string{"--from-block", "yesterday"}},
		{"end_without_start", "false", "watch_log_start_requires_unpaused_logs_and_no_to_block", []string{"--to-block", "12"}},
		{"bounded_watch", "false", "watch_log_start_requires_unpaused_logs_and_no_to_block", []string{"--from-block", "10", "--to-block", "12"}},
		{"paused_segment", "true", "watch_log_start_requires_unpaused_logs_and_no_to_block", []string{"--from-block", "10"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("LST_PAUSE_LIVE_LOGS", tc.pause)
			dir := t.TempDir()
			args := []string{"watch", "--manifest", "../../config/lst-lido-ethereum.json", "--state-dir", filepath.Join(dir, "state"), "--evidence", filepath.Join(dir, "evidence"), "--clickhouse", "127.0.0.1:1"}
			err := run(context.Background(), append(args, tc.flags...))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatal("invalid segment configuration reached source or database", err)
			}
		})
	}
}

func TestLogRateRejectsInvalidValuesBeforeSourceOrDatabase(t *testing.T) {
	for _, value := range []string{"0", "-1", "31", "unlimited"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("LST_PAUSE_LIVE_LOGS", "false")
			t.Setenv("LST_LOG_REQUESTS_PER_5_MINUTE", value)
			dir := t.TempDir()
			err := run(context.Background(), []string{"watch", "--manifest", "../../config/lst-lido-ethereum.json", "--state-dir", filepath.Join(dir, "state"), "--evidence", filepath.Join(dir, "evidence"), "--clickhouse", "127.0.0.1:1"})
			if err == nil || err.Error() != "invalid_log_requests_per_five_minutes" {
				t.Fatal(err)
			}
		})
	}
}
