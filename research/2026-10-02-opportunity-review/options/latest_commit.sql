SELECT * FROM options_live_minute_commit FINAL WHERE minute_time >= '2026-10-01 00:00:00' ORDER BY minute_time DESC LIMIT 1 FORMAT JSONEachRow
