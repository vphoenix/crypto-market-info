SELECT version() AS version,now('UTC') AS now_utc,(SELECT value FROM system.settings WHERE name='readonly') AS readonly
