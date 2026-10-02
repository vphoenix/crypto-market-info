SELECT now64(3, 'UTC') AS server_time, value AS readonly FROM system.settings WHERE name='readonly'
