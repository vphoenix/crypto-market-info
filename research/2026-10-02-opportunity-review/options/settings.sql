SELECT name,value FROM system.settings WHERE name IN ('readonly','max_threads','max_execution_time') FORMAT JSONEachRow
