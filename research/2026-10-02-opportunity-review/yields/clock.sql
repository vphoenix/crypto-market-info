SELECT now64(3,'UTC') AS query_time, version() AS version
SETTINGS readonly=1,max_threads=2,max_execution_time=60,output_format_json_quote_decimals=1
FORMAT JSON
