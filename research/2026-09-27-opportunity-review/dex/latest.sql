SELECT block_number,hex(block_hash) AS block_hash,block_time,received_at,available_at,
        capture_mode,canonical,finality,quote_coverage,expected_quotes,actual_quotes,committed
        FROM dex_block FINAL ORDER BY block_number DESC LIMIT 12 FORMAT JSON
