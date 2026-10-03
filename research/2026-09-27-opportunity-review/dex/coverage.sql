SELECT finality,capture_mode,quote_coverage,canonical,committed,
        count() AS blocks, min(block_time) AS first_utc,max(block_time) AS latest_utc,
        min(block_number) AS first_height,max(block_number) AS last_height
        FROM dex_block FINAL GROUP BY finality,capture_mode,quote_coverage,canonical,committed
        ORDER BY finality,capture_mode,quote_coverage FORMAT JSON
