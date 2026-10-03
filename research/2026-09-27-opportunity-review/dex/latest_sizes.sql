SELECT route_id,toString(requested_amount_raw) AS amount_atoms,
        toString(amount_out_raw) AS out_atoms,
        toString(toInt256(amount_out_raw)-toInt256(requested_amount_raw)) AS gross_atoms,
        status,reason,block_number,block_time,hex(block_hash) AS block_hash
        FROM dex_route_quote FINAL WHERE batch_id=(SELECT batch_id FROM dex_block FINAL
        WHERE canonical AND committed AND finality='finalized' AND capture_mode='live'
        AND quote_coverage='complete' ORDER BY block_number DESC LIMIT 1)
        AND quote_role='strategy' ORDER BY route_id,requested_amount_raw FORMAT JSON
