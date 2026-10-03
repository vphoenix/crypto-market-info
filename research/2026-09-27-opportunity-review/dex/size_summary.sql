SELECT q.route_id,toString(q.requested_amount_raw) AS amount_atoms,
        count() AS observations,countIf(q.amount_out_raw>q.requested_amount_raw) AS positive,
        toString(max(toInt256(q.amount_out_raw)-toInt256(q.requested_amount_raw))) AS best_gross_atoms,
        argMax(q.block_time,toInt256(q.amount_out_raw)-toInt256(q.requested_amount_raw)) AS best_utc,
        min(q.block_time) AS first_utc,max(q.block_time) AS last_utc
        FROM dex_route_quote AS q FINAL INNER JOIN
        (SELECT batch_id,block_hash,manifest_hash FROM dex_block FINAL
         WHERE canonical AND committed AND finality='finalized' AND capture_mode='live'
         AND quote_coverage='complete' AND actual_quotes=expected_quotes) AS b
        ON q.batch_id=b.batch_id AND q.block_hash=b.block_hash AND q.manifest_hash=b.manifest_hash
        WHERE q.quote_role='strategy' AND q.status='ok' AND q.amount_in_raw=q.requested_amount_raw
        GROUP BY q.route_id,q.requested_amount_raw ORDER BY q.route_id,q.requested_amount_raw FORMAT JSON
