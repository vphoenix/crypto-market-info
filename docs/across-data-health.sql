-- Read-only for the live database. The independent history database was deleted
-- on 2026-10-03 at the user's request; do not recreate it for health checks.
-- FINAL must precede canonical filtering. Counts below are captures, not orders.
SELECT chain_id,capture_kind,status,finality,count() AS captures,
       max(available_at) AS last_available_utc,
       countIf(canonical AND committed AND finality!='finalized' AND to_block IS NOT NULL) AS anchored_finality_backlog
FROM crypto_market_info_across.across_capture FINAL
GROUP BY chain_id,capture_kind,status,finality ORDER BY chain_id,capture_kind,status,finality;

-- Fixed-window union and continuous raw/decoded ranges are emitted by report
-- coverage.csv/summary; never sum overlapping capture lengths here.
SELECT chain_id,from_block,to_block,status,unknown_event_count,reason,finality
FROM crypto_market_info_across.across_capture FINAL
WHERE canonical AND committed AND capture_kind='logs' AND status='partial'
ORDER BY chain_id,from_block;

-- One later complete receipt wins over older incomplete captures. Counts are
-- transaction identities, not sums of all immutable capture observations.
SELECT chain_id,count() AS unique_txs,countIf(complete) AS fee_complete_txs
FROM (
 SELECT chain_id,block_hash,tx_hash,max(fee_complete) AS complete
 FROM crypto_market_info_across.across_tx_receipt FINAL
 WHERE capture_id IN (SELECT capture_id FROM crypto_market_info_across.across_capture FINAL WHERE canonical AND committed)
 GROUP BY chain_id,block_hash,tx_hash
) GROUP BY chain_id;

SELECT chain_id,target_delay_ms,probe_status,reason,count() AS observations,
       min(dateDiff('millisecond',planned_for_at,requested_at)) AS min_dispatch_lag_ms,
       max(dateDiff('millisecond',planned_for_at,requested_at)) AS max_dispatch_lag_ms
FROM crypto_market_info_across.across_order_probe FINAL
WHERE requested_at>=now()-INTERVAL 1 HOUR
AND capture_id IN (SELECT capture_id FROM crypto_market_info_across.across_capture FINAL WHERE canonical AND committed)
GROUP BY chain_id,target_delay_ms,probe_status,reason ORDER BY chain_id,target_delay_ms,probe_status;

SELECT chain_id,count() AS promoted_captures,max(revision) AS latest_revision
FROM crypto_market_info_across.across_capture FINAL
WHERE canonical AND committed AND finality='finalized' AND revision>1
GROUP BY chain_id;

-- 完整 native-USDC Transfer 集（含成功空集合），仅计已提交 canonical capture。
SELECT chain_id, count() AS receipt_sets, sum(length(log_indices)) AS transfer_logs,
       max(available_at) AS latest_utc
FROM crypto_market_info_across.across_receipt_transfers FINAL
INNER JOIN
    (SELECT capture_id FROM crypto_market_info_across.across_capture FINAL
     WHERE committed AND canonical) AS c USING (capture_id)
GROUP BY chain_id ORDER BY chain_id;
