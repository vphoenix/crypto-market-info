-- Read-only queries in crypto_market_info_justlend_keeper. UTC throughout.
-- Source complete means a committed page, not full-window receipt verification.
SELECT capture_kind,capture_mode,event_kind,status,count() AS batches,
       sum(indexed_rows) AS indexed_rows,max(requested_to) AS latest_window_to,
       max(available_at) AS latest_available_at
FROM jl_keeper_capture FINAL
WHERE committed AND available_at>=now('UTC')-INTERVAL 1 HOUR
GROUP BY capture_kind,capture_mode,event_kind,status
ORDER BY capture_kind,capture_mode,event_kind,status;

SELECT event_kind,finality,position_status,count() AS rows,
       countIf(block_hash IS NOT NULL) AS known_block_hash_rows,
       max(block_time) AS chain_time,max(available_at) AS observed_available_at
FROM jl_keeper_indexed_event FINAL
WHERE capture_id IN (SELECT capture_id FROM jl_keeper_capture FINAL WHERE committed)
GROUP BY event_kind,finality,position_status;

-- Rental child coverage is the selected cohort; zero selected events is explicit.
SELECT parent_capture_id,capture_id,event_kind,coverage_scope,status,reason,
       discovered_candidates,selected_candidates,event_rows,receipt_rows,
       available_at
FROM jl_keeper_capture FINAL
WHERE committed AND capture_kind='enrichment'
ORDER BY capture_started_at DESC LIMIT 30;

-- Pending raw pages: distinguish no child from failed/partial children.
SELECT p.capture_id,p.event_kind,p.capture_mode,p.requested_from,p.requested_to,
       p.indexed_rows,countIf(c.committed AND c.capture_kind='enrichment') AS child_attempts,
       countIf(c.committed AND c.capture_kind='enrichment' AND c.status='complete') AS complete_children
FROM jl_keeper_capture AS p FINAL
LEFT JOIN jl_keeper_capture AS c FINAL ON c.parent_capture_id=p.capture_id
WHERE p.committed AND p.capture_kind='event_index' AND p.status='complete' AND p.capture_mode!='bootstrap'
GROUP BY p.capture_id,p.event_kind,p.capture_mode,p.requested_from,p.requested_to,p.indexed_rows
HAVING complete_children=0
ORDER BY p.capture_mode IN ('live','catchup') DESC,p.requested_from LIMIT 30;

-- Costs are observation data. NULL fee does not mean zero.
SELECT count() AS receipts,countIf(fee_sun IS NULL) AS unknown_fee_rows,
       max(available_at) AS latest_available_at
FROM jl_keeper_tx_receipt FINAL
WHERE capture_id IN (SELECT capture_id FROM jl_keeper_capture FINAL WHERE committed);

-- Complete source pages must have migrated or newly-written DB pagination metadata.
SELECT count() AS complete_index_pages,
       countIf(p.capture_id IS NULL) AS missing_page_progress
FROM jl_keeper_capture AS c FINAL
LEFT JOIN jl_keeper_index_page AS p FINAL USING (capture_id)
WHERE c.committed AND c.capture_kind='event_index' AND c.status='complete'
SETTINGS join_use_nulls=1;

SELECT count() AS stored_pages, max(available_at) AS latest_source_response_utc
FROM jl_keeper_index_page FINAL;
