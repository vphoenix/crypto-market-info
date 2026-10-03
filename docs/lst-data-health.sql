-- 只读健康检查；所有时间 UTC。观察最新 head 与 partial，不代替 report 的资格筛选和摘要校验。
-- 从仓库根目录用现有 ClickHouse client 的 --multiquery 执行。

SELECT
    'capture_15m' AS section,
    capture_kind, status, finality, canonical,
    count() AS captures,
    max(started_at) AS latest_started_at_utc
FROM crypto_market_info_lst.lst_capture FINAL
WHERE started_at >= now64(6, 'UTC') - INTERVAL 15 MINUTE
GROUP BY capture_kind, status, finality, canonical
ORDER BY capture_kind, status, finality, canonical;

SELECT
    'protocol_15m' AS section,
    p.state_status, p.reason,
    count() AS observations,
    countIf(p.identity_ok) AS identity_ok_observations,
    max(p.observed_at) AS latest_observed_at_utc
FROM (SELECT * FROM crypto_market_info_lst.lst_protocol_state FINAL) AS p
INNER JOIN (SELECT * FROM crypto_market_info_lst.lst_capture FINAL) AS c
    ON p.capture_id = c.capture_id
WHERE c.committed AND c.capture_kind = 'market'
    AND c.started_at >= now64(6, 'UTC') - INTERVAL 15 MINUTE
GROUP BY p.state_status, p.reason
ORDER BY p.state_status, p.reason;

SELECT
    'entry_quote_15m' AS section,
    q.route_id,
    toString(q.purchase_budget_usdt_raw) AS purchase_budget_usdt_raw,
    count() AS observations,
    countIf(c.canonical AND q.buy_status = 'ok' AND q.conversion_status = 'ok'
        AND q.exit_status = 'ok' AND q.hedge_status = 'ok'
        AND q.timing_status = 'fresh') AS canonical_fresh_complete_observations
FROM (SELECT * FROM crypto_market_info_lst.lst_quote_observation FINAL) AS q
INNER JOIN (SELECT * FROM crypto_market_info_lst.lst_capture FINAL) AS c
    ON q.capture_id = c.capture_id
WHERE c.committed AND c.capture_kind = 'market' AND q.quote_role = 'entry'
    AND c.started_at >= now64(6, 'UTC') - INTERVAL 15 MINUTE
GROUP BY q.route_id, q.purchase_budget_usdt_raw
ORDER BY q.purchase_budget_usdt_raw, q.route_id;

SELECT
    'live_log_coverage' AS section,
    countIf(status = 'complete' AND canonical AND committed) AS complete_ranges,
    maxIf(to_block, status = 'complete' AND canonical AND committed) AS latest_complete_to_block,
    countIf(status = 'failed' AND committed) AS failed_ranges,
    if(countIf(status = 'failed' AND committed) > 0,
        maxIf(started_at, status = 'failed' AND committed), NULL) AS latest_failed_at_utc
FROM crypto_market_info_lst.lst_capture FINAL
WHERE capture_kind = 'logs' AND capture_mode = 'live';
