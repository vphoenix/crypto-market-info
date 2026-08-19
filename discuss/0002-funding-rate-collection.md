# 0002 资金费率采集方式与 REST 限速

日期：2026-08-19

## 结论

估算资金费率改用 WebSocket，实际结算费率继续用 REST 历史接口确认。实际值不在整点立即请求，也不允许所有 instrument 同时请求；每个交易所使用一个串行 worker 慢速处理。

该方案只调整资金费率采集链路，不增加 Redis、Kafka、持久化任务队列或新状态表。

## 当前问题

现有程序的估算值和实际值都来自 REST。每分钟都会按 instrument 查询实际费率，整点还会补查上一小时并查询当前估算值。instrument 数量增大后，会形成集中的 REST 请求并可能触发交易所 IP 限速。

当前四个接口为：

| 交易所 | 用途 | 当前接口 |
|---|---|---|
| Binance | 实际结算费率 | `/fapi/v1/fundingRate` |
| Binance | 估算费率 | `/fapi/v1/premiumIndex` |
| OKX | 实际结算费率 | `/api/v5/public/funding-rate-history` |
| OKX | 估算费率 | `/api/v5/public/funding-rate` |

## 调整方案

### 估算费率

- Binance 使用 Mark Price WebSocket；
- OKX 使用公开 `funding-rate` WebSocket；
- 每个交易所一个资金费率连接，在连接内订阅全部已配置永续 instrument；
- 推送值按 `(instrument_id, funding_time)` 保存在内存；
- 每个 UTC 整点从最新有效推送写入一条估算记录；
- WebSocket 不可用或数据过期时不写，不能回退到逐 instrument REST 轮询；
- 结算整点使用结算前针对该时点的最后估算值，不能误用结算后针对下一周期的费率。

### 实际结算费率

- 根据 WebSocket 提供的目标 `funding_time` 建立待确认任务；
- 首次 REST 查询安排在 `funding_time + 2 分钟`；
- 每个交易所只有一个 worker，逐个 instrument 查询，请求之间至少间隔 1 秒；
- 未取得实际值时，在结算后第 5、15、60 分钟重试；
- 不再每分钟查询全部 instrument，也不在整点批量并发；
- 查询成功后以同一 `(instrument_id, hour_time)` 写入 `is_actual=1`，由实际版本替换估算版本。

即使同一结算时点有 500 个 instrument，以每秒一个请求计算，也只需约 8 分钟处理完。实际结算值属于历史确认数据，不要求在结算后立即写入。

## 时间字段

- `hour_time`：小时记录的 UTC 整点逻辑键，继续使用 `DateTime('UTC')`；
- `funding_time`：交易所给出的目标或实际结算时刻，改为 `DateTime64(3, 'UTC')`，保存毫秒；
- `is_actual=0` 时，`funding_time` 是估算值指向的目标结算时间；
- `is_actual=1` 时，`funding_time` 是历史接口返回的实际结算时间；
- 不新增 REST 请求时间或实际值取得时间字段。

实际费率解析必须用任务携带的目标 `funding_time` 与交易所响应匹配，不能先把响应时间截断到整点再比较。

## 最小实现范围

1. 增加 Binance 和 OKX 资金费率 WebSocket parser、runtime 与内存最新值；
2. scheduler 每小时从内存读取估算值；
3. 增加每个交易所一个延迟串行 REST worker；
4. 将 `funding_time` DDL 和读写类型调整为 `DateTime64(3, 'UTC')`；
5. 删除每分钟遍历全部 instrument 的实际费率查询，以及估算费率 REST 调用。

## 验收标准

- 估算费率采集不会调用 REST；
- 多 instrument 在同一结算时点不会产生并发 REST 请求；
- 同一交易所相邻实际费率 REST 请求至少间隔 1 秒；
- 首次实际费率查询不早于 `funding_time + 2 分钟`；
- 未及时出现的实际值可按既定延迟重新查询；
- WebSocket 失效时不会把陈旧估算值写成当前值；
- 实际值不会被后续估算值覆盖；
- 交易所返回的 `funding_time` 毫秒不丢失；
- 原有盘口采集、分钟写入和回放不受影响。
