# 0005 交易所请求限频审查

日期：2026-08-20

## 结论

资金费率实际值的 REST 查询已经串行限速，不会在整点同时请求所有合约。

当前主要风险来自盘口启动和重连：程序为每个交易对建立独立 WebSocket，并且 Binance 每次连接后都立即请求一次 1000 档 REST 快照。多个交易对同时启动或在网络恢复后同时重连，可能形成请求峰值。通用 HTTP 层还会在收到 429 后快速重试，这可能把一次普通限流升级为 IP 封禁。

默认只采集四个交易流且同一 IP 没有其他采集程序时，产生大量 REST 请求的概率较低；但增加交易对前应修复以下问题。

## 1. 收到 429 后不能快速重试

文件：`internal/exchange/http.go`

通用 GET 当前把 HTTP 429 纳入最多 4 次重试，重试间隔依次约为 250 毫秒、500 毫秒和 1 秒。

Binance 明确要求客户端收到 429 后停止请求并退避；继续请求可能返回 HTTP 418 并封禁 IP。现货接口会通过 `Retry-After` 响应头告知至少应等待多久。

修改要求：

- 429 和 418 不使用当前的短间隔重试；
- 读取并遵守 `Retry-After`；
- 同一个交易所客户端在冷却期内不能由其他交易对继续发送请求；
- 5xx 可以保留有限重试，但应增加少量随机抖动。

参考：[Binance REST 限频说明](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/limits)。

## 2. OKX WebSocket 建连需要共享限速

文件：`internal/app/app.go`、`internal/exchange/okx/runtime.go`、`internal/exchange/okx/funding_stream.go`

当前每个 OKX 交易对建立一条盘口 WebSocket，资金费率再建立一条 WebSocket，所有组件在程序启动时同时运行。

默认配置会同时建立三条 OKX 连接：

- 现货盘口 1 条；
- 永续盘口 1 条；
- 资金费率 1 条。

这正好达到 OKX 官方的每 IP 每秒 3 次连接上限。只要增加一个交易对、同一 IP 还有其他程序，或者多个连接同时断开重连，就可能超限。各连接使用相同的指数退避时间且没有随机抖动，网络恢复后仍可能同步重连。

修改要求：

- OKX 盘口和资金费率共用一个 WebSocket 建连门控；
- 相邻建连至少间隔 500 毫秒；
- 重连等待增加少量随机抖动，避免所有交易对保持同步；
- 不需要为此重构订单簿处理或引入外部队列。

参考：[OKX WebSocket 连接限制](https://app.okx.com/docs-v5/en/)。

## 3. Binance 盘口快照需要共享限速

文件：`internal/exchange/binance/runtime.go`、`internal/exchange/binance/client.go`

每个 Binance 交易对在首次连接和每次重连后都会请求一次 1000 档 REST 快照。交易对之间没有共享限速器，因此同时恢复时会并发请求。

按照当前官方权重：

- 现货 1000 档快照的请求权重为 50；
- USDT-M 永续 1000 档快照的请求权重为 20。

一轮同时恢复的请求权重为：

```text
50 × Binance 现货交易对数 + 20 × Binance 永续交易对数
```

若再叠加当前最多 4 次的 429 重试，短时间内的最坏权重还会接近上述数值的 4 倍。

修改要求：

- 所有 Binance 盘口快照共用一个简单的串行限速器；
- 采用保守值时，相邻快照请求至少间隔 1 秒；
- 首次启动和重连都必须经过同一个限速器；
- 重连增加随机抖动；
- 暂时不需要合并所有 WebSocket 或实现复杂的动态权重调度。

参考：[Binance 现货盘口权重](https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints#order-book)、[Binance USDT-M 永续盘口权重](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Order-Book)。

## 资金费率接口检查结果

资金费率部分未发现瞬时大量请求问题：

- Binance 和 OKX 各自只有一个实际费率 REST worker；
- 同一交易所相邻请求至少间隔 1 秒；
- 结算后只在第 2、5、15、60 分钟尝试确认；
- 启动补查不会把已经错过的多轮请求立即全部重放；
- 两个实际费率接口都将单次 HTTP 请求的 `MaxAttempts` 设为 1；
- WebSocket 的重复推送按 `(instrument_id, funding_time)` 去重。

Binance 资金费率 worker 即使一直有积压，最多约为 300 次/5 分钟，低于该接口当前 500 次/5 分钟/IP 的限制。OKX worker 的每秒 1 次也低于资金费率历史接口每个 IP 与 instrument 的限制。

参考：[Binance 资金费率历史接口](https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Get-Funding-Rate-History)、[OKX API 文档](https://app.okx.com/docs-v5/en/)。

## 建议增加的测试

只需增加三类针对性测试：

1. 返回 429 和 `Retry-After` 时，不会在冷却期内再次发出请求；
2. 多个 OKX runtime 同时启动或重连时，实际建连时间满足共享间隔；
3. 多个 Binance runtime 同时恢复时，盘口快照请求被串行化。

本次审查执行了 `go test -buildvcs=false ./...`，全部测试通过。测试只使用本地临时 HTTP/WebSocket 服务，没有调用真实交易所接口。
