# LST dRPC 切换验收

北京时间 2026-10-03；实际请求时间为 2026-10-02 17:39–17:54 UTC。结论：默认 RPC 已切为 `https://eth.drpc.org`，实时协议状态和报价采集落库通过；**按高度的日志查询被该免费端点拒绝，历史回补未通过**。没有运行完整 30/60 日回补或安装常驻服务。

记录时点说明：本页描述切换短跑时的状态；随后在北京时间 2026-10-03 02:36:08 启动了常驻服务，见[启动记录](service-startup.md)。当前仅持续实时采集与有限断线日志补缺，部署和健康检查见[运行说明](../../docs/lst-redemption-data-implementation.md)。本页原验收结果继续保留，不能以服务已启动替代日志覆盖验收。

## 代码与测试

- `cmd/lst-data/main.go` 的默认 URL 改为 dRPC；`LST_RPC_URL` 仍可覆盖。只有原来的 `eth.drpc.org` 和 `fapi.binance.com` 请求，没有增加备用域名。
- `internal/lst/rpc.go` 识别已完整归档的 HTTP 400 JSON-RPC error：必须精确对应 transport 的 `source_http_400`、版本、请求 ID、存在的整数 code 和字符串 message，且不能同时有 result。使用 `errors.Join` 保留 HTTP 错误；非 2xx result 不能变成成功，限流、禁用、网络、归档失败的处理不变。
- `go test ./cmd/lst-data ./internal/lst ./internal/storage/clickhouse` 与同范围 `go vet` 通过。独立 Agent 复审及 LST race 测试通过，见[0016](../../discuss/0016-lst-redemption-data-code-review.md)。新增测试覆盖 15 种响应，以及完整 Backfill 收到 code 35 后只保存一次 failed、零事件、不推进/缩小游标、不继续请求。
- Transport、日志过滤、模型、schema、预算、冷启动和持久冷却均未改。没有引入逐块扫描或其他端点。

最终可执行文件 `var/lst/lst-data` SHA-256：`7b570f6d9735c1c3f28ea5dc0127994b0f006e0d4decc6b5044982ac7cbb715f`。

## 实时采集

未设置 `LST_RPC_URL` 执行：

```bash
var/lst/lst-data watch --once --duration 5m
```

退出码 0，[日志](watch-once.log)记录 RPC 62 次、Binance 4 次，来源限流次数 0。身份预检通过，生成市场 capture `fb2587fe-33a4-44b2-b627-d1f28d7dadc3`，区块 `26106248`。

[只读 SQL](inspect.sql)与[数据库结果](database-validation.jsonl)确认：

- 一条协议状态 `ok`；paused 与 bunker 均为 false。
- 八条 entry 观测落库。10,000 USDT 的 A 路线买入、转换、退出、对冲均为 `ok`，时效 `fresh`。
- 250,000 USDT 两条链上路线买入、转换、退出成功，但十档对冲容量不足；其他成员存在轮次预算用尽，明确保存 unknown。不得把这些成员解释为完整可执行机会。
- 整个市场批次为 `partial`、`canonical=true`、`finality=head`。本次 `--once` 不运行后续最终性维护，不能将其作为 finalized 市场事实；报价可采集不等于利润已验证。

只读 `report --from 2026-10-02T17:39:00Z --to 2026-10-02T17:55:00Z --out var/lst-reports/drpc-validation` 退出码 0，读取三个 capture 的覆盖记录，并按最终性/失败条件全部排除；被排除的批次不进入成员回读验证。报告保留缺失成本限制，没有生成利润或 APR 数值；没有遗留待写 pending 批次。

## 历史回补及端点限制

历史合约状态此前已对原失败块与约 60 日前的块做过按高度和 EIP-1898 blockHash 查询，[证据](../2026-10-02-lst-implementation/public-archive-rpc-probe.json)均通过。这个事实不能证明日志接口可用。

完整 CLI 在 RPC 错误修复前后均执行同一有限范围：

```bash
var/lst/lst-data backfill --from-block 26104300 --to-block 26104811 --duration 5m
```

两次均退出码 1、RPC 40 次、Binance 2 次。预检及历史 queue 身份检查成功，随后 `eth_getLogs` 返回 HTTP 400、JSON-RPC code 35：`ranges over 10000 blocks are not supported on free plan`。请求确为含端点 512 块，不能据这条不一致的消息断定拆小范围即可解决。

修复前[日志](backfill-before-rpc-error-fix.log)只显示 HTTP 错误；修复后[日志](backfill-after-rpc-error-fix.log)保留 `source_http_400` 与 `rpc_error_35`。数据库保存两个 failed 范围，原 PublicNode 的失败批次也继续保留；没有 request/finalization/claim 事实，不能当作成功空范围。

低频对照见[第一组](filter-probe.jsonl)、[第二组](filter-probe-2.jsonl)、[第三组](filter-probe-3.jsonl)：

| 查询 | 实际结果 |
| --- | --- |
| 历史单块按高度 | HTTP 400，code 27，`Unknown state. First available state is 1` |
| 历史/近期 512 块、空 topics、小写 address、address 数组加单个 topic | HTTP 400，code 35 |
| finalized 标签或近期/历史单块 blockHash | HTTP 200，空 logs |
| 非标准十进制字符串或 JSON 整数编号 | HTTP 400，code -32601，无 `0x` 前缀；未采用 |

按 blockHash 返回空 logs 只证明端点接受这种请求，尚未验证它完整返回非空事件。本次没有改为逐块扫数。供应商内部原因尚不确定；[官方 eth_getLogs 文档](https://drpc.org/docs/ethereum-api/eventlogs/eth_getLogs)支持地址和区块范围过滤，但本次有效请求的实测拒绝仍需保留。历史日志覆盖、赎回等待分布和净利润/年化均未得到实网验证。

## 发出节奏与留存

所有探针也使用 `var/lst/state` 的共享锁及原 Transport，失败同样消耗预算；没有清空 gate 或绕过冷却。[HTTP 归档统计](http-validation.json)覆盖上述所有请求：

| 来源 | 请求数 | 最短发送间隔 | 滚动 60 秒峰值 | 状态 |
| --- | ---: | ---: | ---: | --- |
| eth.drpc.org | 154 | 714,487 微秒 | 42 | 145 个 HTTP 200、9 个 HTTP 400 |
| fapi.binance.com | 8 | 5,496,983 微秒 | 4 | 全部 HTTP 200 |

12 次日志请求的最短发送间隔为 10,002,069 微秒。没有 HTTP 429/403/418，不能据有限窗口承诺长期不会限流。原始请求与响应按 SHA-256 保存在 `var/lst/evidence`，统计文件可追到每个 request/response hash。
