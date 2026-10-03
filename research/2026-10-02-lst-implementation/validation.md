# LST 采集实现验收

日期：2026-10-02 UTC（北京时间跨至 2026-10-03）。验收范围为公开数据采集 MVP；未部署常驻 LST 服务、未交易、未自动执行 30/60 日回补。

记录时点说明：上面的“未部署”指本次初版验收时点，后续已完成[dRPC 切换](../2026-10-03-lst-drpc/validation.md)和[常驻服务启动](../2026-10-03-lst-drpc/service-startup.md)。旧失败和样本仍按当时事实保留；当前操作与部署状态以[运行说明](../../docs/lst-redemption-data-implementation.md)为准。

## 表与程序

已执行 `var/lst/lst-data init-schema`，实际研究库 `crypto_market_info_lst` 有七张 `lst_*` 专项表和 instrument。采集以专用可写连接打开现有库；report 服务端只读。入口和操作见[运行说明](../../docs/lst-redemption-data-implementation.md)，独立代码审核见[0016](../../discuss/0016-lst-redemption-data-code-review.md)。

`probe` 在 finalized 高度 26105487 完成链、代理实现、代码 hash、decimals、池 token/fee 核验。候选归档为 [verified-manifest.json](verified-manifest.json)，已用于正式配置。证据根 `0x0771e60467c2eff73dcee85597f1c9dca7fb052770e874861bcbe3f771d619da`；正式 manifest hash 为 `0xcdb1ab2cd55bf49b022210f1f13548f21b91667ee43253504a4403caf1ebea61`。

## 测试与修复

- 全仓 `GOCACHE=/tmp/lst-go-cache go test ./...` 通过。首次受沙箱禁止绑定 httptest 本地端口影响，随后在允许本地监听的执行环境重新运行通过；未启用真实来源测试。
- `go test -race ./internal/lst ./internal/storage/clickhouse` 通过；`go vet ./cmd/lst-data ./internal/lst ./internal/storage/clickhouse` 通过。独立审核者另跑 race 与 CLI 编译通过。
- 存储 Agent 运行 `CLICKHOUSE_INTEGRATION=1 go test ./internal/storage/clickhouse -run TestLST`，隔离临时库全部通过并清理：UInt256/Decimal/NULL/raw hash/微秒往返、冻结重试、最新最终性修订、孤块不复活、原始事件与 gas 补证、资金费冲突、专用 writer 写读、只读与零次重试拒写、缺库不隐式建库。
- 冷启动、稳态、空闲后不积攒突发、滚动额度、429/403/418、持久冷却和重启、慢响应/磁盘延迟、临近 deadline 不发送，使用假时钟/本地 transport 验证。其他回归包括协议部分失败留证据、单个请求的 inclusive finalization、固定原对冲敞口、gas 固定选样、批次恢复、日志连续性、报表覆盖/删失和 unknown 成本。
- 真实启动曾因本地发送预约过期而等待 60 秒重试，没有补发积压。第一次真实批次还检出 Gob 把指向 false 的指针还原为 NULL；已改成带显式 nullable 存在位的版本化 envelope，并测试 false、整数零、UInt256 零与 NULL 的精确恢复。

旧 pending 原文仍在 [failed-initial-pending.gob](failed-initial-pending.gob)，恢复审计 [pending-recovery.json](pending-recovery.json)。只补回原始 RPC 已证实的两个 false，原行 hash 和 FactDigest 完全不变，未重新 Seal 或重新抓行情；完整 Validate、新文件回读、真实落库均通过。

## 实网节奏

两个限时 watch 分别运行 5 分钟和 6 分钟，均包含缓速启动预检。达到 `--duration` 后主动退出，CLI 记录 `context deadline exceeded`、退出码 1；这些时限退出不是来源请求失败。

首个 5 分钟窗口的归档响应验证见 [http-validation.json](http-validation.json)：

| 来源 | 请求数 | 最短实际发送间隔 | 滚动 60 秒峰值 | 响应 |
| --- | ---: | ---: | ---: | --- |
| Ethereum PublicNode | 144 | 688,608 微秒 | 41 | 全部 HTTP 200 |
| Binance Futures | 11 | 5,503,136 微秒 | 5 | 全部 HTTP 200 |

第二次 6 分钟运行记录 RPC 162 次、Binance 13 次，来源限流计数均为 0。只证明这些有限窗口没有触发限流，不保证其他进程共用出口或长期运行不会被限流。预算到期的未发送预约不计为真实 HTTP。

实测固定先采 100k 会挤占其他三档，因此最终实现按分钟轮转四档首位，仅每日 UTC 12:00–12:04 保留 100k 种子优先；到期同量 followup 优先于 entry。请求频率、30 秒轮次与时效标准均未放宽。

## 落库结果与实际边界

2026-10-02 16:11 UTC 的[数据库快照](database-validation.jsonl)由[只读 SQL](inspect.sql)产生：

- 10 个市场批次、10 个协议观测、80 条 entry 观测。所有档位仍有固定成员，未取得值的成员明确为 unknown。10k/50k 的 A/B 各有一个完整 fresh 样本；100k A/B 分别有 4/3 个。250k 未取得完整 fresh 样本，存在十档对冲深度不足或轮次/来源时效不足，不能将其解释为可成交容量。
- 两个 actual funding 批次共 12 行，按 instrument/fundingTime 去重为 7 次结算；历史请求相互重叠是正常采样，不累加为 12 次现金流。
- 两个增量日志范围成功且为空；一个历史范围失败。实际 request/finalization/claim 行数仍为零，gas 无完整 UTC 日覆盖，因此没有假造 gas 样本。事件解码和关联在本地 fixture/真实 ClickHouse 集成中通过，尚未用该默认端点取得非空历史事件完成实网验收。
- 一个旧市场批次已复核为 finalized，九个仍保留 head。离线报告排除未完成最终性核验的市场批次，不以当前已过了若干分钟自动提升身份。

显式小范围 `backfill --from-block 26104300 --to-block 26104811 --duration 5m` 共发出 RPC 38 次、Binance 2 次，在历史 queue 身份校验处收到 `-32000 historical state ... is not available`，原文见 [historical-provider-error.json](historical-provider-error.json)。该范围保存 failed、没有事件事实，不被当作成功空范围。30 日回补需要另行配置支持目标历史状态和 blockHash 调用的 RPC；当前默认源的历史回补未验收通过。

只读报告输出 `var/lst-reports/initial-validation/{coverage,quotes,withdrawals,funding,scenarios}.csv` 与 summary.json，最后一次完整导出耗时 0.10 秒（前次 0.14 秒）。summary 正确保留 `unknown_costs_and_funding_coverage`，净收益/APR 没有伪造数值。

上述时点压缩列数据合计 38,608 字节，active part 合计 108,311 字节，包含 instrument 与失败/重叠采样事实，不含原始 gzip 证据。这是很小的短跑测量，不能外推为已实测日占用；长期压缩率、30 日查询耗时、等待分布和稳定盈利均尚未验收。
