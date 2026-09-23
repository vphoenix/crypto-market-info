# 0010：Ethereum 套利采集与判断代码独立审核

日期：2026-09-22。审核者：独立 Agent `dex_design_review`。范围为已通过 R2 设计的最小实现：`internal/dex/`、`internal/dexarb/`、DEX runtime、五张 ClickHouse 专项表、`dex-check` 及 config/app/collector 的接入。未审核无关的既有工作区修改。

**当前最终结论：审核通过，没有未关闭的 P1/P2 问题。** 下文保留首轮问题及修订过程；最后追加的合成容量验收和报价查询条件修正也已复审通过。

本次审核不改变原有边界：只采集公开数据与判断候选，不要求加入交易执行、完整 ticks、自动 trace 或本地 EVM 平台。

## 首轮代码审核

结论：暂不通过。以下四项为实际控制流或资金筛选问题，已发送作者修正；均可在现有五表与固定路线内解决。

### R1-01 [P2] 已提交块的漏日志没有恢复入口

位置：`internal/app/dex_runtime.go` 的 `step`，`internal/storage/clickhouse/dex_query.go` 的 `DEXGapStart`。

`Collect` 允许报价有效而日志请求失败，并提交 `LogCoverage=missing`。后续只查询缺失高度；该块已经存在且 canonical/committed，因此不再是 gap。它也无法进入只接受 `log_coverage=complete` 的回执补采。这会把一次临时超时变成永久日志缺口。

最小修正：有界选择已提交但日志不完整的块，按同 hash 补日志；先写事实再发布完成 revision，保留原报价 batch、报价时间及报价成员摘要。补采不能把历史数据伪造成实时发现。增加日志先失败后成功的恢复验证。

### R1-02 [P2] 已补齐回执的块仍占据有限重试窗口

位置：`dexRunner.retryReceipts`、`DEXPendingReceiptBlocks`。

补回缺失交易后，代码不更新 `ReceiptCoverage`；已无 pending tx 时只是把进程内重试时间延后 24 小时。SQL 始终取最新 100 个 `receipt_coverage != complete` 的块，这些实际已经齐全的块仍占据这 100 个位置，较早的真实缺口不能被选中。重启还会丢失进程内延后状态。

最小修正：根据白名单日志中的交易集合与已存完整回执集合确认齐全后，发布完成状态，或让 pending 查询直接排除已齐集合。需要验证补采后的实际覆盖及“100 个已齐块后仍能找到旧缺口”，不新增队列服务。

### R1-03 [P2] 先丢弃金额备选导致准备金检查漏报

位置：`internal/dexarb/report.go` 的 `Analyze`、`cost.go` 的 `QuoteCosts`。

Analyze 只保留毛利最大的输入档，QuoteCosts 取得真实 gas 成本后才检查 `input + costs <= EntryUSDC`。当 100 万档占满预算、毛利 1,000，而 50 万档毛利 600、完整情景成本 10 时，当前代码只把最大档判超预算；50 万档明明满足该情景，却已被丢弃。

最小修正：保留已有七档中的备选，在取得固定块的完整成本与准备金之后再次筛选可负担金额，并选择情景净值；不要求新增金额优化器或增加报价档。输出必须区分原毛利最佳档与情景最终采用档，避免 CSV 将两者混用。

### R1-04 [P2] RPC 短暂落后被当成不可恢复 finalized 重组

位置：`dexRunner.step` 的检查点判断。

条件 `head.Number <= checkpoint.Number && head.Hash != checkpoint.Hash` 直接比较了不同高度的 hash。已有 finalized 检查点 100，而 RPC 某次 latest 返回合法但滞后的 99 时，不同高度的 hash 正常不同，却会触发 `errDEXFinality`，然后分支等待进程退出而不再重试。

最小修正：低于检查点的 head 是可重试的来源滞后；同高度 hash 冲突或复查既有检查点确实变化才按 finalized 矛盾暂停。finalized 标签倒退也需与来源滞后和真实 hash 矛盾区分。增加返回旧 head 后恢复新 head 的验证。

## 首轮已确认的正确实现

- 价格和金额经过 `big.Int`/UInt256，PSM 的费用舍入、buyGem 最大整数搜索和独立临时库存有明确实现；没有引入浮点收益计算。
- Quoter 与 Sky 的读取固定 blockHash；未知 RPC/ABI 返回不会复制上一块报价。Quoter gasEstimate 没有被当作完整交易 gas。
- 原始证据采用内容 hash、gzip、临时文件加原子 rename；事实表先写、区块完成标记最后写。
- 区块查询先 argMax 取最新 revision，再由外层检查状态；没有先筛 canonical 而复活旧状态的查询。
- DEX runtime 在分支内处理故障，避免通过现有 component 错误返回取消 CEX。
- 历史独立样本使用 `capture=research`；成本补报是同状态参考，scenario_positive 限定 USDC 库存情景，不声称 USDT 完整往返收益或净 APR。

## 验证与复审状态

首轮 `go vet ./internal/dex/... ./internal/dexarb/... ./cmd/dex-check/...` 通过。首次定向 go test 时相关测试尚未落地，输出为 no tests；因此不把该命令记为功能验证通过。

待修订后重新检查实际代码、针对性测试和数据库内五表 DDL，再填写最终结论。本审核未修改生产实现或运行服务。

## 修订后独立复审

已重新读取修订后的实现、测试、存储字典 §11、运行说明的 DEX 段和 [实现说明](../docs/dex-arbitrage-implementation.md)。最终审核代码指纹覆盖 31 个文件：`internal/dex/`、`internal/dexarb/`、`cmd/dex-check/` 的全部文件、`internal/storage/clickhouse/dex_*.go`，以及 `internal/app/dex_runtime.go`、其测试、`internal/app/app.go`、`internal/config/config.go`、`cmd/collector/main.go`。指纹包含最后新增的容量验收测试与报价查询条件修正。按相对路径排序，对每项“路径 + NUL + 文件字节 + NUL”计算的 SHA-256：

`74fc48c5423b2625231766b116679160f4c617f7f7752b9a58e208ac52933da0`

| 问题 | 实际修订与复审结论 |
|---|---|
| R1-01 漏日志恢复 | 关闭。新增 `DEXPendingLogBlocks`、`retryLogs` 和 `CompleteDEXLogs`；同 hash 补齐日志后再发布完成 revision，原报价 batch、成员和 available_at 保持不变。隔离库集成验证已存在的缺日志块能退出 pending，报价内容及时间保持。 |
| R1-02 回执恢复占位 | 关闭。`completeReceipts` 对照目标日志中的交易集合与已存完整回执集合，齐全后更新 coverage/count/members；完成块退出 pending 查询，重新启动也不会重新占位。集成验证完整回执读回及完成状态。 |
| R1-03 准备金后的金额退选 | 关闭。窗口保留毛利峰值块的已有金额备选；成本补报后按本金加成本重新选择可负担金额。新增测试验证大档超预算时选中较小正净值档；CSV 使用 SelectedAmountUSDC/SelectedGrossUSDC，与情景净值一致。 |
| R1-04 旧 head 误暂停 | 关闭。`checkDEXHead` 把低高度 head 判为可重试滞后；相同 finalized 高度不同 hash 才是暂停条件。finalized 标签倒退也按来源滞后处理。测试覆盖旧头后恢复新头、同高度冲突，以及 DEX 初始化失败不取消既有采集组件。 |

复审时另外要求并确认了三项小修，未增加表或服务：

- 日志补采写入失败后保留不可变 `pendingLogs`，同内容重试到提交成功；每条同 hash 日志的 payload 由定类型日志事实确定，不再因 RPC 请求时间改变。完整原始 RPC 及真实采集时间仍由区块 proof 引用。重复获取同块日志的摘要一致性测试通过。
- 公开 RPC 拒绝过大的 address 过滤组合，现改为每组最多六地址；所有组齐全才标完整，且逐组核验返回 emitter 属于该组。测试覆盖错组 emitter、单组失败及成功空日志，避免把部分结果当作完整覆盖。
- 单次报告报价缓存使用 blockHash 加完整请求作为键。复审指出 head 区块的 canonical 状态不能随历史报价一起缓存；最终 `CostCache.Canonical` 每次直通 RPC，测试验证相同 hash 的两次检查确实访问两次来源。最终指纹已包含该文件和接入。回执补采每次空闲轮次至多处理一个完成块，避免一次遍历大量完成项拖延新块读取。

## 独立运行的验证

- `go vet ./internal/dex/... ./internal/dexarb/... ./cmd/dex-check/...` 通过。
- 定向 `go test -race` 覆盖 Ethereum RPC、ABI、日志/回执、PSM、窗口、成本、报告缓存、stale head 和 DEX 故障隔离，全部通过；最后一次在缓存 canonical 修正后重新运行。RPC 取消与并发上限测试另重复 30 次通过。
- `CLICKHOUSE_INTEGRATION=1` 的全部 `TestDEXIntegration` 在自动清理的临时数据库通过；最后一轮指定真实 `sample-with-events`，验证实际公开样本报价、日志、回执的精确存取与摘要，而非只验证合成数据。覆盖未提交批次不可见、UInt256 大整数/NULL、幂等重试、孤块旧 revision 不复活、补采恢复及服务端只读连接。
- 独立只读查询 `system.tables`：`crypto_market_info` 内恰有 `dex_block`、`dex_sky_state`、`dex_route_quote`、`dex_log`、`dex_tx_receipt` 五张目标表。实际列类型、UTC 时间、原子 UInt256、分区及排序键与代码一致；区块使用 `ReplacingMergeTree(revision)`，回执使用 `ReplacingMergeTree(available_at)`。审核没有向生产表写入样本。
- 独立读取两份更新后的公开样本：区块 26,027,049 为 58 条成功报价、2 条日志及 2 个完整回执；区块 26,026,770 为 58 条成功报价、3 条日志及 3 个完整回执。二者均为 `capture=research`，日志/回执 coverage 为 complete。两目录共 78 个 gzip 证据文件的原文字节 SHA-256 均匹配文件名。
- 26,027,049 的四个策略池无事件，26,026,770 含 DAI/USDC 主策略池 Swap；可以区分该策略池确实无事件与有事件。整体空日志由独立 RPC fixture 验证，没有把早期 `LogCoverage=missing` 的失败样本当成空日志通过。
- 最后补入的实际无事件样本 26,027,046 也已独立读取：58 条报价成功，日志与回执均为 0 且 coverage 都是 complete，`capture=research`；15 个 gzip 证据 hash 全部一致。这给出了整体白名单无事件的真实样本，三份样本合计验证 93 个证据文件。
- 两样本最佳策略毛利分别为 -0.082835 和 -0.082749 USDC；它们是功能验证，不是已发现套利机会。作者另报告完整 `go test ./...` 通过；本审核独立执行范围为上述定向 race、vet 和实际 ClickHouse 集成。

## 最终结论与边界

**本次代码审核通过，没有未关闭的 P1/P2 阻塞项。** 程序、五表模型和候选判断在本次检查范围内一致；必要存储和运行文档已同步。

审核通过不等于持续采集已经上线。当前 DEX 默认关闭，本次未替换正在运行的 collector 或重启服务；24 小时持续负载、补采追赶能力、真实日存储量和七天重复性尚待实际启用后测量。下节合成容量结果是已有规模验证，不能替代真实持续采集。

块末报价与明确成本情景仍不能证明交易纳入、完整路径执行或净年化达到 4.5%。成本补报仅对毛利峰值块的已采金额做判断，结算报价也只是独立同状态参考；不据此声称整个窗口所有时刻均不可获利，或已经实现 USDT 往返利润。

本审核只维护审核记录。除自动清理的隔离测试库外，未修改生产数据库、实现代码、运行服务或资金。

## 日规模合成容量及查询条件补充审核

独立检查了新增的 `dex_capacity_test.go`、最终两份容量 JSON 和 [验证记录](../research/2026-09-22-dex-implementation/validation.md)。测试需同时显式启用 `DEX_CAPACITY=1` 与 ClickHouse 集成开关，沿用自动创建、清理的隔离数据库；每组按 500 块批写，并在五表 `OPTIMIZE FINAL` 后测量。审核曾只读检查临时库列表，确认该轮测试库已经清理。容量负载由作者运行；审核没有重复加载完整合成日。

两组各含 7,200 块、417,600 条报价，使用真实有事件/无事件样本衍生；身份和时间变化，但行情、Sky 状态及日志数值复用 seed。最后一轮结果如下：

| seed | 压缩列数据（字节） | 全日区块及报价读回（毫秒） | 单块报价读取 P95（微秒，20 次采样） |
|---|---:|---:|---:|
| 有事件 | 40,849,739 | 4,244 | 536,758 |
| 无事件 | 34,423,988 | 4,876 | 57,917 |

以上为 `system.parts.data_compressed_bytes`，不含 marks、索引及其他磁盘开销，也不含 gzip 原始证据。固定 seed 的压缩率不代表真实全天变动数据；缓存冷热与宿主机负载未控制，不能据此声称加速倍数，或把两组时延差归因为活跃程度。结果已明确标识 `synthetic_daily_capacity_not_live_history`，没有混入真实历史或收益统计。

测量暴露报价读取仅按 batch 筛选会扫描长历史，最后修订增加已有排序前缀 `chain_id=1`、输入块的 manifest 集合及最小/最大高度，同时保留精确 batch 集合。审核确认最小/最大值遍历全部输入，兼容乱序与多 manifest；不改变 Ethereum 范围内批次身份，不新增 DDL 或索引。该修订后，审核再次独立运行全部 `TestDEXIntegration`，包含真实有事件样本读回，结果通过（1.680 秒）。作者的容量测试与同组集成也通过。

补充复审通过；无新增 P1/P2，最终指纹已更新为上文 31 文件版本。
