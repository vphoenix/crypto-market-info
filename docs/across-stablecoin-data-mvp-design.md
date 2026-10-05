# Across 稳定币中继：最小数据采集设计

状态：设计经独立复审后已实现；2026-10-02 已建独立研究库并完成真实小窗口验收。当前运行说明、实测限制和验证链接见 [实现说明](across-stablecoin-data-implementation.md)。以下保留设计目标；实际能力以实现说明及验证记录为准。

2026-10-04用户修订存储要求：正常RPC/API正文仅在内存解析，定类型事实直接入库，成功提交后丢弃正文，不留响应备份。新增USDC Transfer全集表，退款查询读数据库。首次启动之前不再回补；旧留存原文先迁移必要字段再清理。下文早期raw归档和30日历史方案保留作设计沿革，现行实现以[存储文档](market-data-storage.md#across-稳定币中继2026-10-02-已实现)为准。

## 1. 范围与问题

首版只研究 **Base（8453）↔Arbitrum One（42161）的原生 USDC、空 message 的普通 EVM 中继**。预算仍为 100 万 USDT；研究可以先给这条业务 25 万库存的情景，不把全部桥流量当成新人能接到的单。

需要回答：

1. 有多少无独家期或独家期结束仍未成交的订单，金额、费用空间、日期和方向怎样分布？
2. 用我们的轮询方式发现订单时，目标链是否仍未成交、未过截止时间、没有暂停？增加 2/5/10 秒处理时间后，仍可能参与的量有多大？
3. 用户支付的差额能否覆盖 LP 费、fill/退款交易费用、失败、库存恢复和运行成本？
4. 资金实际退到哪个链、哪个地址？哪些回款能归属，哪些只能知道批次付款而不能归属到订单？

首版是研究采集器。历史数据给规模和收入上限，实时数据给发现延迟与公开状态。它不证明成交胜率，不以未知成本为零，也不把未归属退款加工成逐单回款统计。

### 固定对象

| 链 | SpokePool 发现地址 | 原生 USDC |
|---|---|---|
| Base | `0x09aea4b2242abC8bb4BB78D537A67a245A7bEC64` | `0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913` |
| Arbitrum One | `0xe35e9842fceaCA96570B734083f4a58e8F7C5f2A` | `0xaf88d065e77c8cC2239327C5EDb3A432268e5831` |

SpokePool 来自官方固定提交部署目录；USDC 地址还需与 Circle 目录及链上 `decimals()==6` 核验。不得把 USDC.e、同名币、其他链或非空 message 算入研究路线。固定白名单中允许记录其他路线的日志和排除原因，避免在 RPC 层过早过滤而遗漏更新、退款或无法匹配的成交。

首版不加入第三条交易链、不做自动全链发现、不用 pending/私有订单流，不搭建 Dataworker、全链索引或本地 EVM 回放。Ethereum HubPool 与 SDK 的逐单退款构建仅用于后续少量正样本取证，未完成时对应数据保持 unknown。

## 2. 已核对的协议细节

官方源码固定于 commit `a634bea927668519c748e46036181c89c7bd9b40`。原文及 SHA-256 在 [source-manifest.json](../research/2026-10-02-across-design/source-manifest.json)。**源码核对不等于已核对当前部署实现**；启动时必须读取每条链 ID、代理实现槽、实现 bytecode hash、SpokePool 身份及 token 元数据，按 manifest 的已核验实现白名单选择 ABI。没有已核验实现时只能保存 raw 并报告未覆盖，不能按 master 猜事件或权限。

- 当前 `FundsDeposited`/`FilledRelay` 使用 bytes32 地址和 uint256 depositId；旧 V3 事件保留 decoder，未知 topic 留证据，不静默忽略。[迁移规则](https://docs.across.to/guides/migration/non-evm/indexers)
- relay identity 是完整原始 RelayData 与目标链 ID 的 ABI hash。`(originChainId,depositId)` 只是查询索引；最终匹配还要核对 depositor、收款人、两币地址、两金额、截止时间、独家地址和 messageHash。源码的 relayHash 不含 quoteTimestamp；该字段仍需保留用于 LP 费研究。
- 独家截止时间是 **包含等号** 的：非被指定的新中继只有目标合约时间 `t > exclusivityDeadline` 才通过；可成交截止为 `t <= fillDeadline`。不能把两个链区块时间相减当作真实网络延迟，prefill 可能先于源链存款。
- `fillStatuses(relayHash)` 的 0/1/2 为 Unfilled/RequestedSlowFill/Filled；1 不等于已付款，fast fill 可替代 slow fill。`FillType` 的 0/1/2 为 FastFill/ReplacedSlowFill/SlowFill，slow fill 不计作自筹库存中继收入。`pausedFills()` 独立保存。
- 成交事件的 `relayer` 是指定退款地址，可与交易 sender 不同。必须同时保存这两个地址。`relayExecutionInfo.updatedOutputAmount` 是本次实际付款数，不能总用原始 outputAmount；更新后的收款人与 messageHash 也需保存。
- 原始存款和 `RequestedSpeedUpDeposit` 分开保存。更新未改变原始 relay identity；记录全部候选更新及其真实可见时间，不能事后把更好的新价格套给早先的发现时刻。首版实时只观察原始空 message 条款；更新条款在报告中单列，不预先建设签名验证和更新执行器。
- `ExecutedRelayerRefundRoot` 的一片叶子包含多地址、多笔聚合金额，**没有 depositId**。同一个叶子也可能包含过期用户退款。`deferredRefunds=true` 意味至少部分转账延迟；必须核对同交易 USDC Transfer，不能把整片叶子都当作到账。`ClaimedRelayerRefund` 另记录；caller 可与最终收款地址不同。
- `/deposit/status` 的 refunded 是用户存款退款状态，不能作为中继收回垫付款的证明。当前 `/suggested-fees` 为 legacy，首版不依赖该 API，也不把新 `/swap/approval` 的新用户报价当作旧订单的实际 LP 费。[费用](https://docs.across.to/introduction/fees)、[用户退款](https://docs.across.to/introduction/refunds)

固定源码：[SpokePool](https://github.com/across-protocol/contracts/blob/a634bea927668519c748e46036181c89c7bd9b40/contracts/spoke-pools/SpokePool.sol)、[接口](https://github.com/across-protocol/contracts/blob/a634bea927668519c748e46036181c89c7bd9b40/contracts/interfaces/V3SpokePoolInterface.sol)。

## 3. 一个命令程序

待实现入口为 `cmd/across-data`，建议三个子命令：

```text
across-data backfill --days 30 --manifest config/across-research.json
across-data watch --manifest config/across-research.json
across-data report --from ... --to ... --out var/across-reports/...
```

`watch --once` 用于一个采集循环验收。研究库建议 `crypto_market_info_across`，不接入现有生产 collector 的启动／退出过程。RPC 与 ClickHouse 凭据放环境变量；manifest 只保存链、合约、token、实现/ABI 白名单、轮询与范围配置。程序启动归档不含凭据的 manifest，行中保存 manifest hash。

复用现有 Go、ClickHouse 批写、`big.Int`、RPC HTTP 传输和内容寻址证据工具。现有 `internal/dex/ethereum/chain.go` 把 chain_id 设为 1，receipt parser 也未覆盖这两条 L2 的完整费用，**不能直接拿其标准化结果用于本任务**。写一个小的 Across 专用解析器即可，不为此重构所有采集器。

预计代码：`model.go`、`abi.go`、`rpc.go`、`collector.go`、`runner.go`、`report.go`，以及一个 ClickHouse writer。无 Web 服务、Redis、Kafka、策略插件、钱包或交易广播。只保留两个链轮询任务、一个有上限的补收据任务；不建设通用调度框架。

## 4. 抓取流程

### A. 30 日历史：先看是否有足够业务

以各链 finalized 头为截止，用真实 header 时间二分寻找 30 日起点，不能根据平均出块秒数换算。两个 SpokePool 抓全部日志，按 topic 解码存款、加价更新、fast/slow fill、根中继、退款执行与延后领取；暂停、路由、升级及未知事件保留在原始证据中。

每次 `eth_getLogs` 最多 512 高度；限额或结果达到来源已知上限时二分，直至完整。成功空数组是完整零事件；RPC 错误、null、截断、超上限是缺口。全部分片完成才推进这一链的连续扫描游标。保存区间首尾 hash、每个事件块的 header 和 payload hash。

对范围内路线的 fill 交易和所有 USDC 退款交易，按 txHash 去重抓 `eth_getTransactionByHash` 与完整 receipt；保留 receipt 全日志用于核对付款与收款。两条链其他来源的 USDC fills 也保留基本事实，用于揭示退款聚合污染；来源存款未采到的成交保持 unmatched，不强行补一个订单。普通存款不逐笔抓收据。

只解码有身份依据的 ABI；窗口开始前的实现和升级日志上下文不足时标 unknown。同块升级可能跨 ABI，首版整块保留 raw 并标 `upgrade_boundary_unknown`。不把今天的实现、费用或暂停状态套到 30 日历史。

历史数据可以按完整条款与实际 fill 判断：订单在独家期结束前是否已被填、费用空间、地址集中和方向净流。没有成交的订单必须要求目标链日志覆盖直到 fillDeadline；否则是 `fill_unknown`，不是“没人接”。窗口末尾未到截止的单为右删失，继续采到截止。存款前发生的 prefill 无法由短窗口无日志排除；不确定单不计入已证实开放订单。

### B. 实时：记录我们什么时候真的知道

每链轮询 latest 每 1 秒；读取所有未扫新区间日志。默认新鲜门槛为解析完成时 `available_at - block_time <= 5秒`，且块时间不能领先本地 UTC 超过2秒；否则为 `catchup/stale_head/clock_unknown`，不计当前可达样本。门槛写入 manifest，实测可调整，失败不会修改源块时间。只有新鲜实时来源记录 `live_received_at` 和解析完成后的 `available_at`，查询延迟从后者开始。

对这两条 USDC 路线，维护仅到 fillDeadline 的小型订单集合。新发现时、独家期即将结束及结束后，读取目标链同一 blockHash 的 `fillStatuses(relayHash)`、`pausedFills()` 与合约时钟。新鲜链头和全部 call 固定同 hash，调用结束检查 canonical。一个 probe 保存实际请求/完成时间及该 hash；不会假设请求瞬间已经知道响应。

首版只观察原始空 message 的条款。新鲜存款即使当前独家也采一个 probe，记录排除依据；独家期结束时再观察未关闭订单。到期或已成交后移出内存。重启从库加载未过截止的单，首次恢复记录为 restart/catchup，不延续未记录的在线可见性。

**后续 probe 是实际采集任务。** 非独家新单以存款首次 live available_at 为基准；接替单以首次满足开放规则的 probe available_at 为基准，安排基准后2/5/10秒的三次目标链 probe。保存 planned_for_at、target_delay_ms 和真正请求/完成时间；已经迟到就标 late，不能用任务名充当精确2秒观测。超过目标1秒的结果不计为该目标的准时样本，报告仍保留实际延迟。超预算写 skipped_budget，已终结订单取消未来任务并保留取消依据。一个按到期时间排序的小队列足够，不另建调度框架。

1 秒轮询已经有发现延迟。首版不使用 WebSocket、Arbitrum fast feed、Base 预确认或私有流；报告只评价本轮询实现的可达性。若公开状态一到手就全部成交，可以结束这一接入方式的研究，不先建设更快系统。

### C. 费用和估值证据

fill/退款 receipt 单独补齐，完整性不阻塞日志游标。每交易只保存一份成功收据事实／采集批次；报告按 chain+blockHash+txHash 去重计费，多 fill 或多退款地址不能重复扣整笔 gas。没有 trace 时单列原生币额外付款、私下排序费和失败尝试未知，不声称得到竞争者全部净利润。

Base 保存 execution fee、`l1Fee`、适用的 operator fee 和费规则依据；完整 total 不能只算 gasUsed×effectiveGasPrice。Arbitrum 的 L1 数据成本通常已折入总 gasUsed，不能再加一次 `gasUsedForL1`。新费制/字段不支持时，保留 raw 与已知组件，`total_fee_wei=NULL`。[Base 费用](https://docs.base.org/specifications/transactions/network-fees)、[OP 费用](https://docs.optimism.io/op-stack/transactions/fees)、[Arbitrum 费用](https://docs.arbitrum.io/how-arbitrum-works/inside-arbitrum-nitro)

实时 probe 可携带每 60 秒从用户指定的 `https://api.binance.com` 读取一次的官方公开 ETHUSDT 与 USDCUSDT BBO：ask/bid、各档数量、请求／获取时间、原始 hash。只缓存原样观测，不刷新源时间。gas 的 USDT 参考用购买 ETH 的 ask；USDC 换回 USDT 用 bid。BBO 数量不足、时间超过 60 秒或缺少来源时保持成本未知。它们只是 CEX 估值参考，不证明资金已在交易所或跨链换汇能执行。历史没有同期报价时，不用今日价格补出“历史净收益”。

第一版不做 fillRelay 的 state override、完整交易模拟或假定零成本 gas 估计。历史同类型真实 receipt 可给成本分布；实时给费用上限与明确的成本情景。没有全交易成本时仅输出费用空间和盈亏平衡费用。

### D. 退款证据的边界

捕获两链 USDC 的 RelayedRootBundle、ExecutedRelayerRefundRoot、ClaimedRelayerRefund 及完整收据；根中继的 raw 日志保存在 capture 证据，退款行保存链/SpokePool/rootBundleId/leafId。报告需要 root 时从已保存的根日志证据关联，窗口外的 root unknown，不伪造关联。事件行不保存可变的“已到账”标签；报告读取单独收据证据做逐项匹配，后补收据不会改写旧事件。

退款数据先报告**地址／批次到账金额、链、时间以及未实际到账条目**。选定两链不是所有来源链，叶子也可能包括其他路线和用户退款，因此首版不靠 FIFO／金额巧合／下一次到账时间把退款分配给订单，不生成未经证实的 P50/P95 逐单回款。

对少量最有价值的开放订单，后续可显式取得官方 bundle 成员／逐单退款证据，核对 root、leaf、付款人、退款链与金额；拿不到就维持 attribution_unknown。这一步不要求先建立 Dataworker。`report` 读取已保存证据，不隐式联网。

## 5. 运行预算、失败与恢复

- 单 writer；RPC 每条链最多 2 个在途请求、每批最多 20 个成员。live 轮询优先，收据补采一次最多 20 交易，每 5 秒尝试一次；每轮总超时 5 秒。来源 429/超时退避，不通过不停重启提高请求量。
- 最多 1,000 个待 probe 订单；超预算的任务保存 `skipped_budget`、数量和时间，按固定顺序轮转，不能把缺采记成已成交／不盈利。实施 `--once` 时测量真实 RPC 请求率、延迟和积压，再决定降低频率或缩短观察范围。
- raw 响应与 manifest 用 SHA-256 寻址的 `.json.gz` 本地文件；沿用证据目录的临时写、fsync、rename。source_id 只保存脱敏主机／来源名，不保存带 key 的 RPC URL。
- 先归档证据、再批写事实，最后写 capture 的 committed 标记。写入失败复用冻结的 capture_id、行内容和观测时间；新网络尝试使用新 capture，不改写旧可见性。查询只读取 committed 的对应成员，验证计数和摘要；已提交缺事实仍为 incomplete。
- capture `revision` 仅修订 canonical/finality/说明，保持原成员摘要、锚点和时间。查询先按 capture_id 取最新 revision，再过滤 canonical/committed。
- 每 30 秒核对未 finalized 尾部。重组使旧 hash 涉及的**全部 capture 类型和所有尝试**失效；日志区间相交旧分支则整段撤销、重抓。依赖孤立源链存款的目标链 probe 也不能计为有效候选。保留 raw 并记录重组导致的误识别。
- finalized hash 冲突停止该研究进程。不同链最终性独立维护；不支持 safe/finalized 标签时明确 capability_unknown，不能以固定确认数伪装 finalized。
- 单条链／报价来源失败不会填充假当前数据；另一链可继续采自己的事实，跨链完整性保持 partial。游标由最后连续、成功的日志 capture 恢复，不需要另建游标服务。

## 6. 数据库结构

全部时间 UTC，金额 UInt256 原子单位、应用 `big.Int`，价格 Decimal(38,18)，EVM地址原始20/协议bytes32地址原始32字节，hash原始32字节，不存 Float64。Decimal 只用于明确单位的价格；所有 USDC 原子数除以 1,000,000 时用十进制，不先转换为 float。

| 表 | 一行是什么 | 必要字段 |
|---|---|---|
| `across_capture` | 一次扫描／probe／收据批次的完整性与锚点 | manifest/capture ID、链与区间首尾 hash、模式、时间、canonical/finality/revision、各事实表成员数量与摘要、raw evidence hash、committed |
| `across_deposit` | 原始存款事件 | 完整 RelayData、quoteTimestamp、origin SpokePool、目标链、relayHash、原始 message/hash、ABI版本、链上日志位置、available_at |
| `across_deposit_update` | 一个已发生的加价更新 | origin/depositId/depositor、更新金额/收款人/message/signature、位置、available_at；不覆盖原存款 |
| `across_fill` | 一个 fast/slow 成交事件 | 原始完整条款、实际 updated 条款、fillType、repaymentChainId、退款地址relayer、位置、available_at |
| `across_refund` | 退款叶执行或延后领取事件 | 类型、token、rootBundleId/leafId（claim为空）、地址/金额数组、deferred、caller；实付由同交易收据在报告中核验 |
| `across_tx_receipt` | 一次取得的交易与收据 | sender/to、selector/calldata hash、状态、gas、L1/额外费用、费用完整性、raw hash、获取时间 |
| `across_order_probe` | 我们在目标链观察一笔订单的结果 | 原始 relayHash/订单引用、头 hash/时间、fillStatus、paused、请求与可用时间、失败原因、可选汇率BBO及各自真实时间/hash |

七张小表分离不同事实，避免把业务内容塞进通用 JSON。没有总收益表或预写“可盈利”标签；这些在离线 report 计算。Root/升级/暂停辅助原始日志存本地证据，当前必需状态进入 probe；首版不另建协议配置历史平台。

事实行按 capture_id 去重，重复抓取同一链上事件可物理存在于不同 capture。报告按链、SpokePool、blockHash、txHash、logIndex 去重；冲突比较只覆盖规范化链上不可变字段，排除 capture_id、ABI标签、抓取时间、payload hash 和 source_id 等采集元数据。RPC JSON格式／批次不同不能报协议冲突；两份已知协议内容确实矛盾才报异常。first_seen 取仍canonical的live记录最早真实可用时间，不能取backfill或仅收到raw但尚未解码的时间。收据按链/hash/tx去重计成本；若可选费用组件两份均已知且矛盾才报冲突，缺失后补不属冲突。每份 capture 的成员集合与引用不因其他批次补采而消失。分区按原始 block_time 月，capture 按固定开始时间月；不设置自动清理 TTL。

## 7. 报告与赚钱判断

生成 `coverage.csv`、`orders.csv`、`refunds.csv` 和 `summary.json`：

- coverage：两链日志区间、未匹配填单、未知实现、收据缺失、实时跳过／延迟、最终性、价格过期，以及右删失订单数量。
- orders：路线资格、完整条款匹配、开放前已成交、历史可能开放、实时 first_seen 后 probe 状态、fast/slow、实际付款、用户差额、实际退款链、成本完整性。每笔订单只计一次，probe 数和更新数不是机会数。
- refunds：各地址/批次 USDC 可验证到账、延后、其他路线／用户退款污染、逐单归属未知。不把 root 中继时间当作到账时间。
- summary：按 UTC 日统计候选金额和**收入上限**、地址集中、方向净流、最高几笔／几日贡献，实时样本中一看到就填完的比例和可观测开放比例。

`用户差额 = inputAmount - 实际outputAmount` 是 LP 与中继合计空间；只在同质原生 USDC、完整匹配的普通 fast fill 下计算。不把整差额当成 relayer净利，不把 frontend capitalFee/gasFee 或 appFee 当作自己应收款。

实际净额口径：`可核验中继应收退款 - 支付USDC - fill成本 - 自己承担的退款执行成本 - 失败/竞价 - 库存恢复成本 - 运行成本`。不能同时从应收退款扣已扣 LP 费，又再扣一次 LP 费。链下费用及未归属项独立列 unknown。

首版库存情景固定 **原链还款**，观察两个方向的自然库存恢复；官方费用规则说明该还款选择的 LP 费为零，报告明确标为规则情景，并要求实例路线仍有效。其他还款链不自动套零。即使假定原链 LP=0，用户差额仍只是执行和库存成本前的收入空间。[费用口径](https://docs.across.to/introduction/fees)

历史跨链时间仅用于筛开放的可能性；真实可达性以本地 live available_at、目标合约状态及后续 probe 为依据。2/5/10 秒延迟场景只有对应后续观测时才能判断仍开放，缺 probe 为 unknown；直到该时刻仍未看到 FilledRelay 不等于真实未填。首版不回放自己的交易位置，也不声称已验证能抢到单。

不允许以下高估：全部 bridge 总量计新人流量；prefill 给出负响应时间；独家边界含等号判断错误；同一订单重复按秒累加；slow fill算中继收入；退款叶子全给选定路线；事后更新价格算提前可见；区块末 state 给同块更早事件充当交易前状态。

库存模型分别记 Base/Arbitrum 可用、垫付待退和缓冲，不互借同一余额。退款时间不能逐单核验时，以 3/6/12/24 小时明确假设作敏感性，缺少依据就不输出“实测周转率／已验证净日收益”。自然反向填单也须有已可用退款，不能把尚未到账债权继续花。单向失衡需要的桥／调仓成本首版列盈亏平衡预算，未取得可执行报价时保持未知。

目标可分别看独立200／日和组合贡献20／50／80／日，都是验收阈值。所选条款收入上限按每笔独立候选 `max(0,inputAmount-实际outputAmount)` 求和；不接的负差额单不能抵消可接正单。对有成交单使用实际付款，对未成交原始条款只给该条款的差额；多条公开有效更新若后续研究，按单取当时可见条款的最好上限，不能逐更新累加。

首版实时未研究更新执行时，原始条款上限不足只能否决**原始空message条款／本采集接入方式**，不能否决加价更新或整个Across。未知实现、未匹配成交、prefill起点不足、非空message和缺覆盖都独立列未覆盖容量。只有明确子集完整覆盖且其逐单非负收入上限不足，才停止这个子集；上限足够只表示值得检查少量具体订单的退款归属、实际 gas、库存恢复和竞争，不能宣布适合赚钱。

## 8. 实施验收

1. 两条链原生 USDC 身份、ABI/current implementation、bytes32转换和完整 relayHash 精确核验；旧/新事件、uint256大ID、prefill和同ID不同完整条款不会误配。
2. 独家与成交截止等号、暂停、状态1/2、slow fill、updatedOutputAmount、非空message排除正确；事件最后顺序不代替实时已知顺序。
3. 分页/拆区间完整零事件、RPC错误、截断与部分失败严格区分；游标不跳过缺口，目标链无填单须覆盖到截止，窗口尾单保持删失。
4. 退款叶地址/金额长度一致；一笔交易多个事件不重复扣费；deferred及claim严格按Transfer核到账，用户退款和他链来源不能误归属为选定中继。
5. Base完整费用、Arbitrum不重复L1费用、字段缺失NULL、USDC量与ETH价格不经float，过期BBO不用于当前成本。
6. 先事实后capture；原批重试、跨批重复、ClickHouse合并前后计数与摘要一致，部分写入不可见；缺已提交成员报incomplete。
7. 两次成功一次失败同块尝试重组后全部旧分支不可用；源链孤块不留下可盈利候选；最终性与首次发现时间不被补采覆盖。
8. 用两个代表性单日真实输入检查压缩库占用、gzip证据占用、报告耗时、RPC成员数、实测轮询延迟和待补收据积压；合成结果明确标记。不用连续多天运行作为编码正确的前提。

先完成一个真实来源小窗口和上述必要校验，再回补30日及观察7日；没有可靠数据不继续扩大链数或接口。设计本轮不启动这些任务。
