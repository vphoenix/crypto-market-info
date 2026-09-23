# Ethereum 协议兑换套利：最小采集与判断设计

状态：设计稿 R2，2026-09-21；2026-09-22 已完成最小代码与五表创建，实际行为和验收见[实现说明](dex-arbitrage-implementation.md)，持续采集默认关闭。设计独立审核见[审核记录](../discuss/0009-dex-arbitrage-design-review.md)。

本设计取代此前研究稿的完整 ticks、逐交易 EVM 回放和多表平台方案。第一版交付只有两件事：**持续保存可核验的链上数据；按真实金额判断哪些兑换闭环存在价差、能承担多少成本、是否反复出现。**

## 1. 范围与判断边界

一期固定 Ethereum mainnet（chain_id=1），4个Uniswap v3策略池、2个成本参考池、Sky converter/LitePSM/wrapper；完整地址及2026-09-21核验依据见[白名单](../research/2026-09-21-scientist-direction/collection-spec.md#2-已核验的具体白名单)。实施时固化为一个随代码版本管理的小型manifest，不做自动全链发现。

| 池/模块 | 用途 |
|---|---|
| DAI/USDC 0.01%、0.05% | 两个市场分别与LitePSM比较 |
| USDC/USDS 0.3% | 与USDS wrapper比较 |
| DAI/USDS 0.3% | 与converter及PSM构成闭环 |
| USDC/USDT 0.01% | USDT总资金进出与结算成本 |
| USDC/WETH 0.05% | gas费用折算为USDC |
| DaiUsds、LitePSM、USDS wrapper | 即时协议兑换边；读取Pocket实际库存 |

第一版以**区块末、系统收到后可见的状态**发现候选。它不能发现或争取已在同一区块内被别人消除的价差。目标池日志和命中交易回执仍保留，方便查明是否存在这类漏掉的机制；没有正候选只能说明此范围和观察方式未找到，不能推断DEX没有套利。

第一版不实现钱包、签名、交易执行、闪电贷、清算、mempool/MEV-Share、跨链、Curve、本地tick数学引擎、通用策略图、完整EVM模拟平台、网页或消息推送。使用自有USDC资金作报价情景；100万USDT是总预算，不是假设每笔用满。

## 2. 最短可用链路

```text
Ethereum RPC（固定blockHash）
  → 窄适配器：区块/日志/回执、Sky状态、QuoterV2
  → 定类型校验、原始响应hash与压缩归档
  → 固定8条路径的金额报价采集
  → 现有ClickHouse批写（5张专项表）
  → dex-check只读查询：价差、成本情景、重复窗口、CSV/终端报告
```

用现有`cmd/collector`增加一个默认关闭的独立分支；共用现有ClickHouse连接、HTTP限频/重试基础设施。Ethereum协议解析另写窄client，不能复用写死43114和finalized的Avalanche client。业务公式在`internal/dexarb`，RPC适配器不判断套利。

只增加`DEX_ENABLED`、`DEX_ETH_RPC_URL`、`DEX_EVIDENCE_DIR`三项采集配置；地址、费档和输入金额在版本化manifest中。RPC地址可能含凭据，日志/证据只记脱敏source_id，不保存URL中的凭据。

原始响应使用本地按SHA-256寻址的gzip文件，先原子落盘再提交表；只有证据引用，业务数据不使用JSON大表。无需Redis、Kafka、新数据库或独立采集服务。分析CLI不写生产库。

## 3. 怎样持续采

### 3.1 每个新块

1. 每2秒轮询最新区块头，只对新hash开始一轮；记录区块时间、首次本地接收时间，沿parent_hash核对连续性。轮询增加的延迟原样计入报告，不宣称低延迟抢跑。
2. 固定锚点`{blockHash, requireCanonical:true}`。取白名单合约完整日志、Sky必要状态和当前区块base fee；校验chain_id、ABI长度、RPC响应id集合及来源完整性。
3. 对8个固定方向、7档USDC金额逐条生成QuoterV2调用。每条路径只有一条v3腿，其他腿按该块Sky状态计算。所有引用必须同hash；禁止把旧报价换时间戳复用到新块。
4. 取命中日志的交易及完整receipt，校验blockHash、txHash/index、status和费用。日志量很小，先不做全链交易或trace索引。receipt原文保存全部外部腿日志，热表只保存所选合约日志及交易摘要。
5. 批量写各事实表，最后写`dex_block`的完成标记和成员hash。失败的金额档也有明确状态；未完成的事实不进入有效分析。
6. 复核同高度hash未变，周期性跟踪RPC的safe/finalized标签。收入分析默认只读已finalized且完整的历史，最新候选可以展示head但必须标未最终确认。

每个RPC批次最多20个调用、最多2个并发请求，单请求超时5秒；每个新块实时预算10秒。超时的成员记`rpc_timeout/deadline_exceeded`，不填旧值；预算和数量先在24小时试采中测量，不能用跳过采样的方式制造高覆盖率。Quoter调用较重，公共RPC能完成一次读取不代表能承担持续负载。

四个策略池各两个方向、七档金额，至多56条v3报价/块。参考报价另列为有界请求：每块1个1 WETH→USDC显示参考价，1个100万USDT→USDC入场容量价。collector不预猜任意成本情景；事后dex-check按§6.2可选只读RPC补取指定hash、实际金额的gas及结算报价，归档到报告目录，不写生产库。参考价只用于显示，不能冒称可执行成本。

### 3.2 漏块、重启、重组

- 新块优先。处理滞后或重启时，补齐遗漏区块头和白名单日志；历史金额报价不自动强制回补，记`quote_coverage=missing`，不能因节点现在可读旧块就写成当时及时发现。
- 一次最多补一个100块日志区间，和实时工作共用同一限频器；积压保存在区块覆盖信息中，不建立消息队列。历史receipt按有界队列在空闲时补，尚未齐全时`receipt_complete=false`。
- 读取同一高度出现不同hash时沿父链定位共同祖先，将旧分支标orphaned；原始证据保留，候选/统计排除孤块。hash和finality更新追加版本，不能只用高度覆盖。
- 每个数据库仍只有现有collector一个writer。写入重试复用同一batch_id和事实内容；崩溃后没有完成标记的残留行不可见，重采的新batch最后提交后才可见。
- 持续RPC失败只使DEX分支退避并标缺口，不停止CEX分支、不假造当前数据。finalized高度发生矛盾时暂停DEX分支并记录错误，人工检查来源。

## 4. 固定路线与精确计算

所有扫描以USDC开始并结束，默认输入为1千、1万、5万、10万、25万、50万、100万USDC；不是声称这些金额都能由100万USDT购得。报告用同块USDT→USDC规模报价检查预算，超预算档标记排除；金额优化、动态搜索图以后再做。

| 路线族 | 正向 | 反向 |
|---|---|---|
| DAI/USDC，各2个费档 | USDC→v3 DAI→PSM USDC | USDC→PSM DAI→v3 USDC |
| USDC/USDS | USDC→v3 USDS→wrapper USDC | USDC→wrapper USDS→v3 USDC |
| DAI/USDS | USDC→PSM DAI→v3 USDS→wrapper USDC | USDC→wrapper USDS→v3 DAI→PSM USDC |

### 4.1 v3使用官方Quoter，不自己重写AMM

`quoteExactInputSingle`在指定hash返回`amountOut、sqrtPriceX96After、initializedTicksCrossed、gasEstimate`。存精确输入输出及返回值、原始响应hash；每条路径仅一条v3腿，不拼接相互冲突的多个独立池报价。

Quoter可以跨tick读取真实链状态，因此第一版无需建本地tick库。代价是事后重新执行报价依赖RPC历史state；保存的报价可离线重算现金流，但不是完整离线EVM回放。报告明确区分这两种能力。

Quoter成功不证明执行账户的余额/授权、token转账条件、全额成交或整条路径可执行。对exact-input保守按请求的全部输入扣成本，不将未知的未使用输入计入利润；任何实际执行结论仍需后续完整路径验证。Quoter的`gasEstimate`只记录为报价内部测量，**不能用作完整交易gas**。[官方QuoterV2源码](https://github.com/Uniswap/v3-periphery/blob/main/contracts/lens/QuoterV2.sol)

### 4.2 Sky仅实现这三条兑换边

关键身份及getter已在[合约核验](../research/2026-09-21-scientist-direction/contracts-notes.md)确认。需要读的变化量：`tin/tout`、DAI.balanceOf(PSM)、USDC.balanceOf(Pocket)、Pocket→PSM allowance、相关依赖live/授权状态；保存`buf`作诊断，不当现金。不请求不存在的converter fee/paused或USDSJoin.live。

先检查`tin/tout==MAX_UINT256`方向停机，再进行Solidity同精度整数运算；溢出、下溢或未知版本记不支持，不做浮点修正。

```text
g = USDC原子输入，factor = 10^12，WAD = 10^18
sellGem输出DAI/USDS = g*factor - floor(g*factor*tin/WAD)

buyGem的参数是欲获得的USDC数量g：
所需DAI/USDS = g*factor + floor(g*factor*tout/WAD)
给定DAI/USDS预算A时，用整数二分找满足“所需 <= A”的最大g。
```

检查实际输出库存及授权。`buyGem`后剩余DAI/USDS dust单独保存、收益按零估值，不丢掉；DAI↔USDS按该版本1:1规则计算。普通入口收费，不借用NoFee白名单；不自动调用fill补库存。

每条候选使用独立的PSM临时状态副本，逐腿更新DAI库存、USDC库存和有限allowance。最后一族两次触及同一底层PSM，第二次必须使用第一次之后的状态；不同候选之间不共享变更，也不能把它们的利润直接相加。

## 5. 五张专项表

本节是计划模型，不改变已部署表。实现时再把DDL和编码同步到[存储数据字典](market-data-storage.md)。不复用CEX盘口或yield表；AMM金额报价不生成合成十档盘口。

通用类型：地址`FixedString(20)`，hash`FixedString(32)`；金额/费用原子值`UInt256`，有符号增量`Int256`；tick `Int32`、liquidity `UInt128`；UTC时间`DateTime64(6,'UTC')`，区块秒时间也可同型保存。应用用big.Int并严格检查ABI位宽。

所有业务行带`chain_id、block_number、block_hash、manifest_hash、batch_id、payload_hash`。manifest保存池/币/合约身份、decimals、费档、已核验ABI/bytecode及固定金额清单，以hash归档；升级/身份不符时该路线停止报价，更新manifest后开始新版本。

| 表 | 最小业务字段 | 逻辑键 |
|---|---|---|
| `dex_block` | parent_hash、block_time、base_fee_wei、fee_recipient、received_at、available_at、capture_mode、canonical、finality、revision；log/receipt/quote各自覆盖状态；expected/actual成员数与hash、batch_id、committed | chain+manifest+height+hash，追加revision更新 |
| `dex_sky_state` | tin、tout、buf、dai_cash、usdc_pocket_cash、pocket_allowance、vat_live、dai_join_live、join权限、身份校验状态、state_complete | batch_id+module_id（一期固定一组Sky依赖） |
| `dex_route_quote` | quote_role、route_id、token_in/out、quote_mode（exact_in/exact_out）、requested_amount_raw、amount_in/out_raw nullable、dust_dai/usds、v3_input/output_token及amount、sqrt_price_after_x96、ticks_crossed、quoter_gas_estimate；status/reason、available_at | quote_id=hash(batch_id+role+route_id+token_in/out+mode+requested_amount) |
| `dex_log` | tx_hash、tx_index、log_index、emitter、topics数组、data二进制、event_type、removed | batch_id+tx_hash+log_index |
| `dex_tx_receipt` | tx_hash、tx_index、from/to、type、value、input_selector nullable、calldata证据hash、status、gas_used、effective_gas_price、receipt_log_count、完整receipt证据hash | chain+block_hash+tx_hash；补采独立batch |

`dex_log`保存Ethereum标准日志的typed envelope和ABI bytes，按固定ABI在查询时解码；它不是塞业务对象的通用JSON表。交易原文/完整receipt压缩保存，包含白名单之外的腿；第一版不把trace、余额变动和每种事件再拆十几张常驻表。

`dex_route_quote`统一表达有明确路径/金额的报价事实：策略闭环、USDT入场、gas换汇用`quote_role`区分，合法字段由role约束，不能用空列冒充完整闭环。成本情景和派生净利润在查询计算并导出CSV，不再建结果表。

ClickHouse按UTC月分区；事实以hash和逻辑键去重，查询显式使用FINAL/argMax。`dex_block`必须**先按每个block逻辑键取最新revision，再过滤canonical/committed/finality**，禁止先筛旧canonical记录再取最新，否则orphaned块可能复活。finality或canonical更新沿用该块已提交的batch_id、成员数/hash，只改状态与revision；不得生成新的空数据批次。Sky/报价/日志按最新状态选中的batch_id连接，不能混用部分旧批次。receipt独立补采，仅当自身payload完整且其block最新状态仍canonical时可用，不反向伪造原来的接收时刻。

不同字段的覆盖彼此独立：日志缺失不一定使同hash直接报价失真，但使重复事件统计不完整；receipt缺失不妨碍报价筛选，却不能用于已实现利润分析。只有完全满足某报告所需覆盖条件的行才进入对应统计。

先保留全部实验数据，24小时测压缩字节/RPC次数/查询耗时后再定30天热窗口；本版不自动删数据、不加专用价格索引或物化视图。gzip文件与引用它的表至少同期保留。

## 6. 如何判断和展示机会

`dex-check`只读现有库，按时间、路线、资本及成本情景输出终端表和CSV。默认不访问RPC；明确启用成本补报时才使用配置的只读RPC，并把补报原文与typed结果保存到报告目录，不写生产库。

### 6.1 四个结论等级

| 结果 | 条件与含义 |
|---|---|
| `unknown` | RPC/状态/费用/预算等必要信息不足；原因明确，不能写0收益 |
| `quoted_nonpositive` | 指定状态、路径、金额的保守报价回收不大于投入；不外推其他金额/协议 |
| `gross_candidate` | 完整报价与已检查规则下，USDC回收大于投入；只是候选 |
| `scenario_positive` | 指定并公开全部成本假设后净值仍正；显示所用gas、tip、排序费和其他成本，不称已可执行或已赚到 |

源码/测试中不设置`executable=true`或“净4.5%合格”捷径；本版没有完整路径执行和真实纳入数据。若出现反复毛正候选，再对少量案例补人工trace/外部节点完整模拟，保存证据文件；它不作为整套采集启动前置条件，也不因此扩成交易系统。

### 6.2 收益与成本

```text
G = quoted_output_usdc - requested_input_usdc
N_scenario = G - gas_in_usdc - explicit_ordering_cost - other_costs
```

池费和Sky费用已在输出中，不重复扣。第一版不用闪电贷，其费用为“本情景不使用”，不是漏填零。gas单位和tip不是从Quoter推导：默认报告只给毛利及可承担总成本`G`；用户或研究者显式传入如`--gas-units 400000 --priority-fee-gwei 1 --ordering-usdc 5 --other-cost-usdc 0`才计算该情景净值。示例值无保守性保证，CLI把这些值和cost_scenario_hash印在每份报告中；未指定必要成本时只报预算，不默认填零。

base fee来自观察块，只代表该块成本情景；下一块可能变化，报告可按明确的上浮系数重算，不声称锁定。需要补回实际消耗的gas ETH时，使用**USDC→WETH exact-output**求购得`gas_units*(base_fee+tip)` wei所需的USDC；不能拿WETH卖出所得作为买入gas的成本。

没有对应实际金额报价时，CLI的`--quote-costs-rpc`模式仅对报告选中的正候选窗口补报，最多50个窗口、每窗口最多4个RPC调用；所有报价仍固定原blockHash，检查该hash当前canonical，复用相同参数的本次报告缓存。超预算、历史state不可用、exact-output不足或RPC失败时，成本标unknown，保留毛候选；禁止退回latest冒充历史同状态。只存1 WETH参考价的离线报告可显示明确标注的近似值，但不能据此升级`scenario_positive`。

补报记录`requested_amount/token/mode、required_input/output、block_hash、source_id、collected_at、payload_hash、原文引用`，与已有路线和cost_scenario_hash共同形成可重算报告。它只是在历史状态下计算成本，不补造当时的信息可见性。新支出的gas报价内部计算gas不会递归加到拟执行交易的gas预算中。

100万USDT先对应实际可取得的USDC，剩余gas准备金/资金进出成本列入全本金情景。反复使用已有USDC库存时，换币成本按一次入场/一次退出分摊；逐笔完整USDT闭环另算，不混用。输出USDC利润不自动写成USDT利润。

缺乏整个路径gas、失败损失、竞争支付或真实获胜率时，只输出成本预算和敏感性，不能用默认0做“净年化”。4.5%仍是后续整体经济门槛，对应全年45,000USDT；不是每笔几秒收益的年化。

### 6.3 不把重复看到的价差重复计钱

同一路线多个金额只取同一情景下的最佳可用金额。窗口定义固定为：同一manifest和route_id，连续canonical区块且报价覆盖完整，每块至少一个预算内金额毛正，构成一个连续毛正窗口。金额最优档、利润值、普通Swap/Mint/Burn变化都不拆窗口；只有完整扫描确认全部预算内档不正，才明确结束并允许下一次毛正成为新窗口。

状态/预算/报价缺失、换manifest或重组中断当前连续片段；之后恢复的正片段标`continuity_unknown`，独立新增机会数不增加。报告分别列“确认新窗口”和“连续性未知片段”。部分金额缺失但已有一个完整毛正档时可展示候选，窗口统计仍标覆盖不完整。成本情景只给每个毛正窗口的结果，不用不同情景反复拆分计数。

跨路径共用同一池/PSM的候选标共享资源。同一时刻先展示最大单条候选，其他作为互斥备选；不相加为可用容量。第一版不做投资组合调度，也不累计“每块假想赚一次”。

日报只给：完整覆盖比例、确认毛正窗口数、连续性未知片段、不同日期数、最大连续时间、最佳金额、每窗毛利/成本预算、成本情景下剩余值、缺口及观察到价差消失的时间。没有trace和顺序证据时不把消失归因于竞争者。即使两个确认窗口都有毛正，也不能据此证明我们可以连续抢到；**不输出未经执行验证的累计可得收益/APR**。

## 7. 最小代码改动与实施顺序

建议只增加这些位置，不先抽象通用链框架：

```text
internal/dex/ethereum/      RPC、ABI、Quoter/Sky读取、固定manifest
internal/dexarb/            路线金额传播、PSM整数规则、成本/窗口分析
internal/app/dex_runtime.go 现有collector生命周期中的独立Runner
internal/storage/clickhouse/dex_*.go  5表批写与只读查询
cmd/dex-check/              报告CLI
```

第一步：实现窄client＋同hash状态/金额报价＋原始证据落盘，先跑有限块离线验算。

第二步：接现有collector的5表写入、覆盖/重组/重试与日志/receipt；连续24小时验收。不开第二个同库writer，不改现有采集源。

第三步：完成dex-check成本情景、窗口去重与CSV；同一数据重复分析一致。连续记录7天后才评价重复性，并视证据决定是否值得补块内回放/订单流。实现时一次交付采集与报告，不把分析无限延期。

## 8. 必要验收

- 解析：精确ABI位宽、完整batch、空calldata、RPC失败/限流、不同hash混入均有明确行为；实时成功观测都有原文hash。
- 数学：sell/buy方向、非零费用、MAX_UINT停机、buyGem二分及dust、库存/allowance不足、两次访问PSM的先后更新；金额从未经过float。
- 报价：跨tick由Quoter按指定hash处理；同一块内改变slot0之外的流动性不能被错误缓存；失败不复用旧报价。报价gas不会被用作交易总gas。
- 数据：成功无日志与取日志失败可区分；缺块/重组后旧分支失效；先取最新revision再筛canonical，不能复活旧完成批次；部分写入、超时不知是否提交、重启重试不会重复计窗口。
- 分析：连续100块同一正价差不产生100份利润，普通事件/金额档变化不拆窗口；七个金额不相加；缺口、共享池/PSM、无USDT换汇证据、未指定成本时结论正确降级；RPC补报固定hash且用USDC→WETH exact-output，历史state缺失不换latest。
- 真实小样本：固定hash离线重算路径整数现金流，与所存输入/输出一致；至少各取一条有事件和无事件区间验证覆盖。记录24小时压缩体积、RPC耗时/次数、报告耗时；不预设数据量或盈利结论。

本文件保留设计时的范围；后续实施已新增五张专项表，具体见实现说明。CEX10/50档编码、既有运行服务和资金未由此次实施改动。
