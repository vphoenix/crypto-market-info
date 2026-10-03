# Reserve 拍卖与篮子申赎：最小采集设计

设计日期：2026-10-02。采集核心、五表和独立服务已上线；本文件保留目标设计，实际首版范围以[实现说明](reserve-data-implementation.md)为准。三份CSV、窗口分析和历史权限复原暂缓。设计独立审核见 [0011](../discuss/0011-reserve-data-design-review.md)，代码审核见 [0012](../discuss/0012-reserve-data-code-review.md)。

## 1. 交付目标与范围

只回答三个研究问题：公开可参与的交易发生得够不够多；按真实数量买入并处置全部收到的资产后还有没有价差；这些候选是否值得进一步做少量完整交易模拟。首版交付采集程序、定类型数据库与 CSV/JSON 报告。毛正报价不被描述为可获得利润，历史事件量不被描述为自己可接的订单量。

**规模限定：Ethereum 主网、最多 3 个 Folio、每个篮子最多 8 个成分、仅已核验的 r5.0.0 实现、5,000/20,000/50,000 USDC 三档参考金额。** 先从官方基金目录人工选地址放入一个 manifest，再用 RPC 验证；不实现自动全市场发现。只扩一个实际缺失的版本或交易场所时再修改 manifest/适配器。没有符合条件的对象就报告覆盖不足，不无限增加对象。

报价以 USDC 结算，另抓 USDT↔USDC 和购买 gas 币的有数量成本参考。用户预算是 100 万 USDT，不能将 USDC 当作严格等值 USDT，也不能把 USDC 往返参考直接拼成已验证的 USDT 闭环。首版优先研究一笔交易内归还所有波动币的路线；不设计长期币仓或低杠杆对冲执行。

实现为一个 `cmd/reserve-data` 程序，复用项目 Go、ClickHouse、整数数值和 RPC 工具。独立研究库 `crypto_market_info_reserve`，同时仅一个写进程，避免与现有生产 collector 共用运行生命周期。没有 Kafka、Redis、Web 服务、通用策略引擎、本地全链索引或自动交易器。原始证据保存本机压缩文件，数据库保存可查询的事实。

## 2. 已核对的协议细节

本次核查固定源码为官方 `r5.0.0`，commit `241f0d244c45a311a4cd2af2c383a4f3bbe56265`。不使用随时变化的 main 作为 ABI 真相。来源及文件 SHA-256 记录在 [source-manifest.json](../research/2026-10-02-reserve-design/source-manifest.json)。

- 当前版本 `getBid(uint256 auctionId,address sellToken,address buyToken,uint256 maxSellAmount)`，返回实际 sellAmount、bidAmount、D27 价格。sellToken 是基金卖出、竞买人收到的币；buyToken 是基金收到、竞买人支付的币。旧说明页中的 timestamp 参数不能照搬。
- `getRebalance()` 返回的 `bidsEnabled_` 控制当前再平衡；全局 `bidsEnabled()` 控制后续配置，不能只读取后者。开拍受限期与当前拍卖是否可投标分别判断。`availableUntil` 限制开新拍卖，不能据此截掉仍在运行的拍卖。
- `totalSupply()` 已包含待分发的费用份额；`totalAssets()` 包含合约认可的 trusted fill 余额，不能只读各 token 的 `balanceOf(folio)` 代替。`stateChangeActive()` 的同步/异步标记为 true 时，不使用该份额状态生成有效经济判断。
- 铸造使用 `toAssets(grossShares, Ceil)`，收到的净份额要扣 mint fee 和 DAO 最低费用；赎回使用 `toAssets(shares, Floor)`。DAO fee registry 参数及向上取整都会影响最终份额。赎回必须包括完整篮子，并保持合约返回的原始 token 顺序。
- 最新拍卖通过 `nextAuctionId()-1` 和 `auctions(id)` 读取；无拍卖、迁移遗留或 nonce 不匹配须显式标记。仅在已核验 r5 范围使用这条规则，未知/旧版历史只保留日志证据。

mint 费用按固定版本整数逻辑复现：`t=ceil(grossShares×mintFee/1e18)`，`d=ceil(t×daoNumerator/daoDenominator)`，`m=ceil(grossShares×max(daoFloor,MIN_MINT_FEE)/1e18)`，`feeShares=max(t,d,m)`，`netShares=grossShares-feeShares`。DAO denominator 必须非零；使用大整数并逐步复现源码的 Solidity 溢出检查，checked 乘加的中间值超出 uint256 也必须失败，不能只检查最终商；`Math.mulDiv` 则按该函数的宽乘积/舍入语义处理。pending fee shares 已在 totalSupply 内，不再重复加。

[固定 Folio 源码](https://github.com/reserve-protocol/reserve-index-dtf/blob/241f0d244c45a311a4cd2af2c383a4f3bbe56265/contracts/Folio.sol)、[固定 IFolio](https://github.com/reserve-protocol/reserve-index-dtf/blob/241f0d244c45a311a4cd2af2c383a4f3bbe56265/contracts/interfaces/IFolio.sol)、[官方部署目录](https://docs.reserve.org/core-components/index-dtfs/smart-contracts)。部署目录仅作为发现线索，仍核验链 ID、代理实现槽、代码 hash、版本和依赖身份。

## 3. 程序的三个动作

建议命令形态如下，仅为待实现接口：

```text
reserve-data backfill --days 30 --manifest config/reserve-research.json
reserve-data watch --manifest config/reserve-research.json
reserve-data report --from ... --to ... --out var/reserve-reports/...
```

RPC 和数据库凭据沿用环境变量，不写入 manifest、日志或证据 URL。启动时先做链身份、版本、hash 固定 eth_call、报价池、最终性标签能力探测，再进入采集。`report` 默认只读数据库和本地证据，不隐式重抓或发交易。可以用 `--once` 完成一个采样批次，支持实现验收。

### A. 回补最近 30 日：确认有交易需求

只对 manifest 中 Folio 地址抓全部日志，覆盖 AuctionOpened/Bid/Closed、TrustedFill、Rebalance、费用/篮子变化、ERC20 Transfer 和升级等事件。新/未知事件保留原始 topics/data；解码失败不静默丢弃。无需监听全链 Transfer。

`eth_getLogs` 每次最多 512 个高度，服务端拒绝/达到已知条数上限时二分区间。全部分片成功才把区间标为日志完整；成功空数组计为完整零事件，错误/null/疑似截断计缺失。保存区间端点 hash 和最终性锚点，事件各自包含精确区块 hash/交易与日志序号；核验每个有事件高度的 header。历史扫描只推进到 finalized 范围。

对有经济动作的交易抓 transaction 与完整 receipt，包括同一交易内的所有代币 Transfer、Swap 和协议日志；按 txHash 去重，避免一个交易多次投标重复计费。普通用户份额转账不逐笔抓 receipt。对每个历史事件块读取实现身份；不支持的版本明确列入版本未覆盖统计。

区块末尾的实现版本不一定适用于该块升级前的日志：检查 Upgraded/代理升级相关日志及父块实现；若同块发生升级，首版整块保留 raw 并将协议解码标 `upgrade_boundary_unknown`，不套块末 ABI。未取得完整版本证据也保持 unknown。历史是否公开可投标，仅在完整顺序事件链能恢复对应 nonce 的 `RebalanceStarted.bidsEnabled`、停用状态和拍卖时间时标已知；窗口起点缺少前置上下文或日志有缺口就 unknown。不能由 AuctionBid 出现或今天的配置推断历史权限，也不能由区块末 getBid 成功推断当时能 bid。

这一步输出开拍数、投标数、金额分布、拍卖开放到首投时间、交易地址分布和可见 gas。Transfer(from=0/to=0) 不能直接认作用户 mint/redeem，因为费用份额发放也会 mint；需要 calldata/同交易资产流辅助确认，不确定就 unknown。

**历史首版不承诺赢家净利润、不做逐交易前状态重放。** 交易外观测的 ERC20 收支只是可见现金流，sender 不一定是实际受益人；缺少 trace 的原生 ETH 支付/私下排序费和跨钱包持仓保持未知。也不能用区块结束状态给同一块里较早的赢家交易补报价。

### B. 实时采样：获取可比较的金额报价

轮询 head 每 2 秒一次。追踪白名单日志覆盖所有新区块；行情与状态仅按以下频率抓取：

| 场景 | 状态与报价频率 |
|---|---|
| 无活动拍卖 | 每 60 秒一个最新块；日志中的开拍/篮子/费用变化触发提前采样 |
| 活动或即将开始的拍卖 | 每个新区块，直至拍卖关闭/过期 |
| 篮子申赎首次出现毛正候选 | 临时逐块采样 10 个块，观察下一块/第二块是否仍存在；到期后恢复 60 秒 |

每轮单线程规划，复用 HTTP 批处理，最多 20 个 RPC 成员/请求、2 个请求并发、总共 300 个 RPC 成员及 10 秒采集预算。同一 Folio 不重叠采样。超预算的路线留下 `skipped_budget` 及计划计数，按固定轮转顺序避免某基金长期饥饿；不无限补齐旧报价。实施时用 `--once` 实测是否需要减少对象/金额档，不先扩建调度系统。

每个 snapshot 的所有 eth_call 固定 **同一个 blockHash，requireCanonical=true**，完成后再检查 hash。收到新区块、完成采样、每条报价实际可用的时间均保存。开始时未知当时能否完成的路线不能用之后的补采伪造实时可见性。补采可修复历史事实，但 capture_mode=backfill/research，不能改写原始 available_at。

### C. 三种路线分别抓完整金额链

1. **拍卖：USDC → buyToken → 投标换 sellToken → USDC。** 先对篮子有向 token pair 用 getBid 探测可用数量，再保留最多 6 个全局可投标 pair（不足覆盖明确报告）。对三档目标金额读取实际投标数量、支付量、D27 价格；入口为精确买到所需 buyToken 的 exact-output 报价，出口为卖出实际 sellAmount 的 exact-input 报价。两个 token 中有 USDC 时相应腿为 identity，无手续费。
2. **折价赎回：USDC → 买份额 → redeem → 所有成分卖回 USDC。** 先取三档 USDC 的份额 exact-input 报价，再按实际份额数量逐档 `toAssets(Floor)`；每个非零成分取实际数量的卖出报价。任何成分缺失，完整路线为 incomplete，不能把不可卖部分按标记价格加回。
3. **溢价铸造：USDC → 买齐篮子 → mint → 净份额卖回 USDC。** 用单位份额成本估计三档 grossShares，逐档 `toAssets(Ceil)`，精确计算 DAO/mint fee 后的 netShares；买齐每个非零成分使用 exact-output，净份额卖出使用 exact-input。保存实际总成本，超过目标预算标 `budget_exceeded`，不把金额档标签当作实际投入。

拍卖同样由单位币报价估计 maxSellAmount，再保存实际支付成本；允许合约容量把数量截小，真实成本超过目标就不计入该预算的候选。首版不做连续金额优化。对未知 decimals、fee-on-transfer/rebase 等未核验转账语义或未知实现，停止该对象的报价判断；保留原因。

成本参考作为 quote 的独立 `cost_reference` 行：USDT→USDC 与 USDC→USDT 各取 10 万/100 万输入档；购买 WETH 取 0.001/0.01/0.1 ETH 的 exact-output 报价。报告的 gas 情景只在已有同块报价金额覆盖范围内使用匹配档或更大一档的成本上界，上档时标 `conservatively_bucketed`；不能取下档、线性缩放或把相反买卖方向混用。超过最大档则 unknown。WETH unwrap 的调用 gas 仍包含在完整 gas 情景内。这些参考不与其他共享池腿拼成已执行的稳定币闭环。

DEX 首版只接**白名单 Uniswap v3 路径**，每资产最多 2 条路径、每条最多 2 跳。Quoter 以指定 hash 查询，返回费用已包含在成交数量内，不再重复扣池费。manifest 固定 factory、quoter、pool、token、fee、decimals 和代码身份；不接会回退到 latest 的聚合器。动态 ABI 用一份固定 JSON ABI 解码，建议引入一个成熟 ABI 库，避免手写嵌套 tuple/数组解码器。

**不同腿共用同一池，独立 Quoter 报价不能直接相加。** 首版保存每腿 pools 和顺序，标 `indicative_overlap`，只用于发现线索；没有重叠、同 hash 完整报价也仅标 `quoted_complete`，未证明完整调用可执行或能抢到交易。篮子常共用 WETH/USDC 池，因此报告必须给完整无重叠覆盖率；低覆盖应促使下一步只补一项联合模拟能力，不能报告“Reserve 没有利润”。

## 4. 最小代码划分与复用边界

```text
cmd/reserve-data/main.go           三个子命令、配置、退出
internal/reserve/model.go          capture/state/route 的定类型结构
internal/reserve/folio.go          固定 r5 ABI、状态/费用/事件解析
internal/reserve/collect.go        回补、轮询、采样和完整性检查
internal/reserve/report.go         覆盖、事件、候选窗口及成本情景
internal/storage/clickhouse/reserve_*.go  三新表、批写、读取
config/reserve-research.json       小型固定白名单（实现时填真实地址）
```

复用 `dex.Address/Hash`、UInt256/Decimal、RPC envelope 严格校验、EIP-1898、压缩证据及 ClickHouse 重试。旧 `ethereum.Header/Logs/Quotes` 绑定 Sky manifest；旧 `WriteDEXBatch` 强制 Sky 报价成员形状，不能直接调用来写 Reserve 批次。只给必要底层助手增加显式地址/manifest 参数，或在 Reserve 模块写薄包装；不在此任务重构全项目链适配架构。

首版 Ethereum-only，因此复用 receipt 结构不会漏掉 L2 L1-data fee。如果以后加 Base，必须先扩费用字段/解析和链身份校验，不能只改 RPC URL。

## 5. 数据库：3 张新表，复用 2 张现有事实表

全部放在独立研究库，复用的两张表仅复用定义/写入工具，不跨库依赖生产表。DDL 草案见 [reserve-data-schema.sql](reserve-data-schema.sql)。静态资产/ABI/路径小元数据存在有 hash 的 manifest 中，不再设计基金表、资产表、路由表和任务表。

| 表 | 一行含义 | 主要内容 |
|---|---|---|
| **reserve_capture** 新 | 一次日志区间扫描或一个同块采样批次的完成标记 | 范围/hash、模式、首见/完成时间、finality/canonical、各类计数和摘要、覆盖状态 |
| **reserve_folio_state** 新 | 一个 Folio 在一个指定块的完整状态 | 实现/版本、费率、DAO 费参数、份额供给、完整篮子、当前 rebalance/auction 与权限 |
| **reserve_route_quote** 新 | 一个块、一条路线、一档金额的全部报价 | 精确协议数量、每条 DEX 腿输入输出/路径/池、份额和篮子向量、失败与重叠原因 |
| **dex_log** 复用 | 一条原始链上日志 | 地址、topic、data、blockHash、txHash、txIndex、logIndex、payload hash |
| **dex_tx_receipt** 复用 | 一笔交易及收据事实 | sender/to、gasUsed、effectiveGasPrice、calldata/完整收据证据 hash |

state 中篮子与当前再平衡均为同一 Folio、同一块的状态，适合一行定类型数组；不是通用 JSON。大字段原始证据不塞进表。静态 manifest 不够表达的新动态协议语义应扩定类型列，不能用键值 JSON 兜底。

所有金额/数量为 UInt256 原子单位，D18/D27 比率按原整数保存。地址 FixedString(20)、hash FixedString(32) 保存二进制；未知数为 Nullable，不以 0 表示失败。数组空且 state_complete=false 表示缺失，成功空必须通过协议不变量检验。原生 gas 费用使用整数 wei，计算时 big.Int；最终展示使用 Decimal。quote 表只存事实，不把猜测 gas、年化、对手利润固化成“市场数据”。

### 身份、提交与恢复

- `manifest_hash` 包含 ABI/tag、代码白名单、精确对象/路径/规模和采样规则。变更后为新 manifest，旧数据仍能按原 manifest 解释。
- `capture_id = SHA256(chain, manifest, capture_kind, capture_mode, from_number/hash, to_number/hash)`；单块采样 from=to。相同锚点重新从 RPC 取一轮时构造新的 `batch_id`，整轮的数据库重试必须复用同一 batch 和逐行值。
- quote_id 由 capture/batch、folio、route_kind、auction/pair、金额档、路径 ID 确定；日志仍以 chain/blockHash/txHash/logIndex 去重，收据按 chain/blockHash/txHash 去重。
- 复用的 `dex_log` 物理排序键含 manifest/batch。回补区间重叠、live 与 backfill 都出现的同一事件，在 activity 报告中按 `(chain_id,block_hash,tx_hash,log_index)` 做逻辑去重；不能直接 count 物理行。跨批相同事件内容冲突则标数据错误，不能任取一条。
- `dex_tx_receipt` 是跨批次共享的事实，**不按 receipt.batch_id/manifest_hash 关联 capture**。单 writer 先查询已验证行，存在即复用，首次完整成功才写入；不因重抓更新其 available_at 或所属批次。receipt_members 对已引用的 `(chain_id,block_hash,tx_hash,receipt_hash,calldata_hash)` 排序后取 hash，不复用旧 `dex.ReceiptDigest` 的整行 hash。capture 的证据清单保存期望/实得 tx 集合，actual_receipts 计可用事实数而非本次 INSERT 行数。新采集响应时间单独存在该次证据，首次成功 receipt 的 available_at 保留。固定 hash 下内容冲突标错误，不覆盖先前事实。
- 先完成证据落盘，再批写 state/quote/log/receipt，最后写 capture 提交行。capture 带每表 distinct 计数及成员摘要；失败不会得到完整标记。提交行成功但响应超时，重复相同批次即可。
- 新抓取尝试与数据库重试区分：新尝试有新 batch 和真正 available_at。完整报告只选最新成功提交的批次；保存失败尝试及原因。历史完整批次不能因新失败被当作“最新报价”，按实际时间展示。
- coverage 值严格为 complete/partial/missing/not_requested，彼此独立：日志已完整而 receipt 尚缺失时可统计活动数，不能统计“完整成本”；snapshot 报价计划中失败成员仍有占位行和原因，`actual_quotes==expected_quotes` 不等于所有报价成功。capture 的 plan_hash 指向有固定成员顺序的计划证据，记录限额跳过、动态无效 pair 和未支持路径的数量及原因。
- `ReplacingMergeTree` 只负责物理去重；查询必须 FINAL/argMax，并检查引用 batch 的计数与摘要，不能等后台合并后才正确。
- canonical/finality 更新追加 capture revision，先取每个身份的最新 revision 再过滤；不能先筛 finalized 导致旧的失效记录复活。重组更新旧块 capture 为 canonical=false，再采新 hash。默认统计只用 canonical finalized。前台 head 仅作及时线索。
- 重组失效覆盖旧 blockHash 的**全部** capture/batch，包括同锚点多次成功、失败尝试及覆盖该旧分支的日志 range；不能只失效当前选中的 batch。每个 batch 先按 revision 取最新状态，再从仍 canonical 的已提交批次中选取，避免退选时重新捡起旧分支。
- 最近未最终确认的 capture 定期与 canonical header/finality tag 复核；重启从数据库中的未确认尾部恢复。finalized hash 冲突暂停该研究进程。断档 quote 不补成 live；日志从最后连续完整区间继续补。
- 日志 range capture 只能报告日志覆盖，不能充当状态或报价采样。receipt 补齐通过新批次/提交修订记录，原始抓取时间不变；日志完整、receipt 部分缺失可以推进日志游标但必须继续单列 receipt 缺口。

## 6. 报告能得出什么结论

报告固定生成三份 CSV 和一份 JSON 汇总：

1. `coverage.csv`：按基金/日期列期望与实采样本、日志区间缺口、失败/未覆盖版本/未覆盖 venue/共享池占比、延迟；60 秒采样不能声称覆盖秒级事件。
2. `activity.csv`：真实拍卖/投标/受托成交事件、完整篮子变更、首次投标延迟、可见 gas。事件引起的观察偏差和无法归属的现金流明确保留。
3. `candidates.csv`：三类路线逐档的 USDC 输入/输出、毛差额、所需启动资本、是否全腿完整/重叠、1/2 块后同一机会的变化；没有后续块数据则标 unknown。连续多个块、多个金额档属于同一候选窗口，不能每天求和当利润。

窗口分组使用 folio/route_kind/auction_id/token_pair/固定路径身份，金额档不重复计事件。正样本前没有连续有效的非正样本时，开始时间仅为首次观测时间；跨缺块、限频跳过、版本变化、60 秒未采区间不连接成连续持续时间，也不在恢复时计成“新出现机会”。报告区分已确认新窗口与起点/连续性未知的正片段。

“成本后仍正”只在明确的成本情景下输出。完整交易 gas 尚未知时，给盈亏平衡费用：允许总额外费用 = 完整报价毛差额；例如毛差额小于几十 cents 的路线可以优先淘汰，具体阈值由用户目标/实际成本决定。Quoter 的 gasEstimate 只描述报价过程，不能当作实际整条路径 gas。

若提供固定 gas 情景，保存单位 gas、priority fee、排序/失败摊销与费用来源；用同块 baseFee 计算所需 ETH，按预采的 USDC→WETH exact-output 成本匹配或取上档。无覆盖档则费用未知，离线 report 不隐式发 RPC。池费已包含、协议铸造费用已扣时不再重复扣；借用资金时另加借款费，且不默认存在免费闪电资金。没有完整 gas 证据不能标“已验证净利润”。USDT 结算成本与储备金继续单列。

原始记录允许事后按不同成本阈值重新筛选；首版不运行交易执行或自动对冲。下一阶段仅对重复出现、毛差额足够的少数样本做完整调用模拟和顺序影响检查，避免先建一个通用 EVM 回放平台。

## 7. 实现验收和停止条件

必要测试集中在会伪造收益的数据错误：

- 固定 r5 ABI 的 tuple/array、UInt256、舍入、DAO fee floor、待分配份额、全局/当前 bidsEnabled 差异；新代码 hash 不继续套旧 decoder。
- 同 hash、篮子顺序与非零成员完整；精确数量方向；预算超过/拍卖容量缩量不虚报容量；共享池被标记。
- 空日志与失败、部分 RPC、限频/截断拆分、采集超时不会复用旧报价；live/backfill 可见时间不同。
- DB 半批不可见、写入重试身份稳定、共享 receipt 重用后旧 capture 摘要不变；两次成功与一次失败采样后发生重组，所有旧 batch 都不可见；重启恢复及独立来源失败不污染其他 Folio。
- 选一个有事件和一个无事件区间，以及一个完整篮子样本，对照原始响应复算；未找到实际活动拍卖时保留该验收缺口，不能用 mock 宣称实网验证。
- 运行短时采样，测 RPC 成员量、耗时、证据 bytes、表压缩 bytes 和查询耗时；分别给平时/活动拍卖负载。按实测外推每日量，标明是外推，不冒充连续一天实测。

首先检查最多 3 个对象的 30 日活动和路径覆盖。若都无可参与拍卖、无份额成交池，或篮子大部分不在当前 venue 支持范围，报告原因并停止扩采；这代表首版研究集合/能力不适合，不代表整个 Reserve 不赚钱。若有活动但全部毛差额很小，可按成本上限先否定该集合；毛正较多再进入短期持续采样。

以上为原目标设计。实施已同步本项目数据字典和实际部署说明；设计目标与上线能力的差异见下节及实现说明，不以设计条款代替实网验收。

## 首版实现状态（2026-10-02）

采集核心和五张表已实现，见 [实现说明](reserve-data-implementation.md)。实施审核新增控制表定类型 `receipt_refs`，避免后来取得的共享收据改变旧批成员集合。有效manifest包含ABI/采样语义版本。首版查询提供完整批次和JSON摘要，暂缓本设计的三份CSV、候选窗口连续性和历史权限复原，不能输出已确认新窗口或年化。免费RPC历史缺口按失败范围保存，不以最近成功范围代替30天覆盖。
