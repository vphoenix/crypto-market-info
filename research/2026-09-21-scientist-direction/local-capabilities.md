# 技术套利研究：本地能力与最小试验

核查时间：2026-09-21。范围：仓库代码、部署配置源、运行文档和已保存的研究材料；本轮没有重新查询线上数据库或检查宿主机 PID。运行范围按当天维护的 `docs/runtime-operations.md` 描述，不能把仓库 HEAD 当作正在运行的二进制。

**现有工程没有持续采集 DEX 逐区块/逐交易状态，也没有 AMM 回放或 EVM 交易模拟。因此之前“已经抓了很多数据但没有找到机会”，不能用于判断链上技术套利是否缺乏机会；已抓数据主要没有覆盖这种问题。** 用户希望靠链规则、执行和技术研究获利，后续应先研究已发生的套利及自己能否复现，不再以存款、质押或 PT 年化排行榜为主线。

## 已有能力与真实边界

| 能力 | 实际情况与证据 | 对技术套利的作用 |
|---|---|---|
| CEX L2 连续性与采样 | `internal/exchange/{binance,okx,bybit}/`；`internal/sampler/` | 可供将来链上/链下对照，不提供链内交易顺序或竞价状态 |
| CEX 历史回放 | [replay.go:10](/home/ubuntu/crypto-market-info/internal/replay/replay.go:10) 的 `AtSecond` 应用价格键差量并检查 valid bitmap | 精确恢复所保存每秒前 10 档/历史 50 档；没有恢复被截掉的深层盘口，也不是池子/EVM 回放 |
| 数据模型 | [model.go:163](/home/ubuntu/crypto-market-info/internal/model/model.go:163) 的 BookDelta 与 MinuteBook 只含分钟/秒/价量；[schema.go:84](/home/ubuntu/crypto-market-info/internal/storage/clickhouse/schema.go:84) 对应两表 | 没有 chain ID、pool、交易序号、log index、swap、tick、bin、receipt、trace 或状态差量模型；不能直接复用盘口行冒充链上事实 |
| EVM 固定区块读取 | [Avalanche client.go:77](/home/ubuntu/crypto-market-info/internal/yield/avalanche/client.go:77) 每次获取最新 finalized，随后按 blockHash＋requireCanonical 读状态，并严格核验完整批次 | 同区块锚定、整数解码、hash 与重试思路可借用；不是通用区块索引器 |
| RPC 客户端限制 | 该 client 硬编码 chain ID 43114，最多 40 个读调用，data 仅接受零/一个静态 ABI 参数；[client.go:157](/home/ubuntu/crypto-market-info/internal/yield/avalanche/client.go:157) 拒绝 10 分钟以前锚点 | 不能直接用于 Ethereum、历史区块、复杂 router/quoter、完整交易模拟；不应为新研究放宽现有收益采集的校验 |
| AVAX 已采字段 | [BENQI staking.go:25](/home/ubuntu/crypto-market-info/internal/yield/benqi/staking.go:25) 读兑换率、TVL、限额、费用、暂停、冷却/申领窗和现金；观测保存 hash、高度和 finalized | 可用于协议兑换规则核验；没有 sAVAX DEX 成交、池余额/bin 分布或实际规模报价 |
| 采集频率 | [app.go:138](/home/ubuntu/crypto-market-info/internal/app/app.go:138) 的 AVAX 分支每小时；SOL/TRON 每 6 小时；[运行说明](/home/ubuntu/crypto-market-info/docs/runtime-operations.md:73) 同样登记 | 无法观察一个区块或一笔交易制造并消除的套利窗口 |
| 精确数值与入库可靠性 | [decimal.go:11](/home/ubuntu/crypto-market-info/internal/model/decimal.go:11)、[runner.go:36](/home/ubuntu/crypto-market-info/internal/yield/runner.go:36) | 严格定点/整数路径、完整性校验、原批次重试可复用；按源事件运行需专项 Runner |
| 一次性链上报价研究 | `research/2026-09-16-opportunity-review/pendle_quotes.py` 和 `research/2026-09-21-threshold-review/pendle_probe.py` | 是保存 HTTP 报价的研究脚本，没有逐块历史、节点独立模拟、广播或持续执行能力 |

在 `internal/`、`cmd/` 和研究脚本中检索未发现已实现的 `eth_getLogs` / receipt / trace 索引、AMM Swap 事件解码、tick/bin 状态维护、历史前缀交易回放、anvil/revm/foundry 模拟或真实利润账本。Uniswap、Curve、LFJ 出现在建议文档中，不等于适配器已实现。只读收益 RPC 的 `eth_call` 不代表已有完整套利交易模拟。

## 建议只开一个可否证试验

**建议研究范围：Ethereum，一组经过真实部署与活跃性核验的 Uniswap v3 与 Curve 池，资产仅 USDC、USDT、DAI；WETH/ETH 只先用于 gas 计价。** 池白名单上限约 4–8 个；先只实现白名单实际存在的 Curve 版本，不能把不同版本或包装资产全部放进同一公式。此选择是为了限制状态机数量和资产身份复杂度，并非声称 Ethereum 最赚钱。若节点无法读取必要历史状态或 trace，先记录基础设施缺口；不能因为另一个 RPC 返回了“价格”就绕过可复现性要求，也不能未经比较就断言 L2 更赚钱。

这个范围先回答：**其他地址在这些池之间，是否反复完成同一资产起止、扣掉链上可识别费用后仍有利润的闭环；其中哪些在我们能够获得的信息和位置下仍有利润？** 不以池 APY、TVL 或价差截图作为成功条件。

### 第一步：已有赢家的证据，先取最近完整 24 小时

1. 保存白名单池的合约身份/版本、币地址与 decimals，取覆盖该时间窗的连续区块头及完整 Swap/流动性事件。事件身份为 `(chain_id, block_hash, transaction_hash, log_index)`，保留 transaction index。逐块校验父哈希、缺块及去重，原始响应保留 hash；不要把采集缺失当无交易。
2. 对涉及两个以上目标池、形成币种闭环的交易拉完整 receipt；对最多约 50 个候选再补 call trace / state diff。日志初筛可以很便宜，缺 trace 或资产流不闭合时只能记“疑似”，不能确认净利。白名单外的腿须补全或标成无法估算，不能丢掉后恰好得到正利润。
3. 建“交易级现金流表”：公开执行地址/收款地址、路径、输入输出整数、flash loan 本息、gasUsed×effectiveGasPrice、可识别的显式 builder/coinbase 支付，以及交易前后资产差额。资产转入、借款本金、LP 出入金、领取奖励不能算套利利润。涉及自有地址间转账时逐条展示归属依据，不猜真实个人身份；无法观测的链下付款单列未知。
4. 产出每类机制的真实正净利交易清单、不同日期/区块出现次数、每笔使用资本、净利分布、执行地址集中度和区块内位置。先回答“谁的哪种程序在赚什么”，不要先堆更多采集来源。

24 小时仅用于验证数据及分类方法，不推断年化。方法通过后扩到至少 7–14 个完整日，并检查成功交易之外的同类失败交易和空白时段；历史赢家利润只是后来研究的上界线索，不属于我们已经能赚的利润。

### 第二步：精确重建，解释一笔为何赚钱

对少量已确认交易，拿父区块状态并依顺序应用目标交易之前的交易，恢复它实际执行前的状态。只拿块末状态或给 RPC 一个 blockNumber，不能自动等价于区块内该位置。对目标池建立状态校验点，再按事件更新；用节点执行结果核对至少一个真实交易的每腿整数输出、余额差和 gas，而不只核对价格。

Uniswap v3 需要准确的 tick/流动性状态及舍入；Curve 需要相应版本的余额、费率、放大参数及汇率状态。外部 oracle/包装兑换率、代理实现或其他合约影响交易时，池事件不能独自完整重建，须取其历史状态或用节点 EVM 作为最终校验。所需 tick/bin 范围必须覆盖报价金额真实经过的区域，不能用合成十档替代。

建议最初只保存这批研究的原始证据与定类型离线表，不先改生产服务或扩建通用平台。长期运行时再为 `chain_block`、`dex_pool_state/event`、`transaction_outcome`、`simulation_result` 建专项定型模型与表，不能塞进 yield_observation 或通用 JSON 大表。

### 第三步：从他人利润转成我们可得的份额

以观测发生时间为边界，只使用当时确实可见的信息生成候选。先做保守、容易复现的实验：本地看到区块 n 后，测量接收、解析、报价和模拟耗时，然后在 n+1 的实际状态/可行插入位置尝试同样机制。不得删掉真实赢家交易后假设自己会赢；可以另存“同位置替换”的理论上界，但必须与保守基线分列。

这会主动排除需要私有订单流或仅在当前区块内存在的窗口。将来若研究这类机会，应另外保存 pending/order-flow 的真实到达时间、可获得渠道和提交时延；不能用事后完整区块冒充当时看见了私有交易。

每个候选保存交易前状态 hash、发现时刻、可用区块、具体路线、输入金额、预期与实际回放输出、模拟是否 revert、gas、gas/竞价假设、机会被哪个交易消耗，以及自己的重复/冲突候选。金额可测 1 万、5 万、10 万、25 万、50 万、100 万 USDT 等值，但同一个机会只计一次；不按本金把利润线性扩大。首期不上链、不签名。

影子模拟不能验证真实打包率。结果应依次区分：历史他人已实现净利、我们能及时看到的机会、延迟后仍可模拟成功的净利、在不同竞争支付/打包概率下的可得收益区间。未测的成功率不填成 100%，不能先承诺年化 4.5%。

## 最小交付物与停止条件

第一份有价值的结果应是一张可复算的表：`机制 → 每日独立机会数 → 历史赢家净利润 → 我们延迟后剩余利润 → 竞争成本 → 所需资本 → 数据置信度`，附几十笔交易证据；不是全链百万条价格数据。

如果 100 万是总预算、目标净年化 4.5%，对应年净收益约 45,000 USDT，日均约 123.29 USDT。但应从实际可获得的利润累计判断，不把一次闪电贷套利或小额机会的高资本周转率外推到 100 万全额部署。稳定性的评价是跨天重复、含失败和空闲后的利润，而不是每笔折算出巨大年化。

可尽早停止或换研究分支的结果包括：历史闭环识别大多是假阳性；扣真实可见费用后没有利润；利润只在我们无法获得的区块内位置存在；延后一块全部消失；正利润高度集中在一个异常日；必要历史状态不可取得。每种结果都说明下一步该补什么，避免无方向地增加交易所、币种或理财数据。

本轮仅新增本文，没有修改业务代码、采集配置、数据库或服务；没有声称所提范围已找到可执行套利。
