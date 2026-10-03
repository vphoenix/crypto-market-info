# 科学家一期采集规格：Ethereum / Uniswap v3 / Sky 即时兑换

> 实施范围已按用户“不要过度工程化”要求收敛到[最小设计](../../docs/dex-arbitrage-mvp-design.md)。本文保留已核验地址、活动数据和完整研究字段作为参考；全ticks、逐交易EVM及全量字段不再是第一版前置条件。

2026-09-21。状态：**已核验合约地址、候选池身份和一段真实成交活动；以下是实施规格，尚未上线持续采集，也没有验证套利利润。** 延续用户100万USDT总资金、可重复、净年化4.5%以上的经济门槛。

## 1. 链、范围和第一轮交付

- 链：Ethereum mainnet，`chain_id=1`。一期专门研究同链单笔可闭合的兑换路径。
- 策略资产：USDC、DAI、USDS。USDT用于资金入场/利润结算成本，WETH用于gas成本换算；后两者先作为参考，不扩成全币种搜索。
- 执行规则来源：Uniswap v3四个候选池、Sky DAI↔USDS converter、LitePSM及USDS wrapper。另采两个v3成本参考池。
- 第一个问题：这四个池与三条协议转换边，是否曾形成多日重复的同资产起止、扣成本正收益闭环；这些机会是否在本系统可获得的信息与延迟下仍成立。
- 先完成24小时的事件/回执/状态一致性试采，方法通过后回补30天，同时保留实时接收时刻。历史验证通过再做7–14天前向影子检测。
- Curve需要按具体部署版本建立独立适配；Aave清算需要额外的仓位和oracle链路。这两者列下一阶段，不混入一期的承诺范围。Base、Arbitrum、Solana也不同时展开。

Ethereum的选择依据是已核实的协议转换部署和可读取历史EVM状态的研究路径，不表示竞争较少或利润最高。第一轮结果可能淘汰这些市场；交付标准是可信的利润与竞争分析，不是一定找到正收益。

## 2. 已核验的具体白名单

### 2.1 资产

| 资产 | Ethereum地址 | 原子单位小数位 | 用途 |
|---|---|---:|---|
| USDC | `0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48` | 6 | 策略结算资产 |
| DAI | `0x6b175474e89094c44da98b954eedeac495271d0f` | 18 | 策略与转换资产 |
| USDS | `0xdc035d45d973e3ec169d2276ddab16f1e407384f` | 18 | 策略与转换资产 |
| USDT | `0xdac17f958d2ee523a2206206994597c13d831ec7` | 6，采集启动时再校验 | 全本金及最终收益计价 |
| WETH | `0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2` | 18，采集启动时再校验 | gas换算参考 |

USDC/DAI/USDS小数位已按同一finalized区块调用`decimals()`核对；后两项为配置预期，未混称此次已经调用确认。币名称不能作为主键，所有路径使用链ID与合约地址。

### 2.2 四个策略池、两个成本参考池

通过官方Uniswap v3 Factory `0x1f98431c8ad98523631ae4a59f267346ea31f984` 的`getPool`查询，并在同一hash读取`token0/token1/fee/tickSpacing/slot0/liquidity`核验。

| 用途 | 池 | 费率 | 地址 | 本次约24小时Swap事件 |
|---|---|---:|---|---:|
| 主观察 | DAI/USDC | 0.01% | `0x5777d92f208679db4b9778590fa3cab3ac9e2168` | 216（211笔交易） |
| 跨费率对照 | DAI/USDC | 0.05% | `0x6c6bc977e13df9b0de53b251522280bb72383700` | 2 |
| USDS/PSM路线 | USDC/USDS | 0.30% | `0xa66a2770bc0e0c65b63b5a3bb4560e90f95d6146` | 3 |
| DAI/USDS转换路线 | DAI/USDS | 0.30% | `0xe9f1e2ef814f5686c30ce6fb7103d0f780836c67` | 9（6笔交易） |
| 入场/结算参考 | USDC/USDT | 0.01% | `0x3416cf6c708da44db2624d63ea0aaef7113527c6` | 本次未计活动数 |
| gas换算参考 | USDC/WETH | 0.05% | `0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640` | 本次未计活动数 |

状态锚点：`26,026,824 / 0xb82563fe24bbe9c5b51b788916ca0234032c9d255d25bd8dabbc88a59fb5a67a`，区块时间`2026-09-21 15:37:35 UTC`；查询时为latest，**未声明finalized**，查询结束复核hash未变。

活动扫描为`26,019,625–26,026,824`共7,200块，时间`2026-09-20 15:28:35`至`2026-09-21 15:37:35 UTC`；8个已存在的策略候选池共返回231条Swap。**这是成交活动，不是231次套利，也不说明这些池足以容纳百万资金。** 三条稀疏路线留作机制对照，若30天事件证据不足则停止高频投入。

USDC/USDS的0.01%与0.05%池本次`liquidity()`为0且窗口内无Swap，先留在发现目录。当前活跃流动性为0不等于所有tick都无流动性；未取完整tick和金额报价前，不凭这个单值断言任何金额绝对不可交易。DAI/USDC其他高费池同样不因存在地址就加入核心流。

来源：[官方部署与getPool说明](https://developers.uniswap.org/docs/protocols/v3/deployments/v3-ethereum-deployments)、[池身份原始证据](pool-evidence/summary.json)、[活动扫描证据](pool-evidence/activity-summary.json)。

### 2.3 Sky转换合约

| 合约 | 地址 | 作用 |
|---|---|---|
| DAI/USDS converter | `0x3225737a9bbb6473cb4a45b7244aca2befdb276a` | DAI↔USDS |
| LitePSM DAI/USDC | `0xf6e72db5454dd049d0788e411b06cfaf16853042` | DAI↔USDC |
| USDS LitePSM wrapper | `0xa188eec8f81263234da3622a406892f3d630f98c` | USDS↔USDC |
| USDC Pocket | `0x37305b1cd40574e4c5ce33f8e8306be057fd7341` | 实际USDC库存位置 |

官方chainlog及同块getter已核身份。Sky状态锚点为finalized块`26,026,763 / 0x4029e600a608549b6afc7772ada713b4f7bcda69780a5b4970e882240f397e20`，与上面的池状态**不是同一块**，此次不能组合成可执行套利快照。

本次读取`tin=tout=0`，但采集不能写死零费率；库存必须读取DAI在PSM的余额、USDC在Pocket的余额及Pocket→PSM授权，不把目标缓冲`buf`当实际现金。wrapper的`dai()`兼容getter实际返回USDS；converter不存在通用`fee()`/`live()`字段。`USDSJoin.live()`本次revert，官方源码确认没有该getter，正式采集不请求它，也不可记成“暂停”。完整ABI、事件及依赖见[合约核验](contracts-notes.md)。

来源：[官方chainlog](https://chainlog.sky.money/api/mainnet/active.json)、[USDS转换规则](https://developers.skyeco.com/protocol/tokens/usds/)、[LitePSM规则](https://developers.skyeco.com/protocol/liquidity/litepsm/)、[同块读取证据](contract-sources/sky-snapshot.json)。

## 3. 抓什么数据、什么字段、何时抓

所有链上事实均带`chain_id`、`block_number`、`block_hash`、`source_id`、`collected_at`、`payload_hash`、`schema_version`。交易/日志事实再带`tx_hash`、`transaction_index`、`log_index`；finality与canonical状态按区块事实关联，保留重组版本。

| 数据集 | 必采字段 | 采集方式与频率 |
|---|---|---|
| 合约/池登记 | `address, role, token0, token1, decimals, factory, fee_pips, tick_spacing, code_hash, implementation_address/code_hash(适用时), abi_version, valid_from_block/hash` | 启动；Factory创建事件/升级变化触发；每日复核候选清单 |
| 区块头 | `number, hash, parent_hash, state_root, receipts_root, transactions_root, timestamp, gas_limit, gas_used, base_fee_per_gas, fee_recipient` | `newHeads`逐块；断线按高度补齐；轮询safe/finalized并记录canonical变更 |
| 完整性/采集时间 | `from_block, to_block, expected/returned_count, complete, failure_reason, cursor, first_received_at, available_at, ingest_mode, collector_session_id` | 每批；无事件的完整块也留覆盖记录；历史补抓的历史first_seen为NULL |
| 目标池事件 | `event_type, sender, recipient/owner, amount0_delta, amount1_delta, sqrt_price_x96, liquidity, tick, tick_lower, tick_upper, liquidity_delta`，按事件分型 | 每块完整日志；Swap、Mint、Burn必采，Initialize、Flash、Collect、CollectProtocol、SetFeeProtocol等也保留对应类型；按tx/log顺序处理 |
| v3状态与tick | `sqrt_price_x96, tick, liquidity, fee_protocol, unlocked, tick_bitmap_word_index/value, tick_index, liquidity_gross, liquidity_net, coverage_bounds, state_complete` | 启动bootstrap；事件更新；受影响块同hash校验；每1,000块或断档/重组时做tick checkpoint，完整tick状态按需补齐 |
| Sky状态 | converter依赖地址；PSM `tin,tout,buf,pocket,to18ConversionFactor,HALTED`；DAI/USDC实际余额、Pocket授权；`wrapper.live,vat.live,daiJoin.live`；权限/实现版本；`rush/gush/cut`与Vat参数作为补充 | 第一阶段每块末同hash批读必要状态；兑换、参数、权限或资产变化触发；checkpoint与依赖完整性独立校验 |
| 目标交易及回执 | `tx_hash, from, to, nonce, type, value, input/calldata, gas_limit, max_fee, max_priority_fee, status, gas_used, effective_gas_price, transaction_index`；回执全量logs | 凡交易触及白名单即拉整笔交易/receipt，包含所有外部腿；不只保存自己认识的Swap |
| 调用轨迹与资产流 | `trace_address, parent_trace, call_type, from, to, value, input_selector, success/error, gas_used`；`token,account,balance_before/after,delta,ownership_evidence` | 所有疑似闭环、机制调用和疑似失败先入队；试采最多50笔详细验证；缺trace或余额状态就标待核实 |
| 报价/仿真结果 | `opportunity_id, route_id, route_legs, input_token/amount, output_token/amount, state_anchor, prefix_hash, input_size, gas_used, base_fee, priority_fee, explicit_builder_payment, flash_fee, gross/net_profit, revert_reason, state_complete, engine_version` | 每次相关状态变化生成候选；全路径同状态仿真；标明金额不足、tick缺失、协议禁用及未知费用 |
| 可见性与延迟 | `source_kind, source_event_id, local_received_at, local_monotonic_ns, candidate_ready_at, simulation_done_at, target_block, insertion_position_assumption` | 实时逐事件；先用已确认块作为保守基线。MEV-Share hints后续独立流，历史链数据不能补造pending可见时间 |

上表是面向实现的字段摘要，精确类型、表键和各事件字段见[定类型模型规格](schema-notes.md)。全量EVM回放仍需要相应历史state与目标交易前缀；池字段只足以支持指定数学模型，不能冒称保存了全EVM状态。

### 3.1 数值、时间和状态位置

- 金额用原始原子单位`UInt256/Int256`，小数位由资产身份关联；sqrtPriceX96保留整数Q96，不能先转浮点价。v3流动性用`UInt128`，liquidityNet用`Int128`，tick用`Int32`。费率保留协议原始刻度，例如v3 fee pips分母1,000,000，Sky WAD分母10^18。
- 所有日期时间为UTC；接收时间采用微秒精度，并保留单次collector会话内的单调时钟用于耗时。链上timestamp、收到消息、状态可用于策略三个时刻不能混用。
- 状态位置必须明确`block_end`、`before_tx`、`after_tx`或日志位置。用块号eth_call读到的块末状态，不代表某笔交易执行前状态。
- 日志以`(chain_id, block_hash, tx_hash, log_index)`去重；状态以协议地址和明确状态锚点归档；重组后的旧hash标记orphaned，不覆盖掉审计证据。
- bitmap/tick只覆盖部分范围时保存范围与完整性。报价超出范围就报`insufficient_state`，不填零，不截断后假装可成交。

### 3.2 三条首批策略模板

1. `USDC → v3买DAI → LitePSM换回USDC`及反向。
2. `USDC → v3买USDS → wrapper换回USDC`及反向。
3. `DAI → v3买USDS → converter换回DAI`及反向；如最终按USDT评价，追加可执行结算路径。

输入扫描先设1千、1万、5万、10万、25万、50万、100万USDT等值，再在有利润区间优化金额。小额档用于发现可重复微利，百万档用于检验容量；不能将小额收益线性放大。金额映射需同状态换汇证据。USDT入场和回收是否逐笔发生按真实资金管理情景分别计，不能每笔无故重扣，也不能整期漏掉。

## 4. 接口与基础设施边界

- 链数据：Ethereum JSON-RPC的`eth_chainId`、`eth_getBlockByNumber/Hash`、`eth_getLogs`、`eth_getTransactionByHash`、`eth_getTransactionReceipt`、固定blockHash的`eth_call`/`eth_getCode`。实时可订阅`newHeads`，日志仍做每块覆盖确认。
- v3：Factory发现、池状态getter、TickLens或`tickBitmap/ticks`、QuoterV2交叉校验。Quoter调用与纯数学报价均不是完整套利交易模拟。
- trace/history：对所选交易取得call trace和历史余额；精确前缀回放需archive state或本地执行客户端。`debug_traceTransaction`等扩展接口先探测，RPC不提供时记录缺口，不能用空数组冒充无内部调用。
- 本次公共RPC只证明少量读取可行。连续采集前必须测请求限额、历史状态/trace可用性、head延迟和断线恢复；尚未证明该免费端点能支撑整套负载。读取不需要钱包私钥。
- 持续采集不依赖研究脚本的批量响应恰好完整；上线实现必须严格校验JSON类型/重复id/请求响应集合/ABI长度、同hash、canonical与原始payload hash。

## 5. 存储、验收和结果展示

使用并列的定类型事件/状态链路；不改变CEX每秒10档与历史50档语义，不把新字段塞进yield_observation。原始RPC字节可作为带hash的压缩证据归档，热查询按typed表工作，不建通用JSON事实大表。

建议先保留30天热数据，验证有用后扩90天；稀疏的区块身份、合约规则、最终仿真/利润证据长期保留。不得独立删除仍被回放区间引用的checkpoint、tick基线或合约版本。先测24小时真实压缩占用和查询耗时，再定容量与TTL，不预报未经测量的GB/天。

首轮验收：

1. 所选时间窗连续区块有明确覆盖；无事件与采集失败可区分；断线/重组可回滚并补齐。
2. 池身份及整数状态与同块RPC一致；可复现指定真实Swap的整数输出，tick缺失时明确失败。
3. Sky规则按准确ABI读取，方向停用、库存不足、授权不足与字段不存在分别表示；不把buf等同余额。
4. 每条疑似利润有完整资产路径、gas及可见排序支付；不可见费用和地址归属另标未知；失败样本及无机会时段也保留。
5. 回放一笔交易时使用父块＋交易前缀；不得用块末价或删除赢家的方式把对手收益归给自己。
6. 输出`机制→独立事件数→出现日期→别人可见净利→我们延迟后可行净利→资本需求→未知项`。模拟通过与实际纳入分开，未测胜率不设100%。

100万USDT净4.5%仍对应全年45,000USDT。验收看扣完整成本后的可得累计利润及重复性，不能把单笔原子周转率直接年化。此次231条Swap扫描只完成了地址与活动核验，尚未执行上述利润验收。

研究脚本：[池发现](discover_v3_pools.py)、[活动扫描](probe_pool_activity.py)、[Sky核验](verify_sky_contracts.py)。均为公开只读调用，没有部署合约或发送交易。
