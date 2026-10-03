# Ethereum 科学家一期：采集字段与定类型模型建议

> 本文是完整研究模型参考，不是第一版建表清单。用户要求简化后，实际实施以[5表最小设计](../../docs/dex-arbitrage-mvp-design.md)为准；不要一次实现本文全部模型。

2026-09-21，研究规格，未实现或部署。范围收窄为 **Ethereum mainnet，chain_id=1，USDC/DAI/USDS，Uniswap v3＋Sky DaiUsds converter＋LitePSM/wrapper**。USDC/USDT 仅用于本金/利润换回 USDT 的报价参考，WETH/USDC 仅用于 gas 成本换算。Curve、清算、跨链及其他链暂不进入此规格。

目标事实单位是“某个区块/交易位置的状态、一次可复算兑换、一次真实或模拟现金流”，不是产品 APY，也不是 AMM 伪装成的十档订单簿。本文用 ClickHouse 类型表达物理建议；先用同型离线研究文件验证，不需要一次建完全部表。

## 1. 现有模型如何复用

- 可以复用公共 HTTP 重试/限频、整数/Decimal 校验、payload hash、ClickHouse 连接、显式列批写和稳定重试身份；代码参见 `internal/exchange/`、[decimal.go](/home/ubuntu/crypto-market-info/internal/model/decimal.go:11)、[yield/model.go](/home/ubuntu/crypto-market-info/internal/yield/model.go:169)。
- 可以借用“事实先写、完整批次提交标记最后写”的模式，参见 [options_live_minute.go](/home/ubuntu/crypto-market-info/internal/storage/clickhouse/options_live_minute.go:101)，但新批次按区块及采集范围定义。
- **不能复用** `instrument/order_book_minute/order_book_second_delta` 保存原始池状态或 ticks；也不能用 `yield_observation` 保存 PSM 库存、交易回执、模拟。当前 [replay.go](/home/ubuntu/crypto-market-info/internal/replay/replay.go:10) 只有每秒盘口回放语义。
- Avalanche RPC 客户端可参考区块 hash 锚定与完整批次核验，不能直接换 URL 就用于 Ethereum：它硬编码 43114、最新 finalized、简单 ABI 参数和新鲜度约束，见 [client.go](/home/ubuntu/crypto-market-info/internal/yield/avalanche/client.go:77)。
- 实施修改数据模型/表时另行同步 `docs/market-data-storage.md`，本研究稿不改变 CEX 新 10 档/旧 50 档任何语义。

## 2. 全部新模型共用的约定

| 含义 | 最小字段/类型与要求 |
|---|---|
| 链与位置 | `chain_id UInt64`；`block_number UInt64`；`block_hash FixedString(32)`；交易内事实另有 `tx_hash FixedString(32), tx_index UInt32, log_index UInt32`。地址 `FixedString(20)`，在接口边缘转十六进制显示 |
| 链时间 | `block_time DateTime64(0,'UTC')` 来自区块头。它不是本机看见数据的时刻 |
| 来源及接收 | `capture_id UUID` 引用一次来源记录；`requested_at/received_at/available_at DateTime64(6,'UTC')`。available_at 为解析、校验完成且策略确实可使用的本地时刻 |
| 历史/实时 | `capture_mode Enum8('backfill','live')`。历史补抓保留本次真实接收时间；`historical_first_seen_at Nullable(DateTime64(6,'UTC'))` 未知就为空，绝不回填区块时间 |
| 整数 | 原生币 wei、token 原子单位、费用原始数值 `UInt256`；Uniswap ABI 有符号 amount 用 `Int256`。Go 使用 big.Int/定宽 ABI 校验；不得经过 float。任意无符号余额差先用大整数计算，存有符号结果前检查范围 |
| 专用精度 | `sqrt_price_x96 UInt256` 并约束实际 uint160；`liquidity UInt128`、`liquidity_net Int128`、`tick Int32` 且校验 int24/协议范围；fee 单位百万分之一。Sky wad/ray/rad 原样整数，字段名明确缩放 |
| 证据 | 非空 `payload_hash FixedString(32)`、`decoder_revision FixedString(32)`、`manifest_revision FixedString(32)`；原始响应压缩文件/对象按 hash 寻址。hash 只能证明一致性，不能替代实际留存原文或链状态 |
| 分叉 | 原始事实键包含 block_hash，不能只按高度覆盖。`canonical/finality` 从区块当前状态关联；孤块事实保留，正常收益/回放查询过滤掉孤块 |
| 时间测量 | 实时采集另记 `collector_boot_id UUID, receive_monotonic_ns UInt64`，同进程阶段耗时用单调时钟；不把不同主机的墙钟差当精确网络延迟 |

以下表共用位置与证据列，不在每行重复列全。状态表只描述指定区块结束状态；交易前/后位置必须在模拟中明确，不能把 `eth_call(block N)` 当第 k 笔交易之前的状态。

## 3. 区块、交易和来源事实

### `eth_capture_batch`：来源与完整提交记录

最小字段：`capture_id, source_id, capture_mode, requested_at, received_at, available_at, collector_boot_id, receive_monotonic_ns, rpc_method, request_hash, response_hash, response_artifact_ref, scope_revision, scope_kind, from_block, to_block, anchor_block_hash, status, error_code, expected_count Nullable(UInt64), received_count UInt64, accepted_count UInt64, completeness, decoder_revision, committed_at`。

`scope_kind` 限定为 header / selected-contract-logs / selected-transactions / pool-state / protocol-state / simulation；这是采集元数据，不是承载业务 JSON 的万能表。范围 hash 指向有版本的地址、topic 与字段清单。失败写失败记录，不产生看似有效的业务观测；没有响应时 response hash 可空且状态必须失败。真实写入批次 ID 和内容重试时不变。

只有预期成员、数量和 hash 全部核对后写 committed 标志。预期数量未知则明确 unknown，不能以返回了 200 状态或收到零行推断完整。日志至少区分“提供方声明范围完成”和“已同全块 receipts 交叉核对”；严格历史样本可逐块取 receipts，在内存筛白名单并与 getLogs 比较，数据库只长期保留命中交易。

### `eth_block`：不可变区块头

最小字段：`chain_id, block_number, block_hash, parent_hash, block_time, state_root, receipts_root, transactions_root, fee_recipient, base_fee_per_gas_wei UInt256, gas_used UInt64, gas_limit UInt64, transaction_count UInt32, ordered_tx_hashes Array(FixedString(32)), capture_id`。

保留交易 hash 顺序，完整 raw block 作为证据；仅对回放候选按需补齐前缀交易的原始 transaction bytes。区块哈希只能锚定来源，若未验证 receipts trie/proof 不宣称已完成独立密码学证明。

### `eth_block_status`：最终性与重组历史

最小字段：`chain_id, block_number, block_hash, revision UInt64, canonical Bool, finality Enum8('head','safe','finalized','orphaned'), observed_at, capture_id`。追加写，不删除旧状态；revision 是来源协调后本地单调版本，避免用响应到达乱序覆盖新状态。当前视图按 block_hash 取最新版本。同高度只能有一个当前 canonical hash；head 变更沿 parent_hash 找共同祖先，失效所有依赖孤块的状态与模拟。

### `eth_tx_receipt`：命中范围交易的执行事实

最小字段：区块/交易位置＋`from_address, to_address Nullable(Address), tx_type UInt8, nonce UInt64, value_wei UInt256, gas_limit UInt64, max_fee_per_gas_wei Nullable(UInt256), max_priority_fee_per_gas_wei Nullable(UInt256), input_selector Nullable(FixedString(4)), input_size UInt32, input_hash, raw_tx_artifact_ref, receipt_status UInt8, gas_used UInt64, effective_gas_price_wei UInt256, cumulative_gas_used UInt64, receipt_log_count UInt32, capture_id`。

每笔交易必须同时核对 header、交易 hash/index 与 receipt inclusion。失败 status=0 仍保存；本期没有发送候选交易，不能把历史赢家成功率冒充自己成功率。普通 Ethereum 交易 gas 成本是整数 `gas_used × effective_gas_price`；若出现本期未支持的 blob 交易等费用项，标 unsupported 并补专项字段后再纳入净利，不能按旧公式漏算。

### `eth_contract_log`：范围日志原始事实

最小字段：区块/交易/log 位置＋`emitter_address, topics Array(FixedString(32)), data_bytes String, removed Bool, capture_id`。`String` 在此承载 ABI 二进制 bytes，不存字符串价格/JSON。topics 限制最大 4 个、字节长度按 ABI 检查；只收白名单协议日志，以及命中交易中用于资金流闭合的 token Transfer 等日志。WebSocket removed 只是重组信号之一，canonical 判断仍以区块链关系为准。

Transfer 不能单独证明利润。候选交易按需补 `eth_tx_asset_balance`：`chain_id, block_hash, tx_hash, account_address, token_address/native标识, balance_before UInt256, balance_after UInt256, boundary Enum8('tx_exact','block_only'), evidence_hash, capture_id`。block_only 余额不能用作单笔交易净利。原生币内部转账/显式 builder 支付另存 `eth_native_transfer`，键带 `trace_address Array(UInt32)`，字段 `from,to,value_wei,call_type,success`；trace 不完整时标明利润核验不完整。与余额差已包含的费用不重复扣除。

## 4. Uniswap v3：原始状态及流动性分布

### 定义 `univ3_pool_definition`

字段：`chain_id,pool_address,factory_address,token0,token1,fee_pips UInt32,tick_spacing Int32,pool_code_hash,definition_block_hash,manifest_revision,capture_id`。token decimals 单独放小型 `eth_token_definition`：`chain_id,token_address,decimals UInt8,code_hash,proxy_kind,implementation_address Nullable,implementation_code_hash Nullable,valid_from_block_hash,revision`。

地址需要官方 Factory getPool 非零、token0/token1/fee/tickSpacing 相符和实际 bytecode；符号不作身份。升级 token 的代理实现要按其真实代理标准验证，不能假设全部用同一存储槽。

### 状态 `univ3_pool_state`

每个受跟踪区块、池保存：共同锚点＋`sqrt_price_x96 UInt256, tick Int32, liquidity UInt128, fee_protocol UInt8, unlocked Bool, tick_snapshot_id, state_origin Enum8('rpc','replayed'), complete Bool, state_hash, capture_id`。

一期每块同 blockHash 读取小规模候选池的 slot0/liquidity，与事件回放结果核对。`liquidity=0` 表示当前区间没有活跃流动性，**不等于全池没有流动性**；sqrtPrice/tick/bitmap/可跨越 ticks 未完整确认前不能伪造零滑点、无限容量或“绝对无法成交”的结论。报价范围不足返回明确状态 `tick_coverage_missing`。

### 检查点 `univ3_tick_snapshot`

最小字段：`snapshot_id,chain_id,pool_address,block_number,block_hash,bitmap_word_min Int32,bitmap_word_max Int32,bitmap_word_positions Array(Int32),bitmap_words Array(UInt256),tick_indices Array(Int32),liquidity_gross Array(UInt128),liquidity_net Array(Int128),coverage Enum8('full','bounded'),complete Bool,member_hash,capture_id`。

一条检查点保存一池的一组定类型数组，不把每个 tick 在每个区块复制成常驻完整快照行。数组长度、排序、唯一性、bitmap 与 initialized ticks 一致性必须验证；范围内未初始化的 word 也要有覆盖证明，不能因为没返回就假设为零。bounded 模式只能在已经覆盖的 tick 范围内报价；跨出范围须同一锚点补取，否则失败。

### 事件 `univ3_swap` 与 `univ3_liquidity_change`

`univ3_swap`：位置＋`pool_address,sender,recipient,amount0 Int256,amount1 Int256,sqrt_price_x96 UInt256,liquidity UInt128,tick Int32,source_log_hash`。

`univ3_liquidity_change`：位置＋`pool_address,kind Enum8('mint','burn'),owner,tick_lower Int32,tick_upper Int32,amount UInt128,amount0 UInt256,amount1 UInt256,source_log_hash`。Mint/Burn 按日志顺序改变 tick gross/net；数量为零的合法事件不能误作解析失败。Initialize 保留在原始日志并形成初始状态；Flash/Collect 等涉及费用与现金流的日志保留，但不能错误地作为 active liquidity 增量。

启动时一个完整/覆盖声明清楚的 tick 检查点，后续逐事件增量；建议每 1,000 块或遇断档/重组后重建，并对每块 pool 小状态交叉核验。需要获取多长 tick 范围由报价的最大金额与实际跨越决定，不由“十档”决定。每块全量 ticks 轮询不是一期方案。

上述字段针对 v3，不能拿去解释 v4 的 PoolKey、hooks 或动态费。该部分实施前应以官方 v3 ABI/合约版本固定精确解码和舍入规则。

## 5. Sky：转换边、真实库存与方向停机

本节 getter/事件按 [DaiUsds 官方源码](https://github.com/sky-ecosystem/usds/blob/dev/src/DaiUsds.sol)、[LitePSM 官方源码](https://github.com/sky-ecosystem/dss-lite-psm/blob/main/src/DssLitePsm.sol)、[wrapper 官方源码](https://github.com/sky-ecosystem/usds-wrappers/blob/dev/src/UsdsPsmWrapper.sol) 核验；部署时必须再绑定实际部署 bytecode/实现版本，不能只信 mutable branch 名。

### `sky_converter_definition` 与 `sky_conversion_event`

定义字段：`chain_id,converter_address,code_hash,dai_address,usds_address,dai_join_address,usds_join_address,definition_block_hash,manifest_revision,capture_id`。这些来自 `dai()/usds()/daiJoin()/usdsJoin()`。**converter 没有自己的 fee、paused 或 vat getter，不新增假字段。** 两个 join 的 Vat 一致性及实际依赖 bytecode/授权在定义核验及完整 EVM 模拟中验证。

事件字段：位置＋`converter_address,direction Enum8('dai_to_usds','usds_to_dai'),caller,recipient,wad_amount UInt256,source_log_hash`。1:1、无转换费是该版本规则，记录在 manifest 的精确规则 revision，不用高频伪造一个总为 1 的“收益观测”。依赖 token/Join 的实现升级、权限或暂停变化必须使相关仿真所用版本重新验证。

### `sky_litepsm_definition`

字段：`chain_id,psm_address,code_hash,ilk FixedString(32),vat_address,dai_join_address,dai_address,gem_address,pocket_address,to18_conversion_factor UInt256,definition_block_hash,manifest_revision,capture_id`。wrapper 作为同版本定义的另一记录：`wrapper_address,wrapper_code_hash,psm_address,gem_address,usds_address,usds_join_address,vat_address,ilk,pocket_address,gem_decimals,to18_conversion_factor`。

**wrapper 的兼容 `dai()` 返回 USDS，不能当底层 DAI。** 底层事件 owner 可能是 wrapper；最终收款人与真正调用路径需要 token 流/trace，不能只按 PSM 单条事件认定。

### `sky_litepsm_state`

每区块、同 blockHash 最小字段：

| 字段 | 来源/含义 |
|---|---|
| `tin_raw UInt256, tout_raw UInt256` | 对应 getter；通常 wad=1e18，但 `2^256-1` 表示对应方向 HALTED，不能先转 Decimal 或限制在 [0,1] 而丢失停机状态 |
| `sell_halted Bool, buy_halted Bool` | 从上述特殊值确定，分别对应 USDC→DAI、DAI→USDC；不是读取不存在的 paused |
| `buf_wad UInt256` | `buf()`，目标预铸 DAI 缓冲，不等于当前可兑换额 |
| `dai_balance_wad UInt256` | DAI.balanceOf(PSM)，USDC→DAI 的真实现有输出库存 |
| `gem_balance_raw UInt256` | USDC.balanceOf(pocket)，DAI→USDC 的库存；不要误读 PSM 自身 USDC 余额 |
| `pocket_allowance_raw UInt256` | USDC.allowance(pocket,PSM)，余额与可移动额度分别记录 |
| `vat_live UInt256` | 通过已核验接口读；作为依赖诊断，不能仅凭它推断两个 swap 方向实际可用 |
| `rush_wad Nullable(UInt256), rush_status` | `rush()` 的同块输出或失败；额外可填充 DAI 的估算，不是已经到位的现金 |
| `wrapper_fee_exempt Nullable(Bool), caller_mode` | 若路线确实调用 NoFee，核验 `bud(actual_caller)`；普通路线按标准收费入口，不假设继承他人的白名单 |

可执行容量按方向、输入规模、精确舍入和完整调用结果计算，不能简单把余额字段命名 remaining_capacity。若候选包含 `fill()`，再采同块 `Vat.ilks(ilk)` 的 Art/rate/line、全局 Line/debt、相关 urn 状态，并把 fill 的成功条件与 gas 纳入仿真；只读取 rush 不证明 fill 一定成功。常规直接兑换第一版可以不使用 fill，字段为空须标 not_in_scope。

逐事件保留 BuyGem/SellGem 的 `owner,gem_amount_raw,fee_dai_wad`，Fill/Trim/Chug 的 `wad`，File 参数变化及 Kiss/Diss 授权变化；这些是各自定类型 decoder，不能用一个无模式 JSON 当热数据。资产转账可能在不触发 PSM swap 事件时改变库存，因此小规模第一版仍按块直接读取库存，不只靠 swap 事件推余额。[官方 LitePSM 机制说明](https://developers.skyeco.com/protocol/liquidity/litepsm/)。

## 6. 报价与完整模拟分开

### `eth_route_quote`：同状态的金额报价

字段：`quote_id,opportunity_id,chain_id,state_block_number,state_block_hash,route_revision,route_legs Array(定类型Tuple(protocol_kind,contract_address,token_in,token_out,fee_pips)),token_in,amount_in_raw UInt256,token_out,amount_out_raw Nullable(UInt256),status,missing_reason,state_bundle_hash,requested_at,available_at,quote_engine_revision,capture_id`。

每腿输入必须等于前腿输出，重复访问同池必须应用本候选之前造成的状态变化。分别报价的多条“最优路径”不能直接相加；整个路径净利润不能用中间价代替金额输出。报价失败/状态缺失单列，不能缓存上次输出填本次。

先扫描 1 千、1 万、5 万、10 万、25 万、50 万、100 万 USDT 等值的实际 token 原子输入，再在正收益附近优化金额；保留全部实际 input 值。金额梯度不是可累加盘口档位，100 万也不是必须部署的每笔规模。

### `eth_route_simulation`：完整候选 EVM 结果

字段分为五组：

1. 身份：`simulation_id,opportunity_id,strategy_revision,route_revision,engine_revision,executor_code_hash,candidate_calldata_hash,state_bundle_hash,manifest_revision`。simulation ID 由状态、路径、金额、调用者、EVM 环境和版本确定；相同结果重试 ID 不变。
2. 起点/位置：`chain_id,parent_block_hash,state_block_hash,target_block_number,target_block_timestamp,target_base_fee_wei,position_kind Enum8('block_end','after_tx_prefix','hypothetical_replacement'),prefix_tx_count,prefix_tx_hashes_hash,state_override_hash Nullable`。EVM 环境的 fee recipient、gas limit 等作为有类型的环境结构保存；不能只留一个 blockNumber。
3. 可见性：`trigger_capture_id,input_available_at,detected_at,simulation_started_at,simulation_finished_at,latency_scenario,required_information_mode Enum8('confirmed','public_pending','private_hint','historical_oracle')`。一期以 confirmed 为主；事后重建理想位置必须标 historical_oracle/假设，不混进可得利润。
4. 结果：`status Enum8('success','revert','missing_state','unsupported'),revert_selector Nullable,return_data_hash,gas_used,gas_price_assumption_wei,gas_cost_wei,start_asset,start_amount_raw,end_amount_raw,gross_asset_delta,flash_fee_amount_raw,explicit_ordering_payment_wei,fee_inclusion_flags,borrow_repaid Bool,residual_inventory_complete Bool`。缺状态不等于经济亏损；EVM success 不等于借款还清/没有其他资产库存，二者要独立验证。
5. 经济口径：`settlement_asset,settlement_quote_id,gas_conversion_quote_id,net_profit_amount Nullable(Int256),unknown_cost_flags,minimum_profit_raw,trace_artifact_ref,balance_delta_hash`。gas 和显式支付若已在余额差里扣掉，不重复扣；换 USDT 用真实规模参考路径。未计入竞争返还/纳入概率不能报已实现净利润。

候选的逐资产前后余额用前述 balance 模型记录；每个实际触及的资产都检查，起终同币但残留债务不算闭环。simulator 在父块状态应用前缀交易后再执行候选；回放时不删除真实赢家后假设自己能占位。交易替换仅作为明确标注的理论上界。保存赢家 receipt 与本方影子结果的关联，后续标注被哪笔竞争交易消耗。

## 7. 主键、排序与分区

ClickHouse 不提供普通关系库式唯一约束，下列是逻辑键；应用层去重、内容 hash 与读取 FINAL/argMax 必须配套。不可变事实可使用 ReplacingMergeTree 做相同字节的重试去重，不允许相同键不同内容悄悄覆盖。

| 模型 | 逻辑键/建议 ORDER BY | 常用查询 |
|---|---|---|
| block | `(chain_id,block_number,block_hash)` | 高度范围/共同祖先 |
| block_status | `(chain_id,block_number,block_hash,revision)` | 分支状态历史及当前 canonical |
| tx_receipt | `(chain_id,block_number,block_hash,tx_index,tx_hash)` | 区块内顺序、命中交易现金流 |
| contract_log / decoded event | `(chain_id,emitter_or_pool,block_number,block_hash,tx_index,log_index)` | 某合约连续事件回放 |
| pool/protocol state | `(chain_id,pool_or_module,block_number,block_hash)` | 最近检查点及同块状态 |
| tick snapshot | `(chain_id,pool,block_number,block_hash,snapshot_id)` | 覆盖区间完整检查点 |
| quote/simulation | `(chain_id,strategy_or_route_revision,target_block_number,opportunity_id,result_id)` | 机制、时间区间及同一机会全部金额情景 |
| capture batch | `(source_id,received_at,capture_id)` | 完整性、延迟、失败诊断 |

区块事实按 `toYYYYMM(block_time)` 分区；来源与仿真按真实采集/计算 UTC 月份分区，不用未来模拟 target time 控制保留。小型 token/pool/module 定义表可不分区，版本键包含生效 block hash。跨表查询先确认 committed batch 和 canonical，再按 block hash 联结，不能只按高度。

一期不加价格索引、tx_hash 倒排索引或全字段 Bloom。通过区块/合约位置已经能定位证据；只有实测发现瓶颈才增加辅助索引并记录空间成本。

## 8. 频率、保留与最低验收

| 数据 | 建议采集频率 | 初始保留建议 |
|---|---|---|
| 区块头、顺序、canonical/finality | 每个新块；每次 head/safe/finalized 改变，断线补齐高度区间 | 实验全程；轻量头/状态长期保留 |
| 白名单日志、命中交易/receipt | 每块完整范围；日志到达即处理，receipt 补齐后提交 | 定类型事实先保留 30 天；研究关键交易永久归档 |
| v3小状态、PSM参数/库存 | 每块同 hash 批读；实时以新 head 观察，另跟踪最终确认 | 30 天；按不变量允许对不变状态做有类型差量，不能丢有效性 |
| ticks | 初始检查点＋每次 Mint/Burn；每约1,000块/断档/重组重校验 | 检查点＋后续增量一并保留，不能删掉仍被查询的起点 |
| token/合约定义 | 首次、升级/配置事件；每小时复核，入围候选模拟前核版本 | 全版本保留 |
| trace/state diff | 确认候选与可疑失败交易按需读取，首轮上限约50笔 | 普通原始证据30天；入选研究证据与依赖状态归档 |
| 路径报价/仿真 | 相关池/PSM状态变更触发；相同state bundle不重复算；无机会块保留扫描完成记录 | 成功、失败、missed全部先保留30天，统计长期保留 |

这些是保留策略建议，不在本轮设置 TTL。先以30天为热数据研究窗口，证明有用后扩展90天。第一个24小时小样本先测压缩后字节数、RPC次数、回放耗时和按池/日期查询耗时，再决定是否调频/扩窗。原始响应压缩归档与定类型热数据分离；不能在完成研究之前删除复算所需的原文、checkpoint或前缀状态。

最低验收：固定区块及其前缀可精确复现已发生交易输出；v3跨tick/零活跃L/初始化与撤出正确；LitePSM两方向费率、HALTED、pocket余额/allowance、普通与NoFee权限正确；断日志、缺ticks、节点失败不生成有效报价；重组后孤块候选失效；历史补抓不能产生伪造的事前可见性；幂等重试不重复计利润；报价→EVM→余额变化→未重复扣除的费用能够完整核对。

按这个范围先产出几条可信闭环的“输入、每腿整数输出、谁先成交、真实费用、我们晚到后还剩多少”的证据，比继续扩大资产或 APY 数据更直接。
