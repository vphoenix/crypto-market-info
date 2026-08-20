# ARB-0016 收益数据采集设计

## 1. 目标与边界

ARB-0016 先采集各种单资产收益产品的公开收益率和产品规则，再筛选满足下列条件的候选项：

1. 在协议公布的正常经济规则下，本金不存在不确定损失；
2. 固定手续费、固定罚金和固定折价可以接受，但必须单独记录，供查询时扣除；
3. 非稳定币本金的价格风险可以通过另外的市场头寸对冲；稳定币存款可以不建立价格对冲；
4. 未来利率可以变化。利率变化只影响未来收入，不等同于本金损失；
5. 交易所跑路、合约漏洞、稳定币脱锚、桥或二层网络无法退出等意外风险，留给后续人工审查，不在本阶段建立风险审查表。

本设计只采集公开数据，不执行质押、申购、赎回、借贷、对冲或交易。永续资金费率不计入收益产品的 APY；永续和交割合约盘口继续由现有行情表独立采集。

## 2. 表关系

第一版只使用两张收益表：

```text
yield_route 1 ---- N yield_observation
```

- `yield_route` 定义“在哪里、用什么资产、通过什么产品获得收益”；
- `yield_observation` 保存该产品在某个 UTC 时刻、某个额度档位的利率、费用、期限和规则状态。

不建立 `yield_risk_review`。多奖励币也不拆第三张表，而是在同一条 `yield_observation` 中使用定类型数组保存。

## 3. 收益路线表

表名：`yield_route`

用途：保存收益产品的稳定身份和资产路径。同一产品的利率、额度、费用、锁仓期或罚没规则发生变化时，继续使用原 `yield_route_id` 并写入新的观测；交易场所、产品代码、所在网络、核心合约或存入/赎回资产发生变化时，分配新的 `yield_route_id`。

主键：`yield_route_id`

| 字段名 | 中文名称 | 逻辑类型 | 必填 | 说明 |
|---|---|---:|:---:|---|
| `yield_route_id` | 收益路线 ID | `UInt32` | 是 | 一种具体收益产品及资产路径的唯一标识。 |
| `provider_type` | 提供者类型 | `String` | 是 | `native`、`cex` 或 `protocol`。DEX 上的借贷、质押等归为 `protocol`。 |
| `provider` | 提供者 | `String` | 是 | 例如 `TRON`、`Aave`、`JustLend`、`Binance`。 |
| `product_code` | 产品代码 | `String` | 是 | 数据源中的稳定产品代码；没有官方代码时使用适配器定义且不可复用的代码。 |
| `product_name` | 产品名称 | `String` | 是 | 便于检查的产品名称。不能单独作为唯一键。 |
| `yield_type` | 收益类型 | `String` | 是 | 第一版允许 `native_staking`、`liquid_staking`、`lending`、`fee_share`、`resource_rental`、`single_asset_incentive`、`stablecoin_savings`、`cex_earn`。 |
| `deposit_asset_key` | 存入资产 | `String` | 是 | 精确到网络和合约的资产标识，不能只写可能跨链重复的币种符号。 |
| `redeem_asset_key` | 正常赎回资产 | `String` | 是 | 按产品正常退出路径最终得到的资产标识。 |
| `network` | 所在网络 | `Nullable(String)` | 否 | 链上产品所在网络；纯 CEX 账内产品可以为空。 |
| `contract_address` | 核心合约地址 | `Nullable(String)` | 否 | 链上产品的主合约地址；原生质押和纯 CEX 产品可以为空。 |
| `price_exposure_asset` | 价格暴露币种 | `Nullable(String)` | 否 | 需要关联现有现货或合约行情进行价格对冲的经济资产，例如 `wstETH` 产品填写 `ETH`。按当前模型无需对冲的稳定币产品为空。 |
| `income_source` | 收益来源 | `String` | 是 | `issuance`、`borrow_interest`、`protocol_fee`、`resource_rent`、`offchain_interest` 或 `subsidy`。只描述收入来自哪里，不代表安全判断。 |
| `source_url` | 主数据源 | `String` | 是 | 官方 API、合约或产品页面。适配器可以在不改变路线身份的情况下更新访问地址。 |
| `collection_enabled` | 是否采集 | `Boolean` | 是 | 是否仍按计划采集。产品关闭后保留历史行并设为 `0`。 |

### 3.1 资产标识

`deposit_asset_key` 和 `redeem_asset_key` 必须能够区分原生币、不同网络上的映射币及不同合约版本。例如 ETH 主网原生 ETH、Arbitrum 上的 WETH 和某个废弃二层网络中的映射 ETH 不能使用同一个资产标识。

第一版可以使用适配器内统一的可读字符串，但该字符串一经用于历史数据便不能改变含义。后续若项目建立独立资产注册表，可以再把它替换为数值资产 ID。

## 4. 收益观测表

表名：`yield_observation`

用途：保存收益路线在某个时刻、某个额度档位的完整收益快照。收益数据量远低于盘口数据，第一版保存完整快照，不设计秒级差量。

唯一键：`(yield_route_id, observation_time, tier_no)`

| 字段名 | 中文名称 | 逻辑类型 | 必填 | 说明 |
|---|---|---:|:---:|---|
| `yield_route_id` | 收益路线 ID | `UInt32` | 是 | 关联 `yield_route.yield_route_id`。 |
| `observation_time` | 观测时间 | `DateTime64(3, 'UTC')` | 是 | 本条快照代表的 UTC 时刻。优先使用数据源时间；没有时使用采集时间。 |
| `collected_at` | 采集时间 | `DateTime64(3, 'UTC')` | 是 | 本系统实际取得数据的 UTC 时间。 |
| `tier_no` | 档位编号 | `UInt16` | 是 | 从 `1` 开始。同一产品同一时刻的不同金额档位分别写行；无分档产品固定为 `1`。 |
| `tier_min_amount` | 档位起始金额 | `Decimal` | 是 | 本档适用的最小存入数量，单位为 `deposit_asset_key`；无分档产品为 `0`。 |
| `tier_max_amount` | 档位结束金额 | `Nullable(Decimal)` | 否 | 本档适用的最大存入数量；没有上限或无分档产品为空。 |
| `tier_mode` | 档位计息方式 | `String` | 是 | `none`、`marginal`、`whole_balance` 或 `unknown`。`marginal` 表示分段计息，`whole_balance` 表示按总余额选择整笔利率。 |
| `reported_rate` | 来源公布利率 | `Nullable(Decimal)` | 否 | 数据源直接公布的利率。10% 保存为 `0.10`，不得使用二进制浮点数。来源暂时没有有效利率时可以为空。 |
| `reported_rate_kind` | 公布利率类型 | `String` | 是 | `apr`、`apy` 或 `unknown`。不能把 APR 不加说明地写成 APY。 |
| `rate_mode` | 利率变化方式 | `String` | 是 | `fixed`、`variable` 或 `unknown`。`variable` 只表示未来收入可能变化，不自动否决该路线。 |
| `reward_asset_keys` | 奖励资产列表 | `Array(String)` | 是 | 收益实际以哪些资产发放。已知奖励资产时至少保存一个元素；来源没有说明时为空数组。资产标识规则与路线表相同。 |
| `reward_component_rates` | 分项公布利率 | `Array(Nullable(Decimal))` | 是 | 与 `reward_asset_keys` 同下标对应的来源分项利率。来源只公布总利率时，对应元素为空。非空元素沿用 `reported_rate_kind` 的 APR/APY 口径。 |
| `entry_fee_rate` | 进入费率 | `Nullable(Decimal)` | 否 | 按本金比例收取且规则确定的进入费用。 |
| `exit_fee_rate` | 正常退出费率 | `Nullable(Decimal)` | 否 | 按本金比例收取且规则确定的正常退出费用。 |
| `fixed_penalty_rate` | 固定罚金率 | `Nullable(Decimal)` | 否 | 提前退出等情况下事先确定的固定本金折损。没有则为 `0` 或空。 |
| `entry_fee_amount` | 固定进入费用 | `Nullable(Decimal)` | 否 | 不按比例收取的固定进入费用。 |
| `exit_fee_amount` | 固定退出费用 | `Nullable(Decimal)` | 否 | 不按比例收取的固定退出费用。 |
| `fixed_fee_asset_key` | 固定费用币种 | `Nullable(String)` | 否 | `entry_fee_amount` 和 `exit_fee_amount` 的计价资产；没有固定金额费用时为空。 |
| `lock_seconds` | 锁仓秒数 | `UInt64` | 是 | 申购后不能退出的确定时间；没有锁仓为 `0`。 |
| `unbonding_seconds` | 解质押等待秒数 | `UInt64` | 是 | 发起正常退出后等待本金可用的确定时间；没有等待为 `0`。 |
| `rule_principal_loss_mode` | 规则内本金损失方式 | `String` | 是 | `none`、`fixed`、`variable` 或 `unknown`。只描述协议正常规则，不描述黑客攻击、交易所倒闭或跨链桥失效。 |
| `fixed_principal_loss_rate` | 固定本金损失率 | `Nullable(Decimal)` | 否 | `rule_principal_loss_mode=fixed` 时记录确定损失比例；其他情况为空。 |
| `rule_eligibility` | 理论筛选结果 | `String` | 是 | `candidate`、`rejected` 或 `unknown`。`variable` 本金损失必须为 `rejected`。 |
| `eligibility_reason` | 筛选原因 | `Nullable(String)` | 否 | 被拒绝或规则不明时记录简短原因，例如 `variable_slashing`、`path_dependent_loss`。 |
| `exposure_ratio` | 本金价格暴露比例 | `Nullable(Decimal)` | 否 | 每 1 单位存入资产对应多少单位 `price_exposure_asset`。LST 等兑换率变化产品用于确定对冲数量；无需价格对冲时为空。 |
| `capacity` | 总额度 | `Nullable(Decimal)` | 否 | 产品或本档公布的总可用额度，单位为存入资产。 |
| `remaining_capacity` | 剩余额度 | `Nullable(Decimal)` | 否 | 当前仍可申购的额度。来源不提供时为空。 |
| `tvl` | 产品 TVL | `Nullable(Decimal)` | 否 | 来源公布的产品规模。必须同时遵守适配器定义的计价口径。 |
| `availability` | 产品状态 | `String` | 是 | `available`、`paused`、`closed`、`unavailable` 或 `unknown`。即使暂时不可用也保留观测。 |
| `block_height` | 区块高度 | `Nullable(UInt64)` | 否 | 链上数据必须填写；CEX 数据为空。 |
| `block_hash` | 区块哈希 | `Nullable(String)` | 否 | 对可能重组的链必须填写。 |
| `finality` | 最终性状态 | `Nullable(String)` | 否 | 链上数据记录 `unfinalized`、`safe`、`finalized` 等适配器统一值；CEX 数据为空。 |
| `source_payload_hash` | 原始响应哈希 | `Nullable(String)` | 否 | 用于定位同一来源响应和审计解析变化；不在热表保存原始 JSON。 |

数组必须满足：

```text
length(reward_asset_keys) = length(reward_component_rates)
```

来源只公布总 APR/APY、不公布分项时，仍保存已知的 `reward_asset_keys`，对应的 `reward_component_rates` 元素为空；`reported_rate` 保存总利率。来源连奖励资产都没有说明时，两个数组才都留空。

## 5. 理论筛选规则

`rule_principal_loss_mode` 只回答“按照产品公开规则正常运行时，本金会不会发生不确定损失”：

| 值 | 含义 | `rule_eligibility` |
|---|---|---|
| `none` | 规则内没有本金损失 | 可以是 `candidate` |
| `fixed` | 本金损失比例事先确定 | 可以是 `candidate`，查询时扣除固定损失 |
| `variable` | 是否损失或损失比例不确定 | 必须是 `rejected` |
| `unknown` | 暂时无法确认产品规则 | 必须是 `unknown` |

典型处理：

- 再质押存在不确定罚没：`variable`、`rejected`；
- AMM LP 存在路径相关的无常损失：`variable`、`rejected`；
- 固定提前退出罚金：`fixed`，仍可作为候选；
- 浮动借贷利率但没有规则内本金扣减：本金损失为 `none`，利率方式为 `variable`，仍可作为候选。

固定费用不直接改写 `reported_rate`。查询端必须根据投入金额和持有时间计算净收益，避免把一次性费用错误地年化：

```text
持有期净收益
= 持有期利息和奖励
- 进入费用
- 正常退出费用
- 适用的固定罚金
- 固定本金折损
```

永续资金费率不进入该公式。本表也不保存“当前做空资金费率”或把它合并成净 APY。

## 6. 历史采集规则

1. 所有时间统一为 UTC，所有金额和利率使用十进制定点数；
2. 每次有效的计划采集都保存完整观测，即使数值与上一条相同；
3. 产品暂停、额度耗尽或关闭时继续写入状态观测，不能把“没有变化”和“没有采到”混为一谈；
4. 链上历史必须记录区块位置。未最终确认数据不得与最终数据混淆；
5. CEX Earn 等来源通常无法完整补回历史，应从接入之日起持续采集，不得根据当前页面伪造过去 APY；
6. LST、计息凭证等产品应优先保存可复算实际收益的 `exposure_ratio`，不能只保存页面展示的 APY；
7. `reported_rate` 是来源事实，净收益、统一期限排名和对冲可行性由查询或分析层计算。

## 7. 第一版不做的内容

- 不建立人工安全审查表；
- 不把奖励明细拆成第三张表；
- 不把永续资金费率算入收益；
- 不把做空盘口深度重复写入收益表；
- 不采集 AMM LP、期权卖方、双币理财或带杠杆循环仓位作为理论无风险候选；
- 不执行申购、赎回、质押、对冲或任何需要账户和私钥的操作。
