# 稳定币基础收益与固定期限收益：下一轮采集建议

调研日：2026-09-21。研究约束：总本金 1,000,000 USDT；目标为重复出现的净简单年化至少 3%；仅规划公开数据采集，不执行交易。本文没有查询实时可执行利率，不能据此认定今天存在达标机会。

## 判断

相比继续扩大 CEX 小币资金费扫描，先建立稳定币基础收益的净收益基准更有价值：若只供应稳定币即可达到目标，对冲多一条腿未必划算。这属于承担协议和稳定币风险的利息收入，不是无损套利。DEX 只是交易场所，DeFi 收益来源还包括借贷、协议储蓄和期限交易，不能统称价差套利。

最小一期范围建议只做 Ethereum 上 Aave V3 Core 的 USDT、USDC，以及 Sky sUSDS 三条路线；以不借款、不杠杆、不跨链为初始研究条件。Pendle 的稳定币 PT 放第二期，至多选底层机制透明且有充足深度的两个市场、相邻两个期限，先核验当前是否存在合格市场，不按页面 APY 排名选币。Morpho 暂缓，以免初期把 vault 策略、抵押品、预言机和分配路径复杂度混入基础基准。

现有文档显示当前 Aave 覆盖是 WAVAX 历史来源 APY；不能据此认为已经具备稳定币即时借贷曲线、额度和退出数据。

## 1. Aave：需要收益曲线、份额增长和出入容量

供应利息来自借款人付款，利率随利用率和治理参数变化；只存页面 APY 无法计算新增 100 万后的收入。协议利率策略提供 `calculateInterestRates`，参数包含新增流动性、总债务、reserve factor、virtual balance 等。采集应按实际部署版本使用完整输入，不能把简化 `borrow / supply` 公式当作所有版本的实现。[供应说明](https://aave.com/help/supplying/supply-tokens)、[V3 利率策略](https://aave.com/docs/aave-v3/smart-contracts/interest-rate-strategy)。

最小原始字段：

- 链、pool、协议版本/实现、reserve 地址、token 地址/decimals；USDC 与 USDT 分开，桥接币也分开。
- 同区块 liquidity index、供款与借款总额、可用/虚拟流动性、当前基础供款/借款利率及其单位；指数最后更新时间。
- 利率模型地址与版本、全部模型参数、reserve factor；supply cap 剩余额度、暂停/冻结/可用状态；奖励单列，积分估值为零。
- 治理参数变更时间、链上 block number/hash/time/finality、来源 payload hash 和采集状态。

频率建议：状态每 5 分钟，同步捕捉参数变更事件；指数每日历史锚点至少回补 90 天，能取得更多就保留更多；百万出入容量也每 5 分钟取样。若仅为初筛，可先运行 30 天，但要明确仍缺少完整利率周期证据。

分析同时输出：未投入前基础利率；分别新增 1 万、10 万、25 万、50 万、100 万后的即时基础利率；过去 7/30/90 天指数实际增长；可立即取出本金与利息的容量。指数历史衡量原市场真实经历，不代表新增百万会得到相同收益。新增资金降低利用率的反事实须单独建模。

提现以池中未借出的资产为限，不能把 TVL 或账面存款总额当退出深度。普通不借款供款没有自身杠杆清算，但仍承受坏账、合约和资产风险。[提现规则](https://aave.com/help/supplying/withdraw-tokens)、[官方风险说明](https://aave.com/docs/mcp/safety)。

## 2. Sky：作为储蓄利率基准，同时测回到 USDT 的路线

sUSDS 是 ERC-4626 份额，其 `convertToAssets` 类换算可反映实时累计收益。它提供的是 USDS 收益，不是 USDT 保本承诺；SSR 可由治理调整。USDS→USDC 的 LitePSM 有独立库存和可调整费用，因此仍需逐段验证 USDT→USDC→USDS→sUSDS 及反向路线。[sUSDS 官方机制](https://developers.skyeco.com/protocol/tokens/susds/)、[LitePSM 官方机制](https://developers.skyeco.com/protocol/liquidity/litepsm/)、[SSR 与资产风险](https://docs.sky.money/legal/skybase-international/user-risks)。

最小字段：chain/asset/vault/proxy implementation 身份；总资产/份额、实时份额兑换率、SSR 原始定点数与累计指数/更新时间、存取上限；PSM 的 USDC/USDS 可用库存、双向费用和状态；USDC↔USDT 的 1 万至 100 万双向净报价、gas；利率/合约参数变更生效时间。不得把 sUSDS 与风险和规则不同的 stUSDS 视为同路线。

频率：SSR/份额与 PSM 状态每 5 分钟；重要参数事件立即记录；每日份额增长与 90 天历史回补；相关稳定币出口报价每分钟或复用同链 AMM 报价采样。协议储蓄收益和外部奖励分列，不能把补贴重复算作可持续利息。

## 3. Pendle PT：采到期现金流与大额报价，不只采固定 APY

PT 代表底层会计资产的本金索取权；在正常条件下折价买入并持有至到期形成固定期限收益。会计资产可能是 USDC、USDe 或其他币，和实际赎回拿到的收益币也可能不同，因此不能统一假定为到期 USDT。[PT 机制](https://docs.pendle.finance/pendle-v2/ProtocolMechanics/YieldTokenization/PT)。

最小字段：chain/market/PT/YT/SY/底层协议与 token 地址，accounting asset，maturity，允许的 mint/redeem token，兑换/赎回限制和等待时间，池中 PT/SY 状态、费率与模型参数，SY exchange rate、PY index/watermark，100 万 USDT 完整买入所得 PT 数量以及提前退出的净 USDT 报价，gas 与预期到期兑付路径。

官方说明 spot rate 不包含价格冲击，应使用 router simulation 或 Hosted SDK 的真实金额输出；已经计入输出的冲击和手续费不要重复扣减。价格变动导致的执行滑点需另列。[报价 FAQ](https://docs.pendle.finance/pendle-v2-dev/FAQ)、[交易模拟字段](https://docs.pendle.finance/pendle-v2-dev/Contracts/RouterStatic/ApiReference/SwapFunctions)、[API 对冲击与滑点的区分](https://docs.pendle.finance/pendle-v2-dev/Backend/ApiOverview)。

额外否决条件：当底层 exchange rate 低于 watermark，PT 到期可能小于原预期会计资产数量；需按当前兑换规则做减值情景，不能仍以 1:1 面额算固定无损收益。[负收益机制](https://docs.pendle.finance/pendle-v2/ProtocolMechanics/NegativeYield)。

频率：市场状态与规模报价每分钟；合约/到期/赎回规则每日和变更事件采集；每日比较不同期限的净持有到期收益与提前退出成本。到期后的下一期利率未知，不能把短期收益按同利率自动滚动一年。

## 共用验收口径

收益统一先算最终回到 USDT 的净金额，再除以全部 100 万本金与真实持有天数。留在链上、留作 gas/保证金/退出缓冲的本金也在分母内；稳定币面值不能取代真实兑换价。简单年化与有效复利 APY 分开报告。

建议新增研究闸门：

1. 基础收益不含积分和无法兑现的奖励；观察至少 30 天并争取回补 90 天完整历史，报告全部日收益、缺失率、7/30/90 天结果，不能只看最高值或瞬时值。
2. 每个候选在实际新增资金及全程费用之后才与 3% 比较，分别报告 30/90/365 天假设。3% 对百万本金仅是每年 30,000 USDT，0.1% 一次性摩擦为 1,000 USDT，约吞掉 12.2 天目标收益。
3. 借贷检查供款上限、真实可提现额、流动性急降场景；PT 检查到期路线和提前退出，稳定币检查脱锚与出口容量。历史稳健性不能证明本金无风险。
4. 所有复算用整数与十进制定点，锚定同一区块。失败/未最终确认/重组区块独立记录；禁止用旧成功值当当前成功。
5. 简单单资产收益可复用现有收益模型；借贷曲线、份额索引、PSM 库存、规模报价和 PT 到期现金流具有不同语义，应增加专用定类型模型，不能塞进通用 JSON 或把 PT 当普通 APR 行。

以上闸门是研究设计建议，不表示这些平台当前利率达标，也不是交易授权。
