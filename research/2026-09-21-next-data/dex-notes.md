# DEX 数据增量建议（研究笔记，2026-09-21）

目标口径：总资金 100 万 USDT，持续或可重复，扣除完整路径成本后简单年化至少 3%，不依赖币价上涨。本笔记没有发现或承诺当前满足条件的产品；建议次序是根据可验证性和目标匹配度作出的判断。

**值得扩大链上覆盖，首先补可执行金额和退出权利，不能把 DEX 页面利率或瞬时价差直接当成无损收益。** 同链赎回路径和固定到期资产比广泛扫描长尾 AMM 更值得先投入。

## 优先级与具体来源

| 优先级 | 采集目标 | 核心用途 | 建议频率 |
|---|---|---|---|
| P0，共用基础 | Ethereum 上经过资产身份核对的主流稳定币、ETH/LST 在 Curve / Uniswap 的实际金额报价 | 把标价折溢价变成百万资金能否进入、退出的可证伪假设 | 候选池每区块；未入围池每分钟；每小时发现池与合约变更 |
| P1 | Pendle 的稳定币记账 PT：实际买入数量、到期与赎回资产、SY 到底层兑换、退出能力 | 有明确期限的现金流，避免把未来浮动资金费外推一年；属于固定期限收益研究，并非凭空无风险套利 | 报价每分钟、入围后每区块；元数据每小时及事件触发 |
| P2a | 已有 BENQI 数据上补 Avalanche sAVAX↔AVAX↔稳定币规模报价 | 直接复核既有候选能否通过 DEX/协议铸造/聚合路径降低建仓与退出成本，边际建设成本较低 | 入围池每区块；基准规模报价每分钟；规则与份额兑换沿用已有定型采集 |
| P2b | Lido stETH / wstETH 二级市场折价、份额兑换、退出队列及最终核销 | 对照官方退出路径验证折价是否覆盖等待和对冲成本；资金是 USDT，因此必须额外验证 ETH 对冲 | 价格每区块；队列每分钟＋逐事件；完整规则每日与变更时 |
| P3 | 同链主流稳定币跨池闭环报价 | 研究原子闭环的可实现净利润和真实频次，不能承诺稳态年化 | 每区块及逐 Swap / Mint / Burn / liquidity-change 事件 |

Curve NG 的 `get_dy(i,j,dx)` 给指定输入在池状态下的预期输出；它包含基于失衡程度的动态费。应保存池版本、资产类型、余额、放大参数、费率、`offpeg_fee_multiplier`、兑换率与报价，不使用固定费率猜测脱锚时的成本。[Curve 官方池接口](https://docs.curve.finance/developer/amm/stableswap-ng/pools/plainpool)、[动态费及资产类型](https://docs.curve.finance/developer/amm/stableswap-ng/overview)。

Uniswap QuoterV2 可通过只读模拟返回 `amountOut`、`gasEstimate`、`initializedTicksCrossed`、`sqrtPriceX96After`。保存 token0/token1、fee tier、slot0、流动性和用到的 tick 状态；报价用指定 block，而不是在连续多个 latest 区块中拼出一条路径。独立报价的 gas estimate 仍需与整条路线模拟消耗区分。[Uniswap 官方报价文档](https://developers.uniswap.org/docs/sdks/v3/guides/swapping/quoting)。

v4 不能沿用 v3 池身份与静态费率假设。须增加 PoolManager、完整 PoolKey（两币、fee 字段、tick spacing、hook 地址）、hook 版本/代理实现、动态 LP 费与 hook 费处理；采入前逐类确认报价支持。v4 fee 字段可能只是动态费标记。[v4 建池与身份](https://developers.uniswap.org/docs/protocols/v4/guides/create-pool)、[动态费](https://developers.uniswap.org/docs/protocols/v4/concepts/dynamic-fees)。

前次 sAVAX 百万规模结论排除的是所检查单所单时点一次吃单路径，不排除其他交易场所、DEX 聚合或分时路径。应补 `稳定币→AVAX→协议铸造 sAVAX` 与 `稳定币→AVAX→DEX 买 sAVAX` 的同金额对照，再比较正常协议赎回与 DEX 即时退出。BENQI 官方集成页列出 LFJ；LFJ 文档有 sAVAX/AVAX binStep=5 示例，其 LBQuoter 返回 route、pairs、binSteps、versions、amounts、无滑点虚拟数量和逐腿费用，适合作为首个候选适配器。**这里只确认了官方集成和接口，没有核验某池当前余额或百万容量。** 实施时先从官方工厂/部署注册读取真实池地址，确认资产、版本和非零可用 bins，并让双向规模报价通过后才入采集白名单；不能把历史示例当作当前流动性证明。AVAX↔原生稳定币路径同样需要逐池验证，桥接版本单列。分时建仓要保存未对冲敞口时长、完成率及各片实际成本，不能只用最优片段相加。[BENQI 官方集成](https://app.benqi.fi/integrations)、[LFJ 官方 sAVAX 示例](https://developers.lfj.gg/guides/price-from-id)、[LFJ 最优报价接口](https://developers.lfj.gg/guides/best-quote)。

Pendle PT 到期兑现的单位是对应 accounting asset，不能凭 PT 名称假设兑换 USDT 或对应包装币数量。固定 PT 收益仍受底层资产、协议和退出路径影响。保存 PT/SY/YT/market 合约、accounting asset、expiry、兑换率、实际买入 PT、手续费、价格冲击、最终可提取资产。纯链上模拟与使用链下限价单/聚合路由的 API 报价应标明来源，并记录失效性。[PT 定义](https://docs.pendle.finance/pendle-v2/ProtocolMechanics/YieldTokenization/PT)、[报价模拟输出](https://docs.pendle.finance/pendle-v2-dev/Contracts/RouterStatic/ApiReference/SwapFunctions)、[链下限价单机制](https://docs.pendle.finance/pendle-v2-dev/Contracts/PendleRouter/PendleRouterOverview)。

Lido 退出是异步 FIFO 队列。排队期没有质押奖励，同时仍有极端情况下核销兑换率减记风险。采集请求与核销事件、待核销 stETH、队首/尾、已预留 ETH、暂停/保护模式、实际核销份额兑换率、历史等待时间分布；不要把合约现金当作任意投资者马上可提取额度。wstETH 与 stETH 的份额转换也须同区块。[Lido 官方退出队列](https://docs.lido.fi/contracts/withdrawal-queue-erc721/)。

## 百万规模的最低采集合同

1. 全部金额用代币整数原子单位或精确定点数。资产身份必须包含 chain ID、合约地址、原生/桥接属性和兑换权利；不能把所有 USDC 或包装资产视为相同现金。
2. 固定报价梯度建议为 1 万、5 万、10 万、25 万、50 万、100 万 USDT 等值，同时保存输入币整数金额、输出数量和方向。USDT→USDC→目标资产以及退出回 USDT 的全部兑换成本计入。用于 LST 对冲的保证金和现金缓冲必须从 100 万总预算扣除，不能同时假设有 100 万本金与额外免费保证金。
3. 报价保存 `chain_id/block_number/block_hash/parent_hash/block_timestamp/finality_status/observed_at`、route/pool/contract version、输入输出、调用成功与否、gas units、base fee、priority fee 假设、原始响应 hash、模拟版本。池状态、兑换率与报价必须有同一来源区块锚点。孤块要失效，不用前次成功值伪装本次有效报价。
4. 同链闭环要在同一状态上顺序模拟整条路径，计入每腿对共享池的冲击；多个独立最优报价不能相加，多个候选共用同一流动性也不能重复计入容量。各金额报价是互斥场景，不是相加的盘口档位。
5. 长期收益必须给出百万总本金的 30/90/365 天净现金收益；固定到期路线按真实期限。一次折价等待 7 天不能假设未来每 7 天都可全额再投；统计 30/90 天独立入场窗口、实际可部署资本时间、退出完成率、利润对 gas/延迟/深度减半的敏感性，空闲资金不算相同收益。
6. 同一机会连续出现 100 个区块只算一个持续窗口。逐区块还不足以证明自己能在区块内抢先成交：可以建立观测上界，再做下一可用区块延迟检验，不能把回看最佳点当作可执行回测。

## 为什么原子套利不是稳定被动的无风险利息

原子执行可以让闭环交易满足收益约束才完成，但不保证有机会、能进入区块或能按观测价格成交。公开链上失败交易通常还需付 gas；私有 bundle 可减少这类支出，但仍受竞争者竞价、到达时机及状态变化影响。Flashbots 官方明确列出竞价不足、竞争者更高报价、迟到和冲突等不被打包原因。[Flashbots bundle 排障](https://docs.flashbots.net/flashbots-auction/advanced/troubleshooting)。

因此第一期不应把 LP APY、积分补贴、杠杆循环、跨链桥价差纳入“无损”候选。它们可以另行研究，但无法解决本次对确定现金流、可退出性和百万容量的要求。新增 AMM 状态/金额报价/队列/赎回规则应采用专项定类型模型；合成十档可以用于已有展示，但不能替代真实金额报价和可复现池状态。
