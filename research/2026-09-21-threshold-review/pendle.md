# Pendle：百万 USDT、净简单年化 4.5% 门槛核验

核验日期：2026-09-21。**存在接近 4.8% 的官方 PT 指示 APY，但本次没有证实 100 万 USDT 的完整路径净简单年化达到 4.5%。** 最相关的 sUSDS PT 已拿到真实金额的官方 API 报价；以正常兑付、到期 USDS 与 USDT 平价为前提，额外 gas 与退出等成本的余量仅约 **44.83 USDT**。这不是充足安全边际，更不是本金保证。

## 官方市场快照

读取 Ethereum active markets：2026-09-21 **15:11:44 UTC**（北京时间 23:11:44）。两只比较对象均在 2026-11-26 00:00 UTC 到期。

| 市场 | 官方 implied APY | API 市场 liquidity | 百万规模核验 |
|---|---:|---:|---|
| PT-sUSDS | 4.7588383463% | 3,952,244.32 USD | 本次取得 USDT→PT 百万报价，完整到期退出和 gas 未验证 |
| PT-sUSDe | 4.9773063412% | 3,954,396.01 USD | 仅市场指示值，未取百万报价；底层是另一种资产和风险结构 |

这里的 liquidity 也不是可在某个限定滑点内成交的容量。本次另读官方跨链市场 metadata（15:14:41 UTC）确认，sUSDS 市场的 **totalTvl 是 94,280,084.87 USD，而 liquidity 是 3,952,244.32 USD**；不能把前者当可吃单深度。接口里的顶层 `timestamp=2026-05-15...` 等属于市场记录时间，本报告没有拿它充当当前报价更新时间；当前来源仅有抓取时间和 HTTP Date，未取得统一报价区块锚点。

市场身份：Ethereum `0x9c560ebaf78e596cbcc27411d633a74d628dd7dc`；PT `0xdc169abe56461a2e0c034da431ac2a3ebf596094`。官方 metadata 的 accounting asset 是 **USDS**（`0xdc035d45d973e3ec169d2276ddab16f1e407384f`）；underlying asset 才是 sUSDS。计算正常兑付假设为每 PT 对应一单位 USDS 价值，不是每 PT 兑一个 sUSDS。

市场 API 的 `aggregatedApy`、`maxBoostedApy`、LP 费用、YT 奖励与积分不属于这笔纯 PT 持有收益，本报告全部没有计入。资料：[Pendle PT 语义](https://docs.pendle.finance/pendle-v2/ProtocolMechanics/YieldTokenization/PT)、[官方 API 说明](https://docs.pendle.finance/pendle-v2-dev/Backend/ApiOverview)。

## 100 万 USDT 的只读报价

请求时间：**2026-09-21 15:12:36.285246 UTC**。官方 Hosted SDK `convert` 输入 1,000,000 USDT（`1000000000000` 原子单位），输出上述 PT；使用公开虚拟接收地址，未签名、未授权代币、未广播交易。API 返回四条路由，本报告采用 PT 数量最多的一条（KyberSwap 聚合）。

| 项目 | 数值 |
|---|---:|
| 输入 | 1,000,000 USDT |
| 预期 PT 输出 | **1,008,103.678505979888513243** |
| 剩余期限 | **65.3662466985416667 天** |
| 正常兑付且 USDS/USDT 平价下、未扣剩余成本的收益 | 8,103.678506 USDT |
| 相同现金流的简单年化 | **4.525030584%** |
| 相同现金流的复利年化表示 | 4.609893303% |
| 达到 4.5% 简单年化所需期限收益 | 8,058.852333 USDT |
| 剩余全部成本预算 | **44.826173 USDT** |
| API 的底层计价 `effectiveApy` | 4.697615624% |

公式：`简单年化 = (到期可回收 USDT / 总本金 - 1) × 365 / 实际剩余天数`。直接使用本次输入/输出算现金流，不把 API 的底层计价 `effectiveApy` 直接当 USDT 现金流年化。

API 的 `fee.usd≈178.68` 已反映在预期输出中，不能重复扣除。上述余量尚未覆盖：入场和赎回 gas、未来 USDS→USDT 成交成本或折价、额外等待、报价后价格变化。API 未返回 gas 估计，未做独立节点 `eth_call` 验证，也未验证未来到期退出的资产兑换能力。因此 4.5250% 是条件下的剩余成本前测算，不是已取得的净收益。报价把 100 万全额用于兑换，仅用作容量及收益上界；实际总本金固定 100 万还须从其中留出 gas 和退出预算。

研究请求中设置了 0.1% slippage，生成的 `minPtOut=1,007,095.574827473908624729`，在同样兑付假设下对应 **3.962113389%** 简单年化。它是生成交易参数的保护下限，不是 API 预测的成交数量，也不代表实际一定损失 0.1%；但是该保护下限本身没有守住用户的 4.5% 门槛。未发送任何交易。资料：[Hosted SDK](https://docs.pendle.finance/pendle-v2/Developers/Backend/HostedSdk)、[APY 与 effective APY 定义](https://docs.pendle.finance/pendle-v2/ProtocolMechanics/PendleMarketAPYCalculation)。

## 对之前建议的修正

建议抓 Pendle 数据，是建议研究到期现金流及可执行容量，并非声称已找到 100 万 USDT、净 4.5% 的无损产品。这次报价表明，sUSDS PT 有可研究的真实金额路由，但几乎没有剩余成本空间。

本次读取 Sky 首页为 4.76%，Fixed Yield 专页为 4.80%；这些前端数字可能随时点或缓存不同，均不能替代真实金额报价。该产品到期日为 11 月 26 日，收益只能讨论到这个期限；到期再投资的利率没有锁定。Sky 官方也说明提前退出受市价影响，持有到期仍有协议和合约风险。[Sky Fixed Yield](https://sky.money/fixed-yield)。stUSDS 页面收益属于另一种可变收益产品，不可拼入 PT-sUSDS 的固定收益。

## 复算材料

- `pendle_active_ethereum.raw.json` / `.meta.json`：官方市场列表原始字节、读取时间、HTTP 状态、SHA-256。
- `pendle_susds_metadata.raw.json` / `.meta.json`：准确市场的 accounting asset、liquidity、totalTvl 等。
- `pendle_susds_million_usdt_quote.raw.json` / `.meta.json`：一次百万输入的官方报价及全部路由；报价已经过时，仅作研究复核，不应用于交易。
- `pendle_probe.py`：公开只读请求脚本。最初默认环境 DNS 受限、第一次无参数 URL 返回 403；随后标准公开请求成功。没有使用 API key 或钱包私钥。
- `pendle_calculate.py` / `pendle_summary.json`：全程 Decimal 的计算与结果；运行 `python3 research/2026-09-21-threshold-review/pendle_calculate.py` 可离线复算。
- `pendle_all_first_page.*`、`pendle_all_page_200.*`：为定位准确市场元数据保留的分页原始响应，不代表完整市场普查。

无 collector、数据库、线上配置修改。只做有界的当前报价验证，未验证该结果长期重复存在。
