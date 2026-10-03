# Bitfinex 融资出借的收益来源与底层风险

Bitfinex 的高息有真实市场机制，也有平台公布的成交数据支持；但它对应的是加密资产担保融资和中心化平台债权，不能据此认定为低风险美元收益。对“优先避免本金亏损”的资金，现有公开资料不足以让这项产品通过底层资产尽调，应从优先配置候选中移除。这个判断不等于认定平台正在资不抵债或虚构成交。

最重要的发现有三项：USDT 普遍成交利率低于此前最佳挂单；美元融资高度关联 BTC/USD 账面仓位；当前合同明确账户资产并非为用户独立隔离。能看到利息和部分融资用途，并不等于能逐笔验证抵押品、借款人和整个资产负债表。

**高息确实出现，但不能把最佳报价当典型收益。**

2026 年 9 月 12 日 14:07 至 9 月 13 日 14:07 UTC 的完整 24 小时，官方公共成交接口返回以下去重数据。利率按成交金额加权；“扣分成后”仅扣普通订单 15% 利息费，未扣闲置时间、转账、换汇、税项或本金损失。成交利率是新撮合时的合同利率，不是已经收取整年利息的业绩。[^1][^2]

| 币种 | 成交笔数 | 成交金额 | 加权毛 APR | 扣利息分成后 APR |
|---|---:|---:|---:|---:|
| USD | 88,548 | 3.773 亿美元 | 9.59% | **8.16%** |
| USDT | 11,230 | 4,748 万 USDT | 7.90% | **6.72%** |

此前 8.50%/8.29% 是当时最佳借款档对应的扣费年率，算式本身没有错误，但它们不代表大多数资金拿到的收益。24 小时中，USDT 费后不低于 8% 的成交只有约 128 万，占总成交金额约 **2.70%**。新抓取的两个资金簿中，费后至少 8% 的借款报价各只有一张 120 天订单；一张订单能容纳十万美元，并不意味着这个利率普遍且持久。[^1]

期限差异尤其明显：

| 币种 / 合同最长期限 | 成交量占该币全部成交 | 扣分成后加权 APR |
|---|---:|---:|
| USD / 2 天 | 33.79% | 6.26% |
| USD / 120 天 | 16.42% | 约 9.53% |
| USDT / 2 天 | 81.96% | 约 6.57% |
| USDT / 120 天 | 1.62% | 约 8.12% |

USD 的 120 天成交并不少，但最大十笔占该期限成交金额约 33%；USDT 的 120 天最大十笔占约 80.6%。这些是按最大若干笔成交计算的成交金额集中度，不是借款人集中度：一名借款人可以拆很多笔，多个订单也可能属于同一主体。公共接口没有提供足以计算关联方和独立借款人集中度的身份信息。

另一个关键是不对称的退出权。出借人一旦匹配不能主动提前收回，借款人可以提前偿还、换借更低利率；120 天不等于保证收到 120 天利息。借款人购买了较长的可用资金期限，并保留提前归还的选择，出借人承担续贷利率下降和资金空闲的风险。[^2]

此前 90 天 USD/USDT 费后 FRR 均值约 10.68%/8.16%，只证明存续贷款的历史利率环境。FRR 按既有固定利率融资加权，每小时更新，会与新成交利率不同，尤其在降息阶段可能偏高。数据来自交易所自报接口：它能支持“平台公开记录里有高息撮合”，不能独立证明每个账户的经济独立性、最终偿还情况或平台没有未披露负债。[^1][^3]

**利息由借款人支付，出借人的直接底层是贷款债权。**

融资有两条主要使用路径。第一条是保证金交易，交易者借入 USD/USDT 建立仓位。第二条是 Bitfinex Borrow，借款人把加密资产留作抵押后，可以将借得的资金提离平台。Borrow 也在平台账户内建立对应的 long/short margin position，因此仅看到 BTC/USD 多头记录，不足以证明贷款全部用于站内买入比特币。[^4]

资金关系可概括为：

```text
出借人的 USD / USDT
        ↓ 撮合贷款
借款人：保证金交易，或 Borrow 提款使用
        ↓ 利息、本金偿付
出借人收入（平台扣除利息分成）

偿还不足时：借款人权益与抵押品 → 平台清算及担保权执行
                                   ↓ 无法足额回收的风险
                              出借人可能承担损失

另一个独立层次：USDT 本身 → Tether 发行人的储备与赎回义务
```

借款人不必有一个确定赚 10% 的资产，才会愿意支付 10% 年利率。借 10,000 美元、只用 7 天，10% 年率对应约 19.18 美元利息；对于需要短期杠杆或现金流的交易者，这个成本可能可以接受。这解释了借款需求存在的经济逻辑，却不证明每个借款人的投资有盈利能力。正常偿还也可能来自出售资产、补充现金或再融资。

2026 年 9 月 13 日 **14:09 UTC 同一分钟**，公开资金用途统计显示：

| 项目 | USD | USDT |
|---|---:|---:|
| 已提供融资总额 | 61.745 亿美元 | 4.204 亿 USDT |
| 已用于仓位的融资 | 61.334 亿美元 | 3.612 亿 USDT |
| 其中 BTC 对应交易对的账面融资 | 58.220 亿美元 | 3.211 亿 USDT |
| BTC 相关占“已用于仓位融资”的比例 | **94.92%** | **88.91%** |

这比只看利率更接近底层：大量融资与 BTC/USD、BTC/USDT 账面仓位关联。但这些数字不是抵押品构成，不能说 94.92% 的抵押品就是 BTC，也不能说借款人均在裸多 BTC。Borrow 的合成仓位、外部对冲、跨市场用途与借款人其他资产仍不可见。各接口定义没有单独披露 Borrow 占比。[^1][^4]

“已用于仓位 / 已提供融资”为 USD 99.34%、USDT 85.91%，仅描述已提供贷款如何使用，不包括未撮合报价及全部闲置钱包资金，不能冒充全平台资金池利用率，也不能直接与 Aave 利用率比较。

**担保品是波动资产，强平有效性决定出借人能否收足。**

Bitfinex Borrow 官方 BTC 例子允许价值 1,000 美元的抵押品借出 900 美元，即 90% LTV。当前公开配置中 BTC/USD 保证金交易初始/维持保证金为 10%/5%，ETH/USD 为 20%/10%；这两种保证金比例与 Borrow 的 LTV 是不同规则，不能直接互换。[^4][^5]

允许抵押的资产也不只 BTC、ETH 和美元。平台对较弱抵押资产打折，有助于限制风险。例如当前 SOL 按价值的 70%计入，意味着折扣 30%；ADA 按 30%计入，意味着折扣 70%。API 字段名带 haircut，但其数值不能直接当作“扣除比例”读取。折扣和杠杆限制是风控措施，不是美元现金担保，更不证明当前所有贷款都在限额内。[^5]

正常流程下，借款人的保证金先承担亏损，并在抵押品耗尽前触发清算，出借人不会直接承担整笔 BTC 的正常涨跌。但清算要有可成交的买方、足够深度及能运行的交易系统，盘口跳空或平台故障会让触发价格与卖出价格不同。

假设抵押品最初价值 1,000 美元、贷款本金 900 美元，清算因异常延迟最终只卖得 850 美元，则在未计费用和利息前已有 50 美元缺口，相当于贷款本金 5.56%。这是压力情景，不是当前违约概率，也不是声称价格跌 15%就必然让所有出借人亏损。它说明：初始抵押超过贷款，只能减轻损失，不能消除回收不足。

帮助中心明确说明，极端价格变化可能令抵押品不足，最终损失可以分担给融资提供者。平台可以选择接管问题仓位并自担损益，但现行条款将其列为权利和酌情处置，未将其变成必须履行、金额无限的本金担保。衍生品保险基金属于另一套合约规则，不能据此为 P2P 出借补上一份不存在的保证。[^6]

Borrow 可提走的借款目前每个 Financing Recipient 上限为 250,000 美元，但这是 Borrowing 规则，不能套用到全部 Margin Funding 存量。因此不能用该限额推断六十多亿美元融资一定分散在大量小借款人手中。[^7]

**为什么其他平台的 USDT 收益更低。**

差异不全是显示口径。同期读取 OKX 官方公开 API：USDT 借款 APR 为 **3.50%**，最新出借利率字段为 **2.89%**；最近 24 个小时的出借利率均值约 2.895%。即使拿借款成本比较，仍明显低于 Bitfinex 的 USDT 24 小时新成交毛 APR 7.90%。所以不能用“别家多抽了手续费”解释完这个差距。OKX 当前规则也显示收取 15% 的利息服务费。[^8]

能够确认的结构差异包括：

| 因素 | 对收益比较的影响 | 证据能支持到哪里 |
|---|---|---|
| 已匹配单笔贷款与活期资金池 | 单笔贷款按借出金额报利率；资金池收益受保留现金、资金供求及分配规则影响 | Binance 官方明确部分池子资金留供赎回；Aave 利率与利用率相关 |
| 期限与提前归还权 | 120 天、2 天和活期退出条件不同 | 本轮实际成交已体现明显期限差异 |
| 客户和借款市场不同 | 平台内美元/USDT需求、杠杆额度、担保资产、渠道不同 | 可见账面融资高度集中 BTC 对应交易对；借款主体和外部用途未知 |
| 进入及退出渠道 | KYC、地区、美元电汇最低额和费用妨碍资金即时流入 | Bitfinex 美元电汇最低额 10,000，费用和到账时间真实存在 |
| 信用与透明度 | 出借人承担平台、回收及法律执行风险，可能要求补偿 | 合同可证这些风险存在，不能从利差反推出一个可靠违约概率 |

Binance Simple Earn 的资金同样可以用于贷款和保证金业务，不应把其他平台的低息自动当作国债或银行存款。低息只能说明收益较低，不能单凭利率给平台安全性排名。以上因素解释为何不同市场能长期保留价差；公开信息不足以精确分解，究竟多少来自期限、多少来自供求、多少来自信用溢价。[^9]

借低息 USDT 再贷给 Bitfinex，也不是把差额直接乘总本金的无风险套利。假设 10 万美元自有抵押只借出 7 万，借款率 3.5%、出借率 8.2875%、抵押品不计收益，则赚息差约 3,351 美元，仅为自有资本 **3.35%**，还未计费用。这是算式示例，未声称任何账户当前能获此借款额度。如果 Bitfinex停提，向另一家借的钱仍要归还；两端期限和利率也未匹配。即便抵押品另有收益，仍须把全部资本、借款成本和两端风险一起计算。

**当前合同中的债权和托管结构，比“有抵押”更重要。**

2026 年 8 月 12 日版 Exchange Terms 的主要条款为：3.5 将融资一般定义为用户之间的关系；6.2 由平台代理设置和执行有利于出借人的担保权；17.16 明确账户资产不是以用户身份或受益独立隔离、而是记录在平台账簿；23 明确无政府存款保险，平台自有保险也不自动覆盖其他用户。第 5 条不承诺承担出借人对借款人的损失。[^7]

因此 Funding Wallet 是账户内的业务分类，不是可据名称认定的破产隔离信托。担保权有意义，但不能从一条 lien 条款直接推导为平台破产时必然优先、足额且迅速兑付；具体优先顺位和执行依赖适用法律及实际资产。反过来，也不能在未做法律分析时断言所有出借人必然属于普通无担保债权人。

Bitfinex 公布的钱包地址可以帮助验证某些链上资产的存在，是有用的信息。但地址余额本身不能证明全部银行现金、客户负债、关联方往来、资产是否已另行质押，以及每笔贷款的担保覆盖。本次可核的公开材料没有提供将资产、全部负债、担保受限情况与压力损失逐项核对的完整 Bitfinex 贷款池审计。[^10]

**USDT 发行人的储备是另一层底层，不能替代贷款池验证。**

Tether 的 2026 年 6 月末储备报告列资产 1,877.51 亿美元、负债 1,836.42 亿美元，超额资产约 **41.10 亿美元**，相当于负债约 **2.24%**。主要构成如下；数字为报告日资产，并非 9 月 13 日实时资产：[^11]

| 资产 | 约占储备 |
|---|---:|
| 美国短期国债 | 61.23% |
| 隔夜及定期逆回购 | 13.65% |
| 黄金 | 10.03% |
| 担保贷款 | 7.17% |
| 比特币 | 3.09% |
| 其他投资 | 2.79% |
| 上市股票 | 2.00% |
| 其他现金类及公司债 | 约 0.04% |

约四分之三为现金类与短期工具，但不能称全部是现金或国债。黄金、BTC、股票和贷款的风险各不相同；黄金等可交易资产也不应一概叫作缺乏流动性。超额资产是吸收损失的缓冲，不是外部保险。BDO 对该日数据提供合理保证，但不保证持续经营判断、极端变现价格或其他日期，报告附后的 Notes 不在其鉴证意见范围内。[^11]

需要区分最新审计进展。Tether 在 2026 年 8 月 13 日公告，KPMG U.S. 已对 Tether International 的 2025 年财务报表完成审计并出具无保留意见。这是有利于透明度的重要进展，不能继续说它从未完成完整审计；但公开公告未附全部审计报表与附注，本次能直接核读的储备原件仍是 Q2 BDO 报告。审计对象也不是 Bitfinex 的每笔融资或当前客户债权。[^12]

两套披露的 2025 年末净资产数不完全相同，公告为 68.14 亿、Q2 表中比较数约 63.38 亿；未取得完整调节表，不能自行归因为会计准则或错误。Q2 表的半年度权益桥列期初 63.38 亿、Financial result 负 31.71 亿、净资本变动正 9.43 亿，期末约 41.10 亿；它不提供足够的季度估值、分红及经营项目分拆，不能把该变化直接说成挤兑或主营现金亏损。[^11][^12]

Tether 储备产生的利息不会自动归 USDT 持有人。发行人文件规定持有人不享有超过面值的储备增值，赎回依条款扣费。直接赎回最低 10 万美元、费用至少 1,000 美元或 0.1%取较高，还需满足身份及银行要求；零售卖出通常依赖二级市场。这些赎回条件不能保证在 Bitfinex冻结资产时，出借人能绕过平台直接取回币。[^13]

因此，出借 USDT 相比自托管持币增加了借款人和 Bitfinex 层面的风险；出借真实 USD 可以少承担 USDT 发行人这一层，但仍不能解决同一家平台的托管和贷款回收风险。把 USD 和 USDT 各放一半在 Bitfinex，不是对平台信用风险的有效分散。

**历史事件说明损失路径真实存在，但不证明当前已经出事。**

2016 年黑客事件后，Bitfinex 官方说明损失分配覆盖所有用户及币种，并用 BFX 记录美元化损失。2017 年剩余 BFX 被赎回，其中此前存在债转股；这是后续补偿，不能抹去期间无法按原额取款及提前卖出损失的可能。帮助中心所说清算穿仓分摊“从未发生”，针对的是特定清算机制，不能扩大为平台历史上客户资产从未受损。[^14]

纽约州总检察长 2021 年和解文件记录了 Bitfinex 支付处理商资金受限、约 8.5 亿美元缺口，以及随后动用 Tether 资源与关联交易的历史；和解罚款为 1,850 万美元。协议第57(a)段同时确认，关联额度贷款已于2021年1月全部偿还。该历史说明平台与稳定币发行人风险可能相互传导，也说明应核对完整负债和关联往来。它不是对 2026 年贷款仍存在同样缺口的证据，不能据旧事件断言当前高息用于填窟窿。[^15]

**对本金保护目标的判断。**

| 尽调问题 | 当前结论 |
|---|---|
| 利息支付方与产品机制能否解释 | 能：借款人付息，平台撮合并扣分成 |
| 高息是否只有孤立报价 | 否：有大量官方成交记录；USDT典型成交低于最佳档 |
| 能否确认具体借款人、关联集中度和完整用途 | 不能 |
| 能否逐笔验证当前抵押资产、LTV与清算深度 | 不能，仅能核规则及部分统计 |
| 是否有法律隔离和无条件本金兜底 | 未见；现行合同明确保留相应风险 |
| Tether 的储备与审计能否保证 Bitfinex出借安全 | 不能，它们是不同的债权与风险层 |
| 能否认定当前资不抵债、虚构成交或庞氏融资 | 不能，现有证据不支持这种指控 |
| 是否适合“尽量不亏本金”的优先资金 | **不通过当前尽调，不作为优先配置** |

8%左右利息的直观吸引力，不能替代对本金回收的验证。举例一次损失本金20%，相当于约2.4年8.29%的简单利息；这只是压力比较，不是发生概率估计。没有损失概率、违约回收率、抵押集中度和完整负债，就不能计算一个可信的风险调整后收益，更不能称其“赚得多、风险小”。

若未来要改变这项判断，关键证据应是贷款与全部负债的可核验覆盖、借款人及关联方集中度、抵押资产分布与压力清算结果、以及出借债权的独立法律保障；更多高利率截图或更长的平稳付息记录都不能替代这些证据。

**数据及复核范围。**

市场观察截至2026年9月13日；24小时成交窗口为9月12日14:07至9月13日14:07 UTC，资金用途为9月13日14:09 UTC，OKX比较报价为14:00 UTC。成交按ID去重，分页保留相同时间戳并核查边界；金额和利率用十进制定点计算。90天FRR沿用同日冻结资料，特殊字段按官方定义换算，不将日率重复年化。所有判断限于公开信息，未读取账户或执行交易。

可复核的分项材料：

- [成交、期限、历史与用途统计核验](/home/ubuntu/crypto-market-info/research/2026-09-13-bitfinex-underlying/funding-demand-verification.md)
- [抵押品、Borrow与损失分摊核验](/home/ubuntu/crypto-market-info/research/2026-09-13-bitfinex-underlying/collateral-risk-verification.md)
- [平台条款、Tether储备与历史事件核验](/home/ubuntu/crypto-market-info/research/2026-09-13-bitfinex-underlying/platform-and-tether-verification.md)
- [成交分析数据](/home/ubuntu/crypto-market-info/research/2026-09-13-bitfinex-underlying/demand-analysis.json)、[比较利率及压力算式](/home/ubuntu/crypto-market-info/research/2026-09-13-bitfinex-underlying/comparator-analysis.json)

**来源。**

[^1]: Bitfinex，公共 Trades、Funding Statistics、Stats1 接口，2026-09-13读取。[成交接口说明](https://docs.bitfinex.com/reference/rest-public-trades)、[融资统计](https://docs.bitfinex.com/reference/rest-public-funding-stats)、[用途统计](https://docs.bitfinex.com/reference/rest-public-stats)。原始响应及来源URL、UTC、哈希见分项数据文件。
[^2]: Bitfinex，[Margin Lending](https://www.bitfinex.com/margin-lending/)，2026-09-13查阅；期限、匹配、提前归还及15%利息分成。
[^3]: Bitfinex Help Center，[Funding Flash Return Rate](https://support.bitfinex.com/hc/en-us/articles/213919009-What-is-the-Bitfinex-Funding-Flash-Return-Rate)，FRR构成与更新规则。
[^4]: Bitfinex Help Center，[What is Bitfinex Borrow](https://support.bitfinex.com/hc/en-us/articles/900003195246-What-is-Bitfinex-Borrow)，当前官方正文与Zendesk原始记录，2026-09-13读取。
[^5]: Bitfinex，[Margin call policy](https://support.bitfinex.com/hc/en-us/articles/213895229-Margin-call-policy-on-Bitfinex)及[当前公开保证金配置](https://api-pub.bitfinex.com/v2/conf/pub:spec:margin)，2026-09-13读取；抵押折扣定义和原文见分项核验。
[^6]: Bitfinex Help Center，[Risks associated with offering funding](https://support.bitfinex.com/hc/en-us/articles/213918969-Risks-associated-with-offering-funding-Frequently-Asked-Questions-FAQ)；并参照现行Exchange Terms第9条。
[^7]: Bitfinex，[Exchange Terms](https://www.bitfinex.com/legal/exchange/terms/)，2026-08-12版本；[官网前端实际载入的条款正文](https://api-pub.bitfinex.com/v2/conf/pub:legal:terms:tos)，2026-09-13取得，重点为3.2、3.5、5、6.2、17.16、23条。
[^8]: OKX，[公共借贷API定义](https://www.okx.com/docs-v5/en/#financial-product-simple-earn-flexible-get-public-borrow-info)、[USDT公开借贷历史](https://www.okx.com/api/v5/finance/savings/lending-rate-history?ccy=USDT&limit=100)、[Simple Earn Flexible规则](https://www.okx.com/en-gb/help/introduction-to-okx-simple-earn-flexible)，2026-09-13读取。
[^9]: Binance，[Simple Earn Flexible资金用途与收益](https://www.binance.com/en/support/faq/detail/3bd1a6eba20a445da1e94bf6cfa52e80)，2026-04-22更新；Aave，[Supply Tokens](https://aave.com/help/supplying/supply-tokens)；Bitfinex，[入出金规则](https://www.bitfinex.com/deposits-withdrawals/)。
[^10]: Bitfinex，[官方公开钱包清单](https://raw.githubusercontent.com/bitfinexcom/pub/main/wallets.txt)，2026-09-13取得。钱包地址披露与完整资产负债审计的差异为分析判断。
[^11]: BDO Advisory Services / Tether International，[2026-06-30 Financial Figures & Reserves Report](https://assets.ctfassets.net/vyse88cgwfbl/2kYf7r64h3tzwiu6F0CbUB/2997abd2f11ecea74a21528048b50707/Opinion___Report_-_Tether_International_Financial_Figure_30-06-2026.pdf)，2026-07-31签署；重点见鉴证页2–3及报告页1、3–4。[本地原件](/home/ubuntu/crypto-market-info/research/2026-09-13-bitfinex-underlying/tether-2026-q2.pdf)。
[^12]: Tether，[Tether Completes the Largest Inaugural Financial Audit in History](https://tether.io/news/tether-completes-the-largest-inaugural-financial-audit-in-history/)，2026-08-13；这是发行人关于KPMG审计的公告，未将其当作已取得全部审计原件。
[^13]: Tether International，[Relevant Information Document](https://tether.to/public/Relevant_Information_Document_-_Tether_International%2C_S.A._de_C.V..pdf)，面值及收益权；[当前费用](https://tether.to/en/fees/)及[条款](https://tether.to/en/legal/)，2026-09-13读取。
[^14]: Bitfinex，[Security Breach & BFX Token FAQ](https://blog.bitfinex.com/announcements/security-breach-faq/)，2016-08-26；[100% Redemption of Outstanding BFX Tokens](https://blog.bitfinex.com/announcements/100-redemption-outstanding-bfx-tokens/)，2017-04-03。
[^15]: New York Attorney General，[iFinex / Tether Settlement Agreement](https://ag.ny.gov/sites/default/files/settlements-agreements/ifinex_inc.pdf)，2021年；历史事件与当前偿付能力分别判断。
