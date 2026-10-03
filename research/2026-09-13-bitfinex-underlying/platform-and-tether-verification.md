# Bitfinex 借贷平台与 USDT 底层独立核验

核验时间：2026-09-13 14:04–14:16 UTC。只用公开发行人、平台与监管原始材料；没有账户访问或交易。本报告核验资产与法律结构，不给出违约概率或保证收益。

结论：在 Bitfinex 借出 USDT，同时承担借款人抵押清算、平台托管与 USDT 发行人三层风险。公开材料支持“有抵押、可清算的 P2P 借贷”，不支持“本金无风险”。尤其是现行条款明确账户资产不按客户名义或利益隔离。Tether 储备属于 USDT 偿付支持，并不是放贷人直接持有的国债，也不是 Bitfinex 贷款池的保险金。

## 1. Bitfinex 实际法律关系

官网条款页初始 HTML 只是导航；本轮追踪其公开前端 `fetchLegalTerms` 到 [实际条款 API](https://api-pub.bitfinex.com/v2/conf/pub:legal:terms:tos)，取得最后更新 **2026-08-12** 的完整正文。不能以搜索索引中的 2026-04-06 版本冒充最新。原始 JSON、解码正文及 SHA-256 均留在本目录。[官网入口](https://www.bitfinex.com/legal/exchange/terms/)

| 现行条款位置 | 核验内容及意义 |
|---|---|
| 开篇合同主体 | 非美国用户的服务合同方为 **BFXWW Inc.**；不要把借贷合同方一律写成 Tether 或 iFinex。 |
| 3.5、3.6.1 | 出借人与融资接受方通过融资簿达成 P2P 合同；Bitfinex 通常不作为该合同一方。利率、金额与期限匹配确定，但借款人可以提前偿还并换成更便宜的融资，所以长报价期限不等于能持续收足全期利息。 |
| 6.2 | 借款人委任 Bitfinex 为独家代理，在出借人利益上对其站内法币和代币设立 **lien（担保权利）**，必要时清算。 |
| **17.16** | 账户、子账户及代币钱包中的资产，**不是以用户名义或为用户利益隔离持有的资产**，而是记在 Bitfinex 账簿上。界面上的 funding wallet 分区不能当作法律隔离。 |
| 5、23 | 平台不保证清算能避免出借人损失；法币和代币不受政府存款保险保障，平台自行购买的保险也不等于保障用户。 |

上述 lien 与非隔离托管必须同时看。它既不是一笔简单的“给 Bitfinex 的无担保贷款”，也不能被称为“破产中绝对优先、完全隔离的担保债权”；实际破产分类、担保完善及执行仍取决于适用法律与事实。这是对条款的结构解读，不是法院裁定。

[官方 funding 风险 FAQ](https://support.bitfinex.com/hc/en-us/articles/213918969-Risks-associated-with-offering-funding-Frequently-Asked-Questions-FAQ) 进一步承认：极端价格变化和流动性不足可能造成抵押穿仓，若损失超过平台愿意覆盖的程度，最终可能由融资提供方分担。文中的“未发生过”针对该清算机制描述，不能覆盖所有历史平台事故。

## 2. Bitfinex 的储备公开材料能证明什么

[2022-11-11 官方公告](https://blog.bitfinex.com/announcements/bitfinex-resilient-in-face-of-market-events-committed-to-greater-transparency-and-demonstrating-proof-of-reserves/) 公布了钱包地址及 Antani、Ballot 等储备证明技术计划。本轮重新取得 [官方钱包清单](https://github.com/bitfinexcom/pub/blob/main/wallets.txt)，内容是链与地址，**没有对应时点客户负债、其他债务、借款抵押及关联方余额的完整表**。

地址余额透明与客户负债审计是不同证据。本轮在官方公开材料中没有取得可据以算出当前 Bitfinex 全部偿付覆盖率的独立完整审计，亦不能用技术项目代码或 2022 年余额冒充当前实施结果。这是本轮可核证据的边界，不表示已证明平台当前有缺口。

## 3. USDT 本身的权利、费用与利息来源

Tether 现行 [发行条款](https://tether.to/en/legal/) 更新于 2026-02-26，发行主体为 **Tether International, S.A. de C.V.**。第 4.1 条规定合资格已验证客户按每 USDT 1 美元减费赎回至同名银行账户；代币不是法币，也没有 FDIC、SIPC 或发行人提供的保险。

发行人的 [2026-02-20 Relevant Information Document](https://tether.to/public/Relevant_Information_Document_-_Tether_International%2C_S.A._de_C.V..pdf) 第 15 页明确持有人无权取得储备超过代币面值的增值，售后义务是按条款赎回。因此单纯持有 USDT **不自动获得国债利息或发行人储备收益**；Bitfinex 的额外利息来自另行借贷关系，不能把两层收益混为一谈。

[现行费用表](https://tether.to/en/fees/)：直接申购和赎回最低 **100,000 美元**；赎回费为 **1,000 美元与 0.1% 两者孰高**；验证费 150 USDT 可计入代币赎回，不应无条件重复相加。因此 1 万美元持有人不能直接向发行人赎回；10 万美元赎回最低费相当于 1%，100 万美元时为 0.1%。二级市场卖币不是发行人直接赎回，适用市场价格、交易费和交易所资格；发行人也保留依条款冻结或暂停服务的权利。

## 4. 最新可直接检查的储备鉴证：2026 年第二季度

来源为 [BDO 签署的完整 Q2 PDF](https://assets.ctfassets.net/vyse88cgwfbl/2kYf7r64h3tzwiu6F0CbUB/2997abd2f11ecea74a21528048b50707/Opinion___Report_-_Tether_International_Financial_Figure_30-06-2026.pdf)，报告时点 **2026-06-30 23:59 UTC**，签署日 2026-07-31。它包含资产和负债，不可说成“只看资产、不看负债”。

| 资产类别 | 十亿美元 | 占总资产 |
|---|---:|---:|
| 美国国库券 | 114.960964 | 61.2304% |
| 隔夜逆回购 | 18.625552 | 9.9203% |
| 定期逆回购 | 6.993429 | 3.7248% |
| 非美国国库券 | 0.022375 | 0.0119% |
| 现金及银行存款 | 0.040307 | 0.0215% |
| 公司债 | 0.008711 | 0.0046% |
| 贵金属 | 18.838357 | 10.0337% |
| Bitcoin | 5.801631 | 3.0901% |
| 上市股票 | 3.761439 | 2.0034% |
| 其他投资 | 5.244912 | 2.7935% |
| 有抵押贷款 | 13.453750 | 7.1657% |

总资产 **187,751,426,411 美元**，总负债 **183,641,897,215 美元**，差额 **4,109,529,196 美元**，相当于负债的 **2.2378%**。其中代币负债为 183,622,105,630 美元。前五项“现金及现金等价物、短期存款”合计约 **74.91%**；储备不是 100% 现金或国债。明细由十进制计算并验证相加等于总资产，见 `tether-reserve-calculations.json`。[发行人 Q2 公告](https://tether.io/news/tether-posts-strong-q2-performance-generates-1-5b-net-operating-profit-maintains-4-11b-reserve-buffer-and-expands-gold-holdings-to-more-than-146-tons/)

BDO 使用 **ISAE 3000（Revised）的合理保证**，不能误称有限保证；报告又明确限于上述时点及指定财务信息，**不保证持续经营判断，不保证其他日期**。正常交易条件估值也不等于压力情景下立即变现价值。所附财务信息不是整套 IFRS 财务报表，末尾补充 Notes 不在保证范围。

Q2 文档给出的权益桥为：2025 年末 6,338 百万美元，半年 Financial result −3,171 百万美元，净资本变动 +943 百万美元，得到 6 月末 4,110 百万美元。该文档没有提供 Q1→Q2 各项估值损益、分红等完整归因，不能将储备缓冲下降直接解释为挤兑或主营经营亏损；Q2 公告的 1.5 十亿美元 operating profit 是另一口径。

## 5. 审计的新进展及仍未核到的部分

**不能再照搬“USDT 一直只有鉴证、从无完整财务审计”的旧说法。** [Tether 2026-08-13 官方公告](https://tether.io/news/tether-completes-the-largest-inaugural-financial-audit-in-history/) 宣布 KPMG U.S. 已完成 Tether International, S.A. de C.V. 截至 2025-12-31 年度财务报表的审计，并出具 US GAAP 无保留意见，涉及资产负债表、利润、权益及现金流。

证据层次须准确：本轮直接读到的是**发行人公告**；公告未提供完整审计原件链接，本轮透明度页面与官方限定检索也未取得可逐项检查的 KPMG 签字报告及全部附注。不能声称已独立审阅 KPMG 原件；更不能把发行人财报审计说成 Bitfinex 托管/贷款池审计或当前实时偿付保证。

公告称 2025 年末储备超过负债 6.814 十亿美元，而 Q2 表内比较期权益为 6.338 十亿美元。缺乏 KPMG 审计附注，本轮无法桥接差额；不能擅自归因会计准则差异，更不能混合两套数字计算净资产变动。

## 6. 关联关系与历史事故：发生过什么、后来如何

当前 [Bitfinex 管理层页](https://www.bitfinex.com/about/) 列 Paolo Ardoino 为 CTO、Giancarlo Devasini 为 CFO；Tether 最新公告列前者为 CEO，Q2 报告由后者以董事长身份签署，可直接验证管理层重叠。Tether RID 第 9 页列发行人的直接股东为 Tether Global Investment Fund 与 Tether Operations 两个萨尔瓦多实体。本轮没有完整穿透到 2026 年最终股东名单，故不把“iFinex 直接拥有全部 Tether”写成已核事实。

| 历史事实 | 证据与边界 |
|---|---|
| 2016 年黑客损失分摊 | [2016-08-06 平台公告](https://blog.bitfinex.com/announcements/bitfinex-interim-update/) 将损失一般化分配给各账户，比例 **36.067%**，以 BFX 代币记录损失。不能宣传“平台用户从未损失”。 |
| 后续补偿 | [2017-04-03 平台公告](https://blog.bitfinex.com/announcements/100-redemption-outstanding-bfx-tokens/) 宣布剩余 BFX 按 1 美元全数赎回，此前有持有人转成股份。该结果有助于评价历史处理，但不保证未来也能补偿；提前折价卖 BFX 的用户并不自动获得同等结果。 |
| 2018–2019 年资金问题及 2021 年和解 | [NYAG 原始和解协议](https://ag.ny.gov/sites/default/files/settlements-agreements/ifinex_inc.pdf) 第 48–52 段记载 Bitfinex 无法取得 Crypto Capital 约 **8.5 亿美元**，Tether 此前转给 Bitfinex 的 **6.25 亿美元**并入 2019 年的 9 亿美元额度且未充分披露。第 57(a) 段同时确认额度贷款于 **2021 年 1 月全部偿还**。处罚 1,850 万美元并限制纽约业务；被调查方对监管发现不承认也不否认。 |

历史材料足以说明关联融资、托管损失及披露风险曾经落地，但**不能据此断言 2026 年存在同样亏空或造假**。本轮正面证据包括 Q2 资产负债鉴证和发行人披露的新审计进展；尚未被这些证据覆盖的是 Bitfinex 自身全部负债、贷款池质量、当前可动用担保与客户资产的法律隔离。

审阅用本地原件：`bitfinex-tos.json/.txt`、`bitfinex-rds.json/.txt`、`bitfinex-wallets.txt`、`tether-2026-q2.pdf/.txt`、`tether-rid.pdf/.txt`、`tether-legal.html/.txt`、`tether-2025-audit-announcement.html/.txt`、`nyag-settlement-2021.pdf/.txt`。抓取时间及 SHA-256 见 `sources.json`。
