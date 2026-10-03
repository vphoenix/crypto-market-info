# Bitfinex 融资出借：抵押品、资金用途与损失分摊独立核验

核验日期：2026-09-13。公开原始响应采集窗口为 **14:04:48–14:12:12 UTC**。只读公开 API、官方规则与官方代码仓库；没有读取账户、凭据或逐笔私人交易。价格、金额与比例计算全部使用 Decimal。

**结论：Bitfinex 的出借有抵押及强平机制，但现有公开数据不能证明逐笔、持续、可变现的抵押覆盖，更不能证明本金不亏。** 借款用途包括站内现货保证金交易和可以提款的 Bitfinex Borrow；抵押品可以是多种波动资产，普通保证金采用全仓抵押。官方允许极端亏损传给融资出借人，没有核到对 P2P 融资出借人承诺固定规模、独立隔离并可请求赔付的保险基金。此前扣 15% 利息费后约 8% 的出借收益，仍必须同时承担这类本金尾部风险。

## 1. 产品和债权币种需要分开

| 对象 | 出借款用在哪里 | 抵押与提款 | 与保险基金的关系 |
|---|---|---|---|
| 普通 Margin Trading | 借法币/代币完成站内现货交易；借币也可做空 | Margin Wallet 全仓资产和仓位盈亏共同支持敞口；多空可以共存 | 适用 Exchange Terms 和 P2P funding 风险说明 |
| Bitfinex Borrow | 同一融资订单簿提供的抵押贷款，可用于交易以外用途 | 抵押品锁在站内；满足条件后借款可立即提走；账面生成一个 long/short margin position | 同样适用 P2P funding 风险说明，不能当无抵押个人贷，也不能假设资金一直留在站内 |
| 永续/其他衍生品 | 交易衍生合约，不是向融资簿出借本金赚利息 | 单独的 Derivatives Wallet/衍生品规则 | 官方 Liquidation Fund 属于衍生品制度，不能用其余额替 P2P 出借本金背书 |

来源：[Margin FAQ](https://support.bitfinex.com/hc/en-us/articles/60356897072665-Margin-Trading-Frequently-Asked-Questions-FAQ)、[Borrow](https://support.bitfinex.com/hc/en-us/articles/900003195246-What-is-Bitfinex-Borrow)、[当前 Exchange Terms](https://www.bitfinex.com/legal/exchange/terms/)、[Derivatives Terms](https://www.bitfinex.com/legal/derivative/terms/)。

`fUSD` 是 USD 融资；`fUST` 的 `UST` 是 Bitfinex 内部的 **Tether USDt** 代码，已用官方 currency:sym 映射核验。后者首先是 USDT 数量债权，不能直接承诺相同美元购买力。前者减少了直接持有 USDT 的一层风险，但仍有平台/银行资金链和抵押资产风险；USD 资金贷的借款人也可能使用 USDT 或其他代币作抵押。不能仅凭债权币种推断抵押品构成。另需注意 Exchange Terms §8 对提款处理币种保留宽泛裁量，并非绝对保证所有场景都以申请币种提款。

## 2. 当前杠杆、LTV 和抵押折扣

### 2.1 实时参数

官方 [Configs 文档](https://docs.bitfinex.com/reference/rest-public-conf) 明确 `pub:list:currency:margin` 是可作 margin collateral 的币种列表。当前去掉 `TEST*` 后有 **32 种**，可交易保证金交易对有 **62 个**。

32 种原始 API 符号：ADA、ALG、APT、AVAX、BCHN、BTC、DOGE、DOT、DSH、ETC、ETH、EUR、FIL、GBP、IOT、LEO、LINK、LTC、MXNT、SHIB、SOL、SUI、TRX、TRY、UNI、USD、UST、XAUT、XLM、XMR、XRP、ZEC。列表说明允许资格，不是实际在押数量，更不代表它们风险相同。Borrow 官方帮助页另列 15 种可选抵押资产，不能将 margin 的全部 32 种自动当作 Borrow 界面每一币种组合均可使用。

`pub:spec:margin` 的初始/维持比例，与 `pub:info:pair` 代表性交易对字段交叉检查一致：

| 交易对 | 初始保证金/仓位名义金额 | 维持保证金/仓位名义金额 | 按初始保证金算的最高仓位/权益倍数 |
|---|---:|---:|---:|
| BTC/USD、BTC/USDT | 10% | 5% | 10 倍 |
| ETH/USD、ETH/USDT | 20% | 10% | 5 倍 |
| LTC/USD、XRP/USD | 20% | 10% | 5 倍 |
| XRP/USDT、SOL/USD、ADA/USD | 30% | 15% | 约 3.33 倍 |
| XLM/USD | 50% | 25% | 2 倍 |

初始保证金比例是权益相对仓位的比例，**不是把抵押资产只按这个比例估值**。例如 BTC/USD 10 倍可以对应自有 $10、融资 $90、持仓 $100；借款不等于无抵押放大 10 倍。

### 2.2 `haircut` 字段不能倒读

当前配置 `SOL_haircut=0.7`、`XRP_haircut=0.5`、`ADA_haircut=0.3`。结合[官方保证金教程](https://blog.bitfinex.com/products/how-to-trade-margin-on-bitfinex/)的对应数值，可确定这些原始系数是**计入抵押价值的比例**；实际折减为 `1−系数`。

| 资产 | 计入比例 | 折减比例 | 市值 $10,000 对应计入值 |
|---|---:|---:|---:|
| BTC、ETH、USDT | 100%（无单独覆盖，适用 base=1） | 0% | $10,000 |
| SOL | 70% | 30% | $7,000 |
| XRP | 50% | 50% | $5,000 |
| ADA | 30% | 70% | $3,000 |
| AVAX、DOGE | 20% | 80% | $2,000 |
| LINK、UNI、SHIB、IOT | 10% | 90% | $1,000 |

教程数值标作示例且可变；本表使用本次 API 快照，借其例子验证字段方向。其他风险系数、不同交易对和全仓状态仍会影响账户实际可借额度。**0% 折减不等于资产零风险**；例如 BTC/ETH 仍可能跳空、USDT 仍可能脱锚。

### 2.3 Borrow 的抵押和金额限制

当前 Exchange Terms **最后更新 2026-08-12**，§3.2 的 Borrowing 上限为抵押物价值的最高 90%，具体因资产而异；并写明**一个 Financing Recipient 的 Borrowing 最高 $250,000**。此限制指该条款定义的可用于非交易用途的 Borrowing，不能自动套到普通 Margin Trading 的总借款。帮助页最低借款 $175 等值；法币 Borrow 需要 Full verification。

官方示例：留存价值 $1,000 的 BTC，借出并可提走 $900，LTV=90%，抵押价值/借款=111.11%。帮助页明确 `available for withdrawal immediately in your margin wallet`；还款可提前、可部分，固定利率借款到期未还会转借新的浮动贷款。出借人无法由“120 天借款请求”得出必赚 120 天利息的结论。

仅用于解释风险缓冲的简化算例：若抵押初值 $1,000、借款 $900，抵押价值降为 $900 时，已没有静态权益缓冲。若完全忽略手续费、利息、执行缓冲及其他全仓资产，用 5% 维持要求解 `V−900=5%×V`，得到 $947.37 左右开始不足，距原值跌幅约 5.26%。这不是任何真实账户的强平预测；真实计算还含其他仓位、资产折扣和 0.2% 执行缓冲。它只说明“有初始超额抵押”与“强平一定及时足额回收”是不同条件。

## 3. 从借款亏损传到出借人的路径

以下是对官方风控规则和合同的合并解释，**不是已经公布金额及赔付优先级的法定保险瀑布**。

1. 借款人的 Margin Wallet 先以抵押资产、可用权益和仓位盈亏承受损失。全仓机制可互相支持，也会把其他仓位的亏损传导进来。Exchange Terms §6.2 还授权平台为融资人利益，对借款人在平台控制的账户/子账户/钱包资产设定 lien 并处置。
2. 净权益不足维持要求时强平。官方净权益口径为钱包余额加调整后盈亏、再减融资成本；调整盈亏包括 **0.2% 执行缓冲**。低于维持要求的 1.5 倍时通常警告，但急行情可能来不及。强平参考价不是保证成交价；系统可能在零权益价格下限价单，小仓位可能用市价单。
3. 快速跳空、订单簿不足、集中平仓时，强平可能不能足够快地成交。官方称为避免失控还可能放慢强平，抵押物因而仍有穿仓可能。Exchange Terms §9 允许平台在特定危险状态下接管债务和抵押品，自担接管后的盈亏，**但这是平台权利和裁量，不是对所有融资损失的自动承诺**。
4. Funding 风险 FAQ 说平台会在一定程度内覆盖损失，但未给金额；极端情况下融资出借人承担分摊损失。关键原文：`losses eventually will be shared with margin funding providers`。没有公开核到分摊的币种池、关联借款限定、触发数值、分摊比例、先后顺序或最大自付额。

来源：[Margin call policy](https://support.bitfinex.com/hc/en-us/articles/213895229-Margin-call-policy-on-Bitfinex)、[Funding risks](https://support.bitfinex.com/hc/en-us/articles/213918969-Risks-associated-with-offering-funding-Frequently-Asked-Questions-FAQ)、[当前条款](https://www.bitfinex.com/legal/exchange/terms/)。不能把 FAQ 中“历史未发生”的描述改写成整个 Bitfinex 历史中所有账户或所有损失事件从未被分摊。

当前[费用页](https://www.bitfinex.com/fees/)中的接管费，是按借款人假如立即强平会发生的损失收 **5%**，不是“已预存贷款本金 5% 的担保基金”。出借利息标准平台费 15%、隐藏报价 18%，同样不代表费用已专款保障出借本金。

## 4. 有没有出借人可以依赖的本金保险

**没有核到符合要求的公开承诺。** 当前 Exchange Terms §5 不保证强平能阻止损失，并排除平台对 Financing Provider 向 Financing Recipient 出借损失的责任；§17.16 明确资产只是平台账簿记录，原文包含 `not segregated assets held in your name or for your benefit`；§23 说明没有政府保护/保险，平台或其他主体自购保险不当然赔偿用户。不能从“交易所持有这些代币”推出“它们法律上是该出借人独立保管的担保物”。

衍生品制度有明确的清算基金及不足后的盈利仓位终止规则，见[Termination FAQ](https://support.bitfinex.com/hc/en-us/articles/360035477394-What-is-Termination-on-Bitfinex)。但本次取得的当前 Derivatives Terms（2026-04-06 版本）还明确该基金供 BFXD 及关联方利益使用，并非给用户承保。运营主体为 Bitfinex Derivatives El Salvador, S.A. de C.V.，与 Exchange Terms 的产品、合同和资金用途都需要区分。**即使查询得到衍生品基金余额，也不能加到 fUSD/fUST 融资出借人的抵押覆盖分子里。**

本报告不把任何第三方保险算入净收益。上一轮调查的 Nexus 条件也未形成适用此平台、此规模且覆盖正常穿仓分摊的可执行保单；不能靠一句“另买保险”补齐本金保障。

## 5. 公开数据到底能验证多少

| 可以核验 | 仍不能由这些数据得出 |
|---|---|
| 当前允许抵押资产、保证金比例、折扣规则 | 各借款人实际抵押资产、数量、其他负债及折扣后净权益 |
| 融资买卖簿的利率、期限、订单数和金额 | 最终借款人的身份、独立人数、信用及借款终极用途 |
| 全平台币种融资规模、用于仓位的 credits、各 pair 相关 credits | 全部贷款实时 LTV 分布、抵押覆盖、集中度及强平回收能力 |
| 公开钱包地址及可在区块上查看的资产 | 完整客户负债、银行法币余额、资产法律归属、既有权利负担及破产赔付比例 |

[公开 Stats 文档](https://docs.bitfinex.com/reference/rest-public-stats)中 `credits.size.sym` 的含义是特定交易对账面仓位使用的某币种融资。Borrow 官方确认会生成 long/short margin position，并在 `Taken: Using` 中挂接融资；以 BTC 抵押借 USD，对应 BTC/USD long；借 BTC、USD 作抵押，对应 BTC/USD short。

因此，另一 Agent 观测的“fUSD 用于仓位部分中约 95.5% 对应 BTC/USD”若时间及分母已经校准，**只能描述 BTC/USD 相关账面融资占比，不能称 95.5% 的出借本金在站内用于买 BTC**。同一 position/credit 结构意味着 Borrow 很可能也进入该统计，这是基于产品机制的推断；官方没有公开这一聚合器对 Borrow 的独立纳入/排除细则，未通过私人贷款样本核验。它也不能揭示抵押物构成或外部对冲。

[Funding Credits 私有接口文档](https://docs.bitfinex.com/reference/rest-auth-funding-credits)公开了贷款 ID、资产、金额、利率、期限及 position_pair 等字段；没有逐笔借款人财务报表、抵押清单、实时 LTV 字段。此次只读文档，没有调用私有端点。

[官方 2022 年储备声明](https://blog.bitfinex.com/announcements/bitfinex-resilient-in-face-of-market-events-committed-to-greater-transparency-and-demonstrating-proof-of-reserves/)提供钱包清单，当前清单已从[官方 GitHub](https://github.com/bitfinexcom/pub/blob/main/wallets.txt)保存。它不是这批融资贷款逐笔、同一时点的资产负债匹配证明。未核到一份可将每笔融资债权映射到实时、无重复质押抵押物，且包括银行法币余额与所有其他负债的公开证明。本报告因而**不计算“全平台足额抵押率”，也不给出凭空的年度违约概率**。

## 6. 原始证据与复核路径

网页首个 HTML 仅有加载框，搜索索引所示条款曾是旧版本。本次进一步核查官网自己的公开 JS：`js-31.raw` 将 Exchange Terms 路由映射为 `tos`；`js-8.raw` 的 `fetchLegalTerms` 调用 `/v2/conf/pub:legal:terms:${filename}`。依此读取官方 API，取得 2026-08-12 现行正文，避免拿搜索缓存作为当前条款。

所有 57 份已保存原始响应均复核 SHA-256；索引见 `all-sources-index.json`，包括请求/接收 UTC、URL、原始路径及完整 hash。配置没有单独来源时间，明确使用采集时间；帮助页 JSON 同时保留 `updated_at` 与 `edited_at`，两者可能不同。

| 关键文件 | 来源版本/采集 UTC | SHA-256 前 12 位（完整值见索引） |
|---|---|---|
| `exchange-terms-api.raw` / `.md` | 正文 2026-08-12；14:12:11.386 | `0b9afe9cc384` |
| `derivatives-terms-api.raw` / `.md` | 正文 2026-04-06；14:12:11.556 | `d2d3d76a09a8` |
| `margin-config.raw` | 14:04:48.292 | `125dca422f25` |
| `margin-assets.raw` | 14:06:51.545 | `85a585b1b74f` |
| `margin-pairs.raw` | 14:06:51.704 | `96880a5c0d66` |
| `borrow-help-json.raw` | updated/edited 2026-07-03；14:06:51.912 | `460370ed5dbc` |
| `margin-call-json.raw` | edited 2026-01-17，updated 2026-07-23；14:06:51.895 | `76015409da02` |
| `funding-risk-json.raw` | edited 2024-09-11，updated 2026-07-29；14:06:52.172 | `7ee0c35cf3f6` |
| `fees.raw` | 14:04:48.663 | `20f2e12f7a50` |
| `pub-wallets.raw` | 14:06:52.309 | `1ffb55547342` |

数值派生表为 `collateral-derived.json`，包括 32 种资产、62 个交易对列表、代表性初始/维持保证金、折扣解释和简化算例。关键英文原文在完整法律 `.md`、帮助页 `.raw`/`.txt` 中，便于审阅上下文；上述短引文不替代完整合同。
