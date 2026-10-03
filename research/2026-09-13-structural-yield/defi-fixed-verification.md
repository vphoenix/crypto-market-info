# 固定期限 DeFi 收益与损失保护核验

核验时间：2026-09-13 13:42–13:54 UTC。独立核验 Agent：verify_grid。仅访问公开信息；没有连接用户钱包、购买保单、签名、授权或交易。

**结论：本次查到可量化、可持有到期的成熟稳定币 PT，但没有验证出“持续净年化 5%–8%且美元本金不亏”的组合。** 在 10 万原生稳定币规模、外部总成本 10–50 美元的假设下，11 月到期的 PT-sUSDS 简单净 APR 约 4.46%–4.66%，PT-sUSDe 约 4.32%–4.52%，均没有美元本金保证。增加协议险后，公开保费情景已低于用户 3.5%门槛；再买脱锚险更低。Maple 当前显示约 4.6%–5.0%，不能用旧 FAQ 的约 7%推广值。Aave Umbrella 的额外利息补偿的是承保损失风险。

## 1. 四笔真实的官方预览报价

来源是 Pendle 官方 Hosted SDK，在 2026-09-13 13:48:44–45 UTC 收到成功响应；到期日均为 **2026-11-26 00:00 UTC**，剩余约 73.4245 天。使用公开占位接收地址，没有读取账户或资金授权。因此属于 API 预览路线，并非保证成交或已完成的链上仿真。官方说明：无余额/授权也能查预览，实际可成交结果仍须在执行前复核。[API 使用说明](https://docs.pendle.finance/pendle-v2-dev/Backend/ApiOverview)

| 标的 | 输入原生代币 | 预览收到 PT | 正常偿付时本期限代币增量 | API effective APY | 同一结果的简单 APR |
|---|---:|---:|---:|---:|---:|
| PT-sUSDS | 10,000 USDS | 10,094.792526 | 94.792526 USDS | 4.80175% | 4.71223% |
| PT-sUSDS | 100,000 USDS | 100,947.925262 | 947.925262 USDS | 4.80175% | 4.71223% |
| PT-sUSDe | 10,000 USDe | 10,092.775308 | 92.775308 USDe | 4.69769% | 4.61195% |
| PT-sUSDe | 100,000 USDe | 100,919.681107 | 919.681107 USDe | 4.65607% | 4.57182% |

表内输出已经反映路线的交易费用和价格影响，**不能再扣一次 API 的 fee 字段**。该字段 1 万/10 万 sUSDS 分别约 1.9068/19.0676 美元，sUSDe 约 1.8948/18.9485 美元。它未计入 gas、取得 USDS/USDe 的价差或手续费、将到期所得换回美元的成本、保费、税。APY 是把这段期限收益复利折算到一年的指标，不能证明 11 月后仍可按同一价格再投。[官方报价口径](https://docs.pendle.finance/pendle-v2-dev/Backend/HostedSdk)

价格容忍度设为 0.1%。最低 PT 输出分别为 10084.697734、100846.977336、10082.682533、100818.761426，若只能按最低输出执行，收益还会下降。sUSDS 路线含公开限价单，不能从两档预览近似线性推断无限容量。当前池 liquidity.usd 分别约 338 万与 373 万，仅供背景；本次规模证据是四笔预览，**不是把 TVL 全算成可成交容量**。

合约身份：Ethereum chainId 1；sUSDS market `0x9c560ebaf78e596cbcc27411d633a74d628dd7dc`，PT `0xdc169abe56461a2e0c034da431ac2a3ebf596094`，会计资产 USDS `0xdc035d45d973e3ec169d2276ddab16f1e407384f`；sUSDe market `0x47ad2cd1dd15739a7a035b9d3b7828d916fef77e`，PT `0xb195b618ea52b77cb2a58846f452f59f8dfa9390`，会计资产 USDe `0x4c9edd5852cd905f086c759e8383e09bff1e68b3`。

## 2. 90 天持续性证据

官方 `/core/v3/1/markets/{market}/historical-data` 成功返回：

| 同一期限市场 | 日点范围 UTC | 连续日点/跨度 | 历史隐含 APY 最低/中位/最高 | 底层历史 APY 最低/中位/最高 |
|---|---|---|---|---|
| PT-sUSDS 26NOV2026 | 6/15–9/13 | 91 点，90 天 | 4.73249% / 5.00336% / 5.45953% | 3.51993% / 3.58106% / 3.60000% |
| PT-sUSDe 26NOV2026 | 8/10–9/13 | 35 点，34 天 | 3.55523% / 4.70841% / 4.94668% | 0.44887% / 4.37985% / 4.79505% |

已经逐点核对日期唯一且相隔 24 小时。查询 sUSDe 也要求从 6/15 起，但 API 仅返回上述 35 点，未补零或拼接成 90 天。这里是每日市场参考利率，不是某人的回测净利润，也不证明当时 10 万规模总能成交；低频日点无法排除日内短暂脱锚或无法退出。sUSDS 数据显示中等收益持续存在，不能由此推断没有尾部风险。

## 3. PT 的“本金”是什么

本次 PT-sUSDS 的计价单位是 USDS，PT-sUSDe 的计价单位是 USDe。正常运行时，到期可通过相应 sUSDS/sUSDe 包装资产赎回会计资产数量；它不承诺每枚代币价值 1 美元，也不承诺美元银行汇款。以约 0.99 USDe 买入一枚 PT，即使到期拿回一枚 USDe，若 USDe 只值 0.9 美元仍然亏钱。发行/赎回资格、托管、底层对冲或借贷、合约权限都可能影响最终所得。

Pendle 技术文档对 SY 背书价值下降有偿付能力调整：当当前 SY 兑换指数低于先前 PT/YT 累计指数，资产兑换估值按比例下降。这是底层资产损失，并不会被 PT 自动隔离。提前卖出也按当时市场价格，隐含利率上升、池子流动性变薄或币价下跌都可能导致亏损。[SY 偿付能力说明](https://docs.pendle.finance/pendle-v2-dev/Contracts/Oracle/PYLpOracle)、[提前退出说明](https://docs.pendle.finance/pendle-academy/cheatsheet-for-the-impatient/pt-yt-lp-cheatsheet)

## 4. 保险真的保护哪些损失

Nexus Mutual 是可购买的风险转移，但属 discretionary cover，理赔需符合保单和附件，由理赔机制审核；不是无条件美元保本承诺。[官方产品说明](https://docs.nexusmutual.io/overview/cover-products/)

- **协议险**：当前官方总条款默认免赔额为保额的 5%，附件可以修改；涵盖特定合约错误、预言机/清算失效等，排除一般价格变化、部分脱锚、按既有权限取走资金、桥故障及管理暂停造成的损失等。不能把 Maple 正常信用违约或任何 PT 底层损失都当成必赔。必须确认实际 listing 的指定协议、策略与附件。[当前 Protocol Cover 条款，第 2–4、8 页](https://api.nexusmutual.io/v2/ipfs/QmQQ88vaZkgEY9qXKPtq7Se6caSjB2RTqF1GAnBLW1k8EY)
- **USDS 脱锚险**：公开较新附件包含 Pendle/Spectra 的 sUSDS PT，但触发要求价格偏离超过 10%并持续 7 天；通过交付代币兑换，每单位按 0.975 的索赔单位计算，不是足额 1 美元赔偿。[USDS 附件](https://api.nexusmutual.io/ipfs/QmXL6mHEHJwtW3QLMBvXKL8vqwM4kdsK1dUS9mUhVqEgCa)
- **USDe 脱锚险**：公开较新附件同样泛指包含 USDe/sUSDe PT；门槛更高，为超过 20%持续 7 天，索赔单位 0.95。[USDe 附件](https://api.nexusmutual.io/ipfs/QmVPqHBpDVh2hk64LS7hXPm6BVK5a7UeHdRGpH5SqmU6SM)

所以 USDS 跌 5%或 USDe 跌 10%后一直维持该价，可能并不触发这两份脱锚险；包装资产出问题但指定稳定币没有达到触发阈值，也可能不触发。公开旧 USDe 附件只列若干 2025 年 PT，已单独保存，不能拿旧附件断言新期限不在保障范围；最终仍以买入时绑定的文件为准。

Crypto Cover 的共通条件还要求事前至少持有指定币或认可衍生品 72 小时，达到脱锚持续期后限期申报，提交损失证明并交付所赔代币；提前报备钱包地址很关键。仅在网站上看到币名和保费不等于已拿到全面保护。[Crypto Cover 条款](https://api.nexusmutual.io/v2/ipfs/QmaUcoUq8972J41UzyyT82C9EkVme82UYSCbuQfzkS6pnr)、[购买与证据要求](https://docs.nexusmutual.io/using/buy-cover/)

公开列表的搜索缓存显示：Pendle 协议险年费约 1.28%–1.67%、容量 1540 万美元；Sky USDS 脱锚险 0.56%–0.62%、容量 810 万；Ethena USDe 脱锚险 3.34%–3.9%、容量 270 万；Maple 协议险约 1.51%、容量 120 万。**这些是约两周前抓取的列表参考，并非 9/13 绑定金额和日期的报价，区间也不是收益置信区间。** 本次直接页面只有客户端骨架，未验证当前保单定价、账户资格或可分配容量。[协议险列表](https://app.nexusmutual.io/cover/buy-cover?product-types=0%2C11%2C19)、[脱锚险列表](https://app.nexusmutual.io/cover/buy-cover?categories=depeg)

## 5. 统一全本金分母后的成本情景

以下用简单净 APR 便于与用户 3.5%门槛比较。假设输入币及到期所得均可按 1 美元计价、正常兑付；输入本金为 N、到期面额为 P、剩余天数 d、合计外部成本 G、假设年度保费率 c，则：

`保费 = P × c × d / 365`

`总投入 C = N + G + 保费`

`净简单 APR = (P − C) / C × 365 / d`

G=10/50 美元是取得币、gas、到期赎回和兑换等的**敏感性假设**，未取得链上 gas 实报；若真实成本更高需替换。保额按全部到期面额 P，保费前付且纳入资本分母；未重复扣表 1 的交易费。首次会员费、保险购买 gas 若不包含在 G 内须另加。这里只算无损失情景的现金收益，不是风险调整后的期望收益。

| 标的/原生投入 | 无保险，G=50 到 G=10 | 仅协议险，c=1.28%–1.67%，G=50 到 G=10 的区间 |
|---|---:|---:|
| sUSDS PT / 1 万 | 2.22%–4.21% | 0.54%–2.91% |
| sUSDS PT / 10 万 | 4.46%–4.66% | 2.77%–3.36% |
| sUSDe PT / 1 万 | 2.12%–4.11% | 0.44%–2.81% |
| sUSDe PT / 10 万 | 4.32%–4.52% | 2.63%–3.22% |

若采用公开费率较低端，协议险加 USDS 脱锚险合计 c=1.84%，G=10，则 sUSDS PT 的 1 万/10 万净 APR 约 **2.35%/2.79%**。协议险加 USDe 脱锚险 c=4.62%时，同样情景的 sUSDe PT 约 **−0.54%/−0.14%**。它们还保留免赔额、触发盲区及理赔风险，不能称“收益降低但本金完全安全”。上述是保费敏感性分析，不构成当前可买到的组合报价。

## 6. Maple 与 Aave Umbrella

2026-09-13 直接读取 Maple 官方透明页及内嵌 `poolApys`：syrupUSDC 5.0%，syrupUSDT 4.6%，syrupUSDG 4.9%；当时 AUM 约 26.17 亿、8.63 亿、3.33 亿美元。显示的机构综合 APY 5.4%混合不同池，Secured Lending 单项约 5.3%，不能把其中 AQRU 16.4%当同等级信用风险。当前 FAQ 写展示 APY 包括基础收益及奖励，本次没有取得可完整分离组成的当前 API，不能把 5%全部称为不含奖励的净借款利息。[官方透明页](https://maple.finance/transparency)、[当前 FAQ](https://docs.maple.finance/syrupusdc-usdt-usdg-for-lenders/faq)

Maple 份额对应贷款与策略资产净值，USDC 名字不意味着 Circle 为本金担保。当前介绍明确除了超额抵押机构贷款，还能配置期现货基差与跨链 DeFi 流动性策略。违约时贷款本金与应收利息会减记，回收抵押品再增加净值；减值期间退出可能永久失去后来回收款。短久期、法律隔离及超额抵押能减风险，不能消灭它。[当前策略](https://docs.maple.finance/syrupusdc-usdt-usdg-for-lenders/introduction)、[减值和违约](https://docs.maple.finance/legal/syrupusdc-and-syrupusdt-defaults-and-impairments)

零售 Syrup 的宣传准入为非美国用户、无最低额，因此 1 万和 10 万名义规模不受该最低额拦截；实际地域资格、可存额度和即时赎回流动性未核。直接赎回按份额兑换，必要时排队，官方称正常即时/约 24 小时、最长可到 30 天，不能把 AUM 全当即刻退出深度。机构 Secured Lending 要 KYC、最低 10 万美元，1 万不适用。没有拿到 syrupUSDC 90 天可复核 APY 序列，透明页历史 AUM 不可冒充收益历史。[产品入口](https://maple.finance/app)

即使把当前 syrupUSDC 5.0%按全部可兑现年收益作有利假设，仅付列表参考的 1.51%协议险便只剩约 3.49 个百分点，若以含前付保费的全资本分母计一年收益约 3.44%，还未算 gas、币价损失和手续费；而正常信用违约是否符合保险事故另需核实。故本轮不列为保险后持续高收益候选。

**Umbrella 的质押者是承担坏账的一方。** aUSDC/aUSDT 质押者赚供应收益和额外安全奖励，特定池/币出现缺口可能被烧掉本金，极端损失可接近全部质押额；它不为用户其他 Pendle、Maple 或稳定币头寸出具保单。20 天冷却及 2 天退出窗口期间仍可被罚没，奖励参数可调整。Aave 的坏账保护让普通供应者多一层缓冲，但资金池额度、资产/网络范围有限，也不等于美元脱锚险。[Umbrella 官方机制](https://aave.com/docs/aave-v3/umbrella)

## 7. 追加独立复核：Bitfinex 出借候选

主 Agent 的 13:45 数据之外，本 Agent 于 **13:51:43 UTC** 独立读取同一官方 API，确认：

| 资金簿 | 日利率最高的借款 bid | 单笔可见借款需求 | 最大期限 | 扣普通 15%利息费的年 APR |
|---|---:|---:|---:|---:|
| fUSD | 0.000273972602739726 | 8,470,025.31 USD | 120 天 | 8.5000% |
| fUST（USDt） | 0.0002671232876712329 | 3,760,134.72 USDt | 120 天 | 8.2875% |

独立核对官方文档：funding 盘口 AMOUNT<0 是 bid，表示借钱需求；AMOUNT>0 才是别人出借的 ask。这与现货盘口的正负方向相反。本表没有误拿出借 ask 作为现成可赚收益。[Book 定义](https://docs.bitfinex.com/reference/rest-public-book)

日小数利率×365得简单 APR；普通资金出借利息费 15%，hidden offer 18%。以上扣费后年率仍未扣充提、买卖 stablecoin、美元银行费用、空仓等待、平台损失分摊及税费。当前最高价各只有一个可见订单，用户到达时可能已撤掉；6 分钟后仍在不等于未来 120 天都在。[费用规则](https://support.bitfinex.com/hc/en-us/articles/360024039494-How-are-the-Funding-interest-earnings-and-fees-calculated-at-Bitfinex)

出借人不能提前收回已成交贷款；借款人却可随时提前还，所以“120 天”是最长期限，不是锁定 120 天的利息。FRR 是既有固定利率融资的金额加权指标，官方明确是推算结果，不保证新钱立刻按 FRR 成交；主 Agent 的历史 FRR 研究应视作利率环境而非该用户实际可得净收益。[提前还款](https://support.bitfinex.com/hc/en-us/articles/214441485-Provided-Funding-on-Bitfinex-Frequently-Asked-Questions-FAQ)、[FRR 规则](https://support.bitfinex.com/hc/en-us/articles/60566554934553-Understanding-Funding-Terms)

独立审阅 `analyze-lending.py` 后确认其特殊单位换算正确：`/v2/funding/stats/{symbol}/hist` 的第 3 项是**日 FRR 的 1/365**，故 `原值 × 365²` 才是年 APR 的小数，百分比再乘 100；ticker/book 则仅乘一次 365。此区别有[官方 Funding Statistics 字段文档](https://docs.bitfinex.com/reference/rest-public-funding-stats)明确支持，不是为了得到较高收益而人为加倍年化。

两币均有 2161 个逐小时、无重叠无断档的观察，覆盖 2026-06-15 13:05 至 9/13 13:05 UTC。各自 9 页原始响应 SHA256 已独立复核；90 天平均使用左闭右开的 2160 小时。重新用 Decimal 计算与主脚本一致：USD 90 天平均毛 FRR APR 12.5616%，仅扣利息费后 10.6773%；USDt 为 9.5980%和 8.1583%。7 天仅扣利息费后分别 9.5911%、7.5649%；30 天为 9.6958%、7.5164%。这些仍是等时长观察的 **FRR 基准均值**，并非可用全部本金按该利率持续出借的业绩。历史接口低精度也不支持把报表小数位当实际收益精度。独立结果存于 `bitfinex-independent-validation.json`。

官方明确极端行情清算不足的损失最终可能分摊给融资提供者。Nexus 当前 Crypto Cover Part A 每 custodian 最低保额 100 万美元，1 万/10 万不满足；事故条件为特定犯罪引致全体至少 10%资产损失，或无预告全面停提持续至少 100 天，不能默认覆盖普通清算分摊。本次也未找到当前可购买的 Bitfinex listing，因此没有用假定保险消除此风险。[Bitfinex 风险说明](https://support.bitfinex.com/hc/en-us/articles/213918969-Risks-associated-with-offering-funding-Frequently-Asked-Questions-FAQ)、[Crypto Cover 条款](https://api.nexusmutual.io/v2/ipfs/QmaUcoUq8972J41UzyyT82C9EkVme82UYSCbuQfzkS6pnr)

## 可复核材料

同目录 `pendle-quotes-history.json` 保存四笔完整预览和请求时间/URL/响应哈希；其中附带的未签名 calldata 和市场参与者公开订单签名来自官方返回，不是用户密钥。`defi-initial-public.json` 保存市场元数据；`defi-history-sources.json` 加 `defi-source-0.txt`/`1.txt` 保存两条日历史及原始响应。`defi-source-3.txt` 为完整 Maple 透明页；`defi-source-5.pdf`/`6.pdf` 为协议险/Crypto Cover 总条款；`nexus-annex-sources.json` 和三个 `nexus-*-annex.pdf` 保存附件、时间与哈希。

`defi-bitfinex-followup.json` 保存独立融资盘口及当前 Maple 策略说明。`verify-defi-values.py` 只读冻结文件，用 Decimal 重算期限利润、全资本 APR/APY、保费情景、历史日期连续性和盘口方向，输出 `defi-calculation.json`。已执行通过；没有进行投资或更改采集项目。
