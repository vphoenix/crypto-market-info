# LST 折价买入、官方赎回与对冲：最小采集设计

状态：2026-10-02 已实现采集程序并在独立研究库建表，2026-10-03 已启用常驻 `crypto-market-info-lst.service`。当前按用户要求只持续实时采集，补采限于断线缺口，不运行本设计中的大范围历史回补；dRPC 的历史合约状态与日志接口分别核验，日志覆盖仍受端点拒绝限制。本文件保留原设计目标与可选历史功能，实际部署/操作以[运行说明](lst-redemption-data-implementation.md)为准。代码审核：[0016](../discuss/0016-lst-redemption-data-code-review.md)，DDL：[lst-redemption-data-schema.sql](lst-redemption-data-schema.sql)，设计审核：[0014](../discuss/0014-lst-redemption-data-design-review.md)，语法与公开来源验证：[记录](../research/2026-10-02-lst-design/validation.md)。

## 1. 范围与要回答的问题

首版固定 **Ethereum 主网（1）、Lido stETH/wstETH、Binance ETHUSDT USDT线性永续**。目标是用公开数据判断买入折价能否覆盖赎回等待、对冲、gas和资金占用。总资本100万USDT，无初始币持仓；购买金额档为10k/50k/100k/250k USDT，另计保证金、gas储备及周转现金。

需要回答：实际买到多少赎回权、折价按金额怎样变化；队列多久能finalized、多久被claim；真实兑付是否损失；对冲资金费、基差和全部费用会消耗多少；按全部占用资本计算是否值得继续。首版可以提供否决依据、成本盈亏平衡和条件收益，不能凭同刻价格证明未来的锁定收益。

NFT二级市场、多链、多协议、自动最优路由、私有订单流和交易执行均不进入首版。stETH与wstETH是同一协议赎回权的两条买入路径，不能按独立市场容量相加。

### 固定资产及路径

| 对象 | Ethereum地址／身份 |
|---|---|
| stETH | `0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84` |
| wstETH | `0x7f39C581F595B53c5cb19bD0b3f8dA6c935E2Ca0` |
| WithdrawalQueueERC721 | `0x889edC2eDab5f40e902b864aD4d7AdE8E412F9B1` |
| WETH | `0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2` |
| Ethereum USDT | `0xdAC17F958D2ee523a2206206994597C13D831ec7`，6位小数 |
| 经典Curve ETH/stETH池 | `0xDC24316b9AE028F1497c275EB9192a3Ea0f67022`；coin0原生ETH，coin1 stETH |
| Uniswap v3 | 由官方factory读取指定token pair与fee的pool；选定后固定进manifest，不自动换池 |
| 对冲 | Binance／perpetual／ETHUSDT／USDT结算／当前合约版本，登记既有`instrument`模型 |

Lido地址来自[官方部署](https://docs.lido.fi/deployed-contracts/)。Uniswap的USDT/WETH首选fee=500、WETH/wstETH首选fee=100；这些只是待链上核验的固定候选，getPool零地址、币身份不符或调用失败不能变成可用路线。

- A：链上USDT → v3 WETH → unwrap成原生ETH → Curve stETH → 官方申请提现 → 领取ETH → wrap WETH → v3 USDT。
- B：链上USDT → v3 WETH → v3 wstETH → 官方wstETH提现接口（内部unwrap）→ 领取ETH → wrap WETH → v3 USDT。

路线A不可把Curve的ETH sentinel当WETH；wrap/unwrap不是零gas的免费步骤。路线B读取`getStETHByWstETH(qWst)`后，再读取`stETH.getSharesByPooledEth(qSt)`；不能把qWst直接当入队shares。普通stETH也保存原始stETH及对应shares，舍入逐步使用合约整数结果。[wstETH换算](https://docs.lido.fi/contracts/wsteth/)、[请求实现](https://github.com/lidofinance/core/blob/v4.0.1/contracts/0.8.9/WithdrawalQueue.sol)

manifest包含固定地址、pool/fee、token decimals、ABI/代码版本、CEX合约及tick/lot/乘数、金额档、时效阈值、任务上限和手续费/gas/保证金情景。归档脱敏manifest及SHA-256，RPC与数据库凭据只在环境变量中。启动核验chainId、code、pool token/fee、token decimals、queue/STETH相应代理实现及实现代码hash；未知实现只留原始证据和unknown。stETH与queue使用各自代理机制，不能对所有合约套一个EIP-1967槽。

## 2. 一个独立命令

入口：`cmd/lst-data`，独立研究库`crypto_market_info_lst`，一个writer。

```text
lst-data backfill --days 30 --manifest config/lst-lido-ethereum.json
lst-data watch --manifest config/lst-lido-ethereum.json
lst-data watch --once --manifest config/lst-lido-ethereum.json
lst-data report --from ... --to ... --out var/lst-reports/...
```

预计`internal/lst`只需model/abi/collector/runner/report几个文件，加专用ClickHouse writer。复用既有Go、RPC HTTP/证据归档、严格Decimal/big.Int、instrument登记及批写工具。Ethereum RPC helper返回的固定DEX manifest身份不可直接当LST manifest，覆盖为本任务的已验证身份。不重构全站collector，不搭Web服务、消息队列、插件框架、通用任务系统或本地EVM平台。

程序有一个每60秒市场循环、一个每5分钟的finalized日志增量任务；资金费由同一小型串行REST任务处理。后续询价用最多30项的内存列表即可。报告只读数据库与已归档证据，不隐式联网。

`watch`不隐式执行30日回补。首次无日志游标时，从本次取得的finalized头建立观察起点，明确此前未覆盖；已有游标则按第8节额度逐片恢复。历史30日及额外上下文只由显式`backfill`抓取；`watch`、`--once`、`backfill`共用固定本地状态目录及单一LST进程排他锁，不同时启动两份，mode仅作为锁owner信息，不能成为锁键而分开额度。恢复种子先读本地库，已逾期超过2分钟的followup直接记missed，不补发旧报价。

## 3. 每分钟抓哪些当前数据

### 3.1 同一Ethereum blockHash

选择latest头，所有protocol/view/Quoter调用固定该hash（EIP-1898 `requireCanonical`；不支持则按高度调用前后各验同hash，失败为unknown）。读取stETH总pooledETH/totalShares、1e18 wstETH换算、queue pause/bunker、MIN/MAX请求额、尾部/最后finalized requestId、未finalized stETH、锁定ETH。保存baseFee；可额外读取并归档priority fee参考，明确它不是未来费用。

四档先向USDT/WETH Quoter exact-input得到WETH，再分别Curve `get_dy(0,1,qETH)`、v3 WETH/wstETH exact-input。每档记录各腿整数输入/输出、Quoter返回gas（若有）、原始hash以及可请求stETH、入队shares。所有调用失败、无流动性、数值不合法均保存unknown行。AMM报价是定额转换观测，不制造10档订单簿。

对每档的名义赎回ETH q0，再取得同hash WETH→USDT exact-input报价。它是**当前退出参考**，用于量化折价和费用空间；未来退出另抓，不能直接沿用。代币转换与pool fee已包含在输出，不在分析层重复扣一次池费。

额度按stETH值拆分`MIN/MAX`，保存所需请求数；真实请求/审批/领取费用按拆分后情景计算。名义q0是申请时的兑现上限，未来finalization损失尚未锁定。[队列文档](https://docs.lido.fi/contracts/withdrawal-queue-erc721/)

### 3.2 一份公共ETHUSDT深度与mark/index

同轮只取一次公开`GET /fapi/v1/depth?symbol=ETHUSDT&limit=10`和`/fapi/v1/premiumIndex?symbol=ETHUSDT`，各档共享原始证据。首版只用前10档，所以直接取10档，不多抓100档；不维护WS连续盘口或另建一分钟盘口流。保留depth的E、T、lastUpdateId，mark/index各自source time、请求/收到/可用时间；缺来源时间为unknown。[Binance公共市场接口](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data)

用q0和合约乘数向下取整qty_lot，得到实际对冲量qh；记录未对冲ETH残差、LOT_SIZE/MARKET_LOT_SIZE、MIN_NOTIONAL校验。bid档模拟卖出开空，ask档给当刻同量买回参考。按档逐个求精确名义金额，不用BBO乘金额替代VWAP；10档不足则unknown，不外推。短仓开仓名义金额不是实际收到可自由使用的USDT，不作为购买资金。

交易深度价格/量使用instrument整数price_tick/qty_lot；mark、index及结算mark独立采用**1e-8 USDT/ETH整数价格tick**，不按交易PRICE_FILTER tickSize舍入。CEX名义USDT金额用1e-8整数单位，链上USDT用1e-6，字段后缀和manifest明示精度。需要重算未来相同qh时，离线报告可解析当时已保存的depth证据，仍限前10档，不把REST快照当连续L2。

当前费用、维持保证金与账户可用保证金不经私有API获取。公共exchangeInfo/fundingInfo归档；实际账户档位依赖签名接口时保持unknown，报告采用显式、可改的手续费及保证金压力情景。

### 3.3 新鲜度与边界

默认当前样本要求：完整轮次≤30秒；可用时头年龄≤60秒；depth/mark源时间距其可用时间≤15秒；链块时间与CEX源时间差≤30秒；来源时间不领先本地UTC超过2秒。阈值存manifest。各来源保留自己的时间；网络完成时不会把旧source time刷新成现在。超过阈值保留stale/partial，不计为同期完整报价。

金额报价也不证明quote之后的交易可成交；本方案评价所采分钟和固定路线，不能否决其他池、块内机会或其他交易所。

## 4. 同量后续询价：小规模保留未来价格证据

定额购买后q0会变化，后来固定USDT购买档的退出量不等于旧q0。为避免这个漏项，每天UTC12:00之后5分钟内取第一份完整100k档，按UTC日期奇偶在A/B之间轮换，选**一条**后续询价种子。不以事后盈利选样；窗口缺采则记录missed，不补造当天种子。

这里的完整以该100k quote及其必需协议/来源判断，不要求其他七个金额档也成功；例如250k超前10档容量不会妨碍完整100k档成为种子。

最多保留30个30日内种子；在原报价真正available_at之后+1/3/7/14/30日，询价同一原q0的链上ETH→USDT与原qh的CEX买回，同时采集协议状态。种子存`is_followup_seed`；后续行存`reference_quote_id`、target_delay_seconds、planned_for_at、真实请求/可用时间、固定q0/qh，不重新买LST或把新兑换率改到旧仓位。任务错过2分钟时写missed/late，不把实际时间贴成准时；重启从库恢复种子和已完成目标，小列表按最近到期处理。

quote角色为entry/followup，同一表。followup不必抓新的LST买入，相关字段保持NULL；退出和对冲仍分别记录失败。新增请求每日仅数笔，不需要影子账户、钱包、提现或撮合模拟。

这些数据支持固定期限的价格/资金费情景，不代表我们的真实赎回。种子当时queue尾部之后的第一个真实请求可作排队位置代理，但他人的请求额、我们的新增排队量及提交延迟不同，不据此给自己的确定完成时间。完整实盘可得性还需以后取证。

## 5. 历史及实时提现事件

### 5.1 只扫finalized的三类事实

`backfill --days 30`按真实header时间寻找起点，以finalized头结束。抓queue的全部日志、保留原文，按已核验ABI解码WithdrawalRequested、WithdrawalsFinalized、WithdrawalClaimed；升级、暂停和未知topic留证据。每片最多512高度，来源明确报告范围过大或疑似截断时二分；429/超时不触发立即二分。父片、失败片、二分子片均消耗第8节请求额度，未完成子片留待下轮，单块仍不能完整则gap；成功空数组与失败分开。整片及必要header成功归档/写入后才推进连续游标。header按不同区块取一次并本地缓存，不对同块每条事件重新请求。watch每5分钟补finalized新区间；不为提现事件建head重组回放系统。

每事件包含blockNumber/hash/time、txHash/txIndex/logIndex、queue地址与ABI版本。源事件在finalized头之前仍需核对hash；finalized冲突停止。历史没有核验的实现区间保持unknown，不能将今天的ABI/暂停状态套过去。

### 5.2 排队、领取与删失

请求存requestId、sender、初始owner、stETH与shares；官方`_finalize`将`lastFinalizedRequestId+1`作为事件from发出，首尾均包含（实施时纠正原稿的左开区间错误）；finalization存**[from,to]**和总锁ETH/烧shares/事件时间；claim存requestId、当时owner/receiver、实际ETH。finalized范围不展开为大量逐请求行，也不把批次总ETH平均分给请求。

只有覆盖完整且requestId落在[from,to]中才匹配finalization。请求→finalized是协议等待；finalized→claim是用户领取拖延；request→claim是实际资金释放观察。窗口末尾未finalized者是右删失，保留队列年龄，不从分布删除。起点前请求、窗口内finalize/claim者为左边界缺上下文，单列；30日请求 cohort的完成率不能用“仅已完成样本”的P95描述。

默认向前增加最多30日的**同queue事件**上下文，读取起点前最后一个finalization边界，并寻找期初未finalized请求。达到上限仍没找到请求就标left_censored，不从完成事件猜请求时间；不做从创世全量索引。观察期之后继续watch，等cohort成熟再报告真实等待分布。范围重叠、from/to不连续或缺源日志不能视为FIFO破坏/无人完成，先标覆盖不明。

`claimed_ETH/requested_stETH`只在完整匹配请求与claim时量化实际兑付损失。claimed样本可能有选择偏差；未claim不填零、不自动假定全额。getClaimableEther对未finalized或已claim均返回0，不能把这个0当损失。最小首版不另抓NFT市场/所有owner状态。若要补未领取金额，可针对少量匹配请求在固定hash读取status/hints/claimable，不改变历史事件。[请求与兑付源码](https://github.com/lidofinance/core/blob/v4.0.1/contracts/0.8.9/WithdrawalQueueBase.sol)

### 5.3 小规模真实gas样本

对request和claim，每UTC日各取按txHash排序最小的两笔不同交易，窗口总共最多120笔，有限补transaction与receipt；读取整份receipt，确定同交易操作数量、selector、to、gasUsed/effectiveGasPrice、成功与费用。抽样方法与样本覆盖输出，不能称为整个市场gas P95。

成本字段直接附在已抽中的事件行；同tx多事件可重复保存，但报告**按chain/blockHash/txHash只计一次交易成本**。直接调用queue的已知方法按操作数分组，其他to/selector及混合操作为mixed/unknown，整笔gas不冒充单独一次请求成本。receipt取不到为NULL，不阻塞核心日志，后续重采用新capture保留原批次。

买入、授权、USDT零额重置、wrap/unwrap、request分批、claim、退出的gas均需列出；Quoter内部gas只作局部参考。未取得完整执行成本时，输出盈亏平衡总gas与显式成本范围。程序不搭state override或假钱包交易模拟来伪造gas。

## 6. 资金费独立事实

`backfill`用公共`/fapi/v1/fundingRate`逐页取得30日ETHUSDT实际结算，保存fundingTime、rate、对应markPrice、取得时间与原始hash。只存actual，不年化后当LST利率。分页固定时间窗口、严格递增+去重，最后一页/覆盖摘要完整后才提交funding capture；限额、重复冲突或mark缺失不能算完整成本。[实际结算接口](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data)

watch每15分钟串行查最近48小时，且在premiumIndex返回nextFundingTime之后至少2分钟安排一次确认；未出结果按5/15/60分钟重试。既有实际值不被估算覆盖。fundingInfo和明确的默认规则用于发现当前间隔变化；历史有缺时段但无法证明当时规则时保持coverage_unknown，不臆造8小时固定周期。捕获premiumIndex当前指示rate/nextFundingTime只作为当时估算，和actual表分开。

funding capture的complete仅表示该固定API窗口分页取得完整；具体周期成本还要检查整个持仓区间的来源覆盖、真实结算点及全部结算mark。不能只找到几笔费率或一个最新complete capture就忽略中间缺口，边界或间隔无法识别时成本仍为unknown。

对于qh ETH短仓，一次资金费情景现金流为`+qh × settlement_mark × rate`：正rate短仓收入，负rate支出。按每次实际结算mark估值，不能用入场mark乘平均APR。持仓精确跨结算边界、mark缺失、间隔/规则缺口都单列unknown。

复用现有instrument定义和注册；当前`funding_rate_hourly`缺source hash、取得时间和结算mark，因此新建专项actual结算表，不修改或混用整点表语义。

## 7. 七张小表及一致性

| 表 | 一行是什么 |
|---|---|
| `lst_capture` | 一次market/logs/funding采集的公共完整性元数据：manifest、模式、来源/请求时间、可选链区间锚点、各表成员数/摘要、status/commit、canonical/finality/revision |
| `lst_protocol_state` | 一个market capture同hash的换算、队列、额度、gas状态；不保存预测等待或预测利润 |
| `lst_quote_observation` | 一条entry路线×购买档，或对原quote的同量followup；各链上腿/对冲定额结果、各自时间/hash/status，qty lot与残差 |
| `lst_withdrawal_request` | 一个原始请求事件，原子stETH/shares、初始身份与可选收据gas样本 |
| `lst_withdrawal_finalization` | 一个finalization范围事件，[from,to]、总锁ETH/烧shares，不拆区间 |
| `lst_withdrawal_claim` | 一个实际claim事件，实际ETH和当前领取身份、可选收据样本 |
| `lst_funding_settlement` | 一个instrument的一次actual资金费结算与mark证据 |

另复用`instrument`小表，不新增通用收益/机会/策略/账户/任务/收据/费用/代理历史表。各条业务事实有独立语义；capture只负责共用完整性，不承载JSON业务内容。

时间UTC微秒；链金额UInt256/big.Int；mark/index整数1e-8 tick；交易price_tick/qty_lot整数；rate/tick size/step/乘数用精确Decimal。未知金额、时间和成本使用NULL并有reason；成功零日志/零费率可以为0。地址20字节，hash32字节；原始响应与脱敏manifest通过SHA-256本地归档。无Float64或经float转Decimal。

失败/未请求/missed成员可能没有source请求、收到或可用时间；这些列与失败时未知的protocol块锚点均为Nullable。行自身observed_at/available_at仅表示本地排程与分类完成，始终保留真实本地时间；protocol另有source_available_at，不拿分类时间代替源数据可用时间。只有成功的protocol/view/quote才允许非空、同capture且经核验的block锚点和源时间；不以epoch0、零hash或当前时间填缺失来源。

先原子归档证据、批写冻结事实，最后写capture committed。同一写入重试保持capture_id、成员及全部时间不变；新的网络尝试用新capture。market固定1条protocol与8条entry（加本轮到期followup）；失败档也写unknown。结构完整提交与业务来源完整分开：partial/failed批次可以真实提交用于coverage，不得将整批当作完整盈利统计。logs失败不写不完整事件集或推进游标；空成功可提交0事件。funding成功覆盖窗口才标complete。

market capture.status是全批覆盖，不是所有quote共享的有效性。成员数/摘要完整提交的partial批次，可使用其中协议、买入/换算/退出、depth/mark和时效全部已验证的单个quote；缺项quote仅保留对应费用空间或unknown。统计必须列出被使用与被排除的档位，不将不完整全批当成完整全批。

成员摘要由表类型+固定row_id+规范化row hash按序计算，读回校验数量与摘要。按capture_id最新revision再检查committed/canonical；不能先筛旧canonical。revision只更新finality/canonical，不改原时间、数据或成员摘要。

market头采集结束校验canonical；已有market锚点由后台按第8节额度逐个核验，每分钟最多4个不同锚点，重启不一次性核验所有历史。只有重新取得canonical hash且所在高度已finalized时才提升finality，finalized后停止重复核验；尚待核验者保留head/safe，历史确定性统计只用已核验finalized样本，孤块所有quote随capture排除。历史事件只收finalized；两类证据不混淆。连续日志游标由完整log capture恢复，不另建游标库。事件跨capture按chain/queue/blockHash/tx/log去重，冲突只比较原始协议字段，排除采集时间/payload等元数据以及补采gas/receipt。收据一份未知一份已知属于正常补证，已知费用矛盾才报异常；funding按instrument/fundingTime去重且检查rate/已知mark冲突。cost补采保留新capture，按有收据证据的版本查，不让后来NULL抹掉已有成本。

所有表按固定来源/批次时间月分区，无TTL或额外价格索引；七表DDL见链接。不会在当前库隐式建表，正式实施时才显式初始化研究库。

## 8. 运行预算与失败

### 8.1 发出速率，而不只是并发上限

所有启动身份核验、RPC能力探测、市场报价、日志/header/receipt、finality复核、资金费分页及重试进入各来源的**同一个发出gate**。以下是本研究程序的保守默认配额，不能当作供应商保证；限流状态按host持久化到固定本地状态目录，不含凭据，不增加业务表或任务平台。

| 来源／阶段 | 发出上限 |
|---|---|
| RPC初始化 | 每次实际方法调用至少间隔2秒；核验完成且启动已满60秒后才进入常态。首60秒最多30次 |
| RPC常态 | 每次实际方法调用至少间隔500毫秒，滚动60秒最多120次，最多2个在途；不积攒token、空闲后不突发补发 |
| RPC重型调用 | Quoter额外至少间隔1秒；`eth_getLogs`至少间隔10秒，仍消耗总RPC配额 |
| RPC显式历史回补 | 所有方法调用至少间隔1秒，滚动60秒最多60次，日志仍至少间隔10秒 |
| Binance全部REST | 串行，至少间隔5秒，滚动60秒最多12次及60权重；包含metadata、报价、历史和所有重试 |
| Binance资金费接口组 | `fundingRate`与`fundingInfo`合计滚动60秒最多2次、滚动5分钟最多10次；同时受全部REST额度约束 |

首版关闭JSON-RPC多成员batch，避免一次HTTP内塞20个RPC方法造成瞬时负载；复用RPC helper时也必须经gate拆成单成员，不能绕过限速。时间等待在实际发出前重新检查，禁止多个预留时隙因线程延迟后同时发送。收到响应后的重试同样计数，不把HTTP失败视为未使用额度。

固定ETHUSDT市场轮通常只有10档depth与带symbol的premiumIndex各一次，按当前官方规则为2+1=3权重/分钟；启动metadata也串行限速。资金费组有独立的500次/5分钟/IP官方限制，不能仅看权重为0就无限请求。[Binance接口权重](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data)。本次未找到PublicNode针对这些方法的明确可保证配额，RPC默认值必须在低速验收中验证，遇限流下调，不能承诺免封IP。

### 8.2 启动、积压与新鲜度

启动先读本地manifest、游标、种子和来源冷却状态，再串行核验必要身份与元数据；官方ABI随程序固定，不在每次启动遍历网站/代码仓库。初始化阶段不同时启动market、历史、收据和finality补查；核验完成后重新取当前head，不能把初始化最初的旧head用于首份报价。初始化失败在当前进程内等待，不退出让守护进程立即重放全部初始化。

研究目录内的`verify_public_sources.py`只是人工一次性核验辅助：同host至少间隔5秒，403/418/429立即终止整个脚本，无自动重试。它不参与collector启动，不具备跨进程持久冷却或全IP共享额度，不配置为自动重启服务或反复定时运行。

市场每60秒、每轮不重叠、轮次预算仍为30秒；优先完成当前到期的同量followup；四档按分钟轮转首位，仅每日UTC12:00至12:04种子窗口优先100k档，再尝试其他金额档，共同输入/换算可复用。CEX报价安排在链上报价窗口附近，仍校验第3.3节源时间差。慢RPC或严格配额导致无法完成时，写partial/unknown，不提速、不延长来源时间阈值、不沿用旧quote。漏过的市场周期不排队补跑，下一轮只抓当前状态；首次完整市场样本可在启动数分钟后出现，`--once`也不绕过初始化及限速。

watch日志每5分钟最多发出4次`getLogs`；显式backfill日志滚动5分钟最多30次且间隔至少10秒。两者都计入父片/失败片/二分子片及全部RPC额度；额度耗尽保存未完成范围，不推进连续游标。历史回补可以耗时数小时，具体时间取决于header数量和RPC能力，不在启动时高速追齐。收据每轮最多选择10个tx，transaction与receipt的两个请求分别计入RPC配额；恢复时不把全部120笔样本立即抓完。重复header/receipt按blockHash/txHash缓存。后台finality核验每分钟最多4个锚点，旧followup逾期直接记missed；资金费到期确认与常规48小时查询合并去重，分页也逐页经过gate，不形成多路紧循环。

### 8.3 限流、重试及共用出口

HTTP429或响应体明确限流时，停止该host所有新请求，冷却至少5分钟，并服从更长的`Retry-After`／明确的解禁时间；连续限流冷却10/20/30分钟并加正向随机延迟。遇HTTP418或明确IP封禁/拒绝访问的403，持久化停用该来源，等待人工处理，不循环探测。RPC范围过大、合约revert与来源限流须分开分类，不能把限流伪装成可二分的范围错误。

网络错误/5xx按15/30/60秒加正向随机延迟，同一轮一项任务最多2次额外尝试，连续失败跨轮保留退避计数；超出市场轮截止时间则结束该轮。来源冷却、下一允许发出时间和近期额度发出前原子持久化，重启后沿用；状态文件不可读写时先停止该来源请求。数据库写入重试只重写已冻结批次，不重新抓源数据。任一来源失败仅影响相关结果，不制造上一轮的当前quote，缺口明确保留。

Binance官方明确限流按**出口IP**计，违反429退避可导致418封禁，换API key无助于重置配额。[官方IP限制](https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/general-info)。本项目现有盘口、DEX、Reserve或Across进程可能共用出口；LST的进程内gate不等于全IP gate。部署前须核对同host其他进程的流量并留配额；多个进程需要按实测总量降低各自预算或接入共用gate，本次设计修订不改动这些在运行的服务。若Binance响应头显示已用权重达到官方额度50%，本程序暂停该host至少一分钟并重新检查；其余非本程序流量仍应独立排查。不得自动换节点、换IP或快速重启来绕过冷却。

实施验收先用假时钟和本地HTTP/RPC服务验证冷启动、30/60日回补、二分、header/receipt、429/418、失败重试及重启的真实发出间隔和滚动额度；不靠高频打公共端点来验证封禁。再跑`--once`核身份/八档结构/整数路径、实际请求数/耗时、NULL与hash，确认低速可用后限时短跑测compressed bytes/日估算和查询耗时。每分钟输出每host实际请求数、RPC方法数、权重、在途峰值、限流/等待与积压；不能只报HTTP batch数量。正常entry约11,520条/日、protocol约1,440条/日，followup每日数笔，事件按实际活动。空间只能按真实压缩测量，不承诺纸面估计。连续1小时检查时效/积压再决定研究采集期限，不把短跑当30日盈利证明。

## 9. 离线评估与验收

输出coverage.csv、quotes.csv、withdrawals.csv、funding.csv、scenarios.csv、summary.json。coverage列出来源失败/未知实现/孤块/过期/链范围缺口/删失/采样/相同金额期末缺采；报告不把unknown加工成无机会。

先输出名义赎回ETH的当前定额报价USDT价值减购买USDT的费用空间、四档容量、折价时段、实际队列年龄与完成率、claimed兑付比、历史短仓资金费现金流、gas/手续费/保证金盈亏平衡。再用同量followup作1/3/7/14/30日条件情景。

条件周期净额：`期末链上ETH→USDT输出 − 买入USDT + 开空名义金额 − 买回名义金额 + 实际短仓资金费 − 链上全部gas − CEX费用 − 部署/资金恢复/运行成本`。开平名义差才是线性合约P&L，开仓名义额不计作收到本金。兑付损失、lot残差和gas ETH敞口单列；haircut改变ETH数量时，不把q0退出报价冒充缩量实报。没有对应报价则成本/兑付压力情景，结果标conditional。

未来自己的等待、兑付、报价成交、对冲可用保证金仍未验证；跟随别人的请求和固定期限价格观察不会自动变成真实套利周期。没有同期历史入口报价，不把今日价格配30日前提现做历史利润。

初始全为USDT时，从购买预算之外预留gas ETH，记录bootstrap换汇/跨平台充值提现的公开可核费用或显式固定成本情景；gas储备按可执行购买价值计资，不以零余额approve模拟失败当策略失败。保证金先给100%短仓notional情景，并额外测试ETH上涨25%/50%期间追加现金压力；这不是账户保证金规则或免清算保证。维持保证金和账户准入未知时不给可实盘认证。

各档和路线为同刻替代方案，不加成交易量；重复分钟合并为时段，也不当1440次独立循环。资金分母包括购买、对冲保证金、gas和待回款，并在100万全资本口径同时报告；库存不跨链/跨账户瞬移。手续费示意测试每腿3/5/10bp、等待1/3/7/14/30日，均是参数而非账户事实。日均200目标需要联合30日净额6000，仍应单列最差7日与亏损日。

实现测试必须覆盖UInt256与各精度、份额舍入、qty lot/min notional、[from,to]边界、重叠/缺口与删失、正负资金费/结算mark、同tx去重计gas、捕获中断/重试身份、quote提交摘要、单源失败、孤块/finalized冲突、followup固定敞口/真实延迟、费用未知不当零。实际表结构与落地限制见[存储字典](market-data-storage.md)及[运行说明](lst-redemption-data-implementation.md)。
