# 期权套利研究：优先采什么，怎样提高有效候选的命中率

研究日期：2026-09-20。结论基于本仓库已有实现、官方现行文档、当日公开 REST 样本和短时 WebSocket 探测。本文是采集研究方案，未启用期权生产采集，也未验证可获利交易。

2026-09-21 已形成 [实施设计](../../docs/arbitrage/strategies/arb-0009-options-collection-design.md)。以新数据每秒10档为约束，并纠正此前 grouped 10档频道不能提供前驱连续性证明的问题；实时连续L2采用full book，采样后只保存10档。

## 1. 建议

**先接 Deribit 的 BTC/ETH：同时覆盖币本位和 USDC 线性期权、同到期期货，以及已存在的原生组合盘口。首轮研究同所 put-call parity、垂直价差/蝶式和 box。** 先做全链轻量发现，再把完整候选组合放入持续 L2 采集池；第二个场所优先复用本项目的 OKX 基础设施，并依据 Bybit 的实际深度与持续性比较是否先接 Bybit。

这项排序是数据研究的判断，不是“Deribit 更容易赚钱”的实证结论。理由是：同所合约语义较容易严格匹配；BTC/ETH 本次双边报价覆盖较高；Deribit 已有同到期期货和公开组合簿，可以把终值关系、资金成本和分腿成本研究清楚。跨所同名期权、IV 高低和永续对冲放在后续相对价值研究层。

**提高命中率的重点：抓完整可交易组合、真正的买卖价和数量、报价持续性、原生 combo、费用及结算规则。** 只扩大 symbol 数或保存 mark/IV，很容易增加假机会。

## 2. 当日实际拿到了什么

### 2.1 公开目录与摘要

2026-09-20 14:12:09–14:13:04 UTC，对 Deribit/OKX 共 16 次 REST 请求均成功；原始响应字节、URL、请求/接收时间、SHA-256 位于 [probe-manifest.json](probe-manifest.json)。复核统计用 [probe-public.py](probe-public.py)，结果见 [coverage-summary.json](coverage-summary.json)。价格直接从 JSON 解析为 Decimal。

“双边”指 `bid>0 && ask>bid`，不是容量或成交承诺。Deribit 摘要没有 BBO 数量，必须再看 quote/book；OKX ticker 有数量，本样本这些双边报价未出现零数量。各请求异步，表内不能用于拼合可执行价差。

| 市场 | 目录合约数 | 有正双边价格 | 比例 | 双边且距到期 2–45 天 |
| --- | ---: | ---: | ---: | ---: |
| Deribit BTC 币本位 | 968 | 895 | 92.5% | 384 |
| Deribit ETH 币本位 | 870 | 797 | 91.6% | 362 |
| Deribit BTC USDC 线性 | 634 | 553 | 87.2% | 342 |
| Deribit ETH USDC 线性 | 606 | 529 | 87.3% | 323 |
| Deribit SOL USDC 线性 | 660 | 305 | 46.2% | 185 |
| OKX BTC 币本位 | 1,406 | 1,203 | 85.6% | 565 |
| OKX ETH 币本位 | 1,198 | 1,033 | 86.2% | 469 |

其余 Deribit 线性 AVAX/HYPE/TRX/XRP 的双边覆盖约 47%–57%。单次覆盖支持 BTC/ETH 优先，但不能推导其他资产没有机会，或某场所长期流动性更好。

Deribit BTC/ETH 币本位分别有 12 个期权到期日，本次均找到同到期期货且有正双边价格；BTC/ETH USDC 分别有 8 个，同样全部匹配。这里仅证明目录和价格存在，未验证目标数量下的期货容量。

另一路只读探测核实了 Bybit 与 Binance 的现行目录和 WebSocket，详见 [Bybit/Binance 笔记](source-notes-bybit-binance.md)。Bybit 本次目录共 4,128 条、全部 USDT 结算；不同请求间 BTC ticker 和目录差一条，不能把分页当原子快照。Binance 返回 1,550 条，其中 1,504 条加密期权为 TRADING，另外 46 条贵金属期权为 CLOSED_MARKET，不能把所有 symbol 都放进加密套利集合。

### 2.2 短时真实盘口验证

Deribit 14:14:21–14:15:02 UTC 的一次免登录探测订阅了 12 条 `book.<instrument>.100ms`：BTC/ETH × 币本位/USDC × call、put、同到期期货；另订一条 `quote`。12 本书均收到初始 snapshot 和后续更新，观察期未见 `prev_change_id` 断档或交叉盘口；BBO 频道收到 6 条消息。见 [ws-summary.json](ws-summary.json) 和 [probe-ws.py](probe-ws.py)。

观察结束时，不少期权或线性期货每侧只有 3–9 个价位。这是实际深度稀疏，不能补出不存在的 50 档，也不能由“订阅全深度成功”推导“有足量可成交”。40 秒只验证接口与数据结构，不代表生产稳定性、跨腿原子性、长期带宽或盈利能力。

随后在 14:16:52–14:17:03 UTC 读取组合目录：BTC/ETH 各有 10 个活跃期权 combo，USDC 有 34 个。抽查 BTC/ETH 各一本既有组合簿，均有双边价量（分别 7/7 档和 6/5 档）；两本都是日历组合，不能据此推断同到期 box/蝶式也有现成流动性。该小样本说明组合数据确实可公开采集，未同时检查单腿价差。见 [组合探测结果](combo-probe-summary.json) 与 [证据清单](combo-probe-manifest.json)。

此前 [9 月 13 日的 box 验证](../2026-09-13-structural-yield/locked-payoff-verification.md) 在 5,875 个线性 box 中未找到正的即时费后收益。这是历史静态样本；本次不将其当成长期不存在机会的证据。

## 3. 策略与采集优先级

| 顺序 | 研究对象 | 同时需要哪些腿 | 值得抓的原因 / 限制 |
| --- | --- | --- | --- |
| 1 | 同所 put-call parity | 同 K、同到期 call/put + 同到期期货 | 终值关系明确；三腿；同时研究隐含融资。永续不能替代确定终值的期货 |
| 1 | 同到期垂直价差、蝶式 | 相邻行权价；蝶式中间腿双倍数量 | 检查单调性/凸性不需要依赖 IV 模型；优先观察已有原生组合簿 |
| 2 | Box / 隐含融资 | 两个 K 的 call/put 四腿或原生组合 | 毛终值可固定，但四腿价差、交割费、保证金与资金成本会吃掉收益 |
| 3 | 跨所近似期权 | 双所期权 + 汇率/指数/结算与资金规则 | 合约往往不是同一 payoff；独立抵押资金且不能原子成交 |
| 4 | IV/skew/期限结构 | 全链估值、成交、指数与动态对冲成本 | 是相对价值或波动率交易，不自动构成静态套利 |

原生 combo 有单独订单簿、腿方向和比例，可一次执行组合内的腿；Deribit 某些含买卖方向的 combo 有费用减免。**手工拼出的四腿价差不能套用 combo 的费用和同时成交条件**，组合外的期货对冲仍有分腿风险。[Combo 规则](https://support.deribit.com/hc/en-us/articles/31424954956061-Combo-Books)、[费用规则](https://support.deribit.com/hc/en-us/articles/25944746248989-Fees)

### 关键公式：不能混淆收益币种

以下均按一单位基础币名义、相同到期结算指数 `X`；每腿还需按真实乘数、步长和深度换算。完整推导与约束见 [策略笔记](strategy-notes.md)。

线性期权：`call - put = X-K`。买 call、卖 put、卖同到期期货的毛终值是 `F-K`。零资金成本初筛边为：

```text
方向 A：F_bid - K - (C_ask - P_bid)
方向 B：K - F_ask - (P_ask - C_bid)
```

真实筛选必须把净权利金按实际可获得的资金成本折算到同一时点，扣掉交易费、深度滑点、交割费、保证金资本成本和数量舍入敞口。每日结算还会改变中间现金流。不能把被抵押占用的卖出权利金视为可自由投资现金。

币本位反向期权：`call_coin-put_coin=1-K/X`。配 `K` 美元名义的反向期货空头，组合毛终值才是 `1-K/F` 个币；不是随便卖出“一张期货”。正的固定 BTC 收益也不等于固定美元收益。

线性 long box：`+C(K1)-C(K2)+P(K2)-P(K1)`，毛终值是 `K2-K1` USDC；币本位 box 则为 `(K2-K1)/X` 个币。按未来指数收取的交割费还可能让**费后**终值变化。卖 box 收到的是带到期债务的融资款，不能计作利润。

等距蝶式负成本初筛：`ask(C1)-2*bid(C2)+ask(C3)<0`。中间腿容量需除以 2，非等距行权价需重新求比例并落到可交易整数 lot。

## 4. 公开接口怎么选

| 来源 | 发现与辅助数据 | 候选深度链路 | 当前容易踩的坑 |
| --- | --- | --- | --- |
| Deribit，首接 | `public/get_instruments`；`get_book_summary_by_currency`；`quote.<instrument>`；`ticker`/`incremental_ticker`；`get_combos` | 全量 `book.<instrument>.100ms`，按 `change_id/prev_change_id` 校验，内存维护后每秒保存前 10 档 | grouped 10档无前驱序列，只作快照观察；免登录不能用 raw；`markprice.options` 只有估值；单腿目录不包含 combo |
| OKX，第二来源候选 | `public/instruments?instType=OPTION`；`market/tickers`；`public/opt-summary`；指数、标记价、交割历史 | 公共 `books` 400 档/100ms，按 `seqId/prevSeqId`；接受链连续的合法序号重置，真正断档才重取快照 | 2026-06-23 起旧 checksum 固定为 0；不能照搬旧 CRC32；快速 10ms 频道有权限条件 |
| Bybit，第二来源候选 | V5 `instruments-info?category=option&baseCoin=All` 完整分页；`tickers`；交割价 | `wss://stream.bybit.com/v5/public/option` 的 `orderbook.25.<symbol>`，20ms，snapshot/delta | 25 档足以维护并保存前 10 档；实际不足 10 档时只保存真实档位；以元数据为准识别 USDT/USDC |
| Binance，后续比较 | `/eapi/v1/exchangeInfo`、ticker、mark、index | 当前 WS `wss://fstream.binance.com/public/`；diff `@depth@100ms` + REST snapshot，校验 `U/u/pu` | 旧 `nbstream` 接入资料已不适用；partial 最多20档；本次 runtime限频为400权重/分钟；每连接200 streams；有的标的不支持裸卖 |

来源：[Deribit 期权数据指南](https://docs.deribit.com/articles/options-data-collection-best-practices)、[Deribit 订单簿](https://docs.deribit.com/api-reference/market-data/public-get_order_book)、[OKX V5 文档](https://www.okx.com/docs-v5/en/)、[OKX 变更日志](https://www.okx.com/docs-v5/log_en/#2026-06-23)、[Bybit WS orderbook](https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook)、[Bybit REST orderbook](https://bybit-exchange.github.io/docs/v5/market/orderbook)、[Binance 当前期权公共 WS](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-options/api/ws-streams/public)。具体字段与权限见两份来源笔记。

Deribit `get_instruments` 持续限额只有 1 次/秒；启动完整发现后用 instrument lifecycle 订阅跟踪新上市和到期，断线后补全目录。REST 用于种子、核对与补查，不能逐合约轮询拼“同步全链”。历史成交/蜡烛也不能恢复从未采集的订单簿。[目录文档](https://docs.deribit.com/api-reference/market-data/public-get_instruments)、[行情采集指南](https://docs.deribit.com/articles/market-data-collection-best-practices)

### 规则需要随时间保存

- Deribit USDC 期权自 2026 年 4 月起，ITM 先转为同到期期货，再立即现金结算；现有期货与生成头寸可能净额抵销，不能重复收取不存在的交割费用。[线性期权规格](https://support.deribit.com/hc/en-us/articles/31424932728093-Linear-USDC-Options)
- OKX 2026-03-18 起把结算均价窗从一小时改成最后 30 分钟，每 200ms 采样；部分旧规格正文仍写一小时。Deribit 也是半小时，但指数和采样规则并不相同。因此既不能沿用旧窗口，也不能因为窗口相同就认定跨所收益完全抵销。[OKX 生效公告](https://www.okx.com/en-us/help/okx-announcement-on-the-adjustment-to-the-calculation-method-of-delivery)、[Deribit 结算](https://support.deribit.com/hc/en-us/articles/29734325712413-Settlement)
- Bybit 新旧期权页面对结算币种、手续费 cap 的描述有变化；合约元数据、费表生效时间和来源必须一起存。不要把旧 USDC 规格直接套到本次全部 USDT 的在市目录。[当前费用页](https://www.bybit.com/en/help-center/article/Bybit-Option-Fees-Explained)

## 5. 怎样抓得更可能有用

### 第一层：全链轻量发现

首轮覆盖 Deribit BTC/ETH 两种结算路线，共约 3,078 条期权（本次目录）。全链保存元数据、BBO 价量与观测质量；IV/Greeks、OI、成交只作辅助。按连接、订阅量和入站处理预算分片，容量检查失败就显式缩小覆盖范围，不能静默漏链。其他资产先低频普查。

发现层不以成交量作为硬门槛：长期没有成交的期权也可能有真实挂单；也不只盯最活跃的 ATM。排序结合双边深度、价格间距、到期期限、组合约束偏离、接收延迟和过去持续性。

### 第二层：完整组合提前保留 L2

启动候选预算建议约 160–320 条期权，另加它们所需的同到期期货、相邻行权价和已有 combo；这是试跑上限建议，需根据实测容量调整。基础池放 2–45 天、相对期货价格接近平值的完整 call/put 配对；给 0–2 天、远翼、长到期以及新上市分层保留探测预算。

不能等正价差出现才临时拉四个快照：订阅与快照完成时机会可能已消失。应提前保留完整日期/行权价小网格，一条腿进入候选池时，其互补期权、对冲期货和邻近行权价一起进入；热度下降后延迟退订以减少反复切换。保留少量轮换观察池，防止只观察已有候选而产生选择偏差。

可执行量按统一基础币数量计算，买用 ask、卖用 bid，逐档扫单；至少报告小/中/大三档容量和净边曲线。初始示例规模可取 BTC 0.01/0.1/1、ETH 0.1/1/10，须满足每腿与组合的最小量/lot；这些是研究情景，不是交易建议。

### 第三层：分清“有效状态”和“可执行候选”

每次比较需要同一计算时点下各腿最近可知的书，保存 `source_time`、本地 UTC 接收时间、单调时钟顺序、连接代次、序列、状态和规则版本。断线/丢包后失效，快照恢复前不沿用旧状态。

**报价很久没变不等于断流。** 冷门期权可在连续有效连接中保持相同挂单；连接心跳也不能单独证明某个订阅未丢数据。要结合订阅确认、序列连续性、重连状态和必要的独立快照核对，分别保存“最后变化时间”和“最近确认有效时间”。只用跨腿最后更新时间差做硬过滤会漏掉合法静态报价。

按 100/250/500/1000ms 延迟重估机会的存活性，并对价格/深度恶化施压，但阈值应由实际采集延迟和未来执行条件校准。所有买卖腿报价看似同时存在，仍不能保证同时成交；公共 L2 也不能证明自己挂 maker 单会排到并成交。公开成交可以佐证活跃度，不能证实假设订单成交。

主 L2 历史对新数据保持每秒前 10 档、分钟快照与秒差量，并显式写 `stored_depth=10`；旧 50 档历史继续按各分钟的 `stored_depth` 完整回放。**只有秒级数据时，报告秒级存活性，不声称恢复了 100ms 机会。** 将来若研究秒内候选，可在分析层另建定类型候选观测：记录腿/序列引用、各腿时间、使用的价量或扫单结果、费用版本与拒绝原因；它不是第二套原始 L2 历史，不能据此宣称可回放任意秒内盘口。是否增加该分析表另行设计并同步存储文档。

## 6. 项目需要补的模型，而不是直接打开开关

当前 [Instrument](../../internal/model/model.go) 只支持 spot/perpetual/delivery；不含行权价、C/P、结算指数和期权 payoff。现有 `BookSnapshot` 要求两侧非空、价格为正，[本地盘口](../../internal/orderbook/book.go) 同样不把空边书输出为有效 snapshot。不能把期权伪装成 delivery 来绕过这些条件。

以下是拟议语义拆分，尚未创建表或修改现行标准：

| 拟议模型 | 要保存的核心事实 | 与现有表关系 |
| --- | --- | --- |
| `option_contract_spec` | 标的、C/P、精确 K、UTC 到期、欧式/其他、线性/反向、权利金币、结算币、指数/规则版本、API数量单位、乘数、步长、分段tick | 扩充 instrument 身份；规格按生效版本回溯 |
| 单腿 L2 | 整数价格 tick / 数量 lot、源序列、每秒前 10 档状态 | 身份与有效性扩展完成后可复用现有分钟/秒差量路径；写 `stored_depth=10` |
| `market_sample_quality` | 采样时点、源/接收/确认时间、连接代次、序列、实际边/档数、缺失原因 | 为盘口引用补充质量语义；当前 `valid_bitmap` 不足以说明多腿同步质量 |
| `option_market_observation` | mark、IV、Greeks、OI、underlying、来源时间/哈希 | 独立 Decimal 事实，不把 mark 塞入可成交盘口 |
| `option_combo_spec` + `option_combo_leg` | combo 身份/状态、腿身份、有符号比例、生效版本 | 独立于单腿；报价可为零/负，不能套当前正价格校验 |
| combo 行情 | 有符号的前 10 档价量与质量 | 需另定支持零价、负价的编码不变量；不能用零价同时表示真实报价和空档，不能用单腿拼价冒充原生簿 |
| 结算/费用/公开保证金规则与观测 | 指数成分、采样窗、最终交割价、费率基数/cap/币种/组合折扣、来源、生效时间和哈希 | 专项类型；不放资金费率表或通用 JSON 表 |

分段 tick 应保存稳定的整数存储单位与独立有效报价步长规则，不能每次跨价格阈值就更换 tick 含义。本次核实的 Deribit 经典 JSON-RPC 接口中，inverse future 的 amount 是 USD 名义，option/linear future 的 amount 是基础币；`min_trade_amount` 表示最小量与增量，`contract_size` 用于 amount 与 contracts 换算。已是基础币的数量不能再乘一次合约乘数；其他协议和产品版本须分别做固定样例验算。[字段定义](https://docs.deribit.com/api-reference/market-data/public-get_instruments)

单边为空可能是市场事实，不一定是采集断流。扩展前当前盘口链路会将其判为不可用；至少应在发现/质量模型保留“连接健康但没有该侧报价”的事实，不能以填零价格、旧报价或猜测数量伪造成完整书。稀疏盘口只保存真实档位，填充位不参与成交计算。

架构仍是适配器 → 标准化 → 内存 L2 → 每秒采样 → 批量存储 → 查询/回放；候选筛选位于查询/分析层，适配器不嵌入套利规则。实际实施以上身份、表或有效性变化时，必须更新 [market-data-storage.md](../../docs/market-data-storage.md) 并验证原有数据不变量。

## 7. 建议的试采验收

先跑 7 天，若没有覆盖一次周到期或出现持续性疑问，再延长到 14 天；这只是后续方案，本次没有启动后台任务。

1. **先验数据质量**：每个候选完整配对比例、有效秒、缺口/重连、源到接收延迟；完整 snapshot+delta 回放、同秒最终量、删除、无变化与无效区分、断档后的失效和恢复。
2. **再验价差真实性**：按费用前、普通公开费率、交割与资金情景逐层计数；记录被不同指数、单位、稀疏深度、过时状态、数量步长淘汰的原因。
3. **再验持续与容量**：输出互不重复的机会区间数、持续时间、不同研究规模下容量/净边、延迟压力后存活比例。秒级/秒内证据分开报告；不把每次行情更新重复计为一个新机会。
4. **同时测资源**：活跃/非活跃期权的实测压缩 bytes/日、全链发现流量、CPU/队列峰值、查询/回放耗时和候选查找耗时。不能拿这次40秒原始WS文件体积外推长期ClickHouse占用。
5. **最后决定扩张**：按扣成本后的容量、持续性、数据完整性排序，才增加期限、SOL等资产或第二交易所；不要按最高年化或最大mid价差扩容。

合适的交付结果应是“在什么资金与费用情景下，哪些同所组合持续存在多少可成交深度”，并明确尚缺的执行条件。只出现 mark 偏离、无法覆盖最低交易量，或只有一次异步 REST 正价差，都不足以升级为套利机会。

## 8. 复现与范围

离线核验已保存 REST 响应哈希并重算覆盖：

```bash
python3 research/2026-09-20-options-arbitrage/probe-public.py
```

显式加 `--fetch` 会重新请求公开来源并覆盖本目录同名探测证据；保留本次证据时，应先复制研究目录到新的日期目录。`probe-ws.py` 也会覆盖同名短时WS证据。两个脚本是一次性研究探针，不具有生产采集的重连、持久化与完整运行监控能力。

本次仅新增研究目录、来源笔记与公开行情证据；未改生产 collector、表结构或服务配置。未使用 API key、账户或交易接口。
