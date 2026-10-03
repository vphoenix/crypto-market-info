# Deribit / OKX 期权公开数据源核查

核查日：2026-09-20。范围为公开行情、合约和规则；本笔记没有下单、创建 RFQ、创建 combo 或访问账户。下面的采集设计是研究建议，接口能力和规则则以所链接的当日官方文档为依据。实时目录与行情样本另见本目录主研究材料。

## 结论与采集优先级

优先做 Deribit BTC/ETH 的“全链 BBO 扫描 + 候选组合提前常驻 L2”，并同时看同到期期货与平台已有 combo。第二数据源加入 OKX 的匹配合约。更容易找到能复核的候选的关键是同时取得所有腿的买卖价、可买卖数量、来源时间、到期/结算身份和完整性状态；不是增加一份中间价或 IV 排名。

建议先取期权和匹配期货都有稳定双边量的近期与下一标准到期，再按真实覆盖率扩展。应保留少量远期/非活跃对照组，以免仅看到活跃合约而无法评价遗漏。单腿价差、组合总价、同到期期货合成远期是不同观测对象，应分别记录。

## Deribit：数据入口和容易踩的坑

| 用途 | 已核实入口 | 对采集的含义 |
| --- | --- | --- |
| 合约身份 | [`public/get_instruments`](https://docs.deribit.com/api-reference/market-data/public-get_instruments)，`currency=BTC/ETH/USDC, kind=option`；另外请求 `kind=future` | 使用返回的 strike、option_type、expiration_timestamp、instrument_type、settlement_currency、quote_currency、price_index、contract_size、min_trade_amount、tick_size_steps；不要只拆名称 |
| 新挂牌/状态 | [`instrument.creation.{kind}.{currency}`](https://docs.deribit.com/subscriptions/market-data/instrumentcreationkindcurrency)、[`instrument.state.{kind}.{currency}`](https://docs.deribit.com/subscriptions/market-data/instrumentstatekindcurrency) | 首次 REST 全量，随后生命周期事件驱动；长断线后重新全量对账 |
| 全链轻量扫描 | [`quote.{instrument_name}`](https://docs.deribit.com/subscriptions/market-data/quoteinstrument_name) | 返回 BBO、两侧 size、timestamp；无买价/卖价可能是 null，不等于零价；页面未承诺固定推送频率，需实测 |
| 候选深度 | [`book.{instrument_name}.100ms`](https://docs.deribit.com/subscriptions/orderbook/bookinstrument_nameinterval) | 首包全量，后续按 price 的 new/change/delete；检查 prev_change_id 与上次 change_id 相等。100ms 为公开可用；raw 要认证 |
| 精确 10 档视图 | [`book.{instrument_name}.{group}.{depth}.{interval}`](https://docs.deribit.com/subscriptions/orderbook/bookinstrument_namegroupdepthinterval) | `group=none, depth=10` 返回未合价的截断快照，但无 `prev_change_id`，不作为本项目连续L2主源；group非none还存在价格合并 |
| Greeks / IV 背景 | [`ticker.{instrument_name}.{interval}`](https://docs.deribit.com/subscriptions/market-data/tickerinstrument_nameinterval)、[`incremental_ticker.{instrument_name}`](https://docs.deribit.com/subscriptions/market-data/incremental_tickerinstrument_name) | ticker 含 mark/bid/ask IV、Greeks、用于 IV 的 underlying_price / underlying_index / interest_rate；incremental_ticker 最多每秒一次且需合并变更字段，不应承担亚秒套利触发 |
| 全链估值 | [`markprice.options.{index_name}`](https://docs.deribit.com/subscriptions/market-data/markpriceoptionsindex_name) | 一频道获取整个指数下的 option mark/IV；只做估值背景，不能当可执行报价 |
| 指数 / 临近交割 | [`deribit_price_index.{index_name}`](https://docs.deribit.com/subscriptions/market-data/deribit_price_indexindex_name)、[`estimated_expiration_price.{index_name}`](https://docs.deribit.com/subscriptions/market-data/estimated_expiration_priceindex_name) | 指数和预估交割价分开；最终交割价由 [`public/get_delivery_prices`](https://docs.deribit.com/api-reference/market-data/public-get_delivery_prices) 补齐 |
| 已有组合 | [`public/get_combos`](https://docs.deribit.com/api-reference/combo-books/public-get_combos)、[`public/get_combo_ids`](https://docs.deribit.com/api-reference/combo-books/public-get_combo_ids)、[`public/get_combo_details`](https://docs.deribit.com/api-reference/combo-books/public-get_combo_details) | 不在 `kind=option` 单腿目录中；保存 combo ID、状态、每腿 instrument_name 与有符号 amount。负 amount 表示与组合方向相反 |

生产 WS 是 `wss://www.deribit.com/ws/api/v2` / 官方 schema 中的 `wss://deribit.com/ws/api/v2`；文档大量请求示例使用 test.deribit.com，采样必须显式标记环境。上述频道可由公共 subscribe 接入；raw 的认证门槛不应误写成所有行情都需要 API key。[官方数据采集指南](https://docs.deribit.com/articles/market-data-collection-best-practices)

数量归一化要区分三件事：JSON-RPC `amount` 的单位、合约乘数、下单数量步长。现行合约接口明确：inverse future/perpetual 的 amount 是 USD；option、spot、linear future/perpetual 的 amount 是基础币。`contract_size` 是 amount 与 contracts 的换算，`min_trade_amount` 才同时表达 JSON-RPC 最小交易量与数量步长，不能机械地拿 contract_size 当 lot。Deribit JSON 数值从原始字符直接进 Decimal，不能先经 float。[合约接口](https://docs.deribit.com/api-reference/market-data/public-get_instruments)

批量订阅最多 500 channels/请求。当前 rate-limit 页把 get_instruments 单列为持续 1 次/秒、50 burst，subscribe 单列为约 3.3 次/秒、10 burst；未认证公共调用按 IP 限制，不应把账户默认额度当成无条件公共预算。设计应留限速余量、抖动重连、批量恢复，避免候选每跳一次就 subscribe/unsubscribe。[限频](https://docs.deribit.com/articles/rate-limits)、[批订阅说明](https://docs.deribit.com/articles/market-data-collection-best-practices)

## Combo 值得采，但不能把它当四条独立腿

Deribit 现有 combo 包括 reversal/conversion、vertical、butterfly、calendar 和 jelly roll。官方 combo 一单可同时成交多腿，降低逐腿执行风险；有买有卖的 option combo 还有单独的费用折扣规则。现有 combo book 可能因活跃度规则被停用，不能只在启动时抓一次目录。[Combo Books](https://support.deribit.com/hc/en-us/articles/31424954956061-Combo-Books)、[费用](https://support.deribit.com/hc/en-us/articles/25944746248989-Fees)

研究时可比较：买入 combo 的 ask 与逐腿反向成交净收入；卖出 combo 的 bid 与逐腿复制净成本。每腿必须用正确 bid/ask、符号、乘数和数量，不能用 mark 合成“可成交价格”。组合内成交虽原子化，combo 与外部 hedge/另一 combo 之间仍有执行时间差。

模型建议：combo 用专项有符号价格模型；例如 call-put 的净价格可正、零、负，这是组合价值的代数性质。不要为了通过现有 `price_tick>0` 校验而取绝对值或丢掉它。组合容量由最紧腿及 ratio 决定，不能把不同腿成交量相加。

私有 `user.combo_trades.*`、Block RFQ 报价不可误列为无认证全市场数据。当前研究只发现并订阅已存在的公共 combo；`private/create_combo` 和 `private/create_block_rfq` 都是主动操作，不在采集建议内。[官方期权采集指南](https://docs.deribit.com/articles/options-data-collection-best-practices)

### 当日有界 combo 探测

在 2026-09-20 14:16:52–14:17:03 UTC，以无凭据方式请求 BTC/ETH/USDC 的 `public/get_combos`，各从前两种 currency 取返回列表中的首个 option combo，最多请求两份 `get_order_book(depth=10)`，均 HTTP 200 且有正常 result。只读脚本是 [probe-combos.py](probe-combos.py)，原始字节/hash/请求接收时间在 [combo-probe-manifest.json](combo-probe-manifest.json)，统计在 [combo-probe-summary.json](combo-probe-summary.json)。

| 目录 currency | 活跃 combo 总数 | 含期权腿的 combo |
| --- | ---: | ---: |
| BTC | 88 | 10 |
| ETH | 88 | 10 |
| USDC | 34 | 34 |

两份独立 REST 快照分别为：BTC-CCAL-22SEP26_21SEP26-82000 的 bid/ask 为 0.0017/0.0021，size 15/30，返回两侧各 7 档；ETH-CCAL-25DEC26_25SEP26-1400 的 bid/ask 为 0.0092/0.0107，size 400/200，返回 6/5 档。价格与数量保持来源单位，未当成 USD 利润。该选择不是随机抽样，也没有同时抓齐对应腿；它只证明当时已有公共 option combo 带双边盘口，不能推出全部 combo 有流动性、持续可成交或存在净套利。USDC combo 此轮只有目录、没有盘口探测。

## OKX：公开链扫描与连续盘口

| 用途 | 接口 / 频道 | 要保留的区别 |
| --- | --- | --- |
| 目录 / tick 分段 | `GET /api/v5/public/instruments?instType=OPTION&instFamily=BTC-USD`；`GET /api/v5/public/instrument-tick-bands?instType=OPTION`；WS `instruments` | `ctVal/ctMult/ctValCcy/settleCcy/stk/optType/expTime`；目录 tickSz 对期权只是 tick band 中最小值 |
| 全链初筛 | REST `GET /api/v5/market/tickers?instType=OPTION&instFamily=BTC-USD`；WS 每合约 `tickers` 或 `bbo-tbt` | 保存价格和 size；BBO 快照不等于序列连续的 L2 |
| 候选 L2 | 公共 WS `books` | 400 档初始快照，100ms 增量；内存维护后保存前 10 档 |
| 更快 L2 | `books50-l2-tbt` / `books-l2-tbt` | 10ms；分别需登录且 VIP4 / VIP5，不作为无凭据方案前提 |
| Greeks / IV / forward | `GET /api/v5/public/opt-summary`；WS `opt-summary`, `instFamily` | `delta/gamma/vega/theta` 与 `*BS` 分开；`markVol/bidVol/askVol/fwdPx/ts`，不是订单簿 |
| 指数 / mark / 交割 | WS `index-tickers`、`mark-price`、`estimated-price`；REST `GET /api/v5/public/delivery-exercise-history` | 预估和最终价、交易所指数身份不能混淆 |
| 市场成交 | WS `option-trades`, `instFamily`；REST `GET /api/v5/public/option-trades` | size 是 contracts，不能直接当 BTC/ETH 数量 |

官方入口：[公共数据及期权接口](https://www.okx.com/docs-v5/en/#public-data)、[盘口](https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-order-book-channel)、[接入实践](https://www.okx.com/docs-v5/trick_en/)。本次全球英文文档体积超过浏览器工具限制，字段用同属官方的 [app 文档](https://app.okx.com/docs-v5/en/) 与全球 changelog 交叉核对；不同地区 API 域名与产品权限必须在后续 WS 探测中按实际环境确认，不能从一个地区目录推断全球可用性。

关键现行变更：**2026-06-23 起，books/books-l2-tbt/books50-l2-tbt 的 checksum 已废弃，值固定为 0。不要照搬旧 CRC32 检查；用 prevSeqId / seqId。** 无变更保活可以两侧空数组且 seqId=prevSeqId；维护时允许 seqId 重置变小，只要消息链连接正确。不是所有 seqId 变小都等于断档。真正断档必须失效并重新获得快照。[官方变更公告](https://www.okx.com/docs-v5/log_en/#2026-06-23)

OKX 文档规定每连接 subscribe/unsubscribe/login 合计 480 次/小时、建连每 IP 3 次/秒；大量 50/400 档订阅建议分连接，每连接少于 30 个频道。因此候选升级要有驻留时间和滞回，避免不断换订阅耗尽预算。[连接限制与盘口说明](https://app.okx.com/docs-v5/en/)

`opt-summary` 页面没有可以据此承诺的固定推送间隔；正式设计应把观测频率写成配置与实测指标，不能凭习惯硬编码“2 秒”。

## OKX 组合来源的边界

已核实有公开结构化大宗成交 `GET /api/v5/rfq/public-trades` / `public-struc-block-trades`，以及单腿大宗成交 `GET /api/v5/public/block-trades` / `public-block-trades`。2026 年 group RFQ 变更还明确父级记录可能没有 blockTdId / tradeId，应使用 groupId 等来源身份避免错误去重。它们是**已经发生的成交**，不能当当前可成交的多腿买卖盘。[官方 RFQ 数据变更](https://www.okx.com/docs-v5/log_en/)

Nitro Spreads 官方介绍覆盖基础价差和 calendar 等双腿 spread；本轮没有核实一条可替代 Deribit option combo book 的无认证 OKX 全期权组合报价流。不要因为产品都在 Liquid Marketplace 下，就把 Nitro spread 盘口、期权 RFQ 成交、私有 RFQ quote 合并为同一类型。[Nitro 官方介绍](https://www.okx.com/en-us/help/nitro-spreads-introduction)

## 结算规则要版本化

Deribit linear USDC option 自 2026 年 4 月初起，ITM 先交割成同到期期货，该期货立即 USDC 现金结算；到期已有期货仓位可先净额抵消，delivery fee 也有对应规则。虽然净 payoff 与此前直接现金结算相同，事件类型和费用不能使用旧版本固定假设。[2026-08-25 更新的 USDC 期权规则](https://support.deribit.com/hc/en-us/articles/31424932728093-Linear-USDC-Options)

Deribit 新合约政策说明 linear futures 覆盖对应每个期权到期，inverse futures 也增加 daily expiry。研究合成远期时应先查真实目录能否匹配同到期 future，不必默认只能用永续对冲。[挂牌规则](https://support.deribit.com/hc/en-us/articles/25944688876957-Contract-Introduction-Policy)

OKX 2026-03-18 起结算指数窗口由 1 小时改成 30 分钟，不能依赖仍写 1 小时的旧介绍。即使两家都叫 BTC、strike/expiry 相同，也可能用不同指数、采样规则和结算币，不能直接认定 payoff 完全一致。[OKX 生效公告](https://www.okx.com/en-us/help/okx-announcement-on-the-adjustment-to-the-calculation-method-of-delivery)

## 增加“适合候选”的抓取概率：可验证实验

以下为建议，不是已完成的连续采集结果：

1. 全链 BBO 常驻；小规模、稳定候选集预先常驻 L2。先用两侧量、spread、到期、邻接 strike 配对筛选，不按当天成交量一项排序。只有单边价可以记录，但不产生需要双边执行的候选。
2. 记录 source_ts、received_at、连接会话、source sequence、最后一次有效快照时间；保存每条腿的 age 和最远时间差。连接健康但长期无变化与断线后旧值不能同样处理。
3. BBO 触发后查询已在内存里的多腿深度；临时发 REST 补盘口只能作为后验观察，不能用来证明触发瞬间能成交。若尚未订阅 L2，标注 evidence incomplete。
4. 以相同基础币数量及 Decimal 计算净现金流，分别报 0 延迟、100ms、250ms、500ms 后仍存在的候选，外加手续费、交割费和融资敏感度。时间阈值是实验参数，不是平台保证。
5. 新盘口数据保存每秒前 10 档、分钟快照与秒级差量，并写 `stored_depth=10`；旧 50 档历史按原分钟深度完整回放。生产秒内研究只建议在分析层另建定类型 candidate observations，记录候选所用腿/序列引用、各腿时间、价量或扫单结果、费用版本与拒绝原因；不增加第二套高频 L2 历史，也不声称据此能回放任意秒内盘口。本目录的短时原始协议消息仅是有界研究探针的证据。只有秒级归档时，不能判断一个价差曾持续 50ms 还是 950ms。
6. 做 7–14 天观察实验后比较：有效双边覆盖率、全腿同步覆盖率、净价差持续时间、真实深度容量、候选进入后数据补齐时延、重连/断档比例、每日压缩空间。全链宽扫描和限定期限深采样分别计费，按“可完整复核候选数/采集成本”选下一批 universe。

上述实验才回答哪里更容易抓到适合的，而不是根据交易所名气断言哪里已有套利。需要数据继续验证的包括 quote 实际更新频率、组合活跃数量、同到期期货深度和费用后的持续机会；本笔记没有声称已经发现可执行利润。
