# Bybit / Binance 期权公开数据核查

核查日期：2026-09-20。仅使用交易所官方文档和无需认证的公开 REST / WebSocket。以下把文档约定、当日短时观测和采集建议分开；短时观测不构成套利收益或长期流动性证明。

## 直接影响采集方案的结论

- Bybit 期权适合接入现有 L2 链路：订阅 `orderbook.25.{symbol}`，维护 25 档本地状态、每秒截取实际存在的前 10 档。应以 WS 初始 snapshot 建立状态；盘口不足 10 档时只保存真实档位。
- Binance 期权公开接口已按新版文档核查：REST 仍是 `https://eapi.binance.com`，WS 使用 `wss://fstream.binance.com/public/` 和 `/market/`。不能沿用旧 `nbstream` / `depth1000` 的资料。当前 partial depth 只有 5/10/20 档，项目应使用 diff depth + REST snapshot。
- 当日 Bybit 完整分页返回的合约全部为 USDT 结算；WS 文档仍写 USDT/USDC，不能据此宣称当前两种结算合约都在市。Binance 本次返回也均为 USDT 结算。
- 首批应聚焦 BTC / ETH：两家可直接比较同到期日、同执行价的 Call/Put 组合，但还要核验结算指数和结算窗口。Binance 当前公开元数据仅 BTC / ETH 的 `nakedSell=true`；其他币即使有买卖盘，也不能假定普通参与者能建立所需空头。
- 全链目录和 ticker 用于发现候选；持久采集应包含完整组合的所有腿及相关现货/交割合约盘口。mark / IV / Greeks 是辅助字段，不能当成可成交价。

## Bybit 官方接口

| 数据 | 接口 / 关键约定 | 采集用途 |
| --- | --- | --- |
| 合约目录 | `GET /v5/market/instruments-info?category=option&baseCoin=All&limit=1000`，跟进 `nextPageCursor`；未传 baseCoin 时默认 BTC | 发现、到期/新上市、价格与数量精度、结算币、合约状态 |
| 全链 ticker | `GET /v5/market/tickers?category=option&baseCoin=BTC`，ETH 同理，期权必须给 symbol 或 baseCoin；可加 expDate | 低成本全链初筛，双边价格/数量、成交额、OI |
| 本地订单簿 | `wss://stream.bybit.com/v5/public/option`，`orderbook.25.{symbol}` 为 20ms，`orderbook.100.{symbol}` 为 100ms | 新数据保存前 10 档，选 25 档频道即可并保留更深缓冲 |
| 期权 ticker WS | `tickers.{symbol}`，100ms，snapshot | 双边价量、bid/ask IV、mark IV、指数、underlying、OI、四项 Greeks |
| REST 盘口 | `GET /v5/market/orderbook?category=option&symbol=...&limit=25`，最多 25 档 | 诊断/抽查与恢复核对；实时状态仍以 WS snapshot/delta 为准 |

来源：[合约目录](https://bybit-exchange.github.io/docs/v5/market/instrument)、[REST ticker](https://bybit-exchange.github.io/docs/v5/market/tickers)、[WS 盘口](https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook)、[WS ticker](https://bybit-exchange.github.io/docs/v5/websocket/public/ticker)、[REST 盘口](https://bybit-exchange.github.io/docs/v5/market/orderbook)。

目录提供 `optionsType`、`baseCoin`、`quoteCoin`、`settleCoin`、`deliveryTime`、`deliveryFeeRate`、`priceFilter.tickSize`、`lotSizeFilter.qtyStep`。执行价需要严格解析交易所 symbol 并校验格式；本次 symbol 含 `-USDT` 后缀，例如 `BTC-21SEP26-80000-P-USDT`，不要沿用只支持四段名称的旧解析器。

订单簿先收到 snapshot，后续为 delta；新 snapshot 必须覆盖状态，数量 0 删除价位，`u=1` 为服务重启复位。保留 `u`、`seq`、`ts`、`cts`。`seq` 是跨深度比较生成先后的字段，不是相邻消息必须 +1 的计数器。本次观测的单通道 `u` 连续递增，`seq` 跳跃；上线前仍应把期权通道连续性约定写入适配器测试，对断连/重复/倒序/疑似断档先置无效并重订 snapshot。普通盘口不包含 RPI 订单。[WS 盘口规则](https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook)

连接约束：期权单连接最多 2000 args，并受 args 总长度 21000 字符约束；建议 20 秒 ping。HTTP 默认每 IP 600 次/5 秒，WS 5 分钟建连不超过 500 次，期权市场每 IP 行情连接上限 1000。生产配置应留余量，分片订阅并限制重连风暴。[连接说明](https://bybit-exchange.github.io/docs/v5/ws/connect)、[限流规则](https://bybit-exchange.github.io/docs/v5/rate-limit)

Bybit 官方产品页（更新 2026-04-13）说明期权为 USDT 保证金/结算、欧式现金结算、到期自动行权，结算价使用到期前 30 分钟平均指数价。2026-08-21 FAQ 已列 BTC、ETH、SOL、MNT、XRP、DOGE、XAUT、HYPE；实际可用合约仍应从 API 发现。BTC 最小数量示例为 0.01 BTC，ETH 为 0.1 ETH；不要把它们与其他交易所“合约张数”直接相等。[产品介绍](https://www.bybit.com/en/help-center/article/Introduction-to-Bybit-Options)、[期权 FAQ](https://www.bybit.com/en/help-center/article/FAQ-Options-Trading)

## Binance 官方接口

| 数据 | 接口 / 关键约定 | 采集用途 |
| --- | --- | --- |
| 合约目录 | `GET /eapi/v1/exchangeInfo` | `expiryDate`、`side`、`strikePrice`、`underlying`、`unit`、tick/step、状态、`contractType` / `underlyingType`、运行时限流 |
| REST 快照 | `GET /eapi/v1/depth?symbol=...&limit=1000` | `lastUpdateId`、`T`、bids / asks，初始化 diff book |
| diff 深度 | `/public/stream`，`{symbol}@depth@100ms` 或 `@500ms` | `U/u/pu` 更新范围与前驱校验，本地维护后截前 10 档 |
| BBO | `/public/stream`，`{symbol}@bookTicker`，实时变更 | 全链/候选的低带宽双边报价与数量 |
| mark / Greeks | `/market/stream`，`{underlying}@optionMarkPrice`，1 秒 | 全标的期权的 mark、IV、Greeks、指数及双边价量；筛选后回到深度核验 |
| 指数/新合约/OI | `/market/stream`：`!index@arr` 每秒；`!optionSymbol` 50ms；`{underlying}@openInterest@{expirationDate}` 60 秒 | 元数据生命周期及辅助流动性 |
| 成交 | `/public/stream`，`{symbol}@optionTrade`，50ms；symbol 可是单合约或 underlying | 成交证据；区分 `X=MARKET` 与 `BLOCK`，大宗成交不能当订单簿可得流动性 |

来源：[REST market data](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-options/api/rest-api/market-data)、[public WS](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-options/api/ws-streams/public)、[market WS](https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-options/api/ws-streams/market)。

官方本地簿过程：先缓存 WS，再拿 REST snapshot；丢弃 `u < lastUpdateId` 的事件，首个处理事件覆盖该 ID，随后每个事件必须满足 `pu == previous_u`，否则重新初始化。更新给的是价位最终绝对数量，0 删除。当前 REST 字段是 `lastUpdateId`，不能套旧示例中的 `u`。[本地盘口维护（页面更新 2026-09-18）](https://developers.binance.com/en/docs/products/derivatives-trading-options/websocket-market-streams/How-to-manage-a-local-order-book-correctly)

WS 单连接最多 200 streams，连接有效期 24 小时，客户端到服务器消息限 10/s；symbols 小写。不要套用现货的 1024 streams 配置。官方页面部分示例仍混有旧 ticker 名称，具体 topic 以新版 API Reference 加现场验证为准。[连接说明（页面更新 2026-09-18）](https://developers.binance.com/en/docs/products/derivatives-trading-options/websocket-market-streams/Connect)

2026-01-05 合约规格说明：欧式、USDT 报价和结算、到期自动行权；BTC/ETH/BNB/SOL 每张单位 1，XRP 为 100，DOGE 为 1000。普通可卖空标的为 BTC/ETH，额外标的可按参与者权限开放。费用及保证金条款会变，必须保存规则来源版本，且跨所结算指数不可视为天然相同。[官方合约规格 PDF](https://bin.bnbstatic.com/static/cms/cg08ou2ak0tn7mcplvfg/file/443bcc67fde7274898ff8e3f7af23c7d89e654c5e609d250772e0ddaa96409be.pdf)

## 2026-09-20 无认证现场观测

以下为研究时的短时探测，未部署采集器、未使用账户或凭据。数值文本按 Decimal 读取，未把价格/金额转为 float。初次受沙箱 DNS 限制，允许网络访问后请求成功。

### 合约目录

Bybit `/v5/market/instruments-info?category=option&baseCoin=All&limit=1000` 跟完 5 页，14:13:39—14:13:56 UTC 共返回 4128 条，均为 `Trading`、`settleCoin=USDT`。这是跨页观测，不能声称是同一瞬间的原子快照；正式 collector 需按 symbol / symbolId 去重并做批次校验。

| 标的 | 返回行数 |
| --- | ---: |
| BTC | 782 |
| ETH | 654 |
| SOL | 378 |
| XRP | 304 |
| DOGE | 302 |
| MNT | 332 |
| HYPE | 384 |
| XAUT | 392 |
| NVDA | 246 |
| SPCX | 354 |

Binance `/eapi/v1/exchangeInfo` 在 14:13:42 UTC 返回 1550 个合约：1504 个 `CRYPTO_OPTIONS/TRADING`，46 个 `TRADFI_OPTIONS/CLOSED_MARKET`。全部结算资产为 USDT。

| 标的 | 合约数 | 元数据 nakedSell |
| --- | ---: | --- |
| BTC | 650 | true |
| ETH | 574 | true |
| BNB | 126 | false |
| SOL | 82 | false |
| XRP | 40 | false |
| DOGE | 32 | false |
| XAU | 20 | false |
| XAG | 26 | false |

`optionContracts` 还列 CL/BZ，但本次 `optionSymbols` 没有对应在市合约；不能把支持的产品族当成当前可采集交易流。本次运行时 `REQUEST_WEIGHT` 是 **400/分钟**，而文档示例是 2400，说明必须读实时配置和响应 headers；REST 全链逐合约轮询不合适。

### Bybit 单次双边报价覆盖率

定义为 bid、ask 价格及数量四个字段均严格大于 0，未附加盘口同步、最小数量、最大价差或费用要求。

| 标的 | ticker 行数 | 有双边价量 | 采集时间 UTC |
| --- | ---: | ---: | --- |
| BTC | 783 | 742 | 14:13:39 |
| ETH | 654 | 617 | 14:13:37 |

BTC ticker 行数与目录不同，进一步说明目录和行情异步查询要做身份核对，不能无条件 inner join 后把缺失默认为无机会。交易额较高的示例包括 `BTC-21SEP26-80000-P-USDT`（145 / 150，2.33 / 8.34 BTC）和 `ETH-21SEP26-2550-P-USDT`（5.3 / 5.4，40.4 / 166 ETH）。这只证明当时可取得双边报价，未计算净套利。

### WS 十秒探测

- Binance：订阅 `btc-260925-80000-c@depth@100ms` 和 `@bookTicker`，成功 ACK，接收 8 个 depthUpdate、3 个 bookTicker。首两条 diff 的 `u` / 下一条 `pu` 均为 `39797275275`，字段与新版约定吻合。未做完整 REST 拼接/回放测试。
- Bybit：订阅 `orderbook.100.BTC-21SEP26-80000-P-USDT` 和 ticker，接收 21 个盘口消息、23 个 ticker。初始 snapshot 实际 19 买档、30 卖档；后续前 10 条 `u` 从 271487 连续至 271496，`seq` 跳跃。若实际只有这些档位，不应补造 50 档。
- 本次环境有秒级 REST 和部分 WS 到达延迟；只验证可达和消息形状，不能据此估计生产网络的 100ms 时效。生产验收必须实测 `received_at - source_ts` 分布、连接停顿、断档、两腿时间差以及重连后的无效区间。

### Payload hash 摘要

以下 SHA-256 对实际收到的原始公开响应字节计算。完整原始 body 未长期归档，因此这些摘要用于标识本次观测，不能替代未来生产归档和重放证据。

| 观测 | source time（Unix ms，如有） | SHA-256 |
| --- | --- | --- |
| Bybit 目录 page 1 | 1789913613861 | `b6905c3a085bb0bf8016f212cdf1afdc1a8950b823defa255daf6d82245113eb` |
| Bybit 目录 page 2 | 1789913620920 | `4694a6e84f403902ba53c85e9728ff6987a7f732b045e93ee94ac6b86195dcfd` |
| Bybit 目录 page 3 | 1789913626192 | `ffa7b908cf929b7d630de911f77163eebfadd039de0a3e00bf0fe08acd2b3c43` |
| Bybit 目录 page 4 | 1789913631375 | `bc6a6559500f6ac98d9db14424ca932e8f5014440c2fab01261a5b6656cd3c46` |
| Bybit 目录 page 5 | 1789913635701 | `99cf4ba8f0906d72dbf24a9e420a05ae2b649e12ba7c74bdcfadf92932b1efcf` |
| Bybit BTC ticker | 1789913613879 | `bd18a2d1cbf6daf643c148bcc521ed453e434ffbcedeaf1e378e28b855f8fa47` |
| Bybit ETH ticker | 1789913613532 | `1bcd21cf47c0034ec1284296c201c572c10f0e0bdd48fa2fa76996ff3887cbf0` |
| Binance 目录 | 1789913614632 | `8c3b49120642d7c9664702dfdd3ae47cd456216a724e24bd5f0d6853a2862e7d` |
| Binance 首条 depthUpdate | E=1789913669152 | `6e901ea1001bff22983b0c679f3694406531994d7d25dbc034db62c49333a068` |
| Bybit 初始 WS snapshot | ts=1789913665816 | `549c737c08edb92e038241047260353f47c7d0915d0ded7edc94d711110f1061` |

## 对“更可能抓到合适机会”的采集建议（研究判断）

1. 先把 BTC/ETH 的目录、全链双边报价、同到期合约、结算规则一起采；按“有双边量 + 可建立所需方向 + 足够深度 + 源时间新鲜 + 多腿时间差小”筛选，不能只按交易额或 OI 排序。
2. 订阅池按完整组合闭包扩展：选某个到期日/执行价时同时保留 call、put、相邻执行价和对冲腿。否则容易只采到价差的一侧。
3. 全链 ticker 负责便宜地发现候选，活跃组合持续订深度，不要等出现偏离才开始冷启动深度；Bybit 25 档、Binance diff 本地簿都能服务每秒前 10 档归档。
4. 秒级盘口适合测持续可得性和成本下界；新数据写 `stored_depth=10`，旧 50 档历史按原分钟深度完整回放。若拟研究 100ms 级机会，应另行设计定类型候选证据记录，保留各腿源时间、接收时间、序列、真实 bid/ask/size、费用版本和复核结果，不能改变现有每秒采样语义。
5. 先连续观察不同到期桶至少数个交易日，报告组合有效覆盖率、扣双边 taker 费及滑点后的偏离、最小可成交规模、持续时长、重复发生率。短时探测只证明数据能取，不能据此排序盈利机会。
