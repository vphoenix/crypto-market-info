# ARB-0009：期权公开数据采集设计

日期：2026-09-21。版本：R3。设计审核状态见第 13 节及独立审核记录；尚未实现或部署。

本设计把 [期权研究](../../../research/2026-09-20-options-arbitrage/report.md) 落为可实施的数据链路。以当前 [架构](../../architecture.md)、[存储字典](../../market-data-storage.md)、[10 档兼容规则](../../book-depth-10.md) 和仓库实现为约束。范围是公开数据的采集、标准化、校验、存储、查询和回放；套利计算所需的数据关系在查询端表达。

## 1. 首期范围与明确选择

首期只接 Deribit 生产公共 API，无认证。覆盖下表四个合约族，另采同到期同结算语义的交割期货、对应指数和已存在的相关期权组合。

| 合约族 | 期权名称模式 | 对应期货 | 行权价币种 | 权利金 / 结算币 | 指数 |
| --- | --- | --- | --- | --- | --- |
| BTC 币本位 | `BTC-<expiry>-<K>-C/P` | `BTC-<expiry>` | USD | BTC | `btc_usd` |
| ETH 币本位 | `ETH-<expiry>-<K>-C/P` | `ETH-<expiry>` | USD | ETH | `eth_usd` |
| BTC 线性 | `BTC_USDC-<expiry>-<K>-C/P` | `BTC_USDC-<expiry>` | USDC | USDC | `btc_usdc` |
| ETH 线性 | `ETH_USDC-<expiry>-<K>-C/P` | `ETH_USDC-<expiry>` | USDC | USDC | `eth_usdc` |

名称仅供识别；身份以官方元数据为准。每条期权链全量发现与轻量 BBO 采样，重点合约采 L2。初始重点网格是四族 × 三个不同到期日 × 九个实际行权价 × C/P，约 216 个期权和最多 12 个交割期货；combo 及其必要腿另计。这个规模是配置默认目标，不是保证找到这么多合约。

以下决策在实现时不再隐含选择：

- 新 L2 每秒只落买卖各 10 档，分钟第 0 秒起点，秒 1–59 相对本分钟上一个有效状态保存按价格差量，显式 `stored_depth=10`。
- Deribit L2 使用 `book.<instrument>.100ms` 全深 snapshot/change，通过 `prev_change_id` 校验。内存深度与落库深度分别配置，不能因为只存 10 档就只维护收到的前 10 档。
- 不使用 grouped 10 档频道替代连续 L2：该频道有完整截断视图和 `change_id`，但没有 `prev_change_id`，无法套用完整增量簿的断档证明。此前研究笔记中的这一建议在本设计纠正。[全深频道](https://docs.deribit.com/subscriptions/orderbook/bookinstrument_nameinterval)、[grouped 频道](https://docs.deribit.com/subscriptions/orderbook/bookinstrument_namegroupdepthinterval)
- 新增支持空边、空书、组合零价/负价的定类型衍生品盘口模型和表；保留现有现货/永续盘口的正价、双边及旧 50 档回放语义。
- 首轮试采使用独立数据库与单个 options collector 进程，共享现有 ClickHouse 实例。正式合入同一采集库时仍只能有一个 collector 写入进程。
- OKX、Bybit、Binance、SOL 等后续接入不属于首期实现；数据结构可以表达它们，但不能因字段预留声称已支持其协议。

## 2. 组件与责任

```text
Deribit REST/WS
  ├─ CatalogCollector / RuleCollector → 严格解析、定点数、来源哈希
  │    → typed metadata writer → 已提交目录 / 规则版本
  ├─ quote → QuoteState → 每秒 BBO 采样 → 分钟 60 槽 quote writer
  ├─ full book → DerivativeNormalizer → DerivativeBook
  │    → 每秒前 10 档 + 同步质量 → 分钟快照 / 秒差量 / 分钟质量
  └─ ticker / index / delivery → 独立 typed observations → 批量 writer

已提交目录 + 有效参考报价 → UniversePlanner → 完整组合订阅计划
各表 → 只读查询 / 回放 / 多腿输入检查
```

适配器只解协议、序列、单位和生命周期；不判断“便宜”“盈利”。Planner 使用期限、行权价邻接、配对完整性和容量选择数据范围，不执行策略、创建 combo 或发 RFQ。各源状态独立失效，Greeks 或费表失败不会让正常盘口失效，也不会被旧值伪装成当前成功观测。

拟新增代码边界：

| 路径 | 责任 |
| --- | --- |
| `internal/exchange/deribit/` | 公共 client、共享 gate、JSON-RPC、catalog、book/quote/ticker/index 协议 |
| `internal/options/` | typed spec、目录验证、完整组合选择、版本计划、低频 Runner |
| `internal/orderbook/derivative_*` | 允许空书及指定产品有符号价格的内存书；全深保护 |
| `internal/sampler/derivative_*` | 10 档分钟缓冲、质量冻结、统一秒边界、完成信封 |
| `internal/storage/clickhouse/options_*` | 新表、不可变批次提交、元数据与历史查询 |
| `internal/replay/derivative_*` | 有符号价/空书的精确回放及质量关联 |
| `cmd/options-check/` | 只读目录/覆盖率/容量/回放验收；不创建或修改数据 |

现有 HTTP、限频、transport 可复用；`wsstream` 对每 topic 静默超时的默认行为和固定 targets 不可原样套用。新协议需要 JSON-RPC request ID、完整 ACK 列表、按连接而非盘口变化计的健康状态，以及计划切换后的 target 代次隔离。新模型不能直接使用现有 `BookSnapshot.Validate`、`Book.Snapshot`、writer 的 `unpadded` 和旧 replay 的 `price != 0` 判断。

## 3. 合约发现与版本身份

### 3.1 完整目录

启动串行读取 `get_instruments(currency=BTC/ETH/USDC, kind=option/future/option_combo)` 共九个 scope，USDC 结果筛选 BTC/ETH；再读三个 currency 的 `get_combos`，取得组合腿。`get_combos` 不是 tick/lot 来源，组合规格必须来自 `kind=option_combo` 的目录或 `get_instrument`。保存每个 scope 的 URL、UTC 请求起止时间、来源时间（若提供）、原始响应字节 SHA-256、条数与解析器版本。

目录分两层校验。原始 scope 要求每个请求成功、无 JSON-RPC error、结构可解析、无重复或冲突的源身份；合法空 scope 是空集合，不是失败。九个 instrument scope 和三个 combo scope 全部成功才发布完整批次。分页接口以后接入时必须页完整，游标重复或中断使整批失败。

随后用版本化范围规则将源对象分为 `accepted/out_of_scope/unsupported`：只接受四族 option、对应非 perpetual 的 future，以及全部为已支持期权腿的 option_combo；其他资产、永续和 future combo 明确排除。只有 accepted 对象才要求经济身份、到期、价格/数量编码和完整腿定义通过验证并登记 instrument。目标范围内缺规格、未知腿或未知数量单位记 unsupported，阻止该对象及相关组进入计划，不伪装成范围外；不因此丢弃其他完整有效对象。所有分类均保留源 symbol/ID、所属 scope、证据哈希和原因，unsupported 无需分配可采集 instrument ID。范围识别本身不可靠或响应结构损坏时整批失败。

初次 REST 前先订阅 instrument creation/state，缓冲并按源时间、接收顺序应用目录采集期间的生命周期事件。目录不是原子市场快照：发布前核对缓冲事件与冲突项；未知/冲突身份不进入有效行情。生命周期频道本身无可靠全局连续序列，重连后必须全量重建并重订，不宣称对漏掉事件有历史回补。

启动后使用生命周期流；每 30 分钟低频完整对账作为校验，重连后立即排队对账。目录结果一次遗漏不直接等同下架：对已有在市成员先标 `metadata_uncertain` 并定向重查；明确 terminal state 或复核后的当前目录缺失才终止该身份。新旧目录未完整核实期间，不发布半批新 revision。

### 3.2 instrument 与规格

`instrument` 保留全库统一 `UInt32` ID，拟增加 `market_type=option/option_combo` 的代码校验，期权允许 `expiry_time`；combo 的 `expiry_time` 留空，其各腿到期从子表查询。Deribit 交割仍是 `delivery`。

`venue_contract_version` 拟为 `deribit:<native_id>:<creation_ms>:<economic_definition_hash>`；定义摘要覆盖 payoff 类型、C/P、行权价及币种、到期、index、结算语义版本、native quantity 单位、合约乘数和固定存储单位，combo 还覆盖完整有符号腿结构。native ID 与创建时间缺失则拒绝登记。metadata 时间或 JSON 字段顺序变动不生成新身份。

费用、OI、市场状态、下单 tick/min amount 和报价变动不改变 instrument。改变 payoff/index/结算语义或存储量纲必须分配新 ID；未来生效公告单独保存，届时才切换。源端没有声明生效时间时记录 `effective_time_basis=first_observed`，不反推到历史日期。动态交易规则独立版本化；一分钟内变化用每秒规则引用表达，不修改不可变经济规格。

现有 registry 是全库扫描再分配 ID；必须先让 registry 能读新类型。旧二进制会拒绝新类型，因此不能在已有 option 行的库直接回退到旧二进制；见第 12 节。登记/spec/member 写入的孤立行，只有被已提交 catalog 引用后才成为采集可用定义。

## 4. 精确标准化

### 4.1 存储单位与交易规则分开

首期 BTC/ETH 四族及对应期货选固定 `normalization_version=deribit-jsonrpc-v1`：价格和 native amount 的存储单位均为十进制 `0.00000001`。在 `instrument.price_tick_size`、`quantity_step_size` 中保存这个编码单位，来源实际下单 tick/min amount 在 `derivative_trading_rule` 独立保存。不能根据价格跨越分段 tick 阈值改变历史整数的含义。

```text
price_tick = exact(price / storage_price_unit)   → Int64
qty_lot    = exact(native_amount / storage_qty_unit) → UInt64
```

必须整除且不溢出；拒绝舍入、NaN/Infinity、重复身份/价格、负数量及非法 action。JSON number 由原始字符直接解析 Decimal，禁止先经过 float64。Decimal 字段采用 `Decimal(38,18)`，精度不够则显式报错。若将来来源精度细于存储单位，先使旧目标无效，创建可表示的新版本，再重取快照；不截断源数字。

下单 tick 与数量最小值不能直接作为行情编码拒绝条件：规则变更前的存量挂单可能仍在旧细价位。只要固定编码能精确表示，就保存真实状态，同时附上当时规则版本；“是否允许新下单”由查询使用当前交易规则判断。该区分也适用于 min amount 与遗留小数量。

### 4.2 amount 单位

| 产品 | JSON-RPC native amount | 按基础币比较时 |
| --- | --- | --- |
| BTC/ETH 期权 | 对应基础币 | 原始 amount 已是基础币，不再乘 contract_size |
| BTC/ETH 线性交割 | 对应基础币 | 同上 |
| 反向交割 | USD 名义金额 | 保留 USD 面值；按用途及相关价格换算，不套线性乘数公式 |
| 期权 combo | 以组合官方规格与腿比例共同确定 | 每腿 native amount = combo native amount × signed ratio × 明示单位换算系数 |

`contract_size` 表示 native amount 与合约张数之间的换算，`min_trade_amount` 是来源的交易最小量/增量，二者不能互换。组合只接受同基础币、同报价/结算币且每腿数量换算已通过真实 fixture 核验的结构；未知组合单位标为不支持，仍保留完整目录，不猜测其容量。[合约字段](https://docs.deribit.com/api-reference/market-data/public-get_instruments)、[组合比例](https://docs.deribit.com/api-reference/combo-books/public-get_combo_details)

币本位期权 `strike_currency=USD`、`quote/premium/settlement=BTC或ETH`；线性为 USDC。`btc_usd` 与 `btc_usdc` 不是同一个指数。归一化只统一表达，不把 USD、USDC、BTC 经济身份合并。

本链路所有 instrument 的 `contract_multiplier` 固定为 1，表示存储数量已经是 native amount；官方 `contract_size` 单独存在经济规格中。spec 显式保存 `source_instrument_type=linear/reversed` 及标准 `payoff_type=linear/inverse`，对 option 和 future 均适用；另用 `native_amount_kind/currency` 指定量纲。统一查询按该量纲分派：base 可直接比较基础币，USD 和 combo 必须经过专项换算，禁止调用假定所有数量均为基础币的旧通用换算公式。

固定验算样例来自本次 REST fixture：BTC option 的 amount=0.1 即 0.1 BTC；BTC_USDC future 的 contract_size=0.0001，amount=0.01 即 0.01 BTC、100 张，不能变成 0.000001 BTC；BTC inverse future 的 contract_size=10，amount=100 即 100 USD、10 张，不能当成 100 BTC。

## 5. 采集集合及切换

### 5.1 全链与重点网格

1. `quote` 集合包含四族全部在市期权、所有与这些期权匹配的在市交割期货，以及接受的相关 combo。2026-09-20 样本四族共 3,078 个期权，只作容量起点。
2. 每族先枚举剩余 2–45 天的实际到期日，优先最近周、次周、近月；日期重合去重，按剩余期限升序补足三个，不足则减量。保存选择的源目录及准确 UTC 到期时间，不从本地时区推算合约身份。
3. 每到期必须有相同基础币、结算币、payoff/index/结算窗口版本的交割期货。它的最新正双边 BBO、两侧数量和来源时间可作选链参考；超过 30 秒未确认仅用于保持已有网格，不用于新迁移。没有可用参考时该组标 `selection_reference_missing`，不拿 perpetual 或交易所 IV underlying 估值顶替。
4. 用 `(future_bid+future_ask)/2` 的 Decimal 参考价选最近行权价，距离相同选较低 K；再向两侧各取四个实际存在且 C/P 均在市的 K。行权价不要求等距。保存参考 instrument、价格、源/接收时间和规则版本以复现选择。
5. 同时加入该组两侧所有期权与对应 future；按身份去重。选择期间缺一腿的整组不提交，不把单腿当完整配对。
6. 相关原生 combo 通过 `get_combos` 单独发现；要求至少一腿与网格相交、全部腿属于四族且规格可验证。加入 combo、全部组成腿及每个到期需要的 future。calendar 可以入数据集，但不能标成同到期静态套利。

默认重点期权上限 320，交割上限 64，combo 上限 64，L2 总上限 448；全链 quote 上限 8,192。网格为必需组；额外 combo 扩展按已有覆盖程度优先、再按稳定 combo ID 排序，整组接纳或整组记为 `capacity_excluded`，不能拆腿以凑上限。必需组超限则该计划失败，不静默裁剪。全链超过上限需要显式更改 scope/容量配置后发布新计划。

### 5.2 首次引导与预热

启动先完成 catalog，再生成仅包含全链 quote 与四个 index 的 discovery revision（book/analytics 成员数为 0），提交成员和 revision 后从下一完整分钟开始记录，包括尚未收到消息的明确缺失槽。先订参考 future quote，再按预算分批订全链 quote；订阅计划与实际就绪程度分别记录，ACK 未到也不能省略计划成员的缺失记录。此阶段不依赖 ATM 参考或已存在 L2 universe。

选择器用新的 future quote 消息，或受限 [`get_order_book(depth=1)`](https://docs.deribit.com/api-reference/market-data/public-get_order_book) 定向完整 BBO 核对获取参考；后者只用于选链，保存请求窗口、来源时间、采集时间与 payload hash，绝不接入连续 L2。参考年龄以实际合约级消息/核对时间计，同时校验来源时间；连接心跳不能续期。参考超过 30 秒时优先在共享 REST gate 下逐个核对所需 future，再评估该组；仍无数据则 pending，discovery 持续采集。未来实际迁移用同一方法，避免先收到的静态报价永久阻止选链。

候选完整组进入 prewarm 后预订 book/analytics，等待 ACK、有效 snapshot 和已提交规格；预热占连接、topic、全深状态、队列及新旧重叠预算，但尚不属于当前分钟承诺的 book 成员，也不落候选 book 历史。首次预热失败保留 discovery，而不是等待不存在的旧 L2 计划。

### 5.3 分钟边界提交

Planner 每五分钟评估一次，生命周期变化可提前触发。ATM 中心连续两次偏移至少两个实际行权价才移动；正常成员至少驻留 15 分钟，terminal/定义变更立即标无效，不受驻留限制。首次网格不要求两次历史偏移评估。预热完整组后选择至少提前 5 秒的未来 UTC 分钟边界 T，先持久化成员与不可变 revision，且必须在 T 前收到写入成功确认，才能在 T 激活。

超时未确认则不激活，本分钟继续旧 revision；即使事后发现候选记录已落库，也只表示已准备的计划，实际生效以分钟 commit 引用为准。下次重试保持该写入内容，并为更晚的激活边界另建 revision。已确认计划在 T 前意外失去行情就绪状态，仍按已安排的边界切换成员、把相应质量标无效，不在边界偷偷回滚或修改计划身份。

当新组没准备好时保持旧计划并注明 `pending`；不能把它当新的完整计划。到期/下架使旧组不能交易时，该组各成员保留原计划身份和无效原因直到边界切出。一个分钟只引用一个 universe revision，新合约不会在分钟中段加入本分钟可回放集合。退出目标在旧分钟封包后退订；相同合约继续使用其连续书。

目录是“发现了什么”，universe 是“本分钟计划采什么”，质量是“实际采到了什么”，三者不能互相推断。重连前后连接代次不同，旧队列里的 ACK/行情必须按代次丢弃。

## 6. WebSocket 状态与秒级采样

### 6.1 频道语义

| 数据 | 首期频道 | 本地状态规则 |
| --- | --- | --- |
| 完整 L2 | `book.<instrument>.100ms` | 首 snapshot 全量；change 有前驱链；按价格绝对数量 new/change/delete |
| 全链 BBO | `quote.<instrument>` | 每条为完整 BBO，显式 null 表示无该侧；没有连续序列，只是发现层观测 |
| 辅助估值 | `ticker.<instrument>.agg2`，重点期权 | 完整 ticker 按字段存在性解析，每 5 秒保存；不依赖未验证的嵌套增量合并 |
| 指数 | `deribit_price_index.<index>` | 四个指数独立状态，每秒采样 |
| 预估交割 | `estimated_expiration_price.<index>` | 独立于即时指数/最终交割，缺少来源即缺失 |
| 生命周期 | `instrument.creation/state` | 事件触发目录核对，不能代替断线后的全量对账 |

`incremental_ticker.<instrument>` 有 snapshot/change 且最多每秒一次，但无连续序列；只有完成缺字段保留、null 清除、嵌套 Greeks patch 与断线恢复 fixture 后才允许替换完整 ticker。mark/IV、last、Greeks 均不属于可成交盘口。[ticker 增量定义](https://docs.deribit.com/subscriptions/market-data/incremental_tickerinstrument_name)、[BBO 定义](https://docs.deribit.com/subscriptions/market-data/quoteinstrument_name)

### 6.2 L2 恢复

```text
DISCONNECTED → SUBSCRIBING → AWAITING_SNAPSHOT → LIVE
任何序列/解析/队列错误 → INVALID → 退订确认 / 新代次 → AWAITING_SNAPSHOT
```

ACK 只确认订阅，不等于已有有效书。初始 snapshot 严格校验完整消息后原子替换；其 `change_id` 成为基准。change 仅在 `prev_change_id == last_change_id` 时应用，不要求 `change_id=last+1`。完整重复消息（同代次、同 ID、同摘要）可忽略；同 ID 内容冲突、倒序或前驱不匹配均失效。未建基准时 change 不参与采样。异常期间不把 REST 与 WS 异步拼成一条有效链；重订获取 WS snapshot。

full book 在内存保留全部价位，单书最高 20,000 档/侧、全书估算内存上限由预算控制；达到上限必须使该书无效并告警，不能静默截尾再把后续残缺书当完整。源端有限深频道将来接入时另定义边界补入协议，不能复用 full book 的“已知全深”断言。

单腿 option/future 实际挂单价必须正，combo 允许任意有符号 Int64 价格。每侧严格有序、无重复价，实际 qty>0；只在双方均非空时检查 `best_bid < best_ask`。一侧或两侧空数组可以是有效完整书；盘口有效、市场是否开放、该侧是否有流动性是三个不同状态。

### 6.3 健康与静默

连接配置 10 秒应用心跳，处理 `test_request` 并及时 `public/test`；30 秒没有连接级任何有效应答/流量则断开，全部关联目标先失效。单个冷门期权长时间没有变化不会仅因年龄而失效；保存最后变更、最后 snapshot、最近连接确认的不同时间和 `confirmation_kind`，心跳不更新“该合约最新报价”的源时间。

只有在 ACK 已成功、snapshot 已建立、处理队列未丢消息且代次连续时才持有静态 L2 状态。生命周期重连或订阅状态有疑问时重取 snapshot。BBO 没有序列证明：旧值可在同代次标为 `held_unsequenced`，不能借连接心跳升级成序列验证过的盘口。

JSON-RPC 10028、HTTP 429/Retry-After 触发共享 gate 冷却；失败重连采用有上限的指数退避与抖动，不能靠进程立即重启绕过 gate。公开额度按 IP，不能假定账户额度是本任务独占。[限频](https://docs.deribit.com/articles/rate-limits)

### 6.4 秒边界与分钟锚点

采样以 UTC 整秒 `T` 为截止点。`received_at` 明确定义为完整 WS 消息进入本进程 ingress sequencer 的接纳时间，不是内核收包时间；可另记 `read_completed_at` 作诊断，但不能混作采样截止。只取接纳时间不晚于 `T` 的最后完整版本，不因源时间较早就把 `T` 后才接纳的行情倒填。

每连接一个 ingress 排序所有者，接纳时间/单调时钟、连续接收序号的分配与非阻塞 FIFO 入队必须在同一短临界区内完成。定时器只能通过同一排序所有者追加 `Barrier(T,epoch,last_received_seq)`，不得直接冻结书；接纳时间越过 T 的新消息入队前，排序所有者先补入 T 屏障。因此同连接所有 `received_at<=T` 消息在屏障前，较晚消息在屏障后。临界区内禁止解码/阻塞发送；队列满必须记录丢失并使该连接所有受影响书无效。被抢占的“已记时间、未入队”接纳仍持有排序权，屏障只能等待，不能越过。静默连接也由定时器走同一路径推进水位。

工作线程严格按 FIFO 应用消息，只有屏障水位之前的序号全处理且无缺失，才冻结该连接各书与质量；随后才处理 T 后更新。每连接分别完成截止，某连接积压不把其他连接一起伪装成缺失或成功。内存保留最近两个秒边界的已冻结前10档版本与质量，不落秒内历史。屏障在 `T+250ms` 内未完成则该连接相关秒标 `sampling_lag`，不读取更晚当前书补齐；迟到的冻结结果不得改写已封定的无效槽。真实 `captured_at` 只表示冻结处理完成时间，不刷新来源时间。

质量中的规则、身份及市场状态也必须使用截止 T 的版本，不能在 captured_at 读取其他线程的共享latest。规则/catalog写入确认后，经单独control sequencer发布不可变事件；该事件的运行时 `published_at` 与接收序号一同分配，保存最近边界所需版本。T秒只能选择 `published_at<=T` 且当时已生效的规则；known_from早于T但T后才发布的规则也不能倒填。published_at是内存发布事件时间，选中规则的该时间随秒质量归档，不要求事先写入规则事实行，避免提交时间循环。

生命周期WS连接使用自己的接收水位，先完成T屏障，再以 `received_at<=T` 的事件冻结市场状态；catalog对该状态的校正则按其control published_at截止。采样器合并book、control、lifecycle三个T版本及各自epoch，仅在所依赖水位于T+250ms前完成时形成完整质量。缺失规则引用保留NULL；生命周期/身份状态未确认时标metadata_uncertain、market_state_known=0并使相应replay秒无效。完全不依赖该连接的其他数据流仍可独立有效，绝不取T后的状态填补。验收必须模拟规则或关闭市场事件在T之后发布、而book屏障在T之后才完成的交错。

时间比较用原始高精度 UTC 与单调映射，落库微秒精度造成同值时保留接收序号判定屏障前后。时钟回退/UTC与单调时钟漂移超过 250ms 时标 `clock_discontinuity`，清理当前分钟并从下一完整边界重建；不会对已提交历史回写时间。

第 0 秒必须具有有效、完整的已知书，包括已知双边空书。没有锚点时本分钟没有 L2 快照/差量，即使后续恢复也等下个分钟；质量仍保存 `anchor_missing` 与后续 `stream_valid`，BBO/辅助数据可以独立存在。不能把“第0秒未知”伪装成“第0秒已知空书”。

有效秒先截取实际前10档，再与本分钟上一个有效前10档比较；qty=0 表示该价格退出保存状态。深于10档的变化不写差量。恢复后的首个有效秒允许跨连接代次，但必须已有全新 snapshot，差量与旧的最后有效存储状态比较以正确删除陈旧价格。空书可作为合法状态，转换为空书时删除全部旧价格。

## 7. 拟议数据字典

本节所有表均为拟新增，不代表当前 schema 已部署。金额/比率用 `Decimal(38,18)`，盘口价量只用 Int64/UInt64。时间明确 UTC；来源 UTC 毫秒用 `DateTime64(3,'UTC')`，本地接收/采样完成用 `DateTime64(6,'UTC')`。SHA-256 用 `FixedString(64)`。以下“键”为逻辑去重/排序键，ClickHouse 不提供关系型外键强制约束，由 writer/查询校验。

### 7.1 规格与集合表

| 表 / 键 | 必要字段与约束 |
| --- | --- |
| `derivative_contract_spec` / `instrument_id` | `native_instrument_id UInt64`、`creation_time`、`definition_hash`、`normalization_version`、`source_instrument_type Enum(linear,reversed)`、`payoff_type Enum(linear,inverse)`、`native_amount_kind Enum(base,usd,combo)`、`native_amount_currency`、`source_contract_size Decimal`、`index_id String`、`settlement_semantics_id`、`combo_leg_count UInt8`、`combo_structure_hash Nullable(FixedString(64))`、`definition_evidence_hash`；固定存储单位在 instrument |
| `derivative_trading_rule` / `trading_rule_id FixedString(64)` | `instrument_id`、`source_tick_size Decimal`、`tick_bands Array(Tuple(above_price Decimal,tick_size Decimal))`、`min_trade_amount/amount_step Decimal`、`source_time Nullable`、`observed_at`、`effective_from Nullable`、`effective_time_basis Enum(published,first_observed,unknown)`、`known_from`、`source_url`、`payload_hash`、`parser_version`、`batch_id`；不可变规则观测，ID为准备时确定的内容/证据摘要 |
| `option_contract_spec` / `instrument_id` | `option_type Enum(call,put)`、`strike Decimal`、`strike_currency String`、`exercise_style Enum(european)`、`payoff_type Enum(linear,inverse)`、`premium_currency`；到期与结算币在 instrument，重复来源必须相符 |
| `option_combo_leg` / `(combo_instrument_id,leg_no UInt8)` | `leg_instrument_id UInt32`、`signed_ratio Int32 != 0`、`leg_amount_per_combo_amount Decimal`；组合完整腿数和结构摘要在对应 derivative spec，腿 ID 不准指向未知/组合或跨不支持单位 |
| `derivative_catalog_member` / `(catalog_id,source_kind,source_symbol)` | `native_instrument_id Nullable(UInt64)`、`instrument_id Nullable(UInt32)`、`source_scope_ids Array(String)`、当前源状态与源时间 Nullable、首次观测时间、`classification Enum(accepted,out_of_scope,unsupported)`、原因 Enum、证据 hash、适用的 spec/trading rule ID；只有 accepted 必须具备 instrument/spec/rule，不以缺少 instrument 丢弃排除证据 |
| `derivative_catalog_revision` / `catalog_id UUID` | 请求窗口、九 instrument scope 与三 combo scope 的原始/分类计数、各 scope 原始 payload hash、`member_count`、`content_hash`、`parser_version`、范围规则版本、提交时间；最后写的完整批次标记 |
| `options_universe_member` / `(revision_id,instrument_id)` | `roles UInt8`（quote/book/analytics位标记）、`group_ids Array(FixedString(64))`、`selection_reason Enum`；只写本计划实际选择的成员 |
| `options_universe_revision` / `revision_id UUID` | `run_id UUID`、`catalog_id`、拟激活 `effective_minute`、`plan_hash`、`selector_version`、期日/strike数/预算等定类型配置、`index_ids Array(String)`、各组参考期货价量、来源种类、源/接收时间及 REST 证据的 typed tuple 数组、成员计数、提交时间；实际使用以 minute commit 为准 |

所有经济 spec 记录不可变，同 key 重试只能相同内容；`definition_evidence_hash` 只指首次登记定义的证据，后续目录响应 hash 在 catalog 保存，不覆盖该字段。交易规则调整保持 instrument ID，新增 trading rule；经济或编码含义改变才建新 instrument。`settlement_semantics_id` 标识经济定义，结算规则的再次核验或页面哈希改变只新增观测，不自动改变经济身份。

catalog/member 先写后标记；accepted 的 spec/rule 缺失、成员数不符或摘要不符则 revision 不可见。catalog/plan ID 在写入前生成并在重试中保持；进程重启产生新 run，不能把写入时刻当规则生效时刻。`known_from` 表示响应取得且严格解析完成、形成不可变观测的时间，独立于源端生效时间和数据库写入时间。规则先写对应独立批次 commit，再向采样器发布；采样每秒冻结当时已发布、已生效的 `trading_rule_id`，分钟内变更也逐秒引用，迟发现的公告不改写过去引用。规则 as-known 查询重建证据当时是否已知；精确查询运行时实际使用的规则则读取该秒引用。unknown 不被当作已知适用规则。

### 7.2 专项 10 档盘口

因为旧盘口把空边视为无效、零价格用作空档，新增表的有效性和编码语义不同。Deribit 的期权、对冲交割、combo 全部使用这条链路，避免同一次多腿回放混用两种有效性定义。

`derivative_book_minute`，查询键 `(instrument_id,minute_time)`，物理去重键附加 `batch_id`：

| 字段 | 类型 / 不变量 |
| --- | --- |
| `id`、`instrument_id`、`minute_time` | 与现有 minute ID 公式一致，ID在同库 instrument 命名空间有效 |
| `batch_id`、`universe_revision` | 提交批次摘要 / UUID，必须与该分钟 commit 对应 |
| `encoding_version`、`stored_depth` | UInt8；首期分别固定为 1、10 |
| `valid_bitmap` | UInt64低60位；第0位必须1；表示可回放状态，包括已知空书 |
| `delta_bitmap` | UInt64低60位；第0位为0；第s位为1当且仅当该有效秒应有差量行，用于区分无变化与丢失行 |
| `bid_prices` / `ask_prices` | Array(Int64)，各0–10项，允许combo零/负价，严格有序 |
| `bid_qtys` / `ask_qtys` | Array(UInt64)，与价格数组等长，实际元素全>0；空数组表达无该侧 |
| `row_hash` | 规范化定类型行内容摘要；用于同键冲突检测 |

不使用价格0作填充。`bid_present/ask_present` 从实际数组长度推导，不能凭价格正负判断。单腿与combo的价域由 instrument 类型校验。

`derivative_book_second_delta`：逻辑键 `(minute_id,second_offset,batch_id)`；`second_offset UInt8` 仅1–59；买/卖 `change_prices Array(Int64)` 与 `change_qtys Array(UInt64)`。按价格最终数量编码，零数量删除任何价格，包括价格0；每侧同秒价格唯一。差量仅属于有效秒，回放后每侧最多10档。没有变化不写行。

### 7.3 每秒质量按分钟打包

`derivative_book_quality_minute`：键 `(instrument_id,minute_time,batch_id)`，本分钟所有 book 计划成员都有记录，即使没有锚点。

- `sampled_bitmap`、`stream_valid_bitmap`、`replay_valid_bitmap`、`bid_present_bitmap`、`ask_present_bitmap`、`market_state_known_bitmap`、`market_open_bitmap` 为 UInt64低60位。`replay_valid = stream_valid AND anchor_exists` 并受截止/采样校验约束；与书的 `valid_bitmap` 一致。market_open仅在market_state_known=1时有确定含义。
- 下列字段均为长度60的定类型数组：`source_times Nullable(DateTime64)`、`received_times Nullable(DateTime64)`、`captured_times Nullable(DateTime64)`、`connection_epochs Nullable(UUID)`、`change_ids Nullable(UInt64)`、`last_snapshot_times Nullable(DateTime64)`、`connection_confirmed_times Nullable(DateTime64)`、`trading_rule_ids Nullable(FixedString(64))`、`rule_published_times Nullable(DateTime64)`、`market_state_received_or_published_times Nullable(DateTime64)`、`market_state_time_basis Enum(ws_received,catalog_published,unknown)`、`confirmation_kinds Enum`、`invalid_reasons Enum`、`bid_level_counts/ask_level_counts UInt8`。
- 原因枚举至少包括：`none/not_ready/disconnected/sequence_gap/parse_error/metadata_uncertain/market_closed/sampling_lag/anchor_missing/resource_limit/clock_discontinuity`。未知时间为NULL，不能用采样时间替代源时间。
- `stream_valid=1` 与 `market_open=0` 可以共存；分析是否可交易还必须检查 market_open、各腿所需侧和数量。无锚点时后续 stream_valid 可为1，但 replay_valid 始终0。

采用每分钟60槽而非每秒独立质量行，减少小行开销。全链 quote 同样打包，仍须实际测量压缩空间，不假定空槽零成本。

### 7.4 BBO、辅助数据与来源规则

| 表 / 键 | 主要字段 | 采样及缺失语义 |
| --- | --- | --- |
| `option_quote_minute` / `(instrument_id,minute_time,batch_id)` | 60槽的 bid/ask ticks、qty lots、源/接收时间（均Nullable）；side_present位图；`status Array(Enum(observed,held_unsequenced,missing,disconnected,invalid))`、reason、connection epoch、trading_rule_id | 每个quote成员必写一分钟60槽；取截止点前完整BBO，同代次持有须保留真实源时间；明确无侧与未知报价分开。无序列，不能设置 book_valid |
| `option_analytics_observation` / `(instrument_id,sample_time,batch_id)` | mark_price、bid/ask/mark IV、delta/gamma/vega/theta/rho、OI、24h volume、underlying_price、模型interest_rate 均为 Nullable Decimal；`underlying_index Nullable(String)`、`source_unit_version String`、OI/volume单位 Enum、来源字段/Greeks口径版本；Nullable源/接收时间、epoch、status/reason、available_fields位图 | 每个analytics成员在分钟秒0/5/…/55必写，共12行，未收到也写missing；完整ticker缺字段置NULL，不向前拼接；模型利率不是实际融资率 |
| `derivative_index_observation` / `(index_id,sample_time,batch_id)` | index_price、estimated_delivery_price 各为 Nullable Decimal；每种价格各自的 Nullable源/接收时间、epoch、status/reason；`settlement_rule_id Nullable` | 每个计划index每秒必写，共60行；两种价格分别判定observed/held等状态，不能认为同源同刻；不包含最终交割价 |
| `derivative_delivery_observation` / `(index_id,delivery_time,observation_id)` | final_price Decimal、source_time Nullable、collected_at、known_from、time_basis、source_url、payload_hash、revision_hash、batch_id | 到期后补查并版本化；日期映射到UTC到期时间须由明确规则决定，不能默认午夜；修正新增观测不覆盖旧值 |
| `derivative_settlement_rule` / `rule_id` | settlement_semantics_id、index身份/组成版本、payoff类型、结算币、自动行权条件、采样窗/周期、到期时刻、线性转期货再结算步骤、fee_netting_kind、生效时间/依据、known_from、来源及哈希、batch_id | 低频、来源变更；未知生效时刻显式标unknown或first_observed |
| `option_fee_rule` / `rule_id` | 产品族/普通公开等级、maker/taker、费率基数、费用币、rate、premium_cap、交割rate/cap、日到期豁免、组合折扣公式枚举、生效/观测时间、known_from、来源及哈希、batch_id | 低频、typed parser明确支持的规则才标verified；API裸费率不能推出cap与交割费 |
| `derivative_margin_rule` / `rule_id` | 产品族、margin_mode、公式枚举、initial/maintenance参数、抵押折扣、抵消适用条件、支持状态、生效/观测时间、known_from、来源及哈希、batch_id | 只表示公开规则，未知公式保留unsupported；不输出真实账户保证金或PM资格 |

quote 的每槽、analytics 的每行、index 的每种价格采用相同状态契约：`observed` 表示上一个计划采样点之后且本截止点之前接纳过新完整消息；`held_unsequenced` 表示同一健康连接代次持有旧消息，数值和原始源/接收时间不刷新；`missing` 表示本代次从未取得；`disconnected` 表示连接已失效；`invalid` 表示解析、截止或其他明确错误。后三者全部市场数值及provenance置NULL，保留原因和诊断代次，不携带上代次旧值。observed/held 可以缺单字段，available_fields逐位说明；收到完整ticker时显式缺项或null均清除该字段，不能继续持有该字段的更早数值。

正常完整 quote 中明确无侧用 side_present=0、该侧price/qty=NULL；缺整条报价也可能全NULL，必须由status区别。index/estimated各有自己的状态及时间，一种缺失不抹掉另一种。所有辅助采样同样遵守 §6.4 接收水位与截止；`AnalyticsAtOrBefore(max_age)` 检查原始接收时间，并在来源时间存在时同时限制其年龄，不以sample_time为新鲜度。没有来源时间的频道显式 `time_basis=received`，不得把接收时间填入source_time；估值或发现报价静默持有不代表序列已验证。

IV 若来源以百分数表示，标准化为比例Decimal（如50→0.5），保留单位版本；Greeks记录来源定义与币种/每合约或每基础币口径，不能混用BS delta和账户净delta。OI/成交量有自己的native单位字段，不能默认等同盘口qty。

每次低频实时采集先取原始响应哈希，再严格解析整批；请求失败不生成新市场观测。规则页面结构变化或正文冲突时写采集失败/unsupported状态，历史verified行仍可查询但不更新其核验时间。支持人工维护的typed规则清单时必须标 `manual_verified`、来源内容哈希和证据；自动任务不能在未解析当前响应时复制旧参数并声称当前已验证。实际融资率暂无已锁定来源时只作为查询传入情景，不填0冒充免费资金。

## 8. 写入、幂等与可见性

### 8.1 完成信封

每分钟结束生成不可变 `OptionsCompletedMinute`，包含本分钟固定 universe、全部 quote 槽、book/quality、analytics/index必需采样行及明确缺失状态。无书锚点也必须封包，使数据缺口可以与进程未工作区分。`batch_id=SHA256(schema_version,run_id,minute_time,universe_revision,canonical_typed_payload)`；按表、逻辑键及数组顺序确定性序列化，排除batch_id、row_hash及commit时间本身，避免循环摘要；各事实行另有row_hash。使用定点/整数，不保存通用JSON大表。

第一条insert前校验整个信封：唯一身份、分钟/采样截止、位图与数组长度、price/qty域、所有差量逐秒精确回放、相关spec与universe已提交。校验失败不写部分数据。按最多100个instrument或8MiB未压缩payload分块，先所有数据表，最后插入 `options_minute_commit`。

`options_minute_commit` 键为 `minute_time`（options专用writer的分钟命名空间），字段包含 `run_id`、`universe_revision`、`batch_id`、各表期望行数、book成员/锚点数、整体摘要、`committed_at`。数据表键都带batch_id；查询必须先取得commit，再精确关联该batch，不能直接挑“最新”数据行。不存在commit的孤立数据对业务查询不可见。即使无锚点，quality及缺失状态落盘后也可提交。

固定期望数由已提交计划推导：quote=Nq行、quality=Nb行、analytics=12×Na行、index=60×Ni行；book快照数等于有锚点的成员数，差量行数及所在秒必须匹配各书delta_bitmap。writer和查询均检查身份集合、唯一键、期望行/槽数及引用。查询遇到commit存在但必需行缺少或摘要冲突，返回 `incomplete_batch`；不能把缺差量行解释成无变化。整批校验结果可按不可变batch摘要缓存，但不能通过跳过完整性校验获得有效状态。

ClickHouse不提供这里所需跨表事务；提交标记只在全部同步insert成功后发布。每次重试复用相同batch、行、时间和哈希，`ReplacingMergeTree`按完整键去重，查询用FINAL或等价argMax。重试期间不重新请求行情。已存在同分钟commit但batch不同视为写入冲突并停止，不覆盖；数据内容相同的重复提交允许。

该协议要求单writer，而不是“先读后写”实现分布式锁。首期仅单宿主机专用unit运行；进程在登记instrument和任何写入前获取按ClickHouse地址/数据库命名的本机排他文件锁，第二进程拒绝启动。正式合入现有单collector也只由该进程持有所有写入任务；多宿主机写入不支持，不能仅靠本机锁声称已处理。

新进程只从启动后的下一个完整分钟开始采样归档，避免进程重启对已提交分钟重复起锚。启动检查最后commit和当前时钟；未越过最后完整分钟不写。未提交孤立块不参与新的批次哈希或回放，单独统计其占用，后续按明确保留政策清理。

队列按实际字节及分钟数双限：最多两个待完成信封、总128MiB，最老待写分钟超过45秒则停止options分支并报警；不丢掉数据后继续假装全覆盖。首期独立库的进程可退出由专用unit退避恢复；合入单collector后应隔离options监督任务，公共数据库不可恢复错误再按全局规则退出。

### 8.2 低频独立批次

交割价与规则Runner不进入每分钟市场提交屏障，单源失败不阻止其他市场数据。采用准备时生成的 observation/batch ID、来源时间、collect时间、payload hash；insert超时重试原批，不用新的“现在”更新身份。一个业务批次跨多行时，行先写、完整批次标记最后写；来源无数据和来源请求失败必须区分。

`derivative_source_batch_commit` 以batch_id为键，记录source_kind、scope、请求窗口、来源/采集时间、payload_hash、各定类型表期望行数及摘要、`result=complete/no_data`、提交时间。trading/settlement/fee/margin/delivery各事实行带batch_id；查询只读已提交批次。`derivative_source_attempt` 独立记录attempt_id、来源、请求起止、`success/no_data/request_error/parse_error/unsupported`、可空payload_hash及错误分类；请求失败无响应可以无hash，成功实时事实不得省略hash。失败尝试不能增加有效市场观测或刷新规则验证时间。

## 9. 查询与回放契约

拟提供以下只读接口；不在本阶段实现套利下单或收益承诺：

```text
ListOptions(catalog_id, family, expiry_range)
CollectionMembers(universe_revision)
ReplayDerivativeBook(instrument_id, UTC_second)
QuoteAt(instrument_id, UTC_second)
AnalyticsAtOrBefore(instrument_id, UTC_time, max_age)
DeliveryHistory(index_id, interval, as_known_at)
RulesAsKnownAt(instrument_id, effective_time, known_at)
LoadLegInputs(instrument_ids, UTC_second)
```

回放先读取分钟commit与匹配batch，再读质量；无锚点返回 `unavailable(anchor_missing)`，无效秒返回真实原因。有效则读第0秒数组与截至目标秒的全部差量，以price为键更新，最后排序并校验最多10档。价格0必须作为真实key，qty=0才是删除。

规则查询先过滤已提交观测，按instrument/产品族与经济规则身份匹配，同时约束生效时间和known_from；相同适用区间的后续更正按known_at可见的观测顺序选取，不把未来才知道的更正提前用于旧回测。无法确定有效区间或存在未解决冲突则返回unknown/conflict。运行时实际采用的交易规则以quote/quality中每秒引用为准，不通过今天最新目录补填。

返回对象区分 `SampleTime`、真实 `SourceTime`、`ReceivedAt`、`CapturedAt`、`StoredDepth`、`State/MarketState`、`Bid/AskPresence` 和 `universe_revision/batch_id`。空书有效返回空数组；不能用 `false` 把空书与缺数据混同。现有 `ReplayBook` 会把SourceTime设成查询秒，新接口不沿用此语义。

`LoadLegInputs` 要求全部腿在该秒有对应身份、可回放质量和必要数据，分别返回源时间/接收延迟及缺失原因；它不承诺源端同时成交，也不自动把不同指数视为可对冲。只用前10档能覆盖的数量报告容量，目标数量更大返回 `insufficient_stored_depth`，不能向外插值。

旧 `order_book_minute` / delta 的读取路径保持按 `stored_depth` 完整恢复，50档先全部恢复后才可截取展示。新专表没有旧50档迁移，不回写旧历史，统一查询可通过 instrument/采集后端路由到正确回放器。

## 10. 配置、限频和资源预算

下列为拟议默认配置，不是交易所保证，也没有在本机启用：

| 配置 | 默认 |
| --- | --- |
| `OPTIONS_ENABLED` | false |
| `OPTIONS_FAMILIES` | BTC inverse、ETH inverse、BTC USDC、ETH USDC |
| `OPTIONS_EXPIRIES_PER_FAMILY` / `OPTIONS_STRIKES_EACH_SIDE` | 3 / 4 |
| `OPTIONS_MIN_DTE` / `OPTIONS_MAX_DTE` | 2 / 45 天；UTC精确到期时间 |
| `OPTIONS_MAX_QUOTES` / `OPTIONS_MAX_BOOKS` | 8192 / 448（含所有附加腿） |
| `OPTIONS_BOOK_INTERVAL` / `OPTIONS_STORED_DEPTH` | 100ms / 10，首期深度不可配置成50 |
| `OPTIONS_PLAN_INTERVAL` / `OPTIONS_MIN_RESIDENCE` | 5分钟 / 15分钟 |
| `OPTIONS_ANALYTICS_INTERVAL` | 5秒 |
| `OPTIONS_QUOTE_TOPICS_PER_CONNECTION` / `OPTIONS_BOOK_TOPICS_PER_CONNECTION` | 256 / 32 |
| `OPTIONS_SUBSCRIBE_BATCH` / `OPTIONS_CONTROL_INTERVAL` | 128 topics / 1秒；全进程共享控制gate |
| `OPTIONS_REST_INTERVAL` | 2秒；含所有metadata/重查/规则/交割请求，共享gate |
| `OPTIONS_MAX_WS_CONNECTIONS` | 64，含lifecycle/ticker/index与新旧计划过渡 |
| `OPTIONS_MAX_LEVELS_PER_SIDE` | 20000，全深超限不截尾伪装完整 |
| `OPTIONS_MAX_STATE_BYTES` / `OPTIONS_MAX_QUEUE_BYTES` | 256MiB / 64MiB，含过渡与snapshot/解码峰值 |
| `OPTIONS_MAX_QUEUED_MINUTES` / `OPTIONS_MAX_MINUTE_BYTES` | 2 / 128MiB |
| `OPTIONS_SAMPLE_GRACE` / `OPTIONS_OLDEST_WRITE_AGE` | 250ms / 45秒 |

控制请求按整批ACK核对全部topic，不能只看RPC成功；10028或429覆盖配置并进入共享冷却。get_instruments特殊持续1次/秒的文档额度也受公共IP限制，默认2秒只是保守本地预算，仍须实测。全链BBO、重点book、辅助ticker分连接，避免quote流量挤压book恢复。

正式运行前使用只读dry-run输出完整订阅计划、来源scope、各产品数量、连接/控制请求数、预计内存及硬上限；强制统计新旧计划并存峰值，不能仅测稳态。未知payload大小先按受限探测测量，无法预算的配置不直接升到全量。

数量级须分开测：3,078条全链BBO每秒一行相当于265,939,200行/日，因此选每分钟60槽；约216个期权并不等于总数据只来自216条。分别报告quote、quality、L2快照/差量、analytics/index和目录规则的压缩bytes/日、行数、CPU/队列峰值、磁盘占用与查询耗时。

试跑阶段先完成2小时限额探测；实测外推7天增量不得超过可用磁盘的20%，预计RSS低于专用unit内存预算70%，sampler p99低于100ms，分钟写入p99低于10秒，才开放7天试采。原始WS文件大小不能代替ClickHouse压缩实测。超过预算先终止升级并报告具体表和流，调整必须生成显式新配置/revision，不静默降采样或拆掉组合腿。

## 11. 验收与测试矩阵

| 组 | 必须通过的情形 |
| --- | --- |
| 合约/目录 | 九instrument+三combo scope完整/缺失/合法空；重复与冲突ID；上市并发；其他资产/future combo排除；未知目标combo保留unsupported而无instrument；源ID复用/经济定义变更新身份；tick/min amount调整仍同身份；同定义不同响应hash不改spec；部分写入不可见 |
| 数值 | JSON科学计数与超长小数直接Decimal；NaN/Inf拒绝；tick/lot非整除及Int64/UInt64溢出；固定存储单位跨分段tick和旧挂单；native USD/base/combo单位及乘数不重复 |
| 盘口状态 | snapshot/change正常；new/change/delete；断档/倒序/重复冲突/重连旧消息；空边/空书与断流；combo价格负/零/正；跨价0删除；超深和队列溢出后失效 |
| 截止时间 | T之后接纳但源时间早于T的更新不能倒填；接纳已记T前时间、尚未入队时抢占；不同连接不同积压；静默连接水位推进；多腿冻结期间新消息；屏障积压/GC暂停/时钟回退；质量与书同一版本；分钟内规则引用变更；known_from在T前而规则发布在T后；其他连接T后生命周期事件不得被晚处理屏障倒填 |
| 分钟回放 | 全60秒；同秒同价多更新最终量；无变化与无效；第10退出第11补入；只有深档变化；断档后新snapshot相对最后有效书；空书起点；第0秒未知整分钟不可回放 |
| 提交/重试 | delta成功quality失败、quality成功minute失败、全部数据成功commit超时、部分chunk失败、重试相同内容、冲突batch阻止覆盖、孤立行不可见、重启不改写旧分钟；commit存在但快照/差量/质量/辅助行缺少返回incomplete；第二writer被锁拒绝 |
| 集合 | 首次discovery无quote/无L2仍可提交；静默future受限刷新后可选择；到期去重/不足3期/不足9K；C/P缺一/无future/参考陈旧；combo全腿扩展；完整组排除；预热与新旧并存预算；revision确认超时不激活；已确认后失效仍按计划标质量；分钟只有一个revision |
| 辅助/低频 | quote无序列不能冒充book；null侧与整条未知；全部未收到仍有必需缺失行；静默持有不刷新时间；完整ticker缺单字段清除；index/estimated独立失效；underlying_index字符串；IV单位；估计/最终交割区分；规则hash/生效及知悉时间/失败陈旧；低频单源失败隔离与固定重试身份 |
| 既有回归 | 新10档、旧50档和混合历史逐秒回放；旧11档补入；legacy模型正价限制不变；现有CEX断档、资金费率、收益严格解析与写入测试；新实现不涉及DEX，保留已有相关回归 |
| 资源实测 | 活跃和低活跃期权各连续至少24小时，分表压缩空间；至少100次分散秒回放，报告p50/p95/p99；全链/过渡/重连峰值；现有服务健康指标无明显退化 |

真实协议验收至少覆盖四族各一组C/P/future及一个既有combo；保存无凭据的短时原始响应、UTC时间和payload hash作为测试证据。带前驱的combo WS、完整ticker缺字段、未知单位等尚未实测项目是实现前验收项，不能由9月20日单腿40秒探测自动视为通过。

## 12. 实施顺序、部署与回退

1. **模型与离线回放**：完成独立signed/empty book、定点/spec、分钟质量与提交标记，按fixture执行全矩阵；同步存储字典。此阶段默认关闭Deribit网络任务。
2. **隔离库小集合**：拟用 `crypto_market_info_options_soak` 与专用systemd unit，先四族各一C/P/future和一个combo，验证ACK、恢复、10档写入与只读回放；实际路径/服务状态只有创建后才写入运行文档。
3. **全链发现＋216左右重点网格**：通过资源门槛后开启，持续七天，至少覆盖一次周到期和一次集合换期；失败或缺周期时延长，不能用短时采集替代。
4. **复核后生产集成**：只允许同库单collector；先部署可读新类型但options关闭的兼容版本，再允许新spec/表和采集。禁用options应只停止相应任务，不能使registry拒绝已经存在的新类型。

隔离库回退为停止专用任务，保留已提交数据；现有生产10档服务不受该回退操作影响。若生产库已有option instrument行，旧二进制全库registry扫描会报不支持类型，回退必须使用仍支持这些类型的兼容构建，不能删除历史instrument或重用ID来迎合旧代码。

生产接入前需确认独立writer/数据库的边界、ClickHouse资源预算、新类型兼容和停用/重启流程；这份设计和独立审核均不是已部署声明。

## 13. 审核记录与待实测事项

独立审核记录见 [设计审核](../../../discuss/0006-options-collection-design-review.md)。R1 提出三项 P1 和三项 P2；R2 已分离动态交易规则、定义接收水位/屏障、补齐discovery与预热启动、辅助类型及缺失行、目录分类和数量乘数。R2复审又指出跨连接规则/生命周期状态的截止缺口，R3补齐control发布时间、独立生命周期水位和质量合并条件。最终是否关闭由独立审核记录对实际 R3 文件的结论决定，作者修改说明不替代复审。

需要由实现和现场验收确认的内容：各产品及combo真实数量单位；combo完整WS序列；新catalog/lifecycle竞态fixtures；公共IP限额；全链quote与分表实际容量；无损冻结截止点的实现压力表现；费表/保证金公式的typed解析覆盖。无法确认的来源字段必须显式unsupported或unknown，不阻塞其他已验证的公开事实保存，也不得让查询输出完整成本或可执行套利结论。
