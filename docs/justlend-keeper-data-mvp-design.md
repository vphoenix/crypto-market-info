# JustLend 清理 keeper：最小数据采集设计

状态：2026-10-02 核心设计已经独立 Agent 初审、修订和复审通过；见 [审核记录](../discuss/0015-justlend-keeper-data-design-review.md)。其后按用户确认追加第2.1节来源分流及第7.1节启动／补采限速：PublicNode 直连节点查询，TronGrid 保留事件分页。2026-10-03 已实现采集器并建立五张独立研究表；实际实施范围、命令、恢复及尚未认证的成功模拟见[实现说明](justlend-keeper-data-implementation.md)，代码审核见[记录](../discuss/0017-justlend-keeper-data-code-review.md)。以下为目标设计，不能据此假定短期优先队列、成功 episode 或净收益已验收。

| 设计项 | 当前实施状态 |
|---|---|
| 五表、严格解析、固化事件／收据、真实费用与报价 | 已实现并有真实入库、数据库往返和跨进程报告验证；Rent/Return 扩展 ABI 的押金余额与计息索引也已定型保存 |
| 启动与补采限速、来源冷却、预算及游标恢复 | 已实现；mock 验证与真实启动五分钟间隔见验收记录，完整24小时负载仍待实测 |
| 固定样本与 latest 只读模拟 | 已实现；由 `state.gob` 恢复，manifest 是可核验证据；失败样本有次数与冷却限制 |
| 30日 Liquidate 历史 | 有显式 backfill 命令，常驻 watch 未运行完整30日回补；同状态有另一模式待办时禁止切换 |
| 奖励成功模拟、短期优先队列、连续机会 episode、净收益 | 成功 fixture 尚未认证，优先队列未启用，episode CSV 只有表头；当前不认证净收益 |
| 动态拆分历史窗口、按实现变更时段回溯剔除 probe | 属于目标设计，当前未实现；已有固化冲突和当前身份核验不替代这两项 |

## 1. 边界与要回答的问题

首版只抓 TRON 主网能源租赁代理 `TU2MJ5Veik1LRAgjeSzEdvmDYx7mefJZvd`、`resourceType=1`。它是能源订单清理，不是借贷抵押品清算。分析只考虑少量 TRX 运营余额、奖励兑换 USDT；不建立大额质押库存，也不增加永续对冲分支。

数据回答四件事：过去 30 日奖励规模和集中度；真实交易消耗多少资源；我们的公开采集方式能否在他人执行前看到可清理订单；按不同资源成本和可得份额，是否值得继续研究。历史利润不能直接变成自己的收益预测。

首版为一个独立 Go 命令、五张 ClickHouse 表、本地带 SHA-256 的原始响应文件。复用现有 HTTP 重试、Decimal/big.Int 和 ClickHouse 客户端；不加消息队列、任务平台、全链索引、交易合约、签名或广播。只在专用研究库写入，不接入主盘口进程。

## 2. 程序结构与命令

```text
cmd/justlend-keeper-data/
internal/justlendkeeper/      client、model、collect、report（直接函数，不做插件框架）
internal/storage/clickhouse/  keeper writer + 专项 schema

公开 HTTP → 严格解析／校验 → 单进程定时循环 → 批量写入 → 离线 CSV/JSON
```

预期命令：`init-schema`（显式建研究库表）、`backfill --days 30`、`watch --duration 24h`、`report --from ... --to ...`。watch 可长期运行但首轮有时限；report 只读且不发外部 HTTP。backfill 与 watch 共用本机文件锁及限速状态，初版不并行运行此命令，换研究库名也不绕过锁。watch 不隐式执行30日 Liquidate 奖励／收据回补。schema 文件见 [DDL](justlend-keeper-data-schema.sql)。

一个配置文件限定 chain、合约、来源 URL 标识、模拟 caller、ABI/代码指纹、轮询与请求上限。公开 HTTP 地址可写配置；可选 API key 只从环境读取，日志与 raw manifest 去掉鉴权头。

### 2.1 来源与路由：PublicNode 节点查询，TronGrid 事件分页

采用两个独立来源地址，不把所有 TRON 请求发送到同一个 host：

```json
{
  "node_rpc_url": "https://tron-rpc.publicnode.com",
  "event_api_url": "https://api.trongrid.io",
  "quote_api_url": "https://api.binance.com"
}
```

| 来源 | 请求范围 | 网络配置 |
|---|---|---|
| PublicNode：`tron-rpc.publicnode.com` | `/wallet/*`、`/walletsolidity/*`：最新／固化区块、按高度取块、交易与收据、账户、链参数、合约与实现代码、只读清理模拟 | HTTPS 443；用户已确认可直连，无需 API key |
| TronGrid：`api.trongrid.io` | 仅 `/v1/contracts/{address}/events`：Liquidate、RentResource、ReturnResource 历史分页及实时轮询 | HTTPS 443；按用户网络单独配置路由；可选 key 仅附加到此来源 |
| Binance：`api.binance.com` | TRXUSDT bid/ask 及数量 | HTTPS 443；按该来源的实际连通性配置路由 |

以上都是中心化 HTTP 服务，不自行通过 P2P 同步链。PublicNode 的 `/v1/contracts/.../events` 本轮返回404，不能替换 TronGrid 的事件索引。程序只需按请求类别选择 base URL；路由由运行环境按域名处理，不增加代理调度、节点池或自动来源切换。

本轮用 curl 在 PublicNode 验证了最新／固化区块、历史交易收据和 `triggerconstantcontract`；模拟样本返回 TVM FAILED，不是获奖励成功样本。默认 Python 请求头曾返回403而 curl 成功，实施时 HTTP 客户端应显式设置稳定的 User-Agent，并用实际客户端验证所需方法；普通403按访问错误记录，不当作空数据或限频无限重试。账户、链参数、完整区块和实现代码等其余方法仍需在实现验收中核验。

raw manifest 对每个请求分别记录无凭据的 source_id、路径、请求时间和 payload hash，包含验证事件所用的 PublicNode 区块／收据证据。capture.source_id 表示该批主来源：事件批为 TronGrid，收据／模拟／链参数批为 PublicNode，兑换报价批为 Binance；不能据事件批主来源假定链锚点也来自 TronGrid。跨来源仍按区块高度/hash、交易及日志校验。PublicNode 尚未追上事件高度或请求失败时，等待重试或记 partial/unknown，不提交已核验覆盖；TronGrid 失败时不伪造新事件，PublicNode 失败时不沿用旧模拟充当当前结果。

## 3. 历史抓取：先把奖励和整笔成本抓准

### 3.1 三种租单事件

历史主线只完整回补 30 日 `Liquidate` 及其交易收据；`RentResource`、`ReturnResource` 用于有限候选发现，不要求全量补收据。从 TronGrid `/v1/contracts/{address}/events` 读取，使用固定 UTC 起止时间、`only_confirmed=true`、`limit=200` 和返回的 next/fingerprint 分页。Liquidate 窗口按天，页数过多则缩小时间窗口；达到预算记 incomplete，不能当作无事件。下一页 URL 必须同来源、同合约、同过滤条件。

候选 bootstrap 将过去 30 日按 0–1／1–7／7–30 日三个活动年龄段回扫 Rent/Return，每段每类最多 10 页，总上限 60 页；以实际已扫描的范围和数量报告覆盖，未完分页不声称全量。原始候选页保留证据，只对入选样本及它们后续的事件补区块／必要收据；不为被排除的大量租单逐一取收据。bootstrap 是首次无可恢复 cohort 时 watch 的第一阶段，无需再增加一个服务；各页与补证据请求按第7.1节缓速执行，完整60页本身至少约5分钟，补收据／区块另计，不能作为启动瞬时批次。已有 cohort 时直接恢复，未完成 bootstrap 从固定窗口及分页进度续采，不每次启动重扫60页。bootstrap 期间尚未进入 live 模拟的时间计为未覆盖。

事件主身份是 `(network, contract, block_hash, tx_id, receipt_log_index)`。TronGrid `event_index` 先作为供应商索引保存，用 receipt 的 address/topics/data 匹配确认真正 log 下标；相同内容出现多次时按有序一一匹配，不能只按参数去重。所有价格与金额都走整数或十进制定点数。

同一块内的执行顺序取完整区块中的 `transaction_index`，按 `(block_number, transaction_index, receipt_log_index)` 应用生命周期变化，不能按 tx hash 排序。只有供应商索引而尚未补 receipt 的行标 `indexed_only`，日志下标可空，不能参加精确奖励或同块状态推导；尚无 block hash 的发现只留 raw，不造链锚点。

事件中的 `amount/addedAmount/subedAmount` 是委托 TRX 原子单位 sun，不是 Energy 数量。`usageRental` 是订单资源使用相关款项，`sendBack` 是返还租户的钱，都不能计作 keeper 奖励；仅 `liquidateFee` 为奖励。三种事件属于同一租单生命周期，采用带 kind 的定类型行，互不适用字段用 NULL。

### 3.2 交易、收据与实付

所有包含 Liquidate 的**不同交易**从 PublicNode 抓 `/wallet/gettransactionbyid`、`/wallet/gettransactioninfobyid`；入选 cohort 的 Rent/Return 交易才按需取收据用于日志位置和状态验证，保存成本但不计入 keeper 成本池。保存外层 sender/to/selector、执行状态、原始交易 hash、fee、energy_usage_total/energy_usage/origin_energy_usage/energy_fee/energy_penalty_total、net_usage/net_fee，以及原生 TRX 内部转账的下标、from/to、金额、rejected 标记。

手续费字段缺失保留 NULL 和原始响应，不将 HTTP 200、空对象或 `fee` 省略自动解释为完整零成本。实现时可在明确 protobuf 零默认值语义及 fixture 验证后增加显式解析规则，不能掩盖响应不完整。`energy_penalty_total` 如已包含在 total 内不得再加；fee 总额和 energy_fee/net_fee 组件不得重复扣。

奖励按同交易、合约付款人、liquidator 收款人聚合，与未 rejected 的原生内部转账核对；只匹配相同金额不足以核验。多事件共享一次交易成本。整笔混合交易只能列“清理奖励减整笔 burn”的观察值，不能武断分摊为单次 liquidate 净利；外层 fee 付款人与 liquidator 不同且归属未知时，不能声称计算某个地址的净利润。代理内部 helper 的支出、其他币收入和非 burn 资源成本未知时，分别说明。

### 3.3 区块与覆盖

先从 PublicNode `/walletsolidity/getblock`、`{"detail":false}` 获取固化上界；最新头用同来源 `/wallet/getblock` 同样只取 header。本轮实测两种轻量响应各约 542 bytes，避免循环抓完整区块。仅在验证事件时按高度缓存完整块，核对 blockID、收据高度／时间、tx_id 及交易下标。事件必须位于固化上界内；供应商 `only_confirmed` 不能替代 hash 验证。相同固化高度出现冲突即暂停该采集器。

不扫描 30 日所有区块。完整性标签为“所选供应商分页完成、返回事件已逐笔核验”，不能宣称独立证明节点未漏事件。最近窗口延后至少 120 秒收口，并于次日重扫前一日；对新增迟到事件生成新的 capture 后，报告重新去重。未扫、失败、成功空窗口三种状态区分。只对达到自己采集覆盖要求的时间段给零值。

不全链搜索失败机器人交易。成功事件无法反推出所有失败成本，历史报告固定列“失败尝试覆盖未知”；后续净收益使用显式失败成本情景，不能默认零。

## 4. 实时抓取：有限订单样本的只读模拟

历史毛量足够才进入 watch。候选来自已经获取并校验的 RentResource / ReturnResource / Liquidate，身份为 `(renter, receiver, resourceType)`，同一身份可关闭后再次租赁。保留各事件顺序及最近重开标识，不能把多轮租赁合并为一次机会。

初版固定一个最多 50 个身份的 cohort，按已发现的最新活动距今 0–1／1–7／7–30 日分层各选 15／15／20 个，再按固定 seed 的身份 hash 排序选取；某层不足可以空缺，明确覆盖。入选后不被新租单挤出，跨日也保留，只有固化事件确认关闭才从同层候选补位。每轮最多轮转模拟 5 个，不预测复杂清算截止时间，不重写协议计息器。租单 `ReturnResource.amount>0` 只是部分归还，只有 `amount=0` 或 Liquidate 才关闭；同一身份后续重新租赁是新生命周期。缺同块顺序时状态保持 unknown，不猜测关闭。短时 `no resource` 模拟可以减少无效重试，但不能替代固化关闭确认。

cohort_id、样本身份、来源事件、进入／退出原因、采样规则和轮转位置保存在已有 evidence manifest，重启从最后提交的 manifest 恢复，不另建候选表。首轮名义轮转约 5 分钟，公共接口更慢时记录实际间隔；短窗口可能漏掉。窗口开始前建立且未活动的租单保持未覆盖，不能用这个样本零成功否定整个 keeper 业务。

每次只调用 PublicNode `/wallet/triggerconstantcontract` 的 `liquidate(renter,receiver,1)`，call_value=0；前后链头、账户校验和实现指纹读取也固定用该节点来源。配置两个固定已激活的公开普通地址，先核验 `/wallet/getaccount` 存在、账户类型非 Contract，再选与 renter 和 receiver 均不同的地址；都冲突则 capture 记 caller_conflict/skipped，不假造不可清理 probe。模拟不需要私钥，也不广播接口返回的 unsigned transaction。

分别校验 HTTP/API result、TVM ret、ABI return 长度、模拟日志和返回 reward；成功但零奖励与有奖励成功分开。API true 可以伴随 TVM FAILED，本轮已抓到自单拒绝和独立 caller 的 no-resource 两种反例。protobuf `ret:[{}]` 的枚举零值规则须显式解析：Transaction.Result.code 的 0 为 SUCESS，但这不能取代有效输出、无 revert 及日志一致性核验；缺整个 ret/transaction 或返回结构不全仍是 unknown。失败记录 revert、RPC error、timeout、unknown 等，不能全归为不可清理。EstimateEnergy 不是必要依赖。

**模拟不是固定历史状态回放。** TRON `/wallet` 的状态可能包含节点正在处理的区块或 pending 变化；首版在一轮模拟前后各取轻量 head，保存两端高度/hash、各次请求起止与解码可用时间。即使两端相同，也只标 `node_latest_unpinned`，不能宣称精确绑定某一块末状态，更不能事后升级为 finalized 盈利机会。raw 请求和响应可核验，但没有 archive TVM 执行器就不能承诺完整重现隐含节点状态。

只记录从**本地 live 模拟结果可用时刻**开始的成功观测点及其跨度；两个成功点之间不能推断每秒都可执行。历史 backfill 的事件时间不能充当实时 first_seen。同一生命周期相邻的成功样本合并为离散 episode，不能每 30 秒累加一笔收入；超过计划间隔、出现 unknown、重启或生命周期不明时分段／删失，不把断点连接成连续机会。API 延迟、轮转空档、超时、重启和未采身份分开统计。赢家 block_time 与本地时间比较只能是近似时间线，不能精确证明自己的纳入顺序或获胜率。

在一次有奖励成功之后，可将该身份加入最多 2 个的短期优先队列，每 10 秒复查，最多 2 分钟；常规队列原有配额保留，优先任务在总请求预算不足时先跳过。后续模拟成功支持“又看到一次可执行状态”，不代表已成交；缺采不得填充成功。若成功持续到观测结束，列右删失。

### 已发现的接口风险

官方 ABI 的 `rentals` 只列一个输出且不包含 liquidate；`getRentInfo` 列两个输出，本轮实查两种 getter 都返回 96 bytes。首版不依赖这两个 getter 的字段语义或清算时间预测。当前入口为 `MarketProxy`，其 `implementation()` 本轮返回 `418cb94f0ccb41cc8de909e00b01edef6a717ade8c`（21字节hex），实现名称 MarketG1，原始 bytecode SHA-256 为 `c66f4e441f88f9bc0b2cf31be8417c5ea220596cfc49dd9abfebf85cf92c3782`；代理和实现 ABI 均为空。入口指纹及最小 selector/topic、成功输出约束写入 manifest，不照通用 EVM 存储槽猜代理。

启动及每 5 分钟核 `implementation()` 和实现代码；地址或 hash 变化后停止正面机会分类，保存 raw，等待新 ABI 核验。报告剔除从最后一次旧身份核验到发现变更之间的 probe，不能将采样核验写成连续版本保证。身份超过 10 分钟未核验的 probe 同样仅保留观察，不参加正面机会统计。当前只取得失败模拟 fixture；实现验收还需一份真实成功输出或经协议源码／可复现 TVM 验证的成功 fixture，才能认证 reward 字段，不能用文档推定成功样本已验证。

本轮只读接口核验的原始响应见 [研究目录](../research/2026-10-02-keeper-design/)。实现前应固定可核验的实现与 ABI manifest；不要求搭建升级索引平台。

## 5. 资源价格与 TRX 兑换

每 60 秒抓一次 Binance TRXUSDT bid/ask 及各自数量，来源只提供采集时间时明确 source_time_kind=received。单次奖励／统一出售批次超过对应一档数量时标容量未知，不把买一价外推。该表为独立现金兑换报价，不伪装成项目现货10档回放；价格和数量 Decimal(38,18)。只能使用当时已经 available_at 的报价，来源／接收时间距目标最多 120 秒；不能使用之后才收到的过去时间戳报价。网络无响应时 received_at/payload_hash 为空，available_at 为本地失败分类完成时间。

每 5 分钟抓链参数 `getEnergyFee`、`getTransactionFee` 及代理 `consume_user_resource_percent`、`origin_energy_limit`，带前后链头与 payload hash。固化状态与最新状态分开，不将今天费率用于历史实际成本。首次启动和维护边界价格不明时保持 unknown。

首版报告两类成本：历史已发生的 TRX burn/资源量；我们单独调用的 `energy_used` 全额自付燃烧情景（不依赖开发者补贴）。同笔模拟返回的 penalty 不再叠加 energy_used。带宽成本用显式交易字节预算情景，不能把未签名模拟对象字节数当实际已签名交易大小。

前向情景只能用当时已 available 的资源参数，最多旧 10 分钟；实施时核对节点实际参数维护边界，边界／变更不明保持 unknown。该限制是研究近似，不宣称模拟和资源费来自同一精确状态。本轮代理显示 user_resource_percent=10、origin_energy_limit=90,000，但开发者实际可用资源未验证，默认不把补贴当成必得。全燃烧情景 `R - E×p_energy - B×p_bandwidth - K`，其中 R 为奖励 sun，E 为模拟 energy_used，B 为带宽字节预算，K 为兑换／运行／失败等显式成本。采购盈亏平衡为 `(R-B×p_bandwidth-K)/E` sun/Energy；分子非正即无预算，E未知或0不做除法。失败概率和费用都没有实测时，以成本上限／敏感性报告，不声称已得净利。

暂不接能源商下单接口、不买能量或质押 TRX。对租能路线输出**可承受的最高能量采购成本**，与实际报价另行比较。没有完整的数量、时长、预付／退款、有效期与供给条件，不能宣称租能后可盈利。全燃烧亏损只否定该成本情景，不否定更低成本路线；自有能量同样有机会成本。卖出手续费、最低下单量和批量兑换等待用明确参数，没有有效规则时保留 unknown。

## 6. 五张表及写入语义

| 表 | 一行含义 | 核心内容 |
|---|---|---|
| `jl_keeper_capture` | 一次有界扫描／收据批次／probe批次／成本观测的提交记录 | 来源、模式、UTC范围、固化上界或头区间、分页／任务完成数、四类事实计数与摘要、raw manifest hash、状态与缺失原因 |
| `jl_keeper_rental_event` | 一条带固化区块锚点的租单事件 | 三种 event kind、订单身份、数量／押金变化／奖励等定类型字段；indexed_only 与已验证 receipt 位置分开 |
| `jl_keeper_tx_receipt` | 一次取得的交易与收据 | 外层调用、成功状态、全部资源／burn分项、原生内部转账数组、body/receipt hash；整笔成本只计一次 |
| `jl_keeper_probe` | 一次本地只读 liquidate 模拟 | 候选身份与来源、caller、实际请求时间、head范围、API/TVM结果、reward、能量、错误、payload hash |
| `jl_keeper_cost_observation` | 一次独立资源费率或兑换报价 | 类型、可空链锚点、费用单位、BBO价格/数量、来源时间与hash、实现指纹／资源分摊参数 |

所有时间 DateTime64(6,UTC)，协议 uint256 为 UInt256 原子单位，TRX 使用 sun，节点资源／费用 int64/uint64 严格检查，地址为含0x41前缀的21字节、hash32字节；API base58 与 ABI20字节地址明确转换。无二进制浮点路径、不建通用 JSON 业务表。

证据先落盘，再批写事实，最后提交 capture。capture 保存四表预期成员计数与固定顺序内容摘要；报告核验缺行，不因 commit 行存在就信完整。每次有界采集批次新建 capture_id；批内每次 HTTP 尝试有独立时刻与证据，probe_index 区分真实尝试；只有数据库写入重试复用同一 capture_id 和已冻结内容。ReplacingMergeTree + FINAL 只处理逻辑重试，不依赖后台合并时机。跨 capture 的链上事件／收据按 canonical 身份去重，实时 probe 保留每次真实尝试。事实按 capture 开始月分区，身份包含 capture_id，防止重试跨月制造替换失效。

capture 的 complete 只指明确 coverage_scope：完整返回的清理事件、抽样租单事件、或选定任务；不能把采完50个候选写成全部租单覆盖。不同 capture 同一 canonical 身份的协议金额、地址或已知资源量冲突时隔离并报错，不能用最新抓取覆盖；补全 NULL 可合并，payload 包装或时间不同不构成协议冲突。probe 的 latest 区间永不升级为 solid。

原始响应 gzip 按内容 hash 保存，manifest 记录无凭据请求、各自请求时间和响应 hash。数据库存类型字段及摘要，不把 JSON 塞入事实列。schema 和 manifest 固定版本；必需字段缺失、错误类型／长度或未知事件 ABI 报错，无关新增 JSON 字段保留 raw 后允许忽略。重启从已提交连续覆盖恢复游标，不另建任务数据库。

## 7. 运行预算、报告及验收

建议默认：事件轮询 30 秒，5个常规 probe／30秒，最多2个短时 probe／10秒，费率5分钟，行情60秒；这些是任务目标周期，不是允许瞬时发出一批HTTP。实际发送必须满足第7.1节的启动、全局及来源间隔，预算最多40,000请求／UTC日，均包含重试。PublicNode、TronGrid、Binance 请求都计入总预算，另外按 source_id 统计次数、延迟及错误；已有 TRON 收益采集若同样访问 TronGrid，也可能共享 IP 配额，应给它留余量。限频只对对应来源退避；不换地址绕过限制。任务不重叠，超预算明记 skipped。只存真实完成时间，不能把设定周期当实际采样周期。每个 capture 上限500事实行，凑够或到轮次结束即批写，不逐HTTP请求提交数据库；分页证据可流式落盘，不在内存积攒30日原始响应。

### 7.1 启动、补采和重试不得突发

限速在每次真实 HTTP 发送前执行，包括启动校验、翻页、窗口拆分、交易正文、收据、区块、前后链头、caller／实现检查及每一次底层重试。一次响应包含200条事件仍是一次请求；后续证据查询逐个限速，不能按事件数并发扇出。复用 HTTP 工具时必须将共享发送 gate 接入每次 attempt，不能只在外层 collector 限速；也不直接调用既有 TRON 收益采集的 Fetch（它会遍历见证人），只复用所需单接口方法。禁用自动 HTTP 重定向，遇到重定向记来源错误，防止额外发送绕过 gate。

| 阶段／来源 | 默认发送约束 |
|---|---|
| 每次启动的前5分钟 | 所有来源合计相邻请求开始时间至少相隔5秒；第一次也先等待5秒；前60秒最多12次、前300秒最多60次 |
| 启动5分钟后 | 所有来源合计相邻请求开始时间至少相隔1秒；全局最多1个在途HTTP |
| TronGrid，全阶段 | 同来源相邻请求开始时间至少相隔5秒，三个事件流共用，不是每个流分别获得额度 |
| PublicNode／Binance，启动5分钟后 | 同来源至少相隔1秒，仍受全局间隔约束 |
| bootstrap、backfill、迟到复扫及断线补采 | 所有补采请求合计至少相隔5秒；同样计入全局／来源额度，取其中最慢约束 |

gate 使用真实发送时刻推进下一次允许时间，不积攒闲置额度，不用启动时装满的 token bucket，不按错过的定时点连续补发。慢响应后下一次仍需满足发送间隔；任务完成后的多余额度丢弃。到期 live 任务只保留一个待执行轮次，过时模拟记 skipped／缺采；历史游标逐页缓速推进，不能将恢复后的历史请求冒充 live 可见性。开启定时循环前只缓速校验必要账户、实现身份和接口；其他首次采样错开执行，不全量预热50个候选，第一次也只轮转最多5个。

每个外部请求总超时20秒。超时／5xx每个逻辑请求最多3次尝试，失败后至少等待5秒、15秒再尝试，并重新经过全部 gate。复用现有 HTTP 工具时设 MaxAttempts=1，由同一个主循环登记后续尝试时间，不叠加工具内部250ms重试；等待／来源冷却期间选择其他可运行任务，不在单次调用内长睡阻塞所有来源。429 按该来源整体冷却，至少5分钟并尊重更长 Retry-After；连续限频将默认冷却增为10、20、40、60分钟，不缩短服务端指定时间。冷却结束只允许一个试探请求，不集中释放待办。普通401／403暂停该来源并保存阻断状态，待显式恢复，不自动循环尝试或切换域名；其他来源可继续它们不依赖失败来源的任务。

在现有证据目录增加一个小型原子更新的本地状态文件，保存本命令各来源的 next_allowed_at／blocked_until／阻断状态、UTC日请求计数，以及 bootstrap／backfill 已提交游标；不新增数据库表或调度服务。发送前先记额度，重启不清零当日计数或冷却；冷却未到即等待。游标只在证据及批次提交后推进，崩溃最多重复未提交页，重复请求仍限速。文件缺失可首次初始化，已有文件损坏则停止外部采集，不能当作全新额度启动。warmup 每次重启重新执行，避免服务反复启动形成突发。

以上是本命令的研究默认值，不代表供应商承诺的免封阈值；同出口IP的其他进程不在此 gate 内，部署时还要合计其请求量。限速造成 bootstrap 或轮询超时限时保留 partial／实际采样间隔，不能为了赶上目标周期提速。

### 7.2 报告与验收

输出 `coverage.csv`、`liquidations.csv`、`probe_episodes.csv`、`summary.json`。报告包含奖励和地址集中、成功交易成本范围、混合调用占比、资源价格盈亏平衡、真实观察到的成功episode、发现时已结束比例、未覆盖身份、失败成本情景、未知项。USDT历史估值缺对应时点行情时仅列当前统一重估，不称历史已实现利润。

机会筛选采用奖励减全部可归属成本，完整资本包括 TRX 运营库存、资源预付及现金缓冲。小业务贡献20／30／50 USDT日均均可研究，200不是单场景硬门槛；同时报告完整日中位数、最差7日、前五日贡献。仍未观测到自己的成交，不输出实测胜率或保证日收益。

实现验收至少覆盖：大整数与地址转换；三事件严格ABI；分页边界／重复／迟到／空窗口；多事件一交易只扣一次费；内部转账一一归属、rejected排除；NULL与0；补采不伪造first_seen；复租与重复probe不重复计机会；HTTP成功但TVM失败；头区间不能冒充精确锚定；限频和单来源失败隔离；事实部分写入／commit丢失／重试跨月；固化hash冲突。用一天真实样本实测请求数、压缩后占用和两类报告查询耗时，不把瞬时测量线性外推成已验收日容量。

启动／限速验收使用本地 mock HTTP 服务及可控时钟，覆盖冷启动60页候选及逐笔补证据、200条事件扇出、底层5xx重试、429后重启、慢响应／长时间暂停恢复、UTC日切换及重复进程启动；按每个真实请求开始时刻验证上述最小间隔、并发数、warmup总量、预算不重置及单次试探，不向供应商发送压测流量。自动重定向不得产生第二次发送。这是目标测试范围；已完成的实施验证与仍待验证项见下面的实施更新。

2026-10-03 实施更新：已有采集程序、受控时钟／HTTP mock、真实数据库往返和实际启动验收，上段是原定验收范围。已测项目与未测的全天请求量／压缩空间／长期覆盖以[实现验收记录](../research/2026-10-03-keeper-implementation/validation.md)为准；不把有限 mock 或首批真实数据解释为全部目标均完成。

## 官方依据

- [JustLend 能源租赁](https://docs.justlend.org/developers/energy_rental/)、[官方 ABI](https://docs.justlend.org/developers/abis/energy-market.json)、[代理入口](https://docs.justlend.org/developers/contracts_overview/)
- [TronGrid 事件分页](https://developers.tron.network/reference/get-events-by-contract-address)、[交易收据](https://developers.tron.network/reference/gettransactioninfobyid)
- [PublicNode TRON 主网入口](https://tron.publicnode.com/)、[TRON 节点与扩展 API](https://developers.tron.network/docs/api)
- [不广播模拟及状态语义](https://developers.tron.network/reference/triggerconstantcontract)、[可选能量估计](https://developers.tron.network/reference/estimateenergy)、[限频](https://developers.tron.network/reference/rate-limits)
