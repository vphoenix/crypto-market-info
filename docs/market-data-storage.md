# 市场数据存储数据字典

2026-10-19 MySQL 增加来源登记追加布局扩展：`source_publication_intents_v2`、`source_batch_status_v2`、`public_observation_provenance_v2` 保留对应旧表的全部逻辑字段、键哈希、来源证明和物理重复语义。以内部 `BIGINT UNSIGNED AUTO_INCREMENT` 的 `_storage_id` 作聚簇主键，原 `_key_hash`（前两表还包括 `_occurrence`）成为唯一身份索引；原业务查询索引保留。内部序号不属于业务身份、原始内容或迁移摘要。新记录集中追加，随机访问主要落到较小的身份索引；新索引的空间/写入成本计入对照，不承诺总存储空间下降。事务核验同时读取新旧身份与内容，来源意图的首次知晓时间、事实/来源/发布状态的原子提交不变。查询在同一语句快照中合并两种格式并保持原排序、LIMIT 和重复行检测。安装完整扩展后，新版客户端自动兼容读取；写开关 `MYSQL_SOURCE_STORAGE_V2` / `--source-storage-v2` 默认关闭。旧表与冻结迁移清单不重写，回退写入须保留新版读取能力。[布局、实测与上线记录](redis-mysql-migration/io-spike-fix-20261019.md)。

2026-10-19 MySQL 增加可选质量存储扩展，安装与上线状态见[实施记录](redis-mysql-migration/quality-storage-implementation-20261019.md)。`derivative_book_quality_minute_v2` 保留旧表全部物理字段及 RLE 编码，主键改为 `(minute_time,instrument_id,compact_batch_id)`，另以窄索引支持 `(instrument_id,minute_time)`。目录证据按八组原定类型 RLE 字段拆成 `options_catalog_quality_profile_minute` 和 `options_catalog_quality_member_minute`：profile 主键 `(minute_time,profile_hash)`，成员主键 `(minute_time,run_id,instrument_id,compact_batch_id)`，并保留原 evidence row hash。profile hash 覆盖版本、UTC 分钟及带长度边界的字段编码；NULL、微秒、证据 UUID 和连接代次均不归并。三表保持 8 KiB 页压缩和月分区，属于独立运行时扩展，不改变冻结的 125 表迁移映射、原交付 codec 或批次哈希。profile、成员、原分钟提交声明与 ingest_batch 同事务提交。读取合并新旧格式并拒绝重复成员、缺失 profile 或摘要不符；关闭新写入开关仍读取新格式，启用新格式后不能回退到不识别新表的旧二进制。新结构只保存切换后的新分钟，不重写历史。

2026-10-16 spool 增加 `CMIS 02` 无损压缩信封，保留 v1 读取、原交付身份和逻辑 codec；调度内存预算按解压后长度核算。Redis 可凭 MySQL 完整提交身份回收 collecting 残留，父批次状态检查防止迟到成员重建。事实表、盘口/质量位图及历史编码保持。格式、测试和回退限制见[降写入实施记录](redis-mysql-migration/io-reduction-20261016.md)。

2026-10-15 新增本地运行状态编码的实现说明见[运行状态工具与恢复协议](redis-mysql-migration/runtime-state-operations.md)。这是待部署的状态/恢复格式，不新增 MySQL 事实表，也不改变现有盘口编码、数据完整性或历史迁移状态；实施与验证进度见[记录](../research/2026-10-15-runtime-state-improvement/report.md)。

本数据字典定义当前阶段的六个市场与收益核心表：交易标的表 `instrument`、分钟完整盘口表 `order_book_minute`、分钟内秒级差量表 `order_book_second_delta`、整点资金费率表 `funding_rate_hourly`、收益路线表 `yield_route` 和收益观测表 `yield_observation`；另定义三个永续共有交易对元数据表 `instrument_canonical_mapping`、`perpetual_universe_run` 和 `perpetual_universe_member`。

这些表在整个采集进程中的位置及以后增加其他数据时的扩展原则见[系统总体架构](architecture.md)。

Redis/MySQL 后端已切换为正式采集与分钟写入后端，ClickHouse 旧数据及启动依赖快照继续保留；正式历史导入暂停，完整迁移状态仍为 false。[实现交付](redis-mysql-migration/implementation-status.md)列出部署和验证状态；[逐表 MySQL 映射](../internal/storage/mysql/mapping.json)覆盖 125 张表。数组/命名 Tuple 使用 v1 定类型二进制编码（版本、类型摘要、长度限制），UInt256 为 BINARY(32)，UUID 为 BINARY(16)，Decimal 为 DECIMAL(38,18)，UTC 时间为 DATETIME 对应精度；NULL 和空数组分别编码。CEX 第一版保留 50 档物理列，按 stored_depth 精确读取。事实与 ingest_batch/ingest_member 在同一事务提交；没有成员证明的 CEX 分钟不会被回放接口作为已提交事实。普通 MergeTree 的全部物理副本用 _occurrence 保留，按完整字段规范字节排序，包括同键内容不同的副本；发布读者仍拒绝任何重复发布。相同不可变事实的运行时读取保持幂等。2026-10-13 起已按用户限定范围导入独立封存快照中的启动依赖；正式 producer 已激活，实际运行路径与验收状态见 `docs/runtime-operations.md`。

2026-10-09主库六张热表已设置512 MiB单次自动后台合并输入上限，范围为三个 `derivative_book_*` 表、`order_book_second_delta`、`options_catalog_live_minute_commit` 和 `options_catalog_quality_evidence_minute`。这是物理合并调度限制，不改变字段、5/10/50档回放、差量编码或完整性含义；既有大part保留，查询仍按现有规则逻辑去重。三项fsync保持启用，需持续检查part积压及查询成本。同日13:18（北京时间）已按用户同意重启共享实例，启用全局合并读取16 MiB/s、mutation读取8 MiB/s限速；它不限制整机总写入或fsync IOPS。具体现场设置、适用边界和回滚见[合并I/O排查](../research/2026-10-09-clickhouse-merge-io/report.md)。

2026-10-05 重启核验后，五个生产库（主库、Across、LST、Reserve、JustLend keeper）的59张现有 MergeTree 表已显式设置 `fsync_after_insert=1`、`fsync_part_directory=1`、`min_rows_to_fsync_after_merge=1`。插入与合并结果同步落盘，表设置持久保存；新建生产表也应采用这三项设置。它们不改变字段、UTC时间、编码或完整性含义；`CREATE TABLE IF NOT EXISTS`不会覆盖已有设置。本次逐表执行与核验记录见[重启检查目录](../research/2026-10-05-reboot-check/)。同步落盘降低断电风险，不能恢复缺失的行情，也不能代替底层存储的可靠性。

Reserve 拍卖与篮子申赎已在独立库 `crypto_market_info_reserve` 建表，新增 `reserve_capture`、`reserve_folio_state`、`reserve_route_quote`，复用 `dex_log`／`dex_tx_receipt` 定义。实际结构和不变量见 [DDL](reserve-data-schema.sql) 与 [实现说明](reserve-data-implementation.md)；特别是控制表 `receipt_refs` 固定当时引用集合，后续补收据不会改写旧批语义。

2026-10-04 Reserve `reserve-r5-mvp-3` 共七表，另含联合模拟 `reserve_route_simulation` 和完整交易明细 `reserve_receipt_data`。按用户要求，Reserve 的 API/RPC 返回只在内存中严格解析，定类型数据直接入库，响应正文不落文件。来源 payload hash、成员摘要、UTC 时间与区块锚点仍保存；hash 不承诺原文可下载，报告标注 `raw_response_policy=not_retained`。此项仅修改 Reserve，其他采集器的归档政策以各自说明为准。

2026-10-09 Reserve 恢复路径区分两种错误：RPC 区块结果为 `null` 时返回有类型的 `block_unavailable`，没有有效区块锚点，按原六秒轮询重试；数量字段缺失、格式错误和身份不匹配仍严格拒绝。实时日志或快照结束复查发现 `block_no_longer_canonical` 时，不提交该批，先刷新 latest/safe/finalized 并核对已有未最终确认批次，旧分支记录标记 orphaned 并回退日志游标，再重新采集。已最终确认哈希冲突和持久化完整性错误仍终止。未重新生成旧区块的历史报价；表结构、批次提交身份、来源摘要和原文不留存规则保持。

`reserve_receipt_data` 按 `(chain_id,block_hash,tx_hash)` 唯一，按 block_time 的 UTC 月分区，ReplacingMergeTree。保存原始 receipt/calldata SHA256、二进制 calldata、tx_index、完整日志数量和 `Array(Tuple(log_index UInt32,emitter FixedString(20),topics Array(FixedString(32)),data String))`；包括 Folio 白名单外的日志。空日志必须有明确零数量的行，缺行不能当空。`data_hash` 使用确定性二进制编码，覆盖锚点、原哈希与全部明细；仅排除自身和迁移可用时间 `materialized_at`。校验摘要、数量、calldata hash/selector、唯一有序 log_index 和原 capture 所引用日志后才允许共享回执复用。摘要/回执事实及明细先落库，capture 最后提交。旧 capture 的 `receipt_refs`、成员摘要与首次可见时间不变；旧原文只由离线 `migrate-receipts` 读取一次，迁移全部通过后才清理。版本化配置、ABI、只读模拟编译产物另存小型 rules 目录；备份七表和该目录即可，不依赖响应归档。

所有时间均为 UTC。盘口快照和差量中的价格、数量均为整数：价格使用 `price_tick`，数量使用 `qty_lot`；不得使用字符串价格或二进制浮点数。用于定义换算单位的元数据使用十进制定点数 `Decimal`。

`instrument_id` 是交易流的唯一标识，必须区分交易所、市场类型、标的、结算币种及合约版本。例如，Binance 现货 `BTCUSDT`、Binance U 本位永续 `BTCUSDT`、OKX 永续 `BTC-USDT-SWAP` 和 Bybit 线性永续 `BTCUSDT` 必须使用不同的 `instrument_id`。其定义保存在 `instrument` 表中，不在各事实表中重复保存。

## 1. 交易标的表

表名：`instrument`

用途：保存交易所产品代码与统一交易流 ID 的映射，以及还原整数价格、整数数量和分析现货/合约价差所需的最少元数据。

主键：`instrument_id`

| 字段名 | 中文名称 | 逻辑类型 | 必填 | 说明 |
|---|---|---:|:---:|---|
| `instrument_id` | 交易标的 ID | `UInt32` | 是 | 交易流的唯一标识。交易所产品定义、结算方式或合约版本发生变化时，分配新的 ID。 |
| `exchange` | 交易所 | `String` | 是 | 交易场所，例如 `Binance`、`OKX`、`Bybit`。 |
| `market_type` | 市场类型 | `String` | 是 | 当前取值为 `spot`（现货）、`perpetual`（永续）或 `delivery`（交割合约）。 |
| `exchange_symbol` | 交易所产品代码 | `String` | 是 | 交易所原始产品代码，例如 `BTCUSDT`、`BTC-USDT-SWAP`。 |
| `venue_contract_version` | 场内合约版本 | `String` | 新登记衍生品是 | 标识同一交易所代码背后的具体合约实例。Binance 永续使用 `onboardDate`，OKX 永续使用 `listTime`，Bybit 永续使用 `symbolId:launchTime`；现货及迁移前历史行可为空。该字段变化必须分配新 `instrument_id`。 |
| `base_asset` | 基础币种 | `String` | 是 | 被交易的资产，例如 BTC/USDT 中的 BTC。 |
| `quote_asset` | 报价币种 | `String` | 是 | 表示价格使用什么资产计价，例如 BTC/USDT 中的 USDT。 |
| `settle_asset` | 结算币种 | `Nullable(String)` | 否 | 保证金、盈亏和资金费使用的资产。现货为空；USDT 本位合约通常为 USDT；币本位合约可能与报价币种不同。 |
| `contract_multiplier` | 合约乘数 | `Decimal` | 是 | 合约数量换算参数；现货固定为 `1`。跨市场比较数量时，查询端结合该值及合约类型换算为统一基础币数量。 |
| `price_tick_size` | 最小价格单位 | `Decimal` | 是 | 一个 `price_tick` 对应的实际价格，用于还原盘口价格，并支持 tick 离散规则分析。不得使用二进制浮点数。 |
| `quantity_step_size` | 最小数量单位 | `Decimal` | 是 | 一个 `qty_lot` 对应的实际数量，用于还原盘口数量，并支持 lot 离散规则分析。不得使用二进制浮点数。 |
| `expiry_time` | 到期时间 | `Nullable(DateTime)` | 否 | 交割合约的 UTC 到期时间；现货和永续为空。用于计算剩余期限和年化基差。 |

`quote_asset` 与 `settle_asset` 表达不同含义。例如，BTC 币本位合约可以使用 USD 报价，但使用 BTC 作为保证金和盈亏结算资产，因此其 `quote_asset` 为 USD、`settle_asset` 为 BTC。

本表不维护启用状态、创建时间、更新时间或生效区间。若上述交易定义发生变化，应创建新的 `instrument_id`，避免用新规则解释旧行情。`venue_contract_version` 通过幂等 `ADD COLUMN IF NOT EXISTS ... DEFAULT ''` 兼容旧表；旧行的空字符串只代表迁移前未知版本，不能用于登记新的衍生品定义。首次部署该迁移时，现有 Binance、OKX 永续会以有版本的新定义取得新 ID，旧盘口和资金费率仍留在旧 ID 下，不覆盖也不重写。

注册器先完整校验一批全部定义，再为所有新定义执行一次 batch insert；调用内的重试复用已分配 ID。相同完整定义复用历史 ID；历史 tick、lot 或乘数改变，即使场内上市版本不变，也仍分配新 ID。同一注册批次不能为同一 `(exchange, market_type, exchange_symbol, venue_contract_version)` 提交两种不同定义，这表示当前 catalog 自相矛盾；这项检查不禁止历史定义并存。

## 2. 分钟完整盘口表

表名：`order_book_minute`

用途：保存每个交易流在每分钟第 0 秒的完整盘口。新现货/期货保存买卖各5档；旧历史保存买卖各10或50档，由 `stored_depth` 标明。该记录是恢复本分钟内任意秒盘口的起点。

唯一键：`(instrument_id, minute_time)`

| 字段名 | 中文名称 | 逻辑类型 | 必填 | 说明 |
|---|---|---:|:---:|---|
| `id` | 分钟盘口 ID | `UInt64` | 是 | 分钟完整盘口的唯一 ID，供差量表关联。按照下方公式由 `instrument_id` 与 UTC 分钟序号确定性生成。 |
| `instrument_id` | 交易流 ID | `UInt32` | 是 | 唯一标识交易所、市场类型、标的、结算币种及合约版本。 |
| `minute_time` | 分钟时间 | `DateTime` | 是 | UTC 分钟起始时间，秒和毫秒固定为 0。 |
| `valid_bitmap` | 秒有效位图 | `UInt64` | 是 | 低 60 位分别代表本分钟第 0 至 59 秒是否有有效盘口。位值为 `1` 表示有效，`0` 表示无效或缺失。 |
| `stored_depth` | 保存档数上限（每侧） | `UInt8` | 是 | 支持 `5`、`10` 或 `50`。新现货/期货 writer 显式写 `5`；列默认值固定为 `50`，用于旧 part 与旧 collector。实际挂单不足此深度时仍以零补齐。 |

兼容迁移执行 `ADD COLUMN IF NOT EXISTS stored_depth UInt8 DEFAULT 50 AFTER valid_bitmap`。物理快照继续保留每侧 50 档的价格和数量列，超出本分钟 `stored_depth` 的档位必须全部为零；压缩后主要新增占用来自所保存档位及其差量。不删除列、不重写旧 part、不改变 instrument 或分钟唯一键。新建库也采用同样结构和默认值，避免旧 writer 回退运行时被误标为 10 档。

回放必须先加载完整的历史起点及差量，再按本分钟实际保存深度返回，并在输出 `BookSnapshot.StoredDepth` 中标明上限。不能只读旧起点的前 10 档：未发生数量变化的旧第 11 档也可能因前档删除进入前 10。尚未迁移的旧表可只读回放，缺少该列时按 50 解释；新查询不会为此执行 DDL。新分钟在 UTC 分钟边界建立所配置深度的独立起点，不与旧分钟共用差量链。回滚只能在新分钟开始新起点，不能重写已存在分钟为另一深度。

分钟 ID 的生成公式固定为：

```text
unix_minute = floor(UTC Unix 时间戳 / 60)
id = (unix_minute << 32) | instrument_id
```

`instrument_id` 占低 32 位，UTC 分钟序号占高 32 位。该公式也允许 ClickHouse 从 `minute_id` 推导月份，用于差量表按月分区。

### 买盘字段

| 字段范围 | 中文名称 | 逻辑类型 | 必填 | 说明 |
|---|---|---:|:---:|---|
| `bid_price_01` | 买 1 价 | `Int64` | 是 | 本分钟第 0 秒的最高买价，保存为 `price_tick`。 |
| `bid_qty_01` | 买 1 量 | `UInt64` | 是 | 买 1 价对应的数量，保存为 `qty_lot`。 |
| `bid_price_02` | 买 2 价 | `Int64` | 是 | 第二高买价，保存为 `price_tick`。 |
| `bid_qty_02` | 买 2 量 | `UInt64` | 是 | 买 2 价对应的数量，保存为 `qty_lot`。 |
| `……` | 买 3 价、买 3 量……买 49 价、买 49 量 | `Int64` / `UInt64` | 是 | 命名规则分别为 `bid_price_03`、`bid_qty_03`……`bid_price_49`、`bid_qty_49`；含义与买 1 价、买 1 量相同。 |
| `bid_price_50` | 买 50 价 | `Int64` | 是 | 第五十高买价，保存为 `price_tick`。 |
| `bid_qty_50` | 买 50 量 | `UInt64` | 是 | 买 50 价对应的数量，保存为 `qty_lot`。 |

买盘非零有效价格必须严格递减（新现货/期货到第5档，历史最多到第50档）：

```text
bid_price_01 > bid_price_02 > …… > bid_price_50
```

### 卖盘字段

| 字段范围 | 中文名称 | 逻辑类型 | 必填 | 说明 |
|---|---|---:|:---:|---|
| `ask_price_01` | 卖 1 价 | `Int64` | 是 | 本分钟第 0 秒的最低卖价，保存为 `price_tick`。 |
| `ask_qty_01` | 卖 1 量 | `UInt64` | 是 | 卖 1 价对应的数量，保存为 `qty_lot`。 |
| `ask_price_02` | 卖 2 价 | `Int64` | 是 | 第二低卖价，保存为 `price_tick`。 |
| `ask_qty_02` | 卖 2 量 | `UInt64` | 是 | 卖 2 价对应的数量，保存为 `qty_lot`。 |
| `……` | 卖 3 价、卖 3 量……卖 49 价、卖 49 量 | `Int64` / `UInt64` | 是 | 命名规则分别为 `ask_price_03`、`ask_qty_03`……`ask_price_49`、`ask_qty_49`；含义与卖 1 价、卖 1 量相同。 |
| `ask_price_50` | 卖 50 价 | `Int64` | 是 | 第五十低卖价，保存为 `price_tick`。 |
| `ask_qty_50` | 卖 50 量 | `UInt64` | 是 | 卖 50 价对应的数量，保存为 `qty_lot`。 |

卖盘非零有效价格必须严格递增（新现货/期货到第5档，历史最多到第50档）：

```text
ask_price_01 < ask_price_02 < …… < ask_price_50
```

如果某一侧不足 `stored_depth` 档，从第一个缺失档开始，该档及后续物理档位的价格和数量一律为 `0`。`stored_depth=5` 时第6至50档强制为零，`stored_depth=10` 时第11至50档强制为零；writer 拒绝深度不受支持、超出深度的非零快照或差量恢复后超过该深度的状态。

`price_tick` 以交易流定义的最小价格单位换算：

```text
price_tick = 实际价格 / price_tick_size
实际价格 = price_tick × price_tick_size
```

`qty_lot` 以交易流定义的最小数量单位换算：

```text
qty_lot = 实际数量 / quantity_step_size
实际数量 = qty_lot × quantity_step_size
```

期货交易流的 `qty_lot` 保留交易所原始合约数量语义；需要比较跨市场可成交量时，由查询端结合合约乘数转换。

## 3. 分钟内秒级差量表

表名：`order_book_second_delta`

用途：保存同一分钟内第 1 至 59 秒相对上一个有效采样状态的最终盘口变化。连续有效时，它就是相对前一秒的变化；经过无效区间后的首个有效秒，则相对本分钟上一个有效状态。差量按价格记录，不按买 1、买 2 等档位编号记录。

唯一键：`(minute_id, second_offset)`

| 字段名 | 中文名称 | 逻辑类型 | 必填 | 说明 |
|---|---|---:|:---:|---|
| `minute_id` | 分钟盘口 ID | `UInt64` | 是 | 关联 `order_book_minute.id`。不重复保存交易流和分钟时间。 |
| `second_offset` | 分钟内秒偏移 | `UInt8` | 是 | 当前差量所属秒，取值范围为 `1` 至 `59`；第 0 秒由分钟完整盘口表表示。 |
| `bid_change_prices` | 买盘变化价格数组 | `Array(Int64)` | 是 | 本秒发生新增、修改或删除的买盘价格，元素为 `price_tick`。 |
| `bid_change_qtys` | 买盘变化数量数组 | `Array(UInt64)` | 是 | 与 `bid_change_prices` 同下标对应的最终 `qty_lot`。数量为 `0` 表示删除该价格。 |
| `ask_change_prices` | 卖盘变化价格数组 | `Array(Int64)` | 是 | 本秒发生新增、修改或删除的卖盘价格，元素为 `price_tick`。 |
| `ask_change_qtys` | 卖盘变化数量数组 | `Array(UInt64)` | 是 | 与 `ask_change_prices` 同下标对应的最终 `qty_lot`。数量为 `0` 表示删除该价格。 |

数组必须满足：

```text
length(bid_change_prices) = length(bid_change_qtys)
length(ask_change_prices) = length(ask_change_qtys)
```

同一个秒内，同一方向、同一价格只能保留一个结果；若交易所推送多次变化，只保留该秒采样时刻的最终数量。

数量保存的是更新后的绝对值，而不是与前一秒相比的增减值。例如某价格的数量从 `1.2` 变为 `1.5`，差量中保存 `1.5` 对应的 `qty_lot`，不保存 `+0.3`。

若某个价格从保存的前 `stored_depth` 档状态中移除，写入其价格和数量 `0`。该价格即使仍存在于交易所更深的盘口中，也视为从本系统保存的状态中删除。新数据先截取目标5/10档再计算差量，不能通过截短旧差量数组得到新深度差量。

若某一秒有效但盘口没有变化，不写差量行；其有效性由分钟完整盘口表的 `valid_bitmap` 对应位表示。若某一秒无效或缺失，同样不写差量行，但 `valid_bitmap` 对应位必须为 `0`。

无效区间后的首个有效秒必须相对本分钟上一个已保存的有效状态生成差量，使查询端跳过无效秒后仍能恢复该秒。如果第 0 秒无有效完整盘口，则本分钟没有恢复起点，整分钟不保存。

## 4. 整点资金费率表

表名：`funding_rate_hourly`

用途：每个 UTC 整点保存永续合约当时可获得的一条资金费率。优先保存实际结算费率；没有实际结算值时保存估算费率。

唯一键：`(instrument_id, hour_time)`

| 字段名 | 中文名称 | 逻辑类型 | 必填 | 说明 |
|---|---|---:|:---:|---|
| `instrument_id` | 交易标的 ID | `UInt32` | 是 | 关联 `instrument.instrument_id`，且对应永续合约。 |
| `hour_time` | 采集整点 | `DateTime` | 是 | 本次记录的 UTC 整点时间，分钟、秒和毫秒固定为 0。 |
| `funding_time` | 资金费结算时间 | `DateTime64(3, 'UTC')` | 是 | 当前费率对应的 UTC 结算时点，保留交易所返回的毫秒精度。估算值表示目标结算时间，实际值表示本次实际结算时间。 |
| `rate` | 资金费率 | `Decimal` | 是 | 估算资金费率或实际结算资金费率，不使用二进制浮点数。 |
| `is_actual` | 是否实际结算值 | `Boolean` | 是 | `1` 表示实际结算费率，`0` 表示估算费率。 |

写入规则：

1. 估算费率来自交易所公开 WebSocket。每个 UTC 整点从内存中的最新有效推送写入 `rate`，并设置 `is_actual = 0`；不使用 REST 轮询估算费率；
2. WebSocket 推送必须同时保存其对应的 `funding_time`。结算整点应使用结算前已经收到、且目标为该结算时点的估算值，不能被结算后下一周期的估算值抢先替换；
3. 实际结算费率只通过交易所历史 REST 接口确认。首次查询不得早于目标 `funding_time + 2 分钟`；
4. 同一交易所的实际费率 REST 查询必须由单个 worker 串行执行，请求之间至少间隔 1 秒。不同 instrument 不得在整点集中并发请求；
5. 首次未取得实际值时，按结算后第 5、15、60 分钟重新入队；仍未取得时可低频补查，但不能恢复为每分钟遍历全部 instrument；
6. 取得实际值后，使用交易所返回的精确 `funding_time`，以相同 `(instrument_id, hour_time)` 写入实际版本并设置 `is_actual = 1`；
7. 已保存的实际值不得被后续估算值覆盖；
8. 按照“一时间点只保留一个费率”的规则，结算整点不同时保存针对下一结算周期的新估算值。
9. 进程启动时只扫描最近 24 小时内 `funding_time` 已到的记录；同一 `(instrument_id, funding_time)` 只要存在实际版本就不补查，否则去重后交给对应交易所的同一个串行 worker。错过多个历史重试时点时只立即查询一次，不形成启动请求突发。发生合约版本字段迁移后，补查集合还包含与当前启用流在交易所、市场、代码及资产身份上兼容的旧 `instrument_id`，但实时盘口和估算资金费率只写当前版本 ID。

`hour_time` 是本表的整点逻辑键，继续保持秒和毫秒为 0；`funding_time` 是交易所定义的实际或目标结算时刻，不得为了匹配 `hour_time` 而截断。当前不记录 REST 请求时间或取得实际值的时间，因为它们不参与套利分析。

本表不保存资金费率上下限、结算标记价格、结算状态、资金费间隔或分别独立的估算/实际费率字段。需要研究资金费规则边界、指数结算或标记价格偏差时，应另建低频规则或指数数据表，不扩充本表。

## 5. 收益路线与收益观测

表名：`yield_route`、`yield_observation`

两表为一对多关系：`yield_route` 保存收益产品及资产路径的稳定定义，`yield_observation` 通过 `yield_route_id` 保存该路线在某个 UTC 时刻、某个额度档位的完整收益快照。路线主键为 `yield_route_id`；观测逻辑键为 `(yield_route_id, observation_time, tier_no)`，并按 `observation_time` 的月份分区。

收益数据量较低，每次有效采集直接保存完整快照，不使用盘口的分钟锚点和秒级差量编码。时间统一使用 UTC，利率、金额、费用、额度和兑换比例均使用 `Decimal`，不得经过二进制浮点数。

收益批次先验证来源值的符号、范围和语义，再用十进制运算向零截断到 `Decimal(38,18)` 的 18 位小数，之后再次验证；不允许借截断隐藏负利率、超出 100% 的费用，或把必须为正的兑换比例截成零。整数部分必须小于 `10^20`；`pool_cash` 仍要求来源值原本就能精确表示为 18 位小数。来源 payload hash 保留原始响应的证据身份，未知值保持 NULL。底层通用 codec 仍拒绝未标准化的超精度值。

MySQL 实时收益写入以 `collected_at`（UTC 毫秒）作为同一观测逻辑键的刷新版本：较新的采集可更新来源修订值与摘要；较旧批次重试不覆盖新值；相同版本必须整行一致，否则整批回滚。已有区块高度或哈希不得改变或丢失，最终性只能保持或提升。路线分配、观测刷新与整批提交仍在同一事务中，不改变表结构或主键。历史迁移的通用不可变行冲突检查保持，恢复迁移时仍须显式处理与实时刷新重叠的历史行，不能自动覆盖新数据。

收益路线和观测独立于盘口及资金费率表：收益率中不合并永续资金费率，也不重复保存做空市场深度。`unbonding_seconds` 为 `Nullable(UInt64)`：`0` 表示明确没有制度性等待，正整数表示协议规定的固定等待秒数，`NULL` 表示等待信息未知或无法预先确定固定秒数；该字段不保证流动性充足或实际即时到账。完整字段字典和筛选语义见 [ARB-0016 收益数据采集设计](arbitrage/strategies/arb-0016-yield-data.md)；TRX 的 JustLend 与原生质押采集、区块锚点和写入规则见 [ARB-0016 TRX 收益采集实现设计](arbitrage/strategies/arb-0016-trx-yield-implementation.md)；SOL 第一阶段的 bSOL、JitoSOL、mSOL、验证者白名单和 Marinade Native 见 [ARB-0016 SOL 收益采集第一阶段实现设计](arbitrage/strategies/arb-0016-sol-yield-phase-1.md)。

[AVAX 第一阶段实现](arbitrage/strategies/arb-0016-avax-yield-phase-1.md)复用这两张表保存 OKX、Aave V3/V4 的固定来源历史曲线，不修改 DDL。Aave 历史日期标签对应来源平均 APY，不能当即时利率；同一路线不混写当前快照或其他聚合窗口。历史没有给出的费用、额度及状态保留未知，不用当前值填充。

[AVAX 第二阶段实现](arbitrage/strategies/arb-0016-avax-yield-phase-2.md)在同一 `yield_observation` 表增加以下两列。模型、DDL、writer 和旧表幂等迁移已实现，并已迁移生产库：

| 字段 | 物理类型 | 语义 |
|---|---|---|
| `pool_cash` | `Nullable(Decimal(38,18))` | 指定池合约的底层现金余额，单位 `redeem_asset_key`；不代表保证可立即赎回金额，也不复用 `remaining_capacity`。 |
| `redemption_window_seconds` | `Nullable(UInt64)` | 完成退出等待后的有限申领窗口长度；正数表示有限窗口，0 表示实读零长度、没有可用窗口，未知/不适用时 NULL；0 不是无限期。 |

两列都是同一路线、同一区块的观测属性，不增加第三张表。新版 `InitSchema` 同时包含建表定义及旧表的 `ADD COLUMN IF NOT EXISTS` 迁移；writer 使用显式列名，不依赖新旧表的物理列顺序。旧数据和旧 collector 默认 NULL，不回填，不改变逻辑键或分区。第二阶段读取固定 finalized block hash 的合约状态，`finality=finalized`，不是把无区块的页面/APY 或多个 latest 读数拼在同一锚点上。

## 6. 盘口恢复关系

查询某交易流在某分钟内第 `n` 秒（`0 ≤ n ≤ 59`）的盘口时：

1. 按 `(instrument_id, minute_time)` 读取 `order_book_minute` 的完整盘口；
2. 检查 `valid_bitmap` 的第 `n` 位，若为 `0` 则该秒盘口无效；
3. 读取同一 `minute_id` 且 `second_offset ≤ n` 的全部差量；
4. 按 `second_offset` 从小到大应用差量：数量为 `0` 则删除该价格，数量大于 `0` 则新增或覆盖该价格的数量；
5. 买盘按价格从高到低、卖盘按价格从低到高排序，返回本分钟保存的全部深度（5/10/50档）；返回 `StoredDepth`，稀疏盘口可以少于此上限，超出上限则报错。

分钟完整盘口和差量表之间不跨分钟依赖；下一分钟第 0 秒的完整盘口是新的独立恢复起点。

## 7. 整分钟完成屏障与批量写入

采样器在每个 UTC 分钟切换时发送一个不可变的 `CompletedMinute{MinuteTime, Batches}`。`Batches` 按 instrument ID 排序，只包含第 0 秒有有效 anchor 的交易流；即使没有有效 anchor，也发送一个空 envelope，明确该分钟已收齐。一次全部来源的采样超过下一秒边界时，采样器终止并丢弃当前未完成分钟。

完成分钟的 channel 最多缓存 2 个 envelope；`MINUTE_QUEUE_CAPACITY` 的计量单位是已入队和 writer 正在处理但尚未完成的 instrument batch，空 envelope 按 1 计。未配置时使用 `max(512, 2 × sample_sources)`，显式容量不足两个完整分钟时拒绝启动。queue 满、最老未写完分钟达到 45 秒或写入重试失败均终止采集，不静默丢弃后继续运行。

writer 在第一条 insert 前校验整个 envelope，再按最多 100 个 instrument 分块，不跨分钟拼块。每块先批量写入全部 `order_book_second_delta`，再批量写入对应 `order_book_minute` 可见行。后者成功前，该块的新分钟不具备恢复入口。部分失败时重试完全相同的块；两表使用既有逻辑键和 `ReplacingMergeTree`，查询使用 `FINAL`，因此重复写入不改变回放结果。空 envelope 不写事实表。该机制不承诺跨表事务。

## 8. 规范化交易对映射历史

表名：`instrument_canonical_mapping`；逻辑键：`(instrument_id, mapping_revision)`；引擎：`ReplacingMergeTree`，按逻辑键排序。

| 字段 | 类型 | 含义 |
|---|---|---|
| `instrument_id` | `UInt32` | 已登记的场内 USDT 线性永续交易流。 |
| `mapping_revision` | `FixedString(64)` | 映射算法版本和严格字典规范化 JSON 的 SHA-256 小写十六进制摘要。 |
| `canonical_market_key` | `String` | `<canonical_base_asset>-USDT-PERP`，用于跨交易所分组。 |
| `canonical_base_asset` | `String` | 满足 `[A-Z0-9][A-Z0-9._]{0,31}` 的规范化基础资产。 |
| `canonical_quote_asset` / `canonical_settle_asset` | `String` | 本版均为 `USDT`。 |
| `canonical_base_units_per_venue_base_unit` | `Decimal(38,18)` | 1 个场内 BaseAsset 单位对应的 canonical base 数量，严格大于 0。 |
| `mapping_kind` | `LowCardinality(String)` | `identity` 或 `alias`；identity 必须保持原始 base，且 factor 为 1。 |
| `recorded_at` | `DateTime64(3, 'UTC')` | 该 revision 行首次准备写入的时间，重试保持不变。 |

异名字典按 `(exchange, market_type, exchange_symbol, venue_contract_version)` 精确匹配，不支持通配版本。它不覆盖原始 instrument 字段和盘口数值。查询跨交易所数量和价格时必须使用：

```text
raw_price = price_tick × price_tick_size
raw_size = qty_lot × quantity_step_size
canonical_base_quantity = raw_size × contract_multiplier × canonical_base_units_per_venue_base_unit
canonical_price = raw_price ÷ canonical_base_units_per_venue_base_unit
```

每次启动为历史已登记的全部 USDT 永续及本次新入选 instrument 写入当前 revision。writer 先读取既有 `(instrument_id, mapping_revision)`：语义一致时保留原行，不一致时失败；同一次写入的重试复用原始行及时间。错误 alias 删除后，新 revision 下对应的 identity mapping 仍可解释不再入选的历史 instrument。

映射查询必须指定当前或历史 `run_id`，或者显式 `mapping_revision`；`recorded_at` 不能决定权威解释。字典 A → B → A 回滚时，当前 run 引用 A，早于 B 的 A 行仍是正确解释。精确 canonical JSON、hash 前缀和选择规则见[永续共有交易对设计](perpetual-common-universe.md)。

## 9. 每次启动的 universe 快照

`perpetual_universe_run` 保存一份启动计划，逻辑键为 `run_id`；`perpetual_universe_member` 保存成员，逻辑键为 `(run_id, instrument_id)`。两表均为 `ReplacingMergeTree`，按各自逻辑键排序。

| run 字段 | 类型 | 含义 |
|---|---|---|
| `run_id` | `UUID` | 每次非 dry-run 启动生成的新 ID，写入重试复用。 |
| `selection_revision` / `mapping_revision` | `FixedString(64)` | 此次选择和解释使用的精确 revision。 |
| `started_at` | `DateTime64(3, 'UTC')` | 本次启动计划时间，重试不变。 |
| `selection_config_json` | `String` | 参与选择的规范化配置；venue 是稳定排序的任意长度数组，包含模式、include/exclude 与容量，不包含凭据。 |
| `enabled_venues` | `Array(String)` | 稳定排序的启用交易所。 |
| `canonical_include` / `canonical_exclude` | `Array(String)` | 稳定排序的规范化交易对选择条件。 |
| `canonical_group_count` / `instrument_count` | `UInt32` | 最终规范化组数及场内 instrument 总数。 |

member 只有 `run_id UUID`、`instrument_id UInt32`、`canonical_market_key String` 三列。每个 canonical group 至少属于两个不同 venue，同一个 group 不得重复同一 venue。

启动先登记 instrument、写 mapping，再无网络副作用地构造和校验所有 runtime。其后先批量写全部 member，最后写 run 行作为可见提交标记，成功后才启动网络采集。没有 run 行的孤立 member 一律忽略；重试已有 run ID 必须保持整个 run 及 membership 完全一致。

当前健康检查的 expected instruments 来自进程当前 run 的 member，不取全部历史 mapping 的并集，也不按数据库最近写入时间猜测当前进程。该快照只在启动时生成，metadata 或配置变化要到下次启动才重新选择。

`perp-check` 通过只读连接按 run 成员统计。盘口范围从 `started_at` 之后第一个完整 UTC 分钟开始，到查询时刻或同库下一次已提交 run 开始之前的完整分钟结束；旧 run 被后续 run 替代时只输出历史范围并标记 pending，不能把后续进程的数据累计为旧 run 的运行时长。差量活跃度只统计存在分钟可见标记的 delta，排除孤立差量；随机回放使用固定 seed 对有效秒做无放回 reservoir sampling，每个有效秒入选机会相同。

资金费率按小时桶统计，当前 run 的初始小时可能包含原有行，不能证明其插入进程；旧 run 的最后一个不完整小时也不计入。验收同时报告估算/实际行数并检查小时数量、陈旧状态和 24 小时窗口内是否出现实际确认。`system.parts` 的压缩空间属于整个数据库，不是单次 run 的增量。`passed_checks` 只表示这些数据库和回放检查通过，`full_soak_verified` 始终为 false；重叠进程、连续运行、CPU/内存/网络、限速响应和采样/写入 deadline 仍须独立运行证据，不能从事实表没有 `run_id` 的数据推断。

## 10. 期权采集

2026-09-21 已按 [ARB-0009 R4 设计](arbitrage/strategies/arb-0009-options-collection-design.md) 实现固定合约清单采集。[离线基础](arbitrage/strategies/arb-0009-options-phase-1.md) 与[实时实现](arbitrage/strategies/arb-0009-options-live.md)共享规格、规则与盘口事实表，使用各自提交表。2026-10-04代码已增加新挂牌自动发现R5；其模型见第10.4节，[实现与验收](arbitrage/strategies/arb-0009-options-lifecycle-implementation.md)记录本次验证。R5已于2026-10-04部署到常驻服务；[部署验证与旧历史坏分片限制](arbitrage/strategies/arb-0009-options-lifecycle-deployment.md)另有记录，已有R4读取语义保留。

- 已扩展 `instrument` 的类型与版本校验，并实现定类型经济规格、组合腿和交易规则。实时run/成员、所选元数据复核证据与状态、指数观测及分钟发布见10.3；完整结算/费用/保证金规则、原生combo和全链BBO按需扩展。行权价币种、权利金币、结算币、native amount 和合约张数不能混用。
- 经济/编码规格保持不可变；下单tick、分段tick、最小量与增量放入独立 `derivative_trading_rule`，逐秒引用当时规则。该链路 `contract_multiplier=1` 表示保留native amount，USD名义和combo必须经专项换算，不能直接套第8节USDT永续的基础币换算公式。
- 已提供 `derivative_book_minute`、`derivative_book_second_delta`、`derivative_book_quality_minute` 的显式建表与离线读写接口。新期权仍保存每秒前10档，交割期货默认5档，`stored_depth=10/5`；允许已知空边/空书，combo允许零价和负价，因此不复用现有正价/双边盘口的校验语义。
- 新分钟起点用等长typed价格/数量数组，各侧长度0–10，真实数量大于0；价格0可为真实combo档位，只有差量数量0表示删除。没有有效第0秒锚点的分钟不生成可回放盘口，但保留独立质量记录。
- 每秒质量按分钟60槽保存；所选合约BBO可直接从L2提取，无需先建全链quote表。真实源时间、接收时间、采样截止、连接代次、缺失原因与盘口状态分开表达。
- 实时分钟需关联本次run及不可变 `batch_id`，先写数据后发布提交标记；跨表部分写入不可见，重试复用原内容与身份。当前 `derivative_book_foundation_commit` 仍只接受离线来源，不能冒充实时提交。所选指数和低频元数据使用定类型字段，不进入资金费率或收益表。
- 低频实时来源按本次请求范围核验完整性，保存来源时间/采集时间、URL和payload hash；规则区分生效与知悉时间，失败不刷新历史事实。盘口的 `delta_bitmap` 标明应存在差量的秒，提交后缺行必须返回incomplete，不能解释成无变化。
- 现有5/10档及旧50档历史的编码、物理列和回放路径保持本字典第2–3节语义；不得先截旧50档起点再应用旧差量。

### 10.1 已实现的离线基础表（显式初始化）

`DerivativeSchemaStatements`/`InitDerivativeSchema` 提供以下六表；原 `InitSchema` 不调用它们，app仅在期权任务启用时通过 `InitOptionsLiveSchema` 初始化。均为 `ReplacingMergeTree`，以下是实际代码的逻辑键和数据口径。热路径没有JSON、字符串价格或二进制浮点数。

| 表 | 排序/去重键 | 实际字段与约束 |
| --- | --- | --- |
| `derivative_contract_spec` | `instrument_id` | 源ID、创建时间、经济定义hash、normalization_version、linear/reversed、native amount种类与币种、Decimal(38,18) contract_size、index、结算语义ID、Nullable期权类型/strike/strike_currency、等长组合腿ID/有符号比例/Decimal换算系数数组、首次证据hash。与instrument经济版本一起不可变；本阶段不单独建option和combo子表 |
| `derivative_trading_rule` | `trading_rule_id FixedString(64)` | instrument、Decimal tick/min amount/step、等长分段阈值/tick数组、observed_at/known_from/effective_from、明确first_observed或published口径、来源URL与payload hash；时间DateTime64(6,UTC)；规则变化新增ID |
| `derivative_book_minute` | `(instrument_id,minute_time,batch_id)` | id、encoding_version=1、stored_depth=5/10（期权固定10）、signed、valid_bitmap、delta_bitmap、四个买卖price Int64/qty UInt64数组，每侧0–stored_depth档；价0不是填充；按月分区 |
| `derivative_book_second_delta` | `(minute_id,second_offset,batch_id)` | 秒1–59，买卖各price Int64与qty UInt64等长数组，qty=0删除；按minute_id内分钟推导月份分区 |
| `derivative_book_quality_minute` | `(instrument_id,minute_time,batch_id)` | signed、整份该instrument分钟内容hash；sampled/stream_valid/replay_valid/market_known/market_open位图；所有时间、epoch、序号、交易规则、市场状态依据、原因和实际档数按60槽保存；时间统一Nullable DateTime64(6,UTC)，UUID/hash未知为NULL；按月分区 |
| `derivative_book_foundation_commit` | `(run_id,minute_time)` | batch_id、origin仅fixture/synthetic、evidence_hash、prepared_at、完整instrument_ids与member_hashes数组、anchor_count与delta_count；按月分区；最后写以发布离线分钟 |

质量原因的UInt8编码由 `model.DerivativeReason` 定义：0=有效、1=未就绪、2=断线、3=断序、4=解析错误、5=元数据未知、6=采样迟滞、7=缺锚点、8=资源超限、9=时钟异常、10=明确关闭或已到期；市场状态依据0=未知、1=生命周期接纳时间、2=目录发布时间。market_known=0时market_open不能当作明确关闭。当前实时实现使用依据2，不宣称具有独立生命周期频道的实时确认。

分钟内容摘要包含锚点、全部差量和60槽质量，使用确定性二进制编码；nil和空价量数组等价，时间按UTC值编码。batch摘要再包括run、分钟、origin、证据hash、准备时间和有序成员摘要；重试不得修改这些字段。读取完整成员再校验各行存在性、delta_bitmap与内容摘要，失效秒和缺锚点返回明确质量，提交后缺数据返回incomplete。离线提交标记只证明指定离线批次完整，不能证明实际采集已运行。

每批最多448个instrument，预校验后每100个instrument分块写入差量、快照、质量，全部成功后写提交标记；进程内互斥，但不声称支持多writer。离线期权时间不改写成查询时间。真实原始数值按1e-8编码，source_contract_size不再重复乘到native amount。

### 10.2 第一阶段离线测量

2026-09-21 的整日10档合成测试通过：单流1440分钟的快照、差量、质量及提交标记合计，活跃输入为2,282,784压缩字节，静默输入为1,121,074压缩字节。最终查询代码在相同整日样本上抽取100个分钟，完整单成员批次读取与回放P95分别为31.49 ms、31.54 ms。测量口径、分表占用及测试记录见[离线实现记录](arbitrage/strategies/arb-0009-options-phase-1.md#整日合成容量与查询测量)。这些是离线合成结果；真实活跃/冷门交易流的日量与查询测量在接入后随持续采集记录，不再设置七天运行准入要求。

### 10.3 实时新增四表

`InitOptionsLiveSchema` 显式初始化原六表与以下四表。期权关闭时不执行这些DDL；新表同为 `ReplacingMergeTree`，不是JSON大表。

| 表 | 排序/去重键 | 实际字段与约束 |
| --- | --- | --- |
| `options_live_run` | `run_id UUID` | `run_hash FixedString(64)`、UTC微秒起始时间、REST/WS URL、选择方法；有序等长成员instrument ID/原始symbol/经济定义hash/index ID数组、唯一有序index ID集合；选择参考symbol、Int64买卖价格tick、源/接收时间、hash等长数组。一个run最多32个合约、4个指数，校验C/P及同到期期货配对 |
| `options_metadata_observation` | `(run_id,attempt_id,instrument_id)`，按观测月分区 | scope、symbol、URL、UTC微秒requested/observed时间、payload hash、status、definition hash、rule ID、state、active、scope_complete及UInt32 raw/accepted/excluded计数、row_hash。相同响应的成员共享attempt UUID，成功scope满足raw=accepted+excluded；request/parse_error不能伪造完整计数，missing/definition_changed来自完整scope但不能携带当前有效规则或状态 |
| `options_index_minute` | `(index_id,minute_time,batch_id)`，按分钟月分区 | row_hash与固定60槽：`prices Array(Nullable(Decimal(38,18)))`、source/received UTC微秒Nullable时间数组、UUID epoch数组、UInt8 state数组。采样时间由minute+槽位推导 |
| `options_live_minute_commit` | `(run_id,minute_time)`，按分钟月分区 | batch_id/run_hash、UTC微秒prepared_at、有序instrument_ids/member_hashes、UInt32 anchor_count/delta_count、index_ids/index_hashes。整个分钟的数据写成功后才发布；内容不同的同键重试拒绝 |

指数state为0=缺失、1=本秒收到、2=沿用此前观测、3=断线、4=无效。1/2必须有正价格、真实源/接收时间与非零epoch；其他状态价格和时间均为NULL，不能拿断线前价格冒充当前值。held只表示健康连接上暂未变化，不改写原始源时间；指数失效与盘口有效性独立。

元数据响应没有独立来源时间时，`observed_at` 明确采用完整响应解析后的采集时间；所有实时scope保留URL、请求窗口、hash和解析计数。规则和证据写入成功后才经有序入口发布，质量行中的rule/state发布时间不能早于观测，也不能晚于对应采样秒。当前规则引用必须能在本run的成功元数据行中核验，读取也会检查；缺证据、缺指数或缺盘口行返回incomplete。

实时批次ID绑定run hash、有序完整成员和指数摘要，使用不同于离线信封的身份域。读取 `LoadOptionsMinute` 只认实时提交表，核对持久化run/规格、完整成员、规则及元数据证据后逐秒回放；`LoadDerivativeBookEnvelope`仍只认fixture/synthetic离线提交表。已存在的历史50档使用原回放路径。

秒入口的256条/16MiB上限、时钟异常或持续写入失败会结束当前run并重试，尚未完成/提交的分钟形成缺口。逐秒冻结迟于T+250ms则保存无效质量；缺第0秒锚点时该分钟不可回放。停止最多45秒排空完整分钟，末尾不完整分钟不提交。实际短窗口容量与查询测量见[实时记录](arbitrage/strategies/arb-0009-options-live.md)。

### 10.4 期权自动发现模型（R5）

2026-10-04代码已按[R5设计](arbitrage/strategies/arb-0009-options-lifecycle-design.md)将auto改为四族全部新挂牌期权的持续发现、独立准入和自动到期退出。以下五表由 `InitOptionsCatalogSchema` 幂等初始化，已在生产及隔离库验证，并于2026-10-04部署到常驻服务。第10.1–10.3节仍适用于原固定清单及其历史。

| 新表 | 逻辑键 | 语义与约束 |
| --- | --- | --- |
| `options_catalog_scope_observation` | `(observation_id,scope)` | 定类型目录成员及排除数组，完整scope数量校验、来源URL、请求/观测时间与payload hash；失败不能刷新当前目录 |
| `options_lifecycle_observation` | `observation_id` | state/platform/status及协议错误证据，source/received时间、epoch/接纳序号、目标身份、状态、锁定/maintenance及hash；creation完整定义存目录表；未知不伪造open |
| `options_collection_plan` | `(session_id,revision)` | 不可变plan ID/hash、全局前驱ID/hash、生效UTC分钟、唯一成员owner run、证据及pending/excluded原因；跨session不分叉，生效分钟严格递增 |
| `options_catalog_quality_evidence_minute` | `(run_id,instrument_id,minute_time,batch_id)` | 固定60槽状态依据、规则目录、status基线、maintenance、单指数锁定证据，以及生命周期epoch/确认时间；未知引用NULL，质量与规则知悉分别核验 |
| `options_catalog_live_minute_commit` | `(run_id,minute_time)` | 绑定plan/run、完整成员及质量证据/指数摘要，全部事实成功后发布；分片缺失不会隐藏其他已提交分片 |

R5复用整数价量事实（期权10档、交割期货默认5档）和固定成员run；新增 `catalog_v2` 准入独立于C/P/期货配对，旧selection及旧run/hash/commit读取不变。R5分钟使用独立v2摘要域，含计划和60槽证据；不直接修改旧结构的JSON序列化来重算旧摘要。原旧50档回放不变。

目录定义与规则整批校验后批量写入；同批或已存重复定义保留首次EvidenceHash，规则重试保留原来源及知悉/生效时间和内容ID。分钟引用只批量读取目标instrument及所用规则，缺引用、owner不符或任何秒早于规则知悉/生效仍拒绝；批量化不改变表结构或分钟编码。

持续运行修订分别保存规则与目录状态的真实观测时间，采样器和writer均按所引用目录证据的35分钟有效期校验。完整目录可更新已确认且没有较新屏障的目录状态引用；当前生命周期epoch的WS状态仍由其独立事实确认。WS源时间只在同一epoch内排序，不与HTTP本地观测时间比较；重连重置序号及源时间排序，旧epoch已应用序号不能确认新事件。creation不能借用后来状态通知的接纳序号或覆盖当前epoch已确认的WS状态。

生命周期按最多128条、5ms聚合窗口写入，逐条保留原身份、内容hash、epoch和接纳序号；批内及已存内容冲突整批拒绝，歧义重试复用身份，落盘成功后才发布。工作与重试数量界限为`2*MaxBooks+512`，同时受共享32MiB字节预算约束。资源拒绝保留有界symbol屏障，定时fresh单合约确认可恢复未知、新增或尚未准入的未到期成员；请求受inflight合并与退避控制。到期/terminal停止健康恢复请求，持久化terminal可清理无entry且屏障匹配的恢复意图。原始响应先在内存校验，再持久化定类型证据，不因恢复而伪造open。

成员退役和部分迁移合并为每个physical的待退订集合，最多512频道一批；subscribe和unsubscribe共用一个待ACK批次，移除路由也不能提前释放未ACK的订阅屏障。历史频道在同一epoch不复用；连接预算耗尽且历史使用阻塞时，有界轮换一个稀疏epoch并失效其盘口，待新连接和快照确认后恢复。冷却期间保留恢复意图。以上修订不改变表结构、价格数量编码或旧历史回放语义，验证记录见[持续运行修复](arbitrage/strategies/arb-0009-options-sustained-repair.md)。

2026-10-06 按用户要求取消期权原始响应归档。公开目录、单合约与生命周期响应先在内存校验 SHA-256，再按原顺序写入定类型事实、发布状态；读取与回放只依赖数据库。旧 `OPTIONS_EVIDENCE_DIR` 接受但不使用，目录缺失或旧 gzip 损坏不影响采集。时间为UTC `DateTime64(6)`，观测与计划携带不可变内容hash；序号/计数/身份为整数，金额仍为Decimal，未增加通用JSON事实表。来源 hash 不能还原原始 JSON，也不承诺原文仍可下载。质量、状态依据、分钟提交表仍保留，批次摘要与历史查询语义不变。严格字段定义见 [options_catalog_schema.sql](../internal/storage/clickhouse/options_catalog_schema.sql)。

新合约的定义、规则和状态确认后立即预订阅；计划默认在当前UTC分钟之后第二个边界生效，为落盘及预热留出1–2分钟，未来计划队列有界，迟到计划顺延。缺第0秒锚点如实不可回放。T切换后旧writer仅排空<T的冻结分钟，新run立即采样T；查询按唯一生效计划认证owner。全量检查通过计划返回预期/有效/缺失合约及未提交分片，不能只读最新一个run。分钟验证/提交限三路并发，控制证据使用独立连接额度与锁集合，不让整个分钟hash计算阻塞采样入口。

2026-10-09 起 R5 将写库失败与行情接收分离：每个分片保留最多两个待写完整分钟及一个在写分钟。采样线程将已完成的不可变分钟交给后台，后台编码最多四路并发；按展开后的保守大小先预留独立缓存预算（入口预算的两倍，默认128MiB），压缩后释放差额，避免编码占用采样窗口，也避免数据库积压耗尽网络入口预算。缓存只在内存保留，不生成备份文件。等待三路写入槽位不消耗40秒执行预算，取得槽位后才解压并开始计时；暂时失败按原身份和内容退避重试，不销毁采样分片或重订阅。队列或缓存预算耗尽时明确记录 `options minute not queued`，该分钟没有提交标记、查询仍报告缺失，后续接收继续。停服仍最多45秒排空，未落库的内存分钟不能跨进程恢复。盘口连接新建按全局每秒一个错开，初始及恢复订阅每批最多64频道、确认后继续，完整覆盖范围和保存深度不变。

2026-10-09 的持续恢复修订为完整目录刷新预留独立16槽队列，并在共享REST工作线程中优先处理；逐币恢复仍用原64槽队列，两者继续共用请求限速和证据字节预算。刷新时钟使用响应的真实 `observed_at`，同时检查未到期成员实际引用的规则和HTTP状态证据，30分钟开始刷新、35分钟后仍如实无效。批量刷新在途时合并普通已准入成员的恢复查询；未知协议、尚未准入成员及未确认状态屏障仍须独立确认。只取消尚未获取源响应且已被新证据确认的重试，已准备证据的歧义写入重试保持原身份。Deribit WS的 `received_at` 使用独立socket读取线程读完消息时的UTC微秒时间，不使用稍后回调处理时间；控制发送时隙优先回复 `test_request`，普通命令保留到下一时隙，共享限速不绕过。上述修改不改变表结构、档数和差量规则，不补造旧缺失秒。

### 10.5 辅助表紧凑存储

2026-10-07 增加辅助表的无损紧凑物理格式。上文表字典继续描述读取口径；实际新建表的物理列由 [options_compact.go](../internal/storage/clickhouse/options_compact.go) 转换生成。目录 SQL 是逻辑模板，不能跳过该转换直接作为新格式 DDL。采集启动只幂等建表，已有表不会自动迁移或删除。

适用表：`derivative_book_quality_minute`、`options_catalog_quality_evidence_minute`，以及离线、固定清单实时、自动发现实时的三种分钟 commit。盘口快照、按价格差量、价格数量单位及深度版本保持原定义。

- SHA-256 摘要以完整 `FixedString(32)` 保存；旧 64 字符字段通过 `ALIAS lower(hex(...))` 精确还原。完整成员摘要仍用于单批核验，未缩短摘要或取消成员证明。
- 每分钟的 60 槽时间、连接、规则及状态数组改存 `compact_<field>_offsets` 和 `compact_<field>_values`：第一个偏移必须是 0，偏移严格递增且小于 60，两个数组长度相同。相同状态只存一次，变化才存下一项。`NULL`、零和已知空值保持区分。
- 时间值保存相对 UTC 分钟起点的有符号微秒差，保留原精度和负偏移；使用整数运算及 `fromUnixTimestamp64Micro` 还原。时间/序号值采用 Delta＋ZSTD，其他紧凑列使用 ZSTD。每个原数组由 ALIAS 按变化区间展开为完整 60 槽，展开工作量与槽数成正比。
- 查询显式选择原逻辑列，原内容 hash、batch ID、发布时间及采样截止校验均保持成立。`SELECT *` 默认不包括 ALIAS；需要原口径的 SQL 应列出逻辑字段。writer 按连接首次检测物理格式，旧表仍可写，新表写紧凑列；切换表后必须重启 writer。
- 本次精简删除重复存储，精确采样完成时间、序列末值和档数仍可无损恢复。取消这些诊断信息的长期留存是另一项有损取舍，未混入此次迁移。

`go run ./cmd/options-compact-plan` 只输出五类辅助表的建表与转换 SELECT，不连接或修改数据库。迁移须先复制旧事实、逐字段核验，再停止 writer 补齐尾部，确认逻辑成员数一致，交换表并重启；同时继承生产表的插入、目录和合并 fsync 设置。原表只有在原查询/摘要/每秒回放和新写入验证通过后才删除；旧摘要域无需伪造或重算。旧库和新格式、旧 10/50 档盘口的读取兼容均在隔离 ClickHouse 测试中验证。

## 11. Ethereum DEX 协议兑换实验（2026-09-22）

本次新增五张专项表，DDL 入口为 `DEXSchemaStatements` / `InitDEXSchema`；`collector --init-dex-schema` 只建表并退出。当前 `crypto_market_info` 已实际建表，DEX 持续采集默认关闭。完整范围、使用方法与验收记录见 [DEX 实现说明](dex-arbitrage-implementation.md)。原盘口、期权、资金费率和收益模型不改。

通用列：`chain_id UInt64`、`block_number UInt64`、`block_hash/manifest_hash/batch_id/payload_hash FixedString(32)`、`block_time DateTime64(6,'UTC')`。地址使用 `FixedString(20)`；原子金额、费用用 `UInt256`，未知数量为 `Nullable(UInt256)`，应用通过 `big.Int` 读写，不经浮点。所有表按 `toYYYYMM(block_time)` 分区，不设自动删除 TTL。

| 表 | 额外字段及语义 | 引擎、排序/去重键 |
| --- | --- | --- |
| `dex_block` | parent_hash、base_fee_wei、fee_recipient；received_at/available_at UTC微秒时间；capture_mode、canonical、finality、revision；log/receipt/quote三项coverage；expected_quotes/actual_quotes/log_count/receipt_count、三项成员SHA-256、committed | `ReplacingMergeTree(revision)`；chain+manifest+height+hash |
| `dex_sky_state` | module_id；tin/tout/buf/dai_cash/usdc_pocket_cash/pocket_allowance；vat_live/dai_join_live/dai_join_ward/usds_join_ward；identity_ok/state_complete/reason | `ReplacingMergeTree`；chain+manifest+height+hash+batch+module |
| `dex_route_quote` | quote_id、quote_role、route_id、quote_mode、token_in/out、requested_amount_raw；amount_in/out_raw、dust_dai/usds；v3_input/output_token、v3_input/output_raw、sqrt_price_after_x96、ticks_crossed、quoter_gas_estimate；status/reason/available_at | `ReplacingMergeTree`；chain+manifest+height+hash+batch+quote_id |
| `dex_log` | tx_hash、tx_index、log_index、emitter、topics Array(FixedString(32))、ABI data String（二进制）、event_type、removed | `ReplacingMergeTree`；chain+manifest+height+hash+batch+tx+log_index |
| `dex_tx_receipt` | tx_hash/index、sender/recipient、has_recipient、tx_type、value_raw、input_selector Nullable(FixedString(4))、calldata_hash、status、gas_used、effective_gas_price、receipt_log_count、receipt_hash、available_at | `ReplacingMergeTree(available_at)`；chain+height+hash+tx |

`capture_mode` 为 `live`（当时新块）、`backfill`（补头/日志，不补历史金额报价）、`research`（显式历史样本，不能计入实时完整覆盖或确认新窗口）。`finality` 为 head/safe/finalized/orphaned；三项覆盖为 missing/partial/complete。空日志请求成功是 complete 且 count=0，请求失败是 missing，绝不混用。quote status 为 ok/unknown，unknown 必须有具体原因，缺少任一必要值不得填成零。

`quote_id` 是 batch、role、route、输入输出币、模式及请求原子金额的确定性摘要。每批固定56条策略金额观测和2条参考观测，失败档也保存 unknown。quote成员摘要按quote_id排序，日志按log_index排序，回执按tx_hash排序，再对定类型序列计算SHA-256；真实读回测试检查UInt256、NULL、时间及摘要完全一致。

写入顺序为 Sky/quotes/logs/receipts，再写 dex_block 的 committed 标记；进程内写入失败保留同一不可变批次重试，未提交事实不可见。查询**先对每个block逻辑键 argMax(tuple(...),revision)，再筛canonical/committed/finality**；不能先筛旧canonical行。报价只按最新完成行选择的batch读取，并校验成员数和成员摘要。finality/重组版本保留原batch、成员、received_at与available_at，孤块事实保留而统计排除。

已经提交但缺失的日志另行补采：先写日志事实，再追加coverage完成revision，保留原quote成员与可见时间；补采的原始证据记录真实获取时间。相同hash的每条日志使用确定性定类型日志响应作为payload证据，完整RPC信封由该区块的proof引用；不能因RPC id或请求时间变化而改变同一日志事实。日志补写同样保留不可变pending直到完成。回执独立batch按available_at去重，补齐目标交易集合后更新receipt coverage/count/members，不让已完成块占住补采窗口。这些是补采完成版本；纯finality版本不重新生成数据。

2026-10-06 起主 collector 的 Ethereum DEX 适配器使用 `Archive.HashOnly`：RPC 响应、calldata 和 manifest 仅计算摘要，所需字段严格解析后直接写现有定类型表，不再另存正文文件；重试、补采、最终性及历史报告均从数据库读取。`DEX_EVIDENCE_DIR` 为兼容旧配置保留，主 collector 不再使用。SHA-256、UTC获取时间、区块锚点和批次计数/摘要保持原含义，但 hash 不承诺原文仍可回读。完整 calldata、非采集目标的 receipt 日志及未选择字段不在现有研究数据范围内，删除旧原文后不能从摘要恢复它们。固定 manifest 在程序内嵌版本中保留，不依赖运行归档。`dex-check --sample-block` 或 `--quote-costs-rpc` 显式研究命令仍可在指定报告目录保存独立研究样本；其他采集器使用共享 Archive 的策略不受影响。主 collector 旧响应目录在停写并核验历史查询后可以清理，不以原文归档替代数据库备份。

报告连接使用服务端 `readonly=1`，不执行数据库引导或DDL。成本补报只写报告目录的证据与JSON/CSV，不修改生产事实。参考1 WETH卖出价格不参与可执行gas成本计算；成本必须用同hash、实际gas数量的USDC→WETH exact-output，Quoter内部gas不作为交易总gas。

日规模存储/查询验收见[容量记录](../research/2026-09-22-dex-implementation/validation.md#日规模合成容量)：真实样本衍生的两份7,200块合成数据分别占40,849,739及34,423,988压缩列字节，另列查询耗时；不包含gzip证据、marks和真实行情变化的额外熵，不当作实采日量。

## Across 稳定币中继（2026-10-02 已实现）

独立研究库 `crypto_market_info_across` 使用八张定类型表：`across_capture`、`across_deposit`、`across_deposit_update`、`across_fill`、`across_refund`、`across_tx_receipt`、`across_order_probe`、`across_receipt_transfers`。入口 `AcrossSchemaStatements` / `InitAcrossSchema`，命令 `cmd/across-data`。字段见 [DDL](across-stablecoin-data-schema.sql)，操作见 [实现说明](across-stablecoin-data-implementation.md)。

2026-10-04按用户要求改为内存解析RPC/API响应，事实直接入库，不保存响应正文或原文备份。SHA256、来源时间、链高度/hash和最终性保留。金额用UInt256，价格/数量用Decimal(38,18)，时间统一UTC微秒；协议bytes32/address分别32/20字节。未知费用仍为NULL，请求/解析失败不制造成功空数据；链日志和收据通过保存的锚点补查，只补首次启动之后范围。

用户放弃的历史解码补采用capture新revision追加reason标记`repair_abandoned=user_requested_historical_state_unavailable`。自动partial补采跳过该标记；status仍为partial，已存事实、成员摘要、链锚点和最终性均保留。放弃修补不增加完整解码覆盖，也不代表该区间没有交易。

2026-10-10 文件系统损坏后，对全量核验明确证明成员丢失或原始紧凑证据丢失的四个capture执行了人工存储完整性撤回：追加更高revision，仅将`committed`设为false并记录`storage_integrity_loss`原因，保留原始source status、canonical/finality、身份、来源时间、链锚点、成员数量和哈希；事实行不删除、不改写。查询和恢复先选最新revision，再筛committed，不能把旧成功版本重新计入完整覆盖。此操作属于有前后字段核验记录的故障维护例外，普通`WriteAcrossRevision`和冻结重试规则仍禁止变更committed或成员。对应区间重新请求来源，使用新的capture ID、实际采集时间和证据；不得重造旧payload或沿用旧时间。具体四个记录及核验见[存储撤回审计](../research/2026-10-10-reboot-check/across-storage-withdrawal.json)。运行状态转换将这些已撤回登记完整保留在 `WithdrawnDeclarations` 中，继续排除其有效覆盖；普通未决提交或新的事实损坏仍需单独恢复，不能套用此分类放行。

`across_receipt_transfers`每份收据保存一行原生USDC Transfer全集：交易/区块锚点、token、收据成功状态、来源hash、UTC可用时间，以及等长的log_indices/senders/recipients/amounts_raw数组。index严格递增且唯一，金额为UInt256。成功解析但无Transfer保存空数组，缺少这行表示尚未获得全集，不能当成零笔。正常收据capture先写收据及Transfer事实，最后提交capture。新成员表编号7；原六组成员编码及旧数据摘要不变。查询核验最新revision、canonical/committed、精确计数及摘要。

旧留存退款收据通过`migrate-transfers`本地解析入库，新增`receipt_transfers` capture保留原始来源时间和链锚点，不修改旧事实及旧capture的成员摘要，也不向链请求历史。迁移可重跑；旧退款缺少Transfer，即使费用已知也进入现有补采队列，并优先于旧未知费用重试；已删除的普通收据原文不伪造空Transfer。退款查询只读取定类型全集。capture成员/区块时间及最终性复核还保存紧凑元数据，查询不再逐份回读RPC/API正文。重组revision只改canonical/finality/说明，旧成功版本不会重新出现。跨capture按不可变链上事实去重，采集时间和payload格式不同不算协议冲突。

旧文件用`prune-responses`清理：先核验数据库成员和紧凑元数据，再限并发删除已转存及未被引用的旧response；保留尚无完整替代采集的日志原文和未迁移退款收据，保留两分钟以内可能仍在途的旧请求。不会删除紧凑元数据、数据库或RPC额度状态。实时writer不再生成正文，所以不需要每批归档再删除的后台机制。

原始/更新条款、真实live首见、计划/实际后续probe各自保存；原始响应先收到不代表解码已可用。重启/补采不能制造历史live可见性。聚合退款仅通过同交易Transfer核验地址到账，不伪造逐单归属。整笔gas按交易去重，未知费用/过期价格保留NULL。库存及成本情景只在离线报告计算。已通过[独立代码审核与必要验证](../discuss/0013-across-stablecoin-code-review.md)，完整验收见[记录](../research/2026-10-02-across-implementation/validation.md)。

## LST 折价、官方赎回与对冲（2026-10-02 已实现）

2026-10-05 mvp11修复日志恢复：持久Next不能代替DB完整覆盖证明。读取成功后从原Start重建匹配manifest且complete/committed/canonical/finalized的连续范围，遇到真实断口可回退；数据库读取失败不改状态。已验证人工补采可衔接现有范围，未建立的旧流不以backfill选择起点；两段缺口分别保留。不改七表、成员摘要、链锚点、金额和时间编码。实际重启、1852块人工补采及8块断口修复见[运行验收](../research/2026-10-05-lst-reboot-check/report.md)。

Ethereum Lido stETH/wstETH 与 Binance ETHUSDT 首版设计见 [方案](lst-redemption-data-mvp-design.md)、[七表 DDL](lst-redemption-data-schema.sql)、[独立审核](../discuss/0014-lst-redemption-data-design-review.md)。专项表为 `lst_capture`、`lst_protocol_state`、`lst_quote_observation`、`lst_withdrawal_request`、`lst_withdrawal_finalization`、`lst_withdrawal_claim`、`lst_funding_settlement`，另复用既有 instrument 定义。已在独立库 `crypto_market_info_lst` 建表；入口 `cmd/lst-data`、`InitLSTSchema`，采集连接 `OpenLSTWriter` 只打开现有库，只有显式 `init-schema` 执行 DDL。实现见[运行说明](lst-redemption-data-implementation.md)，代码审核见[0016](../discuss/0016-lst-redemption-data-code-review.md)。

启动与来源限速见[运行说明](lst-redemption-data-implementation.md)：watch不隐式回补历史，初始化缓速，实际RPC单成员均匀发出，Binance只取10档，二分/重试共用额度和持久冷却。2026-10-03 已配置用户级 `crypto-market-info-lst.service` 并启用开机启动，只运行实时 watch，补采限于断线后的日志缺口；此前因预检网络 timeout 停止，随后按用户要求恢复并降低发送密度，但 Binance 真实 timeout 又触发停止；实际来源限制和部署状态见[运行文档](runtime-operations.md)。

时间 UTC 微秒，链上原子金额 UInt256，交易深度按 integer tick/lot；mark/index/结算mark独立用1e-8 USDT整数tick，链上/CEX USDT金额分别用1e-6/1e-8单位。当前及同量后续金额报价、请求、finalization范围、claim、actual资金费分别保存；来源缺失与失败锚点为NULL。提交成员数/摘要冻结，canonical/finality取最新revision；同刻路线和金额档不累加容量，队列等待与用户领取拖延分开，unknown不当零成本。

完整DDL已在隔离 `/tmp` 的ClickHouse local通过建表语法检查，公开来源字段验证见 [记录](../research/2026-10-02-lst-design/validation.md)。七表已另经真实 ClickHouse UInt256/NULL/微秒/摘要往返、失败重试及孤块修订集成测试。事实先写、capture 后提交；查询核对完整成员摘要，纯最终性修订不改原批次。失败日志范围不推进游标，成功空日志与失败分开。2026-10-03 的 receipts 模式复用现有事件类型：event payload hash 可指向整块收据响应，evidence root 包括逐块头部/收据和端点 canonical 核验。Bloom 阴性仅在严格有效、父哈希连续的 finalized 头部上认定；阳性校验全部交易收据、全局日志索引及重建 Bloom 后过滤队列。失败整片不提交事件，不能把截断数组当空覆盖。这是 RPC 一致性校验，没有独立 receipt trie root 验证；未增加列或通用 JSON 表。

2026-10-04（北京时间）mvp9固定新日志段与旧缺口使用独立持久游标，共用既有七表、锁和限速；每条capture仍只表示实际校验并提交的区间。主RPC区块锚点与独立日志源末头hash必须一致，历史queue实现状态由支持archive读取的LogRPC核验。新段完成不会覆盖旧缺口；报告只合并相邻或重叠的真实complete/finalized/canonical区间，跨缺口等待与兑付关联保持unknown。未新增通用JSON表或更改事件整数、摘要和键语义。

2026-10-04按用户要求，一次性删除已提交并通过成员/摘要/类型校验的旧原文，保留近期、未提交及必要核验样本和共享依赖；不删除数据库行、payload hash、来源时间或链上锚点，不改变七表及提交语义。LST报告核验数据库成员摘要，不依赖原文文件；对已清理的hash不再具备原始响应回读验证能力，不能将清理后的报告成功宣称为原文重新核验成功。未启用自动TTL，实际删除与服务核验见[清理记录](../research/2026-10-04-lst-cleanup/report.md)。

mvp10在LST `Batch` 增加仅用于本机pending恢复的 `RawEvidenceHashes` 文件清单，未增加数据库列或改动成员摘要。该清单随原批次持久化，数据库写入成功后即清理原文；写库失败保持原ID、UTC时间及已知零/NULL语义重试。旧gob缺省清单为空，兼容读取。七表继续保存来源hash、时间、链上位置和事实摘要；成功响应不长期保留，失败/解析不完整仅保留有界诊断样本。report说明原文留存策略，成员校验不依赖文件存在；详细边界和实际核验见[运行说明](lst-redemption-data-implementation.md)。

`lst_withdrawal_finalization.from_request_id` 保存官方事件原始首 ID，范围为 **[from,to]（两端包含）**；合约发出的是 `lastFinalizedRequestId+1`。`from==to` 是合法单请求完成，匹配不得漏掉首 ID，不对源值减 1。

每轮八条 entry 身份保持稳定。2026-10-03 的生产配置每两分钟调度一个金额的一条路线，其余七条；CLI 兼容旧默认每分钟 A/B 两条、其余六条。未调度成员 `timing_status=not_scheduled`、`reason=not_scheduled_this_round`，所有来源/数值观测为空、各腿 unknown；完整性校验禁止这类成员携带旧数据。健康统计区分未调度、已调度失败及 `cex_ten_level_capacity_insufficient`，不能更改旧窗口分母来宣称采集改善。

`partial` 是批次成员完整性状态，不能直接判整批全部可用或全部无效；每条报价分别检查 buy/conversion/exit/hedge/timing 状态。head、canonical、committed 也分别表示不同条件。历史报告先排除未接受的 capture，只有接受的批次才回读并核验成员摘要；健康 SQL 不代替这一步。实际资金费按 instrument/funding_time 去重，重叠窗口不能重复累加现金流。只读检查见 [LST 健康 SQL](lst-data-health.sql)。

## JustLend 能源租单清理 keeper

2026-10-08 本地恢复状态升级为 v2 压缩检查点：主 `state.gob` 原子引用按内容哈希命名的候选缓存，先同步缓存再提交主文件，提交后清理旧缓存；状态未改变时跳过写入。预算、冷却、游标与冻结重试批次保持同步持久化，旧单文件状态可迁移读取；不改变七张数据库表、事实摘要、UTC/整数/Decimal 或查询语义。备份需停服并保存主文件及引用缓存；旧二进制不能直接读取新格式。详见[恢复说明](justlend-keeper-data-implementation.md#限速与恢复)。

本链路只采集/校验/查询公开数据，获利分析由其他程序完成。独立库 `crypto_market_info_justlend_keeper` 有七表：capture、index_page、indexed_event、rental_event、tx_receipt、probe、cost_observation（均带 jl_keeper_ 前缀）。定义见[DDL](justlend-keeper-data-schema.sql)，运行与实现见[实现说明](justlend-keeper-data-implementation.md)，[当前设计](justlend-keeper-data-mvp-design.md)和[代码审核](../discuss/0017-justlend-keeper-data-code-review.md)。

金额 UInt256 sun、资源/费用有范围校验的整数、BBO价格/数量 Decimal(38,18)；时间 UTC微秒、地址含0x41前缀21字节、hash32字节。Rent/Return旧/扩展ABI按精确topic及word数严格区分；扩展字段 security_deposit_sun、rent_index 为 Nullable(UInt256)，旧版NULL。实时响应保存SHA和来源时间，模拟为node_latest_unpinned，不能冒充固化历史。

### 源观测与核验事实

TronGrid源页内存严格解析并计算SHA后，将全部目标能源事件写入 typed jl_keeper_indexed_event，来源不是固定样本全集的估计。月分区、ORDER BY(capture_id,row_ordinal)；原页ordinal保留重复供应商下标的独立记录。保存块高/链时间、供应商下标、协议字段与实际本地请求/响应可用时间、payload_hash。block_hash Nullable(FixedString(32)) 为NULL、finality=provider_claimed_confirmed、position_status=indexed_only，不冒充独立核验。

源Capture kind=event_index、scope=indexed_energy_events；complete表示一页完整提交，pagination_exhausted另外表示最后页。旧RentalEvent仍是固化块hash、交易序号、receipt日志位置核验的事实；供应商下标不能作收据位置。源观测提交后推进源游标，后台补区块/收据/交易体，不覆盖原索引行。

后台child kind=enrichment，parent_capture_id指向完整源页；writer检查父已提交、kind、cfg、合约、事件、窗口及发现数一致。后台直接读取索引表的已解析字段并核验typed摘要，不读取完整父响应；父子关系从capture核对。Liquidate全核验、Rent/Return仅当前固定样本；EvidenceCursors只表示该样本的连续生命周期证据，不表示全量索引行receipt核验。晋级必须有从空token到终页的完整分页链，每个父页均有成功child，不能跳过前面的失败缺口。空样本child表明该页没有选中核验对象。失败child最多3次、至少30分钟重试；它不阻塞源抓取。

新enrichment的事实available_at为核验完成时刻，原索引响应时刻不改。旧事实保持原值，完整上下文可用时间为max(row.available_at,capture.available_at)，不可用链上block_time替代。未知费用保持NULL，不把零行或known费用覆盖解释为零成本。

### 提交、兼容和导出

2026-10-24 机会探测增加两个可选专项表：`jl_keeper_rental_state_observation` 和 `jl_keeper_rental_state_batch`。前者是 `getRentInfo(address,address,uint256)` 的定类型结果，按 `(capture_id, observation_index)` 定位；保留 pair/resource、origin、caller、UTC 请求/接收/可用时间、前后块头、实现身份、API/TVM/错误、energy 与请求/响应 SHA256。返回值分别为 `security_deposit_sun Nullable(UInt256)`、`rent_index Nullable(UInt256)` 和 `liquidatable Nullable(Bool)`，没有租赁 amount；NULL 为未知，false 与零必须精确保留。状态查询不复用 liquidation probe 的奖励语义，`liquidatable=true` 不是可执行或可获奖的保证。

新 `capture_kind=rental_state` 强制有 batch proof，保存 capture 身份、`row_count` 与 `jl-keeper-fact-v1` 成员摘要；零行失败批次也必须有空集合摘要。旧 Capture 字段、125 表迁移目录与旧四类成员摘要不变。旧 kind 不允许新成员或 proof。两扩展表先写、Capture 最后发布；读取包含预期空表，缺 proof、多行、重复 index 或摘要不符均拒绝，重试另外比较 proof，不能只比较旧 Capture。MySQL 两表采用 8 KiB InnoDB 压缩页和独立 runtime mapping，无新的历史扫描索引；ClickHouse 按 capture UTC 月分区。机会模式启动只检查扩展，缺表禁止发送新检查；仅七表的旧历史读取仍可用，已安装扩展时不会忽略额外成员。

MySQL 使用显式 `storage-migrate install-keeper-rental-state --family crypto_market_info_justlend_keeper --target <数据库> ...` 安装，不运行旧 `generate_schema.py` 或改变历史 schema hash；ClickHouse 使用显式 `justlend-keeper-data init-rental-state-schema`。普通采集不执行 DDL。新导出格式 `jl-keeper-data-export-v3` 加入 `rental_states.csv` 与 `rental_state_batches.csv`，proof digest 输出十六进制；CSV 延续 `unknown` 空值表示。它们均按 Capture 认证，不声称 latest 查询是固化历史；尾部 head 失败保留已严格解析的 tuple 和已经发生的 probe，但降为 unknown，不能认证正奖励。

Capture追加 indexed_rows UInt32 DEFAULT0、indexed_digest Nullable(FixedString(32)) DEFAULT NULL、parent_capture_id Nullable(UUID) DEFAULT NULL。旧四类事实及固定二进制 jl-keeper-fact-v1 摘要不变；新索引表有单独摘要，空索引页也须非NULL摘要。成员先写、Capture最后提交，预期0行表也读取认证。Frozen重试复用原ID、起始时间及成员，旧表默认NULL/旧gob已验证兼容。旧分页前缀（含页间停机）从原窗口空token重索引，旧已存事实不删除。旧Frozen批次原样完成后恢复，预算和冷却不清空。

init-schema幂等创建第七表index_page并保留既有兼容列，collector不做DDL。所有表按冻结capture_started_at月分区；索引表按ordinal查询，事实查询按各自typed主键。查询时间显式DateTime64(6,'UTC')，不使用驱动默认整秒time.Time绑定。

CLI export与兼容命令report只导出七份typed CSV（含index_pages）及metadata，不输出获利、年化或成本情景。按capture_started_at半开窗口选择；captures.csv保留撤回/未提交/其他配置的审计记录，成员只输出已提交当前配置、计数/摘要校验的批次；同时核对分页来源hash/时间及父capture关系。第一批四条旧报价仍保持uncommitted及原值/摘要，不改回或计入认证成员。实际验收见[本轮记录](../research/2026-10-03-keeper-collection-split/validation.md)。

### 发送与运行

PublicNode承担节点只读查询，TronGrid事件分页，Binance公开BBO；端点和路由不自动改变。每次启动前5分钟所有来源间隔至少5秒，此后全局至少1秒且单请求在途；TronGrid及后台另有5秒gate、所有来源合计40,000请求/UTC日，重试计数。实时索引优先，固定历史逐日源回补；其他请求后台核验，不突发补发。source游标与样本证据游标分开；晚启动的probe不补发，已尝试的观测补后置头。缺省API success仍为NULL；明确REVERT、其他TVM失败、网络/API失败分开保存。实际状态见[运行文档](runtime-operations.md#justlend-keeper-独立研究采集2026-10-03)，服务保留自启动。

2026-10-04增加小型jl_keeper_index_page表：capture_id UUID（主键）、capture_started_at UTC微秒、scan_id String、fingerprint_in/out String、request_started_at/available_at UTC微秒、payload_hash FixedString(32)。月分区，与索引成员先写、capture最后提交，重试内容稳定。完整空页仍有分页来源记录；token链从空起点完整连接到空终点才允许连续核验进度晋级。旧metadata一次性从manifest迁移并与DB索引成员hash/时间交叉核验，旧Capture及事实摘要不变。正常链路不保存request/response/summary正文；export只访问DB。没有通用JSON表，payload hash保留但删除原文后不能重放原JSON。迁移/清理验收见[验证记录](../research/2026-10-04-keeper-no-raw/validation.md)。

## OKX USDT 配对目录与公开借贷政策（2026-10-07）

新增两张定类型完整观测表，源响应只在内存校验，不保存 JSON 原文或备份文件。字段与运行衔接见[实现说明](okx-paired-five-level.md)。

- `okx_public_loan_policy`：UUID、请求/观测 UTC 时间、来源 URL 与 payload SHA256；基本币种、**每日**借贷利率、公开额度；特殊币种利率；vip/regular/config 级别、币种、策略、额度、利息折扣、额度系数。金额与利率统一 Decimal(38,18)，可空字段 `Nullable`，空值表示未知。并列数组长度经模型及数据库约束校验，完整批次一行写入。
- `okx_paired_catalog`：UUID、观测时间、生效分钟、借贷观测 ID；依次 SPOT/SWAP/MARGIN/借贷的四组来源 URL/hash/请求与接收时间/原始数据条数；基础币种、两腿 instrument ID 与最小下单数量。四个来源缺一即拒绝。目录提交前与读取时都核对借贷来源、已登记定义以及正公开额度。目录只在 `effective_minute<=查询时间` 时可见。

两表按 UTC 观测月份分区，ReplacingMergeTree 以稳定观测 UUID 去重，重试不换身份；保存规范化行摘要并在读取时验证。公开额度用于界定研究集合，**不是账户实际剩余可借量**；额度系数不是分档边际借贷利率，弃用折扣为空时不能补成零。所有已选永续复用现有资金费率采集，采样频率仍为整点估算与结算后确认。本次不增加分钟成交量表，也不将公开政策冒充账户实际借款成本。


## OKX 交易初筛定类型输入与耐久发布（2026-10-08）

2026-10-16 资本规则无损精简设计见[存储与交付协议](redis-mysql-migration/storage-design.md#2026-10-16-数据精简设计先改代码暂不操作数据库)。三个可选 MySQL 专项扩展表分别保存折扣内容版本、风险内容版本和轻量资本观测；保持原完整逻辑事实、来源与发布时间，新物理表示以独立 codec 标识。压缩完成后已在隔离 MySQL 验证事务、新旧读取和重试，并显式安装三张生产 8 KiB 压缩页扩展表。写入开关仍关闭，采集及历史迁移保持暂停；旧 125 表迁移映射、既有 schema 登记及迁移断点保持，未重写历史。实际边界与验证见[执行记录](../research/2026-10-16-capital-versions/report.md)。

新增实现及字段/时间/单位完整合同见[找币数据衔接](discovery-trading-contract.md)，DDL为 `internal/storage/clickhouse/discovery_schema.sql`。独立下一期资金费及标记/币种指数进入 `funding_forecast_observation`；上一完整分钟基础币成交量进入 `cex_trade_bar_1m`；公开标准组费用、资本语义引用和实际公开风险数组分别进入 `cex_fee_schedule_rule`、`cex_collateral_risk_rule`、`okx_public_capital_parameters`。金额/费率/区间为 Decimal(38,18)，时间为 UTC Unix 毫秒，原生目录/政策微秒时间保持不变。

`cex_book_minute_commit` 绑定原分钟快照、保存深度、valid_bitmap 和全部差量集合摘要；writer使用手中完整待写批次生成，禁止事后用现存行补造历史完整证明。`source_publication_intents` 在写事实前耐久固定源身份/摘要/首次知悉及必要源成员元数据；`source_batch_status` 最后固定实际全部完成时点。成员数量及有序摘要绑定 `public_observation_provenance` 的官方URL、响应hash、来源/请求时钟，严格查询读取完全部来源成员后重新核验完整manifest。新事实及intent/complete采用不可变 MergeTree 记录及fsync设置；来源元数据的小型重复记录按稳定身份去重，已发布内容冲突拒绝。

原预测实际混合小时表仅保留原用途，不能替代独立下一期预测。API有原生ts时保存ts；无来源时钟的静态规则明确用请求时点且有固定有效期，不能用新收包时点覆盖旧ts。全目录公开额度与Basic/Regular政策仍属参考，私人账户费用、余额、杠杆和最大借额只留在交易项目私有SQLite。无原始响应JSON备份。现货/期货5档、期权10档及历史10/50档编码保持兼容。

实际隔离验证库 `discovery_validation_20261008_capital_final`：从完整167配对目录中订阅BTC/ETH四条流，UTC 2026-10-07 23:04分钟完整发布，未漏采样，分钟写入941.223606ms；交易严格SQL读者及独立审核SELECT均通过。真实ClickHouse故障/重试/缺失来源与5/10/50混合回放10项顶层测试及4子项通过。隔离验证不是生产常驻替换；旧生产数据未获补造证明，正式迁移、全币覆盖和每日存储/查询负载验收仍需按运维流程进行。证据在交易项目 `artifacts/collector-discovery-sql-tests.txt` 和 `artifacts/public-capture-2026-10-08.log`。

### 分钟发布元数据批量化修复（2026-10-08）

交易发现的分钟发布链路与事实writer使用相同的100产品分块。每块批量读取intent/commit/status三表，逐币验证所有不可变字段、stored_depth及完整delta_seconds；重复记录拒绝，不能以LIMIT 1或map覆盖隐藏冲突。全envelope所有intent耐久完成后才写任意分钟事实；每块依次批量写差量、起点、commit，最后complete。published_ms在全部依赖commit耐久成功后取真实时间。整批或部分成员耐久后ACK丢失，只插入缺失成员并保留原known/published。源观察时间不重置，历史完整性不补造；表结构、5/10/50深度回放与45秒终止限制不变，没有新增原始JSON。

真实隔离ClickHouse：334流、每侧5档、60秒且59秒均有差量，共19706差量行；不开发布链路0.508秒，批量新链路1.145秒；3组并发1002流/59118差量2.578秒。另测101成员跨块、intent/commit/status整批ACK丢失与50行耐久前缀后故障，补齐与重复重试逐币保持时钟并且三表各101唯一行。慢commit时完成时间不能提前，实际篡改深度/秒集合重试拒绝。代表性故障和吞吐已验证，长期全市场部署尚未验收。证据在交易项目 `artifacts/blockers-v051-collector-tests.txt`、`artifacts/blockers-v051-collector-prefix-tests.txt`，独立复核另存；原采集进程未替换。

后续正式部署已于2026-10-08完成，见[部署验收](../research/2026-10-08-okx-five-level-deployment/report.md)。全量运行补充的请求节奏修复只改变发现接口限流及无原生时钟来源的取时位置：`request_without_source_clock` 的 `source_ms` 取成功物理请求经过冷却和门限后的发送时刻，`observed_ms` 仍为接收时刻；每次重试分别经过同一端点门限。来源响应摘要、原生 ts、UTC/Decimal、表结构及历史行保持原语义，未重写已有数据。

2026-10-09 发现事实恢复修复：独立资金费预测与上一完整分钟成交量分别请求、分别严格验证并发布。一方来源失败、分钟尚未确认、资本参考暂缺或公开费率组不受支持，均不阻断另一方已完整取得的事实。预测仍必须包含其全部资金费、mark 与两种币价来源；未确认分钟没有成交量提交，未知手续费没有规则提交。参考规则沿用独立的失败重试与有效期；查询/交易复核仍须同时取得所需全部输入，不能把已发布预测当成费用与资本规则已经齐全。采集优先执行市场事实，来源时钟和身份不重置，表结构与发布摘要保持。


### Redis/MySQL 审核修复（2026-10-13）

2026-10-16 用户要求将 MySQL 表改为压缩存储：采用 InnoDB 原生 `ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8`，作用于业务表和 `cmi_storage` 控制表；分区表逐个核对真实 InnoDB 分区的 `ROW_FORMAT` 与 `ZIP_PAGE_SIZE=8192`，不能只看表级选项。仅改变物理页存储，列、主键、索引、分区边界、BLOB codec、精度、成员摘要与重复行语义保持。目录中的历史映射 DDL 及其摘要继续作为冻结迁移的身份；实际安装 DDL 由管理员应用压缩策略，生成的 `schema.sql` 同步输出压缩格式。既有表通过显式离线重建转换，采集器启动不执行 ALTER，历史迁移保持用户要求的暂停状态。执行状态和压缩效果见 `var/storage/mysql-compression/20261016/`。

普通 MergeTree 的迁移组允许内容不同的原始副本，完整保留字段与重数并独立核对多重集。它不选择胜出记录，也不改变发布查询的歧义拒绝规则。目标已有部分同键副本时禁止机械追加；保留冲突等待明确裁决。旧迁移器把这种源组误报为不可变键覆盖时，仅在同一 run/块内全部原始副本逐字段回读一致、诊断中的两份原摘要均被保存后，记录 `preserved_complete_source_multiset_ambiguity` 裁决；原诊断记录保留，业务歧义不解除。

MySQL 的 `jl_keeper_capture` 另有管理员安装的 `cmi_keeper_capture_time(capture_started_at,capture_id)` 非唯一索引，用于 UTC 微秒半开时间窗口和最新批次读取。它只增加查询索引，物理列映射、迁移摘要和原字段不变；安装时验证完整列顺序、可见性和非前缀形态，同名异形拒绝覆盖，collector 仅检查不执行 DDL。每行额外保存 8 字节 DATETIME(6) 与 16 字节 UUID，另有 InnoDB 记录和页开销；本次 290,432 条历史声明实测额外占用 611 页（10,010,624 字节，约 9.55 MiB），记录在准备目录的 `keeper-time-index-install.log`。迁移闭包读取按实际主键分页，Across 每页以有界批量读取逐批核验原声明、修订号和全部成员摘要。

2026-10-14 运行复核发现待补充选择的 `NOT IN` 被优化为逐父记录执行的关联子查询，现场遗留 SELECT 超过 200 秒。改为一次性物化完成/冷却/重试已用尽的子记录集合，并增加两个非唯一元数据索引：`cmi_keeper_enrichment_parent(config_hash,capture_kind(32),committed,status(16),requested_from,capture_started_at,capture_id)` 和 `cmi_keeper_enrichment_child(config_hash,capture_kind(32),committed,parent_capture_id,status(16),available_at)`。字符串使用字节前缀索引，查询仍比较完整字段，不以索引前缀替代状态判断；只对父候选排序和子记录分组。两个索引每行键内容至多 113/105 字节（不含 BLOB 前缀长度标记、InnoDB 主键、记录及页开销），本次约 30 万行表上实测父索引 1963 页（32,161,792 字节）、子索引 1899 页（31,113,216 字节），合计 63,275,008 字节（约 60.3 MiB），详见切换记录的 `keeper-enrichment-index-shapes-and-size.log`。它们不索引盘口价格，也不改变原字段、主键、迁移摘要或历史完成状态；管理员在线安装，采集账户不执行 DDL。


MySQL `options_collection_plan` 的物理主键仍为 `(session_id,revision)`。管理员额外安装非唯一索引 `cmi_options_plan_id(plan_id)` 与 `cmi_options_plan_time(effective_minute,plan_id)`；实际启动复测发现无索引的计划 ID 查询会扫描历史大数组、超过原 30 秒读取限制。最新计划先只取 ID，再通过 ID 索引读取完整计划，写入接续同样先定位前序；摘要、直接前序及重复生效边界检查保持。两个索引不新增唯一约束，保留原历史歧义；不改变物理列映射或迁移行摘要。每行额外索引键为 16 字节 UUID、8 字节 DATETIME(6) 加 16 字节 UUID，另含原主键、InnoDB 记录及页开销；本次 824 条封存计划实测各 4 页、合计 131,072 字节（128 KiB），执行计划为索引反向扫描一项，证据见 `research/2026-10-14-startup-dependencies/installed-options-plan-indexes.log`。

启动依赖准备新增 `cmi_storage.bootstrap_source_gap` 控制表，保存用户明确放弃的历史提交键、完整原始定类型行及 SHA256、原因和证据路径。它不改写原始 `committed`、修订号、成员数量或摘要；仅允许 bootstrap 的恢复闭包验证识别完全一致的旧声明。正常批次查询仍执行原完整性检查，缺成员批次不能成为有效事实。同键内容改变、未知批次或其他表不适用此例外。迁移报告显式列出缺口数量，存在登记缺口时 `finalize` 拒绝打开完整历史。

旧端持续写入时，启动依赖可以从独立 ClickHouse 快照库读取；只允许保守清单中的 59 张表，不包括高频盘口和分钟发布记录。快照保留原引擎、版本列和普通 MergeTree 的重复行；源 DDL、快照 DDL 和整个清单摘要共同固定 run。复制及独立验证仍逐字段核对完整键范围、重复次数和原始类型编码。普通不重叠小块使用有界批量写入及全字段回读；冲突、修订和大键组继续使用原审计路径。快照通过不代表服务已切换，切换前仍须停旧端排空后补齐快照后的增量。

CEX 缓冲交付 codec=2 在原 50 档物理快照/差量编码之外增加定类型 `Tuple(known_from_ms Int64, books String)` 二进制信封；`books` 是原始 codec 字节，不是 JSON 或字符串价格。采样完成时冻结首次知晓时间；重试复用同一交付，手工调用准备接口时也须保留同一 `KnownFromMS`。Redis/spool 持久化整个带摘要的交付，无需预先访问 MySQL。消费者恢复时将该时间用于 intent，并在同一事务内写事实、发布和交付证明；重试不重置 known/published。codec=1 保持可读，其旧发现交付仍须使用预先耐久的 intent，缺失则拒绝补造。

分钟 writer 的新事实路径仅对 `order_book_minute`、`order_book_second_delta`、`derivative_book_minute`、`derivative_book_second_delta`、`derivative_book_quality_minute` 和 `options_catalog_quality_evidence_minute` 使用普通批量 INSERT 的唯一键检查，且要求表不保存历史 multiplicity。任一事实重复键均先确认整个事务回滚，再用同一交付身份、内容、计划、代次和首次知晓时间执行原完整读取与摘要检查；回滚未确认则留待恢复，不在当前调用内重试。两个尝试共用 40 秒期限，不能把事实重复键当作整批成功。发布、元数据与历史重数的完整检查保持，表结构和编码不变。

分钟 writer 的只读准备池另外预取期权快照、质量和 catalog 证据表的完整主键页，只投影 instrument_id，不读取宽字段；仅对有锚点的分钟预取快照，每次最多 500 个键（当前 live 信封最多 32 个成员）。原 ASCII 64 字节 batch_id、压缩后的 32 字节 compact_batch_id 及证据 run_id 分别按实际主键传参。预取不保存内容或缺失证明、不取得代次锁，后续普通 INSERT、完整回滚重试和发布校验保持；准备时间与内存仍计入原 writer 预算。

writer 启动和周期校验对每个既有连接池各执行一次有界 schema 查询，完整核对全部 33 张分钟依赖表；缺失、重复、版本或 DDL 摘要不符均拒绝继续。物理连接重建仍独立校验，兼容性失败会清空不可变缓存；合并查询不降低检查范围，也不增加连接池配额。

普通 MergeTree 的 `_occurrence` 保留物理重复，查询不对相同内容做通用折叠。发布身份重复仍属无效；单次已提交事实的写入重试保持幂等。UUID 迁移游标采用与源 ClickHouse 相同的后 64 位优先顺序；UInt256 按完整 256 位整数比较。

迁移控制表 `state=5` 表示 CEX 字段校验完成、查询发布待完成。两张盘口表的历史分钟全部校验并具有 `ingest_member`/`ingest_batch` 证明后才一起标记 `state=2`；`finalize` 再核验覆盖，避免中断后提前宣告完成。该状态使用既有整数列，无业务表 DDL 变更。

Redis 旧分钟预览以全部保留帧的成员并集检查提交证明，不要求存在第 59 秒。清理扫描游标持续推进，删除前核对预览版本；未提交分钟无 TTL，当前及上一分钟仍保留。

2026-10-13 第二轮源查询修复：迁移游标解码后按源列类型构造 SQL 参数。UInt256 以十进制字符串传参并显式 `toUInt256(?)`；DateTime64 以原列 0/3/6 位小数的 UTC 字符串传参并显式 `toDateTime64(?, scale, 'UTC')`，避免驱动默认的整秒格式化。上下界采用同一转换，分别保持 `>` 和 `<=`，保证原始上界行可被纳入、已完成下界行不被重复分页读取。二进制检查点、事实编码、表结构及源时间语义不变，没有浮点数转换。

切换最后同步采用独立封存的启动子集，逐字段独立重读后仅标记 `MigrationRowsVerified=5`；它不调用全历史领域扫描，不能由 finalize 接受为完整历史。真正恢复依赖仍通过实际 collector 读取核验。Reserve 的最终性修订允许更新 capture 级 `payload_hash`（`Reconcile` 的链头证明），原成员摘要、成员锚点、观察时间和窗口保持不可变；同版本异内容及已 finalized 的降级仍拒绝。该规则同时用于 live revision 与迁移 revision，避免合法最终性证据更新阻断新服务。


2026-10-14 写入积压改进：已封口的 CEX、`options_live`、`options_catalog` 分钟使用专用不可变提交路径。事务内对 producer generation/holder 取得 `FOR SHARE` 并一直持有到提交或回滚；身份分配、计划发布、发现事实及链上修订仍使用原有 `FOR UPDATE` 串行路径。代次激活仍为独占 CAS，不能越过正在提交的分钟。

分钟事务先 INSERT 既有 `cmi_storage.ingest_batch` 的完整身份与摘要，再复核有效计划、写入事实及领域提交记录，最后更新本事务刚插入行的 `committed_at` 并 COMMIT。占位行在提交前对已提交读不可见，失败时与事实一同回滚；只把该 INSERT 的唯一键冲突作为竞争批次处理，回滚后严格核对全部身份、分钟、codec、成员数及摘要。已存在的成功行只核验，不改摘要或完成时间。ACK 仍须取得匹配的持久提交证明。

2026-10-15 补充有界准备：CEX v1/v2 字节格式保持不变，直接读入整数切片，保留 50 档物理锚点。大 CEX 批次逐本校验、计算原成员及发布摘要，再按约 1 MiB 工作块写入立即解除目录链接的私有临时准备文件；临时块有长度和摘要校验，提交时在同一个事务中依次加载，只有全部成员完成才发布原 ingest_batch。准备文件关闭或进程被终止后由系统回收，恢复依据仍为原 Redis/spool 字节；它不属于新的持久事实或队列格式。任一块失败、损坏或成员数不符都会回滚整批，未提交原件仍保留。期权宽数组在分配前检查既有 32 本、4 个指数、59 个差量及固定 60 秒数组的约束，不修改合法数据的编码或质量规则。

Keeper 新增管理索引 `cmi_keeper_evidence_frontier(config_hash,capture_kind(32),committed,status(16),event_kind(32),requested_to,requested_from,capture_started_at,capture_id)`。前缀列仍按完整值过滤；它服务于链窗口进度查询，不改变事实字段或批次编码。现场索引占 2,222 个 InnoDB 页，即 36,405,248 字节（约 34.7 MiB）。同条件 EXPLAIN 的扫描估算从 301,535 行降至 106 行，此为计划估算；持续运行耗时另行验收。显式安装命令为 `storage-migrate init-schema`；可独立撤销 `ALTER TABLE crypto_market_info_justlend_keeper.jl_keeper_capture DROP INDEX cmi_keeper_evidence_frontier, ALGORITHM=INPLACE, LOCK=NONE`，无需撤销事实。新索引增加每次 capture 写入的索引维护，实际写入成本在运行报告中记录。

2026-10-15 OKX 采样有效性修复：已确认订阅、收到完整快照且序列连续的盘口，在共享连接持续健康时可长时间无价格变化。这些秒保留有效位及上一状态，不因单条流 30 秒无消息而伪造断档；收到下一次真实更新仍按原序列规则校验。等待初始快照的流仍有期限，未确认订阅、序列缺口、队列溢出和连接失败仍使相关数据无效。首次配对采集保留全部来源，已有合法快照的流可开始采样，未就绪流独立恢复；替换已运行来源代次保留完整预热要求。表结构、5/10/50 档编码、UTC 时间和整数价格/数量不变。


2026-10-15 DEX 查询辅助索引：主库 `dex_tx_receipt` 增加非唯一 `cmi_receipt_hash(chain_id,block_hash,tx_hash)`，保留所有高度的同哈希物理匹配及冲突拒绝；不通过增加 block_number 筛选隐藏错误高度记录。`dex_block` 增加非唯一 `cmi_dex_committed(chain_id,manifest_hash,canonical,committed,block_number)`、`cmi_dex_pending(chain_id,manifest_hash,canonical,committed,log_coverage(16),receipt_coverage(16),block_number)`、`cmi_dex_finality(chain_id,manifest_hash,finality(16),block_number)`。覆盖字段仍以完整值筛选，字符串前缀只用于索引定位；缺口、待补、最终性与排序规则不变。维护查询显式使用该索引，collector 启动只核验形态，管理员通过 `storage-migrate install-dex-indexes --family crypto_market_info --target ...` 安装；同名异形、不可见或非预期唯一索引拒绝。首次主库实测四个索引共 76,005,376 字节，后续随事实增长；每条新回执额外索引键约 80 字节，每条区块的三个键合计约 298 字节，另有页和记录开销。没有增加按价格的检索索引。物理事实列映射和迁移摘要保留，回退先换回兼容程序再删除辅助索引，详见实施记录。

2026-10-15 写入准备补充：不可变衍生品规则按原 `trading_rule_id` 与完整字段摘要在事务外有界读取，不缓存缺失结果；最终 INSERT 仍在原独占代次事务内，同批全部 SQL 块共同提交/回滚。并发重复键在回滚后重新读取完整身份与内容，不能把普通唯一键冲突当作成功。计划的不可变 run、合约与目录证据也在事务外准备，既有发布的内容、前序与生效边界仍在事务内复核。DEX 同批事实批量写入保留完整成员和共享回执证明。以上没有改变事实列、multiplicity、first-knowledge 或迁移行摘要。TradeBar `is_final` 使用 UInt8 的 Go 值，JSON 中的 0/1 与旧业务摘要保持一致。

DEX 历史内部缺口检查现在在单条 SELECT 的一致快照内按 `cmi_dex_committed` 覆盖索引顺序流式读取高度，不再使用全历史窗口排序/物化；重复高度、最早内部缺口和 manifest/canonical/committed/chain 筛选保持。读取结束时检查完整结果错误，内存不随历史高度数增长，没有缓存“无缺口”结论。

writer 的 32 MiB 不可变资料缓存现按单项合约/规则身份保存，跨分组共享已核验项，仅批量读取缺项；保留排序、完整身份/内容校验、独立返回值、无负缓存以及 schema/代次隔离。计划前的新 run 组可按原身份有界批量写入，同一组的全部 SQL 块共同提交/回滚；计划的前序与生效边界仍单独在原互斥事务内复核。以上没有改写现有事实字段、编码或摘要。


2026-10-15 发现事实批量写入补充：同次取得的完整资金费预测与已确认成交量，以及资本参数与其参考事实，可按原观测身份组成最多 32 条、准备后不超过 8 MiB 的同步写入组。同组 instrument 只读取一次，来源角色、时间和全部 payload hash 仍逐事实验证；不同事实类型保持各自表。首次已知 intent 单独耐久保存，事实、完整 provenance 与发布状态在原独占代次事务中共同提交，任意 SQL 块冲突全部回滚。重试沿用已存 known/published 时间，查询全部物理 occurrence，重复、未 complete 或发布时间早于 known 的既有状态均拒绝。组失败后采集器仍可对原观测分别重试，保留单一来源失败不阻断另一完整事实的行为；返回成功仍要求实际提交，没有新增异步任务表。


2026-10-15 Keeper enrichment 选择补充：复用已有 parent/child 辅助索引，将 live/catchup 与其他非 bootstrap 来源分两级选择，单级按 requested_from、capture_started_at、capture_id 顺序读取，避免每轮按优先级表达式排序全部历史候选。两个级别与最终 capture 读取使用同一个只读 REPEATABLE READ 快照，保留原优先级、NULL 排序、完成子批次、三次尝试上限和严格 30 分钟冷却边界；子批次继续按现有索引成组计算，不改成对每个历史 parent 反复扫描所有子记录。没有新增索引、事实字段或写入锁。


2026-10-15 writer 启动恢复补充：Redis ready 索引恢复仅按有界页读取 header/state/digest 与现有 score，不在领取前展开所有盘口 payload。补索引脚本在同一 Redis 原子操作中重验 sealed 状态、header 与 digest，ACK/清理并发发生时不复活已提交批次；租约及独立 retry_at 不变。内容仍在领取后完整解码与哈希核验，损坏内容保留并拒绝 ACK。候选分页按原始 zset 页长推进，不能把并发 ACK 后较少的有效 header 当作扫描结束。诊断区分启动阶段及其有限执行时间，并记录 Redis 索引是否完成；验收同时要求新鲜 writer 指标以及两处索引完整，不能用陈旧的空队列指标放行。

2026-10-15 LST 本地恢复编码扩展（尚未部署）：业务表及原 `pending-batch.gob` 编码不变，新增定类型 runtime checkpoint 保存原分段起点、连续覆盖、分页待办引用与恢复绑定。操作意图引用原 pending 的同一 inode；当前和上一份 checkpoint 所需引用都安全覆盖后才回收。保留最近处理 pending 的身份/摘要，防止“已提交并纳入 checkpoint、工作 pending 尚未删除”的重启重复派生子待办。恢复绑定包含实际库/server UUID、schema、采集参数、producer generation/holder 及数据库备份之外的恢复世代；任何不匹配都要求显式恢复，不以尾行时间戳证明历史一致。

Keeper 本地状态的运行状态扩展正在实现，尚未部署：保留原 State/Ops 与冷候选对象，增加定类型分页窗口/父子依赖和带目标/恢复世代绑定的 current、previous checkpoint。SQL 前以硬链接复用已经同步的冻结 Ops 所在 checkpoint，当前进度与派生工作及应用边界一起提交到本地状态。新本地编码保留已知零值指针，不改变 Keeper 表、capture/member 摘要或原数据库事务。转换/当前状态回退的边界见 [运行状态操作说明](redis-mysql-migration/runtime-state-operations.md)，实际验收见其实施记录。

主 DEX 本地状态扩展正在实现：最终覆盖压缩为连续高度区间，尚未最终确认的实际工作按 `(height, block_hash)` 分页，同高度多个 canonical 记录按原高度存在语义处理。记录原 block/batch/revision 和期望旧修订后才允许 SQL；成功返回后读取准确主键的实际当前版本，防止旧修订的成功 no-op 恢复过时 finality。没有新增数据库状态表或修改 DEX 事实键。正常 gap 查询不再遍历完整历史；转换和旧版回退仍显式核对完整适用输入，当前尚未部署。


运行态维护清单同属于本地 checkpoint 格式：记录原始清单摘要、有限操作的不可变引用、下一项位置及完成标记。进度与领域状态在同一 checkpoint 安装；未完成交接拒绝普通采集启动。原操作附带领域任务身份，同一任务未决时拒绝另一修订，原身份重试可继续。没有新增 MySQL 事实表、Redis 队列或永久变更日志。维护 payload 使用原定类型编码与硬链接，二进制哈希和已知零值不经 JSON 往返；具体入口见 `docs/redis-mysql-migration/runtime-state-operations.md`。以上为代码协议，不代表生产已转换或部署。


## 2026-10-24 Keeper 动态检查扩展

动态 40＋10 模式新增四张定类型专表，属于原 Keeper 低频链路；原 capture、七张旧表及冻结 125 表迁移清单不改。模型定义在 `internal/justlendkeeper/dynamic_model.go`，MySQL 为独立 runtime 映射、8 KiB 压缩页；ClickHouse 为 `ReplacingMergeTree`。安装、导出和恢复见[实现与运行](justlend-keeper-dynamic-selection-implementation.md)。

| 表 | 键与语义 |
|---|---|
| `jl_keeper_rental_rate` | `(capture_id, observation_index)`；rate 为 Nullable(UInt256)，scale 为 UInt64=1e18；请求/响应 UTC 时间和哈希、前后块高/hash/time、实现身份、API/TVM 状态、latest 未固定状态绑定；失败保持 NULL/unknown |
| `jl_keeper_rental_rate_batch` | `capture_id`；同 capture 起始时间、row_count、定类型编码 digest；0 行失败批次也必须有 proof |
| `jl_keeper_selection_member` | `(capture_id,slot_no)`；round/订单/generation、请求及实际 lane、选中时间/原因、nullable A/D/r/t0、origin/输入摘要、rate/state 来源 capture 引用、覆盖时间、结果/子 capture、RPC 预留次数 |
| `jl_keeper_selection_round` | `capture_id`；round_id/policy、开始/结束时间、40/10 目标和实际数量、终止原因、订单身份/隔离数、最长未尝试和未成功秒数、来源覆盖语义、member_count/member_digest，兼作本轮 proof |

`selection_round` 与 `rental_rate` 是独立 capture kind，不混入链上事实表。成员先写、proof/capture 最后提交；预期为空的表也读取认证，重试比较独立 proof，不因原 capture 未新增 digest 而接受内容变化。采样输入在准入时冻结；整数交叉乘法得到的压力只用于选择，不是收益率或已可清算事实。RPC 计数包括发送边界不确定的持久预算预留。

native snapshot v3 的新字段只能追加到原结构末尾：旧 typed codec 的 presence tree 按字段位置编号，前插字段会破坏旧 NULL/已知零/false 的恢复。已增加部署 v1 布局回归。订单、检查、不可变轮成员使用独立页/引用，current/previous checkpoint 一起保护；runtime 原预算、冷却、producer 和恢复世代保持。旧 binary 拒绝新格式，回退使用兼容读取程序。导出 v4 增加四份 typed CSV。
