# 行情采集程序设计

本文说明第一版程序如何实现 [市场数据存储数据字典](market-data-storage.md)。目标是复用 `crypto-arb-observer` 已验证的 Binance、OKX 接口代码，用尽量少的新组件完成公开行情采集、标准化、每秒采样、ClickHouse 存储和盘口回放。

本文是编程边界和实现顺序，不重复定义表字段；表结构与字段语义以数据字典为准。

## 1. 第一版范围

第一版只实现：

- Binance 和 OKX；
- 现货、Binance USDT-M 永续和 OKX USDT 线性永续的前 50 档 L2 盘口；
- 永续合约整点资金费率；
- 每分钟完整盘口、分钟内秒级差量；
- 从 ClickHouse 查询并恢复任意有效秒的盘口。

第一版不实现：

- 套利机会计算和自动下单；
- Redis、消息队列和跨进程实时状态；
- ARB-0002 的结算窗口采样和 ARB-0022 的机会生命周期；
- DEX、交割合约、标记价格和指数价格历史；
- 多数据库兼容层。

## 2. 程序结构

采集程序采用单进程：

```text
Binance / OKX REST 与 WebSocket
              ↓
         交易所适配器
              ↓
      标准化的整数 tick / lot
              ↓
       进程内本地 L2 盘口
              ↓
          每秒采样器
              ↓
       当前分钟内存缓冲
              ↓
        ClickHouse 批量写入
```

Redis 不参与任何环节。最新盘口只存在于采集进程内存；数据库只保存已经结束的分钟和整点资金费率，不承担实时消息传递。

建议的最小目录如下：

```text
cmd/collector/                 程序入口、配置装配和优雅退出
internal/model/                与交易所无关的 instrument、盘口和资金费率类型
internal/exchange/binance/     Binance REST、WebSocket 和序列规则
internal/exchange/okx/         OKX REST、WebSocket 和序列规则
internal/orderbook/            本地 L2 状态、排序和前 50 档输出
internal/sampler/              每秒采样、分钟缓冲和差量计算
internal/storage/clickhouse/   表初始化、批量写入和查询
internal/replay/               分钟快照加差量回放
```

暂不拆分多个可执行程序，也不为未来可能增加的数据库或交易所预先建立插件系统。

## 3. 旧项目代码复用

旧项目目录为 `/home/ubuntu/crypto-arb-observer`。复用采用“迁移代码和测试”的方式，新项目不得在 `go.mod` 中直接依赖旧项目，否则会把 Redis、策略模型和旧配置一起带入。

### 3.1 基本原样迁移

- `src/arb-core/strategies/arb0002/provider_http_retry.go` 及其测试；
- ARB-0002 中 Binance USDT-M、OKX USDT 线性永续的 instrument、funding 响应结构和严格解析测试；
- Binance、OKX 永续盘口消息解析及序列连续性测试。

### 3.2 保留核心逻辑后改名泛化

- `perp_book_model.go`：保留价格键更新、数量为零删除、排序和锁盘检查；移除 Redis 字段，只有采样输出截取前 50 档；
- `perp_book_collector.go`：保留本地 L2 状态和失效处理，名称从 `PerpBook` 泛化为 `OrderBook`；
- `providers_binance_perp_book*.go`：保留 REST 快照、diff 缓冲、`U/u/pu` 连续性和断档后重新快照；
- `providers_okx_perp_book*.go`：保留 `snapshot/update`、`prevSeqId/seqId` 连续性和重新订阅；
- 两个交易所 runtime：只提取 WebSocket 连接、重连、ping、分片和快照调度，不迁移 Redis publisher、TTL 和 ready key。

旧 Spot metadata parser 只能复用 REST 响应结构和字段映射。其十进制解析失败时可能静默返回零，因此十进制解析和必填字段校验必须改成显式返回错误。

### 3.3 不迁移

- `redisstate`、Redis key、Redis DTO 和 `go-redis` 依赖；
- `arb-opportunity`、机会 scanner、机会状态机和机会数据库表；
- `adaptive_*`、`window_depth_*` 和 settlement sampler；
- Doctor、monitor-agent、生产发布门禁和旧 Compose 编排；
- 旧 funding observation、settlement window 和 replay JSON 表。

## 4. 交易所适配

适配器只负责协议差异，不做套利分析。两个交易所最终都向本地盘口提交以下信息：

```text
instrument_id
source_time
sequence information
side
price_tick
qty_lot
snapshot or update
```

原始字符串价格和数量先用十进制定点数严格解析，再按照 `instrument.price_tick_size` 和 `instrument.quantity_step_size` 转换为整数。不能整除时视为协议或元数据错误，不得静默四舍五入。

### Binance

- metadata：复用旧 Spot、USDT-M `exchangeInfo` 的响应结构和字段映射，改用严格解析；
- 永续盘口：复用旧项目的 REST snapshot + diff-depth 状态机；
- 现货盘口：新写 Spot snapshot + diff-depth 衔接，但复用永续实现的连接、缓冲和失效模式；
- 发生序列断档、队列溢出或快照衔接失败时，立即把该 instrument 标为无效并重新获取快照。

### OKX

- metadata：复用旧 Spot、Swap instruments 的响应结构和字段映射，改用严格解析；
- Spot 和 Swap 都使用增量 `books`，共用泛化后的 `snapshot/update` collector；
- 使用 `prevSeqId/seqId` 检查连续性；断档后标为无效并重新订阅或重新取得快照；
- 不使用旧项目的 `books5`，因为它不能满足前 50 档采集。

任何增量盘口队列都禁止使用“丢掉旧消息、只保留最新消息”的策略。队列满意味着连续性已经不可信，应失效并重建盘口。

本地盘口不能只保留 50 档，否则边缘价位删除后无法补入原第 51 档。内部保留交易所快照支持的较深范围：Binance 使用 1000 档快照，OKX `books` 使用 400 档；每秒采样时才截取前 50 档。

## 5. 每秒采样和分钟缓冲

采样器按 UTC 秒边界读取每个 instrument 的本地盘口，输出排序后的买卖各前 50 档。

每个 instrument 只保留一个当前分钟缓冲：

1. 第 0 秒保存完整的前 50 档；
2. 第 1 至 59 秒，将本秒前 50 档与上一个已保存的有效状态按价格比较；
3. 新增或数量变化保存新的绝对 `qty_lot`；
4. 从前 50 档移出的价格保存数量 `0`；
5. 有效但没有变化的秒只设置 `valid_bitmap`，不生成差量行；
6. 无效秒不生成差量行，并将对应有效位保持为 `0`；
7. 盘口恢复有效后，首个有效秒的差量必须相对上一个已保存的有效状态计算，使回放不依赖无效秒；
8. 分钟结束后将完整盘口和全部差量作为一个批次交给数据库 writer，然后释放该分钟缓冲。

若第 0 秒没有有效完整盘口，该分钟没有恢复起点，整分钟不写盘口数据；等待下一分钟重新建立起点。程序不得用第 1 秒或更晚的盘口冒充第 0 秒快照。

## 6. ClickHouse 写入

ClickHouse 是第一版唯一数据库。程序使用官方 Go client，连接配置只需要地址、数据库、用户名、密码和批量写入超时。

物理表遵循以下简单规则：

- `instrument` 使用 `ReplacingMergeTree`，按 `instrument_id` 排序；instrument 定义变化时插入新 ID，不更新旧行；
- `order_book_minute` 使用 `ReplacingMergeTree`，按 `(instrument_id, minute_time)` 排序，按月分区；
- `order_book_second_delta` 使用 `ReplacingMergeTree`，按 `(minute_id, second_offset)` 排序，按从 `minute_id` 推导出的月份分区；
- `funding_rate_hourly` 使用 `ReplacingMergeTree(is_actual)`，按 `(instrument_id, hour_time)` 排序并按月分区。`is_actual=1` 的实际值优先于 `is_actual=0` 的估算值；`funding_time` 使用 `DateTime64(3, 'UTC')` 保存交易所返回的毫秒时间，`hour_time` 仍是整点 `DateTime('UTC')`。

分钟 ID 按数据字典中的确定性公式生成。这样差量表不需要重复保存时间，也能从 `minute_id` 推导月份。

ClickHouse 的替换在后台合并时发生，不提供即时唯一约束。盘口回放和资金费率当前值查询必须使用 `FINAL`；资金费率也可以使用等价的 `argMax` 查询。写入重试必须保持分钟 ID、行内容和行顺序不变，不得把一次重试变成新的逻辑记录。

第一版只有一个采集进程可以登记 instrument。启动时读取已有定义：存在完全相同的定义就复用其 ID，否则用当前最大 ID 加一并插入新行。第一条 ID 从 `1` 开始，`0` 保留为无效值；不为尚未需要的多写入者分配机制增加额外服务。

一分钟结束后先批量写入该分钟的差量，成功后再写分钟完整盘口。分钟完整盘口是查询可见标志：如果写差量后进程退出，只会暂时留下不可查询的孤立差量；重试相同确定性数据即可。禁止逐条写入 WebSocket 消息或逐秒执行一次数据库 INSERT。

第一版不额外建设写入 WAL、Kafka 或 Redis 缓冲。数据库暂时不可用时，在进程内有限重试；超过限制后记录错误并丢弃尚未提交的分钟，不阻塞交易所接收循环。

## 7. 资金费率

每个 UTC 整点为永续 instrument 生成至多一条逻辑记录。估算和实际值采用不同的数据通道：

- Binance 估算费率订阅 Mark Price WebSocket，使用推送中的费率和下一结算时间；USDⓈ-M 深度使用 `/public/ws`，Mark Price 使用 `/market/ws`；
- OKX 估算费率订阅公开 `funding-rate` WebSocket；
- 每个交易所只建立一个资金费率 WebSocket runtime，在同一连接中订阅本项目配置的永续 instrument，不为每个 instrument 新建资金费率连接；
- runtime 只在内存中保留按 `(instrument_id, funding_time)` 区分的最新推送。UTC 整点由 scheduler 读取最新有效值并写入估算版本；连接失效或没有可用推送时宁可缺失，不使用旧值伪造当前整点；
- 结算整点必须保留结算前针对该 `funding_time` 的最后估算值，避免 WebSocket 切换到下一周期后写错目标结算时间。

实际结算值继续使用 Binance funding history 和 OKX funding history REST 接口。scheduler 根据 WebSocket 给出的目标 `funding_time` 建立待确认任务，不在整点请求，也不再每分钟遍历全部 instrument：

1. 首次请求时间为 `funding_time + 2 分钟`；
2. 每个交易所一个串行 REST worker，请求间隔至少 1 秒；
3. 未查询到时，在结算后第 5、15、60 分钟重新入队；此后如需补查，只做低频、有限范围的历史补查；
4. 实际响应按目标 `funding_time` 匹配，不再要求响应时间必须等于代码推导出的 UTC 整点；
5. 取得实际值后，以相同 `(instrument_id, hour_time)` 写入 `is_actual=1` 的版本；已有实际值时不再写估算版本。

第一版不为待确认任务增加 Redis、Kafka 或新数据库表。请求队列保存在进程内；进程启动时使用 `FINAL` 检查最近 24 小时内 `funding_time` 已到且仍没有任何实际版本的结算点，按 `(instrument_id, funding_time)` 去重后交给对应交易所的现有串行 worker 低频补查。已经错过的多个重试时点只立即补查一次，不集中追赶发出多个请求；后续仍按尚未错过的第 5、15、60 分钟时点执行。OKX 历史费率请求使用 `limit=100`，确保每小时结算时单次响应仍能覆盖这个 24 小时窗口。资金费率上下限、标记价格、结算状态和 REST 取得时间不进入本表。

## 8. 查询和回放

查询接口第一版只需要一个核心能力：输入 `instrument_id` 和 UTC 时间，返回该秒的前 50 档盘口或“该秒无效”。

实现顺序严格按照数据字典：读取分钟完整盘口、检查 `valid_bitmap`、读取目标秒之前的差量、按价格应用绝对数量、删除数量为零的价格、重新排序并截取前 50 档。

不建设独立查询服务。先以 Go 包和测试提供该能力；确有外部调用需求时再增加 HTTP 接口。

## 9. 失败处理

只保留影响数据正确性的状态：

- `connecting`：尚未得到有效快照；
- `valid`：快照和增量序列连续，可以采样；
- `invalid`：发生断档、解析错误、队列溢出或连接中断，禁止采样；
- `resyncing`：正在重新取得快照。

不为这些状态单独建数据库表。日志至少包含 exchange、instrument、失败原因和重新同步结果，不记录凭据和完整敏感配置。

## 10. 实现顺序与完成标准

按以下顺序开发，每一步保持可测试：

1. 建立 Go module、公共 model 和 ClickHouse 四张表；
2. 迁移 Binance、OKX metadata parser，写入 `instrument`；
3. 泛化永续 L2 collector，接入进程内盘口；
4. 补齐 Binance Spot 和 OKX Spot 前 50 档；
5. 实现每秒采样、分钟缓冲和 ClickHouse 批量写入；
6. 实现盘口查询与回放；
7. 接入估算资金费率 WebSocket、延迟串行的实际费率 REST 确认和毫秒结算时间。

第一版完成必须通过：

- 快照加差量可精确恢复一分钟内每个有效秒；
- 同秒同价格多次更新只保存最终绝对数量；
- 数量为零能删除价格；
- 无变化秒和无效秒可以区分；
- Binance、OKX 序列断档后不会产生看似有效的盘口；
- 实际资金费率不会被估算值覆盖；
- 估算费率不调用 REST，实际费率请求不会在整点并发突发；
- 实际费率 REST 首次请求晚于结算时间，且同一交易所请求严格串行；
- `funding_time` 可按交易所原始值保留到毫秒；
- 对代表性 instrument 实测一天压缩后占用和一分钟回放耗时。
