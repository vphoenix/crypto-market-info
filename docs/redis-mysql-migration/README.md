# Redis 与 MySQL 改造设计

本方案把本项目的长期存储从 ClickHouse 迁至 MySQL，Redis 保存当前分钟、上一分钟及尚未交付的标准化采样。目标是减少小批次写入引起的后台合并负担，同时保留现有采集精度、覆盖范围、历史回放和完整性证明。

**执行顺序采用用户确认的方案：设计 → 安装与编程及验证 → 停旧采集并启动新采集 → 迁移历史并验收。** 迁移期间不要求开放历史查询；新数据持续进入 MySQL，旧数据随后导入。上线前只准备新服务启动必需的元数据和恢复依赖，不等待大批量盘口历史。

本目录的设计交付日期为 2026-10-12。后续已完成安装、编程、启动依赖准备，并于 2026-10-14 执行五个采集服务切换；运行覆盖和持续吞吐尚未完全验收。切换过程见[现场记录](../../research/2026-10-14-cutover/report.md)，当前积压的处理设计见[新采集写入积压改进方案](write-backlog-improvement.md)。完整历史迁移仍须在实时采集稳定并有容量余量后推进；原[实现交付记录](implementation-status.md)中的阶段状态按其日期理解。

## 阅读顺序

1. 本文件：范围、架构与已确定的取舍。
2. [存储与交付协议](storage-design.md)：Redis 内容、保留时间、MySQL 模型、提交和故障恢复。
3. [实现与验收](implementation.md)：代码改造、部署工具、测试和性能门槛。
4. [切换与历史迁移](cutover-and-migration.md)：启动依赖、切换边界、断点续传、冲突处理、回退。
5. [新采集写入积压改进方案](write-backlog-improvement.md)：慢查询、队列调度、批量提交、安全并发及持续吞吐验收。
6. [采集运行状态与历史读取改进方案](runtime-state-improvement.md)：运行中直接维护进度与待办、减少历史重读、可靠保存和恢复状态，并明确与写入积压方案的分工。

[逐表清单](source-tables.csv)列出现场 125 张表的数据库、引擎、版本列、排序键和体积；[DDL 清单](source-inventory.json)和[逐字段类型清单](source-columns.json)保留实际表结构，包括紧凑物理列和 ALIAS。它们是 2026-10-12 15:20 UTC 的只读快照，执行前须刷新并对比，不能据此前缀自动修改新出现的库。

## 范围

正式采集链路全部纳入，不能只改此前压测的八张热表。

| 现有用户服务 | 数据库 | 表数 | 切换内容 |
| --- | --- | ---: | --- |
| `crypto-market-info-collector.service` | `crypto_market_info` | 40 | CEX、期权与交割期货、DEX、资金费率、收益、发现事实及查询 |
| `crypto-market-info-across.service` | `crypto_market_info_across` | 8 | 实时采集、恢复、历史与报告接口 |
| `crypto-market-info-reserve.service` | `crypto_market_info_reserve` | 7 | 实时采集、修订、共享收据及报告 |
| `crypto-market-info-lst.service` | `crypto_market_info_lst` | 8 | 定额报价、日志、资金费、pending、游标及报告 |
| `crypto-market-info-justlend-keeper.service` | `crypto_market_info_justlend_keeper` | 7 | 分页、事件、收据、模拟、成本与报告 |

另有七个历史或验收库，共 55 张表，保留原库边界迁移：`crypto_market_info_across_livecheck`、`crypto_market_info_perp_soak`、`crypto_market_info_perp_soak_20260928`、`crypto_market_info_perp_validation`、`crypto_market_info_reserve_history`、`crypto_market_info_reserve_scope`、`crypto_market_info_reserve_simtest`。历史和合成验收数据不得混进正式事实。文档提及但本次清单未出现的库，例如 `crypto_market_info_across_history`，应在执行时重新核实，不能声称已经迁移。

状态指示器及所有查询 CLI 同步适配。共享 ClickHouse 含 `crypto_grid_trading` 等其他项目，因此“停旧服务”指切换本项目旧采集进程；保留 ClickHouse 运行和本项目源数据。停整个共享数据库或删除源数据不属于本次切换步骤。

## 现场规模与容量依据

| 2026-10-12 现场盘点 | 数量 |
| --- | ---: |
| 五个在用库 | 70 张表，约 4.404 亿条物理记录 |
| 在用库 active parts | 29.64 GiB 压缩占用，138.05 GiB 未压缩列数据 |
| 全部十二个项目库 | 125 张表，约 4.525 亿条物理记录 |
| 全部项目库 active parts | 31.66 GiB 压缩占用，148.43 GiB 未压缩列数据 |

以上来自 `system.parts`，包含尚未替换合并的物理重复，不是逻辑行数，也不是 MySQL 体积预测。现场根盘剩余约 759 GB，内存约 8.7 GiB，已有约 2 GiB swap 在用。迁移时 ClickHouse、MySQL、Redis 与原采集器将并存，必须限制内存和历史导入速度。

此前[隔离压测](../../research/2026-10-12-redis-mysql-evaluation/summary.json)覆盖 337 条 CEX 流和 2,768 条衍生品流：

- 正常一分钟约 8.8 万行、18–19 MiB 原始数据；Redis 一分钟约 29 MiB，两分钟约 57 MiB。
- 全部保存档位每秒完全换价的合成压力场景，Redis 两分钟约 297 MiB；它不涵盖所有质量证据和目录变动极值。
- MySQL 三个真实分钟端到端各用 7.6–16.3 秒，合成压力分钟约 16.9 秒；264,905 条真实记录全字段回读一致。
- 上述测试使用逐字段定类型二进制数组，MySQL 开启 redo、binlog、doublewrite 和提交同步；Redis 当时未开启持久化。

这些结果支持开始实现，不等于生产验收。按短样本外推约 1.27 亿行/日，冷历史、索引、持久化、后台刷盘和迁移竞争都须重测。不得承诺换库后整机 I/O 会下降某个固定比例，也不得按 ClickHouse 压缩体积直接预留 MySQL 磁盘。

## 目标架构

```text
公开数据源
  → 适配器与标准化
  → 本地完整 L2 或 AMM 状态
  → 每秒采样与分钟封口
  → Redis 当前分钟及上一分钟 / 持久交付确认
  → 有界 writer → MySQL 定类型事实与提交记录
                         ↑
                ClickHouse 历史迁移器

低频 collector → 完整性校验 → 原有 pending 与批量 writer → MySQL

回放与报告 → MySQL 已提交批次
实时预览 → Redis 已采样部分，并明确当前分钟尚未封口
```

每个来源的完整 L2 仍由采集进程维护。Redis 不接收每条 WebSocket 消息，也不替代序列校验。低频收益、规则、链上事实继续按来源频率处理；它们无需为了使用 Redis 而增加一分钟缓存。

采集程序与存储之间使用按职责划分的接口，ClickHouse 和 MySQL 各自实现。现有标准化、模型哈希和回放算法复用；SQL 方言和驱动不进入采集模型。Redis 负责分钟交付，不成为第二套永久事实库。

## 已确定的设计选择

- 保留全部逻辑历史和原有数据语义；不减少深度、采样频率、合约覆盖或来源。
- 新现货、永续、交割期货每侧 5 档，期权每侧 10 档；历史按原 `stored_depth` 回放 5/10/50 档。
- Redis 正常保存当前分钟和上一分钟；未写入 MySQL 的批次不按时间自动淘汰。初始 `maxmemory=512 MiB`、`noeviction`，进程含持久化余量预算 1 GiB。
- Redis 默认 AOF 每秒同步，封口批次交接须取得本地持久化确认；当前分钟不作零丢失承诺。Redis 满或不可用时，封口批次进入有界磁盘待写目录。细节见存储协议。
- MySQL 采用 InnoDB，redo 提交同步和 doublewrite 保持开启。2026-10-16 按用户明确指令关闭 binlog，原有日志文件保留；随后采用 `ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8` 物理页压缩，原编码和冻结迁移身份保持。以上覆盖早期方案中持续启用 binlog 和未压缩表的设置。buffer pool 及容器实际资源以运行说明顶部为准，早期压测配置不代表当前值。
- MySQL 保留分表的定类型模型。数组使用有版本、受长度限制的逐字段二进制编码；UInt256 使用固定 32 字节编码，禁止 Float64 或通用 JSON 事实表。
- 新采集与历史导入使用不同连接池、账户和调度配额。历史迁移主动让出资源，不能耗尽新数据写入窗口。
- 新服务启动前导入身份、计划、恢复依赖；完整历史在切换后迁移。迁移期间默认关闭面向使用者的历史报告，内部健康与恢复检查继续运行。
- 原库保留以便核验；若坏 part 导致某范围无法读取，记录缺口并单独处理，不能把跳过错误称为全量完成。

## 四步交付

| 用户步骤 | 实现 Agent 应交付的具体结果 |
| --- | --- |
| 1 写设计 | 本目录的协议、逐表清单、实现顺序、切换与验收要求 |
| 2 安装工具及编程 | 独立 MySQL/Redis、Go 驱动与存储实现、writer、查询、迁移及校验工具；隔离测试通过 |
| 3 停旧服务启动新服务 | 必要状态就绪；逐服务记录旧端最后提交与新端首个有效提交；新端持续写入并可恢复 |
| 4 迁移 ClickHouse 数据 | 固定源范围、分块导入、回读和回放校验；无未解释冲突后开放完整历史查询 |

本项目没有实盘交易，因此不需要为迁移增加交易停机、订单切换或资产安全流程。实际需要防止的是采集缺口被掩盖、旧数据覆盖新修订、身份改变，以及尚未完整的批次被当成可查询事实。
