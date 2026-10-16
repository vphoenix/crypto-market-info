# 实现与验收

## 实现边界

本设计后的安装、编程、部署和迁移由用户指定的另一个 Agent 执行。本轮只交付文档和只读盘点。实施者先读取根目录 `AGENTS.md`、[现行架构](../architecture.md)、[数据模型](../market-data-storage.md)和[运行说明](../runtime-operations.md)，不把方案描述误报为当前状态。用户交给后续 Agent 的实际指令决定其执行范围；本目录不能自行授予部署或迁移权限。

工作区已有与恢复、发现事实、Reserve 和 Across 有关的未提交改动。不得重置或覆盖；构建时保存源文件快照、Git 状态和二进制摘要，明确实际包含的版本。不要将临时容量测试容器直接更名为正式数据库。

## 开发顺序

| 阶段 | 工作 | 完成证据 |
| --- | --- | --- |
| A 类型与接口 | 收敛各领域 Store 接口；实现无损 codec 和每表 DDL/键/版本规则 | 全部现场类型有映射；边界值与历史 codec 测试通过 |
| B MySQL 存储 | 身份登记、CEX、衍生品、发现事实、DEX、收益及四个独立 collector 的读写 | 每个生产调用路径有 MySQL 实现；事务与幂等测试通过 |
| C Redis 交付 | 秒缓存、封口、AOF 确认、writer、背压与磁盘待写 | 杀进程/重启/超时/队列满测试通过 |
| D 迁移与核验 | allowlist、bootstrap、全量分块、回读、回放、冲突清单、恢复点 | 隔离库重复导入及中断续跑结果一致 |
| E 运行集成 | 五个服务、所有 CLI、状态指示器、配置与监控 | 隔离运行和生产切换演练通过 |
| F 正式切换 | 必需数据准备、旧端 drain、新端启动、边界核验 | 五个服务分别满足切换验收；记录真实缺口 |
| G 历史迁移 | 新端持续采集时限速搬迁、逐块核验、最终报告 | 清单逐项有结果，完整历史查询在核验后启用 |

开始迁移时不重新请求交易所全量数据，不通过再开一套公开数据订阅来代替存储回放测试。实测尽量使用冻结的真实样本和独立数据库，避免重复耗尽现有 RPC/REST 配额。

## 代码边界

建议新增以下目录，命名可按项目惯例调整，职责不能混淆：

| 目录或命令 | 职责 |
| --- | --- |
| `internal/storage/contracts` | 按领域拆分的存储接口与共有错误；不依赖数据库驱动 |
| `internal/storage/codec` | 数组、Tuple、NULL、UUID、UInt256 和紧凑质量字段编码 |
| `internal/storage/mysql` | 显式 DDL、事务、分区、身份登记、领域 writer/reader |
| `internal/storage/redisbuffer` | 秒采样缓存、封口、租约、确认与保留窗口 |
| `internal/storage/spool` | 持久待写文件、配额、恢复和提交后清理 |
| `internal/storage/migrate` | 每表源/目标映射、分块读取、冲突裁决和验证 |
| `cmd/storage-writer` | 消费已封口盘口批次；处理 Redis 与磁盘待写 |
| `cmd/storage-migrate` | inventory、bootstrap、copy、resume、verify 等子命令 |
| `cmd/storage-check` | 新端写入、积压、回放、恢复依赖和迁移状态检查 |

`cmd/*` 与 `internal/app` 多处直接接受 `*clickhouse.Client`，必须逐项替换为使用侧所需接口。不要通过文本替换 SQL 来兼容 MySQL，也不要新增一个把所有方法包起来的隐式 ClickHouse 代理。保留 ClickHouse 实现，供旧端读取、对照和回退工具使用。

重点改造入口：

- `internal/sampler/engine.go`、`buffer.go`：增加每秒不可变采样交接；保留完成分钟、有效位和队列容量语义。
- `internal/optionslive/catalog_runtime.go`、`catalog_pending.go` 及 worker：保持 plan/run/minute 身份、证据、共享写入额度和恢复优先级，不能在采样协程同步写 Redis。
- `internal/storage/clickhouse/writer.go`、`options_catalog_minute.go`、`discovery*.go`：提取复用校验，保留数据可见性与 first-knowledge 语义。
- `internal/storage/clickhouse/{dex,reserve,across,lst,keeper}*`：逐项迁移读取与修订，保留批次成员、收据引用、reorg 与最终性校验。
- 主 collector、四个独立 CLI 以及 `perp-check`、`options-check`、`dex-check`、`discovery-probe` 和其他引用存储的命令：选择后端与管理连接生命周期。ClickHouse 专用维护命令明确标为 legacy，不假装支持 MySQL。
- 状态指示器及健康 SQL：改读新端，区分“新采集正常”和“历史迁移完成”。

低频路径可以直接提交 MySQL；盘口 writer 的异步接纳必须使用新的返回语义，不能沿用“返回 nil 表示数据库已完成”来提前推进原业务游标。

## 安装与资源隔离

使用独立数据目录和独立账户。可沿用隔离测试验证过的官方 MySQL 8.4 与 Redis 7.4 系列镜像，但固定实际版本与 digest，安装时核实受支持版本和安全补丁。Go 依赖选择维护中的 `go-sql-driver/mysql` 和 `redis/go-redis`，固定版本进入 go.mod/go.sum。

本机已有 Docker，适合使用仅挂载 Unix socket 的独立容器；由新的用户 systemd unit 管理启动与恢复，避免 Docker restart 策略和 systemd 同时竞争。采集服务仍在宿主机运行，不迁成可选 Compose 开发环境。若改用本机二进制，须提供等价的生命周期、权限、版本锁定和资源隔离。

拟新增 `crypto-market-info-mysql.service`、`crypto-market-info-redis.service`、`crypto-market-info-storage-writer.service`。五个采集 unit 保留原名称、RPC gate、来源配置和状态目录，更新二进制与显式后端配置。不要把整份旧 unit 环境打印到报告或提交凭据。

初始参数：

| 项目 | 起始值与处理 |
| --- | --- |
| MySQL buffer pool | 512 MiB，进程预算 1.5 GiB；并存实测后调整 |
| MySQL redo | 256 MiB 起步，观察 log waits 与 checkpoint，再决定是否扩大 |
| 提交耐久性 | redo 每提交同步、doublewrite 开启；2026-10-16 按用户明确指令关闭 binlog，原日志文件保留 |
| MySQL 连接 | 所有进程连接池总预算 24；初始实时池共 16、历史迁移 1、健康与管理预留 7；不能每进程各开 24 |
| Redis | maxmemory 512 MiB、noeviction、AOF 每秒同步及封口确认，进程预算 1 GiB |
| 历史导入 | 一个 worker 开始，每块字节/行数有界，独立连接；实时队列优先 |
| 待写目录 | 20 GiB 上限；报警早于耗尽，不能自动删除未提交批次 |
| 日志 | 正常运行避免逐消息/逐行日志，保留有上限的错误诊断与批次摘要 |

账户按实时写、查询、迁移和 DDL 分权；不开公网监听。数据库密码如需生成放在仓库之外的受限配置文件，不提交 `.env` 或凭据。MySQL 已有 binlog、Redis AOF、磁盘待写和迁移临时文件都计入剩余空间；关闭 binlog 不等于删除已有日志，本次保留原文件。

## 正确性验收

必须使用生产 MySQL/Redis 配置运行有意义的集成测试，不以纯 mock 或几条 INSERT 成功替代。

| 验收类别 | 必须覆盖 |
| --- | --- |
| 盘口回放 | 新 5/10 档、旧 10/50 档和混合历史，逐秒逐价逐量相同 |
| 差量 | 同秒多次更新最终值、零数量删除、无变化秒、无效秒后恢复、全部换价 |
| 序列与重连 | CEX 断档、重建、采样溢出、跨分钟重连不能造有效盘口 |
| 元数据身份 | 已有 ID 保持；新登记不撞号；同 symbol 不同市场/版本不合并 |
| 数值编码 | int64/uint64 极值、UInt256 的 0 与 2^256−1、Decimal 精度、NULL/空数组、零字节、UUID、微秒时间及嵌套 Tuple |
| 期权 | 分片计划、目录变化、指数、质量、证据、完整成员计数、原摘要与不可变 batch |
| 发现事实 | 来源时间、首次已知时间、发布顺序、来源证明、重复发布仍不合法 |
| 低频 | 严格解析、定点路径、完整批次、稳定重试身份、单来源失败隔离 |
| 链上 | canonical/revision、重组、最终性、缺失成员、跨批收据引用、完整区间及游标恢复 |
| 幂等与崩溃 | 提交前断线、提交后应答丢失、ACK 前重启、Redis 重启、AOF 恢复、spool 转移中崩溃、双消费者 |
| 容量故障 | Redis 拒绝写、MySQL 停机、磁盘队列满、迁移抢占资源；明确报警和真实缺失 |
| 历史迁移 | 同范围重复导入、中断重跑、源坏块、同键冲突、旧修订与新修订重叠 |

对每个读取入口证明不能读出未完成批次；不能只测主查询而遗漏 CLI report/export。重启测试应通过原有恢复接口核对游标，而非只检查进程存活。

## 性能与磁盘验收

上线前完成真实样本连续回放至少 60 分钟，并运行合成全档换价、Redis AOF 重写、MySQL checkpoint 与历史导入并发场景。至少导入一个完整历史日，覆盖代表性活跃与非活跃流，并使用超过 buffer pool 的数据测冷读。正式切换后再观察至少 24 小时；这是持续运行验收，不要求等 24 小时后才开始历史迁移。

初始上线门槛：

- 全体流每个封口分钟的最后一个分片，正常负载下在封口后 30 秒内提交的 P99 达标；任何分钟耗时达到 60 秒需明确原因，持续积压不得上线。
- 实时 writer 已封口积压正常时小于两分钟；历史导入导致延迟达到 30 秒时先降速，达到 60 秒或触发内存/磁盘保护时暂停导入。
- 与旧系统相同负载比较采样迟滞、无效秒与重连，不把减少采集覆盖当成性能改善。
- 代表性一分钟逐秒回放 P95 小于 1 秒；一天单流查询记录冷热 P50/P95，目标不劣于同机旧端两倍。若不达标先定位并修改设计，不能隐藏查询退化。
- 记录总磁盘读写、fsync、IO PSI、swap、数据库进程及 cgroup I/O。比较时将一次性迁移 I/O 与稳定采集阶段分别列明。
- 测量全天压缩后表数据、索引、binlog/AOF、待写目录与临时空间。保留全部历史的前提下给出 7/30/90 日预测与剩余容量，容量不足时扩容或调整经验证的无损编码，不能擅自删旧数据。

物理空间预算用实测估算：`现有占用 + MySQL 目标全量估计 + 迁移期间新增数据 + binlog/AOF + 待写与临时峰值 + 余量`。导入前要求仍能保留至少 `max(50 GiB, 实测七日新增占用)` 的余量；运行中低于该值暂停历史导入并报告。此阈值不等于七日后可以删除数据。

此前 264,905 行字段一致和分钟写入耗时是输入依据，不替代以上验收。不得将 MySQL 已用表空间的三分钟固定分配开销线性外推为每日增长。

## 实现交付清单

- 全部生产链路与查询适配完成，无隐式回落到 ClickHouse 写入。
- 表结构迁移有显式版本，运行账户不在每次启动随意执行 ALTER。
- 新端 writer、迁移器和校验器支持有限重试、中断恢复、结构化进度和稳定 ID。
- 单元、集成、race、故障注入、真实回放和容量结果可复查；失败范围如实列出。
- 同步更新 `docs/market-data-storage.md`、`docs/architecture.md`、`docs/runtime-operations.md` 及相应 CLI 使用说明；保留专项模型约束。
- 保存部署源快照、旧程序和 unit、配置摘要、切换清单、覆盖与冲突报告；不将敏感值写入记录。
- 最终分别报告“新采集已切换”“历史已导入”“校验已通过”，三者不能相互替代。
