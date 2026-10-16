# Redis / MySQL 实现交付与检查入口

2026-10-16 压缩后的资本规则扩展：三张生产压缩表和 schema 登记已安装，隔离真实事务/混合格式验证及生产只读摘要核验通过；旧 schema、125 表映射和历史迁移断点保持。新写法尚未启用，采集及历史迁移继续暂停；管理和只读查询工具已更新，常驻采集器未替换。见[执行记录](../../research/2026-10-16-capital-versions/report.md)。

2026-10-16 更新：按用户明确要求关闭生产 MySQL binlog，配置及实机核验均为关闭；redo 提交同步与 doublewrite 保持，旧日志文件保留。已开始的历史迁移随后按用户要求暂停，不自动恢复，全部事务断点保留且跨重启一致。当前运行状态以[运行说明](../runtime-operations.md)顶部为准，下文安装与容量参数包含早期历史记录，不能据此重新启用 binlog 或历史迁移。

初次交付只到用户要求的第二步：写代码、安装工具、创建 MySQL 表，供用户检查。2026-10-13 用户追加限定授权启动依赖导入、恢复验证和新服务配置准备；使用独立固定快照导入保守 59 表闭包，详细执行结果保存在 `var/storage/bootstrap-preparation/20261013/`。五个原采集服务继续运行原二进制、原配置和 ClickHouse，正式 MySQL producer 未激活。本文不表示已经满足生产切换或全量历史验收条件。

## 已安装

| 项目 | 当前安装 | 路径或状态 |
| --- | --- | --- |
| MySQL | 8.4.11，固定镜像 digest | 私有 Unix socket：`var/storage/mysql/run/mysql.sock` |
| Redis | 7.4.11，固定镜像 digest | 私有 Unix socket：`var/storage/redis/run/redis.sock` |
| 数据库服务 | 两个独立用户 unit，已运行并启用自启动 | `crypto-market-info-mysql.service`、`crypto-market-info-redis.service` |
| 分钟 writer | 二进制和 unit 已安装，保持未启动、未启用 | `crypto-market-info-storage-writer.service` |
| 程序和工具 | 全部 `cmd` 程序已构建到独立目录 | `/home/ubuntu/crypto-market-info/var/storage/bin/` |
| MySQL 表 | 明确 allowlist 内的 12 个库、125 张业务表，加 `cmi_storage` 控制表 | 不按数据库前缀扩大范围 |
| 凭据 | root、live、query、migration、ddl 分离，文件权限 0600 | `/home/ubuntu/.config/crypto-market-info-storage/`，没有提交凭据 |

安装入口为 [tools/storage/install.sh](../../tools/storage/install.sh)。它只安装准备阶段；不会修改原采集 unit、激活 producer、启动 writer 或导入源数据。MySQL/Redis 配置分别见 [mysql.cnf](../../deploy/storage/mysql.cnf)、[redis.conf](../../deploy/storage/redis.conf)，服务文件见 `deploy/systemd/crypto-market-info-{mysql,redis,storage-writer}.service`。

MySQL 只开放本机 Unix socket；UTC、严格 SQL 模式、redo 提交同步、binlog 同步和 doublewrite 均开启。buffer pool 512 MiB、容器内存上限 1.5 GiB；Redis `noeviction`、AOF everysec、512 MiB maxmemory，容器内存上限 1 GiB。MySQL 全局连接上限 24；四个独立低频服务每池 1 个连接，主库各用途每池 2 个，实时账户上限 16，查询/历史导入/DDL 配额分别为 3/1/2。

`cmi_live` 可写业务与交付控制记录，可锁定 producer 行，但没有修改 producer generation/holder 或执行 DDL 的权限。MySQL 8.4 的 `FOR UPDATE` 需要额外锁定或 DML 权限，因此单独授予控制库 `LOCK TABLES`，不授予 producer 表 UPDATE。[MySQL 官方说明](https://dev.mysql.com/doc/refman/8.4/en/innodb-locking-reads.html)解释这一权限要求。`cmi_query` 仅 SELECT；`cmi_migrate` 写事实及迁移记录，无 DDL/producer 激活权限；`cmi_ddl` 用于明确的管理操作。

## 代码检查入口

- [职责接口](../../internal/storage/contracts/domains.go)与[后端工厂](../../internal/storage/backend/backend.go)：采集层依赖职责接口；ClickHouse 和 MySQL 各自使用本地驱动与 SQL。MySQL 错误不回退到 ClickHouse。
- [MySQL 实现](../../internal/storage/mysql/)：CEX、期权、交割/永续、DEX、资金费率、收益、发现事实以及 Across、Reserve、LST、Keeper 的写入和恢复/查询接口；身份分配、修订优先级、成员证明和 producer 代次锁在事务中验证。
- [明确逐表映射](../../internal/storage/mysql/mapping.json)与[完整 DDL](../../internal/storage/mysql/schema.sql)：125 张表；保留物理列、原版本和排序身份，ALIAS 在读取时恢复。业务事实不用通用 JSON 表。
- [二进制编码](../../internal/storage/codec/)：逐字段版本编码、类型摘要、长度/分配限制；整数 tick/lot、UInt256、Decimal(38,18)、UTC 时间、NULL、数组和命名 Tuple；拒绝二进制浮点数。普通 MergeTree 的物理重复由 `_occurrence` 保留，查询不折叠重复记录；重复发布即使内容相同也保持无效。
- [Redis 交付](../../internal/storage/redisbuffer/)与[持久待写目录](../../internal/storage/spool/)：标准化每秒预览、完整分钟封口、同连接 WAITAOF、ready 修复、lease、MySQL 提交证明后 ACK、Redis 满/故障时落盘；收集或未确认批次无自动 TTL。待写文件 fsync、校验、原子 rename，重启恢复时保留未完成文件供检查。
- [writer](../../cmd/storage-writer/main.go)：独立有界消费；故障保留批次，重试退避；旧 collecting 只有在明确被新代次隔离后才能标记 abandoned；改变已有 producer 代次的管理命令先检查 Redis/spool 已排空。
- [查询工具](../../cmd/storage-check/main.go)：schema/持久化/UTC、事实计数、Redis backlog、spool 占用、明确标为 provisional 的已采样预览。桌面状态工具可显式选择 MySQL；当前桌面运行配置仍沿用 ClickHouse。
- [迁移 CLI](../../cmd/storage-migrate/main.go)与[迁移执行器](../../internal/storage/migrate/)：固定上界 plan、逐类型冻结文件、按完整业务键分块、copy/resume、事务检查点、回读 verify、冲突审计、bootstrap 依赖清单、report、finalize 和分区扩展。初次交付没有导入；2026-10-13 的限定启动依赖准备使用独立快照和五个独立 run，不打开完整历史。

## 安全检查命令

以下命令只检查已安装对象，不迁移数据。启动依赖已开始入库，逐表精确计数可能较慢；常规健康检查省略 `--count-facts`，固定快照的逐表精确计数由限定准备工具的 `check` 阶段核对。

```bash
cd /home/ubuntu/crypto-market-info
var/storage/bin/storage-migrate check-mappings
var/storage/bin/storage-check --all \
  --socket /home/ubuntu/crypto-market-info/var/storage/mysql/run/mysql.sock \
  --user cmi_query \
  --password-file /home/ubuntu/.config/crypto-market-info-storage/cmi_query-password \
  --redis-socket /home/ubuntu/crypto-market-info/var/storage/redis/run/redis.sock \
  --spool /home/ubuntu/crypto-market-info/var/storage/spool
systemctl --user is-active crypto-market-info-mysql.service crypto-market-info-redis.service
systemctl --user is-active crypto-market-info-storage-writer.service
```

最后一个命令应显示 `inactive`。原五个采集服务不应被这次准备重新启动。初次安装实证在 [installed-fact-counts.json](../../research/2026-10-12-redis-mysql-implementation/installed-fact-counts.json)，其中 0 条事实是 2026-10-12 的安装快照，不能代表启动依赖导入后的当前计数。完整历史仍保持关闭。

采集器在未来切换时才设置 `STORAGE_BACKEND=mysql`、`MYSQL_SOCKET`、`MYSQL_USER=cmi_live`、`MYSQL_PASSWORD_FILE`、`MYSQL_DATABASE`、`STORAGE_SERVICE`、`STORAGE_GENERATION`、`STORAGE_HOLDER`、`REDIS_SOCKET` 和 `STORAGE_SPOOL_DIR`。四个独立服务支持 `ACROSS_` / `RESERVE_` / `LST_` / `JUSTLEND_KEEPER_` 前缀；具体变量解析见后端工厂。当前没有修改任何原服务的这些变量。

## 工具使用边界

迁移操作必须明确指定 run UUID、family、target、table 和私有 staging-dir；每表先 plan 再 copy/resume，再 verify。verify 读取已保存的块边界，复核源摘要和目标字段，不因后来修改 `--rows` 重新定义同一个块。事务同时提交事实、摘要和检查点，失败保留冻结文件，冲突不靠覆盖处理。单个块最多 500 个普通行，完整键组最多 100,000 行/64 MiB；staging 上限 20 GiB，并保留至少 50 GiB 可用磁盘。历史导入可根据 Redis 实时 backlog 暂停。

`bootstrap` 使用完整依赖清单，不能把任意一个 `--table` 当作整个服务已就绪。四个独立低频库包含其全部 8/7/8/7 张表；主库采取保守的 29 表依赖闭包，排除高频盘口/质量/指数分钟和相应分钟提交标记，保留完整身份、规范化、计划、规格、发现、资金费、链恢复和收益数据。它仍可能需要较长时间，不承诺“几分钟即可完成”。源损坏或缺成员会阻止该闭包通过，不能重置恢复起点来绕过。

CEX 两张表字段验证后先进入 `state=5`（发布待完成）；逐分钟校验回放并写齐查询证明后，才在事务中一起转成 `state=2`。发布失败或进程中断可重跑 verify，已提交证明按摘要核对后复用。`finalize` 仅在同一 run 中 125 张正式表全部验证、固定范围完整、CEX 分钟均具有查询证明且无未解决冲突时打开 history_complete。bootstrap 不打开全量历史；用户报告/导出入口在完整历史准备好前返回 `ErrHistoryNotReady`，内部健康、恢复和新分钟回放仍可使用。

月分区包含 `p_future`，需要通过明确的 `extend-partitions --family ... --target ... --through YYYY-MM` 管理命令提前拆分未来月份；工具只拆分 `p_future`，不删除历史。binlog 当前不自动到期，正式运行前需按备份/恢复方案确定保留周期。

## 验证与后续门槛

准备阶段的测试覆盖：5/10/50 档混合历史的 60 秒精确回放、删除和无效秒、并发 ID 分配及资金费优先级、期权元数据与完整成员证明、链重组/修订/共享收据、发现首次已知时间及未发布事实不可见、收益严格解析与十进制定点路径、二进制类型边界、Redis 持久确认与重启恢复、无 TTL 保留、spool 重试/容量/损坏/崩溃恢复、账户权限、普通 MergeTree 重复记录和迁移事务检查点。

详细结果和构建摘要存于 `research/2026-10-12-redis-mysql-implementation/`；该目录按项目既有规则不提交 Git。最终通过的结果分别为：`go-tests-fixtures.log`（全部实际程序包）、`integration-tests.log`（隔离存储 race/集成）、`migration-tests-final.log`（迁移检查点与重复数量）、`final-fence-permissions-tests.log`（并发代次锁与账户权限）、`final-drain-tests.log`（Redis 交付与排空检查）以及 `desktop-tests.log`。Redis AOF 重启验证在 `final-runtime-tests.log` 的对应测试段通过；同一文件里的早期账户计数断言已由后续权限验证替代。该目录的 `build-manifest.json` 保存当时源文件和 13 个工具/程序的摘要；本轮修复后的摘要见下文新证据目录。

全项目测试使用内存文件系统保存临时响应样本，避免当前 ClickHouse 磁盘负载影响数秒的 RPC 预约窗口。普通磁盘样本目录的两轮全项目测试有既有 LST 预约超时，单独复查两项通过；这些原始失败日志仍保留。它们提示当前整机 I/O 负载需要在正式性能验收中单独测量。实际 MySQL、Redis/AOF 和初始 spool 集成测试仍使用已安装的磁盘服务，内存样本测试不替代持久化证明。

启动依赖导入与恢复验证已获得 2026-10-13 的限定授权，源恢复结果见下方记录。尚待用户检查后另行执行：停止旧端并排空写入、补齐快照后增量与刷新状态、各服务停止/启动边界和 gap 记录、真实完整负载连续 60 分钟及 24 小时压测、每日磁盘/索引/binlog 增长与查询耗时、断电/整机磁盘故障演练、冷历史全量复制与逐表验收。此前三分钟压测不能替代这些门槛。

## 2026-10-13 六项审核问题修复

- CEX 入队路径移除同步 MySQL intent 写入。采样完成时冻结 `KnownFromMS`，启用发现发布的交付使用 codec=2 将该时间放入带摘要的二进制 payload；Redis 拆分/重建、spool 和 MySQL 恢复均保留它。消费者在同一事务内写 intent、事实和发布证明。codec=1 保持可读，旧发现交付仍要求已存在的 intent，不能以恢复时间补造首次知晓时间。
- UUID 迁移键按 ClickHouse 的后 64 位、前 64 位顺序比较；UInt256 支持解码产生的 `big.Int`，包含 256 位边界。
- 普通 MergeTree 查询保留物理重复，intent、publication 和成员唯一性检查不被通用去重绕过。
- 历史 CEX 查询发布加入完成状态流程；最终检查也拦截旧代码遗留的“state=2 但证明缺失”。
- 预览清理核验所有保留帧的成员并集，缺少第 59 秒不再阻止清理；持久扫描游标让未提交旧分钟不阻塞后面的分钟，版本检查防止并发新增帧被误删。未提交数据仍无 TTL。

原始 5 个复现用例已纳入正式 Go 测试；另有真实隔离 MySQL/Redis/spool 故障恢复、部分历史发布失败/重试、超过 100 个未提交分钟、成员变化和清理并发测试。最新修复证据与构建摘要位于 `research/2026-10-13-redis-mysql-fixes/`，原审核结果保持原样。此次没有改变正式服务、生产者代次、业务表结构或执行正式迁移。

## 2026-10-13 源查询游标参数修复

第二轮审核发现的 UInt256/DateTime64 绑定问题已在 `migrate/source.go` 修复。冻结查询的上下界共用按源列类型绑定的构造函数：UInt256 使用十进制字符串和 `toUInt256(?)`；DateTime64 使用保留原列 0/3/6 位小数的 UTC 字符串和 `toDateTime64(?, scale, 'UTC')`。不再直接把 `big.Int` 或需要亚秒精度的 `time.Time` 交给驱动默认格式化。检查点本身的二进制编码及比较器保持不变。

测试包含所有现有源键类型的自身上下界条件、UInt256 全宽边界、毫秒/微秒相邻记录、混合 UInt256/时间复合键、完整重复键组以及 `Freeze` 的分块/断点重试。真实 ClickHouse 执行的都是常量 SELECT；测试仅将源表和 DDL 读取重定向为常量夹具，没有创建或更改源表，也没有写入 MySQL。证据和复跑脚本位于 `research/2026-10-13-redis-mysql-fixes-2/`。

原审核脚本直接调用旧的驱动绑定方式，保留其内容和失败日志作为复现证据；修复后的正式回归调用生产 `sourceBounds` 和完整 `Freeze` 路径。更新已安装的 `storage-migrate` 工具，仍不运行正式迁移或切换服务。

## 2026-10-13 Reserve 源坏块无损恢复

已修复 `reserve_folio_state/202610_7483_7487_2` 导致的读取阻塞。该 part 的列定义实际为 `dex_log`，数据流也无法完整解压，元数据中的 10,503 行不是丢失状态记录数。原文件已复制保全并逐文件记录 SHA-256。

从只读检查的 `/lost+found/#22056395/data.bin` 内部偏移 1,200,128 找到连续 3,690 字节、48 个完整压缩流，恢复出原始 5 条状态。官方 ClickHouse 解压器校验全部压缩块通过；只将 Array 的存储长度转换为 Native 格式累计偏移，没有重算业务字段。三个已提交批次（区块 26160933、26160940、26160947）的完整 `StateMembers` SHA-256 与原 capture 逐一相同，原时间、整数金额、身份、篮子顺序和 payload hash 均保留。

先在独立审计库 `cmi_reserve_recovery_20261013.recovered_state` 写入回读，Native 字节与恢复文件完全一致；随后恢复源表 5 行，再次通过同样校验后，仅将坏 part 移入 detached。原 part 全部文件哈希保持不变，没有删除。三个 capture 的 committed、revision、finality 和成员声明完全未改动；另两条状态没有存续 capture 声明，保留为未发布原始事实，未补造提交或可见性。

修复后整张活动状态表全字段读取通过；随后 10,355 个快照批次的成员哈希全量核验通过，缺失/不符为 0，另有上述 2 条未发布事实。随后通过现行业务读取接口分批核验全部 62,502 个已提交 Reserve capture，状态、报价、日志、完整批次与收据引用校验全部通过，错误为 0。510 个活动分片的列定义检查仅发现该错配；此检查不替代其他大表的全历史数据校验，不能据此宣布全库历史无损。

本轮属于用户明确要求的、单个已定位坏块的人工恢复，不是迁移器遇错自动隔离。没有执行历史迁移、启动新采集或切换服务。审计恢复库不在迁移白名单中。证据见 `research/2026-10-13-reserve-recovery/resolution-summary.json`、`source-repair-complete.json`、`carved-state-member-validation.json` 和 `state-members-after-repair.json`。早先准备的 `proposed-isolation.sql` 已明确标为废弃，未执行任何撤回批次或接受缺口方案。

## 2026-10-13 源历史全量读取与成员核查

14:46:52（北京时间）完成迁移白名单 125 张表的全部字段、全部记录读取，124 张通过，`crypto_market_info.order_book_second_delta` 失败。其活动 part `202609_90466_116798_41` 的 `bid_change_prices.bin` 报 CHECKSUM_DOESNT_MATCH，指定 part 校验再次返回失败；原件仍在活动表中，未隔离或删除。同表其余 93,492,126 条差量全字段读取通过。坏 part 元数据的 1,544,322 行和对应 9 月 8–12 日范围属于整个分片，不代表全部行损坏或丢失。

另确认两组会阻断领域迁移校验的历史成员缺失：DEX 有 27 个存续已提交批次不完整，其中 7 批缺 406 条报价、26 批缺 154 条日志、5 批缺 13 条收据（批次重叠）；生产读取器逐项核对数量和摘要后确认。Keeper 全量核查 285,481 个已提交批次，3 批各缺 1 个事实，合计 2 条费用、1 条探测；生产单批读取器复现 capture_members_mismatch。不能把表可读取当作这些提交仍完整。

Keeper 另有 14,114 个成功提交的历史分页缺少索引页，现行兼容逻辑允许页为空，普通成员校验不会拦截该缺口。Across 173,243 个、LST 16,989 个已提交批次的定类型成员校验通过；不据此宣称采集覆盖从无缺口。共享实例其他项目 `crypto_grid_trading` 的旧 UNKNOWN_CODEC 活动分片仍在，不属于本次迁移白名单。

本轮仅核查并记录，未修改源事实、DETACH/删除分片、运行迁移或切换服务。结果是新定位出的历史问题，不能据发现日期推断损坏发生日期。尚未完成所有 CEX/期权历史分钟逐秒回放或所有 detached 历史缺口核验。当前不满足正式全量迁移完成或服务切换条件；完整证据和复跑入口见[源完整性报告](../../research/2026-10-13-source-integrity/report.md)。

## 2026-10-13 后续源历史恢复

17:43:37（北京时间）完成本轮恢复及复核。上节记录的是修复前快照；当前已补回原始 116 条 DEX 报价、154 条 DEX 日志、1 条 Keeper 探测和 14,114 条 Keeper 索引页。报价和探测来自已有 detached 父块，逐批符合原提交成员 SHA-256。索引页来自找回的完整旧合并父块及其原校验文件，通过官方压缩块校验、原文件校验和、全部字段回读一致性，以及原 capture/索引成员领域校验。

DEX 日志通过正在运行采集器已有的公开 RPC，按原区块哈希取回；其中三个区块的日志接口报告历史裁剪，改从其已有八个交易收据取回日志。全部 26 批、154 条日志均符合原提交 `LogMembers` SHA-256，隔离表及源表回读再次通过。新读取证明单独保存，原提交声明、观测时间、区块身份和 payload hash 未替换或重算。

生产读取器复核原问题范围 27 个 DEX 批次，21 批已完整恢复，6 批仍缺成员：五批缺 290 条历史报价，五批缺 13 条历史收据（批次重叠），日志缺失为零。Keeper 全量重核 289,048 个已提交批次，探测和成功分页缺口均消除，仍有两批各缺一条原费用观测。

CEX 坏 part 的 424 个压缩块中定位到一个含连续 64 KiB 清零的损坏块；未找到对应原始副本，不能通过重算校验和或改程序补造盘口。原 31 个文件已复制保全，修复前后 SHA-256 均相同，活动坏 part 未隔离。当前剩余报价、收据和费用观测也未找到可认证完整副本，其原观测时间和响应哈希不能以当前 API 数据替代，继续无损恢复需要原始备份或其他原始证据。

本轮没有重新全量读取全部 125 张源表，不替代上节全库审核范围。正式 MySQL 的 125 张业务表精确计数仍全部为零；原五个采集服务、ClickHouse 和迁移写入器的 PID、重启计数与上一轮审核相同，写入器仍 inactive。仅人工补回已认证源事实，没有执行 MySQL 迁移、切换服务、撤回提交、删除或隔离原件；隔离库 `cmi_source_recovery_20261013` 不在迁移白名单内。过程、剩余批次和复核入口见[恢复报告](../../research/2026-10-13-source-recovery/report.md)及[汇总证据](../../research/2026-10-13-source-recovery/summary.json)。
