# 2026-10-16 进一步减少磁盘写入

用户授权执行降写入建议。本轮上线交付优化，对 MySQL 后台刷盘做有限对照，并用真实数据验证质量字段共享。历史迁移保持暂停；生产数据、未提交队列、恢复 epoch 和 producer generation 均保留。证据目录：`var/storage/io-reduction/20261016-execute/`。

最终结论（21:24，UTC+8）：保留已上线的暂存压缩、已提交 Redis 残留回收及 pipeline。MySQL 候选参数未证明稳定降低写入，已自动恢复 `10000/20000`，未修改持久配置。质量共享只完成只读原型，尚未启用生产新表。**本轮没有证明整盘写入明显下降。**

## 已上线的交付改动

### 有版本的无损暂存压缩

`spool.Queue.Accept` 对至少 1 KiB 的 payload 尝试 zlib BestSpeed；只有包含新长度字段后仍更小时才写 `CMIS 02`，否则保留 `CMIS 01`。原始 `Marshal` 仍输出 v1，冻结验证归档不变。

v2 保留原四个身份字符串、generation、UTC 分钟、逻辑 codec、成员数、原始 payload 长度和 SHA-256；在 digest 后增加大端 uint32 压缩长度，再写一个 zlib 流及整文件 SHA-256。原 payload、交付 ID、文件名和逻辑 codec 均保持。文件与目录 fsync、原子 rename、所有权转交、MySQL 核验后 ACK 删除的顺序不变。

读取同时支持 v1/v2，先验证文件长度和整文件摘要，再按声明的原始长度加一限制解压输出；拒绝未知版本、超过 32 MiB 的原文、长度不符、损坏 zlib、尾随流/字节及 payload 摘要不符。调度器候选字节数和 `List` 总预算按**解压后**大小计算，磁盘配额按实际文件大小计算。恢复 `.pending-*` 和跨格式重试比较完整逻辑内容。

### 回收已提交的 Redis 不完整副本

原问题：Redis begin 已留下 collecting header 和部分成员，但后续超时让完整交付转存 spool；writer 从 spool 提交后，没有 Redis 租约，因此不会清理那份 collecting 副本。正常重启 generation 不变，按旧代次清理也不能解决。

writer 每十秒另外给予最多两秒的回收预算，保留独立扫描游标和未处理页；每轮最多扫描 16 页、检查 100 个 header。只处理超过两分钟的 collecting 批次，核对 database/kind/ID/service/generation/minute/codec/member_count/digest 和本批次成员键。调用 MySQL `Committed` 验证原子事务提交凭据的完整身份，不仅凭年龄或当前代次删除。该凭据由事实及成员清单同一事务提交，不单独给缺失或冲突批次补造凭据。

证明成立后，Lua 再比较完整 header、digest 和 collecting 状态；并发封口或 header 变化会使本次清理失效。只解除该 header 列出的本批次成员，留下 24 小时 `retired` 墓碑。无证明的 collecting 批次不设过期时间。成员写入同时核对父批次状态和 digest，begin 不允许重新打开 retired/abandoned；迟到写入不能重建孤儿成员。墓碑过期后，缺失父批次的迟到成员写入仍被拒绝。

Redis begin、成员写、seal 和 WAITAOF 使用同一连接；成员命令改为每组最多 32 条的 pipeline。任一命令错误都不进入 seal；seal 仍核验全部成员 digest，最后验证本机 AOF 和连接身份。未获确认仍走有界 spool。采样、数据有效性、秒级差量及查询窗口不变。

回收释放内存后，也可能恢复先前被容量门槛阻止的预览和 AOF 写入，不能将释放内存直接等同于磁盘写入下降。初期回收会读取长期未访问、部分已换出的页面；此阶段与采集重启预热均排除在性能验收之外。

## 验证与部署

- 存储、采样、回放和 optionslive 回归通过；旧 5/10/50 档、缺秒、删除、有效性和原 codec 测试保留。
- 新增新旧信封重试、临时文件恢复、调度内存预算、截断、解压长度伪造、内部校验和、尾随数据测试。
- 独立 Redis 7.4.11 集成测试覆盖缺证明/冲突保留、并发 seal、跨批次键拒绝、迟到成员/迟到 begin、跨页否定证明扫描进度、当前分钟保留。
- pipeline 以 100 本 CEX 跨四个分块验证完整回读；第二分块冲突不会发布；实际生产期权交付逐字节回读通过。Redis/spool/writer race 回归分别通过，耗时 9.565、3.169、1.481 秒。测试 Redis 使用独立目录/socket，结束后正常停止。

20:19:50（UTC+8）上线第一版压缩/回收；补充 pipeline 后于 **20:31:59（UTC+8）** 正常重启 collector 与 writer。先停 collector，再停 writer，原子替换后先启兼容 v1/v2 的 writer，最后启 collector；没有清除队列。

| 服务 | PID | SHA-256 |
|---|---:|---|
| collector | 396944 | `53068b5c960e0321854f2291b3533449216fb573c33523792c5ca3bee07d9eb6` |
| storage-writer | 396936 | `bfb279a671024d4e60444dd7e319edde03fccf59478b2bbd9713eb7880f3f2d3` |

两者 `NRestarts=0`，collector/LST generation 均保持 1，holder 前后一致，其他采集服务未重启。最终部署清单为证据目录的 `final/deployment.json`。已观察到生产 v2 spool 文件，样本保存在 `options-delivery.batch`。

该实际期权样本原 payload 193,824 字节，v2 文件连同头部和校验为 20,307 字节（原 payload 的 10.48%）。这是暂存文件体积，不是数据库或整机 I/O 降幅。

启动恢复期间出现来源订阅 ACK/HTTP 超时，以及期权证据事务超时，不能把该阶段的低写入视为收益。20:42 起 OKX 配对目标 332/332 就绪、期权目录 2,956 条且 unknown/stale 为零。数据库核验 UTC 12:43 分钟：CEX 5 批/337 条，全部 60 秒有效；期权 93 批/2,956 条，回放有效秒 177,037/177,360，2,633 条全 60 秒有效，剩余缺秒保持无效，没有补造。该读回结果和收益新观测在 `health-after-recovery.log`。

20:39:50 的 Redis 核验：989 个残留批次已凭证明转为 retired，collecting 为零，Redis ready 和 spool 均为零，内存 64,781,240 字节。当前批次恢复后内存会随两分钟缓存窗口波动，不能将此刻数值视为固定内存上限。

21:23:54 最终核验：六服务均 active、`NRestarts=0`，collector/writer 实际哈希匹配部署清单，generation/holder 保持；Redis ready、spool、writer 旧待写及阻塞均为零。Redis 内存 106,960,984 字节（102.01 MiB），状态扫描见 1,164 个 retired、54 个 committed、5 个 collecting；新分钟仍可能产生部分副本，回收按原证明条件持续进行，不能宣称 collecting 永久为零。UTC 13:21 分钟实际提交 CEX 5 批/337 条、期权 93 批/2,958 条，全部 60 秒有效；期权 sampled/stream/replay/market-known/open 各 177,480 秒，完全一致。证据为 `final-state.json`、`health-end.log` 和 `health-end/`。

该完整分钟不代表全部来源持续健康：最近十分钟仍有 OKX 发现/贷款接口失败、DEX 不完整和缓存维护超时后重试。候选与恢复阶段均出现短暂盘口重同步；最终快照已回到 332/332 就绪，期权 unknown/stale 为零。缺失区间按真实无效状态保留，不补造为完整历史；低频来源的新旧观测清单另存于 `health-end/yield-latest-summary.json`。

**回退限制：** 不能把 writer 直接换回只读 v1 的旧版本。须先停止新增 v2 的生产者，保留兼容 v2 的 writer 完成全部 v2 交付并核验排空；或构建保留 v2 读取、恢复 v1 写入的版本。所有回退保留队列、代次、epoch 和 checkpoint，不清队列。

`final/backup/` 是本轮第一版 v2 程序，支持 v2 回读，可用于仅回退 pipeline 补充；外层 `backup/` 才是本轮上线前的 v1 程序，受上述排空限制约束。

## MySQL 刷盘对照

`tools/storage/io-reduction/trial.py` 固定生产 UUID 和私有 socket，等待六次连续全范围来源就绪且无旧待写，再观察 5 分钟 `10000/20000`，将 `innodb_io_capacity/innodb_io_capacity_max` 暂调 `1000/2000` 观察 30 分钟，自动恢复后再观察 5 分钟。持续 checkpoint 高水位、旧待写/阻塞或新增 redo 等待提前恢复；异常和正常终止信号也执行恢复。初始设置和每阶段数据记录到独立目录，重跑须使用新目录。试验不改提交 fsync、doublewrite、flush_sync、binlog 或内存，不自动固化候选配置。

依据是 [MySQL 8.4 官方后台 I/O 配置说明](https://dev.mysql.com/doc/refman/8.4/en/innodb-configuring-io-capacity.html)：过高的后台刷新速度会削弱缓存合并写的效果，取值仍须保证跟上负载。验收结合实际成员提交量、来源完整性、checkpoint、延迟和项目进程总写入；不把脏页暂存当作永久节省，也不使用重启后的空载样本。

### 完整对照结果：候选值不固化

21:23:03 有限观察进程正常退出，`complete.json` 记录自动恢复及 `accepted=false`。21:18:22 独立读回与最终状态再次确认 `10000/20000`、提交 fsync=1、doublewrite=ON、flush_sync=1，原有 binlog=OFF 保持；没有数据库重启。试验早期版本把恢复参数和恢复阶段数组写到同名文件，已将工具修正为 `restore-settings.json` 与 `restored.json` 分开；本次恢复凭据用独立只读查询补存，原阶段数组完整保留。

| 阶段（UTC+8） | 项目进程写入 MiB/s | MySQL 写入 MiB/s | 整盘读 / 写 MiB/s | 整盘忙碌率 | MySQL KiB / 实际提交成员 |
|---|---:|---:|---:|---:|---:|
| 原值 20:42:48–20:47:54 | 8.54 | 6.73 | 1.08 / 18.96 | 56.89% | 132.14 |
| 候选 20:47:54–21:18:01 | 8.98 | 7.14 | 0.63 / 17.52 | 37.93% | 133.16 |
| 恢复 21:18:01–21:23:03 | 10.25 | 8.16 | 0.34 / 21.65 | 31.19% | 151.65 |

进程写入为 `/proc/<pid>/io` 计数；各进程合计不能视作整盘物理归因。分母使用 MySQL 提交时间范围内的实际分钟成员数：15,953、99,225、16,639；不是十秒日志边界的近似值。阶段输入量、行情活跃度和读取量不同，恢复段还包括待刷页偿还，因此不能把三行直接当作严格同输入压测。

候选期相对前置原值没有降低每成员写入。恢复原值后磁盘忙碌率反而继续下降，进一步说明不能将忙碌率差额归因于候选值。来源完整性也未贯穿全程：原值 31 个样本全部就绪；候选 176 个样本有 29 个不完整，最早 21:08:51、OKX 最低 272/332；恢复 31 个样本中有 3 个不完整、最低 312/332。21:09 发生来源 HTTP/订阅超时、采样错过截止、一次 writer SELECT 超时和期权证据锁等待超时，随后重试；不能将该段较少有效输入视作节省，也没有足够证据把全部异常归因于参数。

全部阶段旧待写、blocked、新增 redo 等待和 buffer-pool 空页等待均为零。候选 checkpoint age 峰值 66,577,413 字节，低于 256 MiB redo 的 75% 回退阈值（192 MiB）；恢复后从 17,410,095 降至 4,045,078 字节，脏页由 4,588 降至 1,149，没有借持续积压隐藏写入。21:05 的两批规则刷新约 4.7/2.9 秒完成，期间分钟写入继续。原始证据、各五分钟分段、源完整性计数和数据库分母保存在 `io-trial-final/{baseline,candidate,restored,summary,commit-counts}.json`。

暂存压缩样本本身减少约 89.5% 文件字节，但清理 Redis 后也恢复了正常缓存/AOF 写入。当前 MySQL 仍是项目最大的写入者；本轮只确认交付副本更小、缓存残留能够回收，没有确认项目总物理写入下降。下一步优先验证下述目录证据结构的物理收益，不继续仅凭瞬时忙碌率调整参数。

## 质量表共享原型

`tools/storage/quality-profile-probe` 为只读原型，每表只取指定 UTC 分钟、最多 4,001 行、SELECT 限时五秒。将已有定类型 RLE 列组成带长度边界的字段集合，按 SHA-256 分组；按每行引用还原后逐字段比较原字节。没有改生产表、历史字节、有效性位图或证据引用。

| 表/分钟 UTC | 行数 | 不同字段集合 | 原字段字节 | 共享字段及每行 32 字节引用 |
|---|---:|---:|---:|---:|
| 盘口质量 11:45 | 2,954 | 2,954 | 3,106,140 | 3,413,356 |
| 盘口质量 12:14 | 2,956 | 2,956 | 3,130,748 | 3,438,172 |
| 目录证据 11:45 | 2,954 | 7 | 1,587,282 | 99,031 |
| 目录证据 12:14 | 2,956 | 11 | 1,576,748 | 101,941 |

全部字节还原一致，四次读取分别耗时 617、433、278、324 毫秒。盘口质量候选包含 captured/connection/market/rule 时间和连接 epoch，每个 instrument 都不同；整组共享增加约 10% 逻辑字节，**该方案拒绝上线**。不能为共享抹平微秒时间或来源身份。

目录证据八组 RLE 字段高度重复，原型减少约 94% 的这部分逻辑字节，适合进一步验证独立定类型 profile 表和分钟成员引用表。此数值不包含行身份、索引和事务记录，也不是物理写入降幅；InnoDB 已压缩，须在隔离库同输入、同查询、充分刷盘后测量增量收益。两张旧表的 `information_schema.TABLES.DATA_LENGTH` 合计估算约 16 GiB，这不是实际压缩文件体积；直接改结构会重写大量历史，本轮不据逻辑字节原型执行生产重建。

生产启用门槛：新旧历史统一读取，原 row/batch hash、全部 60 秒质量、NULL、微秒及来源证据一致；成员与 profile 同事务发布，悬空引用不可见；重试冲突整批回滚；索引支持原 run/minute/instrument 查询；真实活跃/非活跃样本验证物理页写入、每日占用和回放时间。需独立版本开关和历史读取回退能力。当前此项仅完成真实数据原型验证，未启用新事实格式。

### 后续结构实现的具体边界

可行方向是只为目录证据增加两张专项表，避免全量重写已有约 4 百万行：

1. `options_catalog_quality_profile_minute`：主键 `(minute_time, profile_hash)`，保留现有八组证据字段的 offsets/values 定类型列。`profile_hash` 覆盖版本、UTC 分钟和全部字段的规范字节，NULL 和空数组保持区别，不保存通用 JSON。
2. `options_catalog_quality_member_minute`：保留 run、instrument、minute、原 batch hash、原 evidence row hash，新增 profile 引用；候选主键 `(run_id, minute_time, instrument_id, compact_batch_id)`。同一分钟的 32 成员分片尽量相邻写入。原 run/minute/instrument 查询的 EXPLAIN 和实测必须保留，不能只按行大小判断收益。

新写入只能通过独立开关启用；读取始终支持旧 evidence 表和新成员/profile 表。历史按原表读取，不在上线时回填。profile、成员引用、原分钟提交清单与 ingest_batch 在一个事务提交；相同 profile 的并发首次插入不能用忽略冲突掩盖异内容。准备阶段可缓存已验证的不可变 profile，首次重复键必须回滚后核对完整内容。读取从原提交清单恢复全部成员，逐成员恢复原 `QualityEvidence` 并复核 evidence hash；跨两种格式重复发布或悬空引用均拒绝。

关闭写入开关只让新批次回到原格式，不能回退到不认识新历史的读取器。冻结历史迁移的旧 mapping/摘要不变，新表按独立扩展 schema 安装。隔离库需比较：原压缩表、只重排主键、共享 profile 三种方案，计入辅助索引、redo、doublewrite、最终脏页偿还与共享行锁等待。原型的 94% 逻辑字段缩减不作为上线阈值；以同输入的实际总写入下降且回放/提交延迟不退化为准。
