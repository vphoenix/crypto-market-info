# 当前部署与运行说明

2026-10-29 02:37:26（UTC+8）：按用户要求结束 Across Base↔Arbitrum 原生 USDC 路线采集，执行 `systemctl --user disable --now crypto-market-info-across.service`。进程约 8 秒正常退出（Result=success、退出码 0），02:37:33 写出最终运行断点；实际为 inactive/dead、MainPID=0、disabled，default.target 启动链接已移除，验证无循环诊断。依据为[四项目评估](../research/2026-10-19-four-project-evaluation/report.md)：近七天正原始价差约 19 USDC/日（未扣 LP 费、gas 与库存成本），低于目标所需 82.19/日，实时探测未确认任何可由我们成交的订单。停采前只读核查确认实时采集仍在推进（两链最新收据 capture 为 complete/committed/finalized，Arbitrum 已越过重启前锚点）；最终性核对频繁 `deadline_exceeded` 是每轮 20 秒网络预算对 24 个 capture 偏紧、共享限速与慢 MySQL 写入叠加所致，不是停采原因。历史数据库、运行状态、来源额度与证据目录全部保留，未删除。其他服务 PID 与重启计数前后一致，状态记录在 `var/storage/across-retirement/20261029/`。结论只针对当前路线，不代表所有 Across 策略都没有机会；Ubuntu 重启后应保持停用，不要依据下文旧记录重新启用。

2026-10-29 01:11:41（UTC+8）修复 OKX 全部停采：自 10-27 17:03 起 OKX 在 8443 端口连接上先推送 `64010 WebSocket port 8443 will be retired soon`，适配器将 notice 视为连接错误，导致全部 OKX 盘口与资金费连接循环重连；CEX 从 10-27 09:10 UTC 的 337 路逐步降至 09:35 UTC 起仅剩 Binance/Bybit 3 路，其间约 31.6 小时的 OKX 实时盘口无法补回。实测 443 端口无该 notice、快照/增量正常。collector unit 新增 `OKX_WS_URL=wss://ws.okx.com:443/ws/v5/public`（实际二进制未替换，按配置读取），同步把代码四处默认值改为 443；安装前 unit 备份在 `var/storage/okx-port-443/20261029/`。重启后 332/332 配对盘口就绪，17:17、17:18 UTC 两个分钟 337/337 CEX 及 2,646 个期权成员全部 60 秒有效。同晚另有两次约 12 分钟的期权/盘口缺失（13:39–13:51 UTC 含 systemd-oomd 杀 collector，15:40–15:52 UTC 换页风暴致 MySQL 锁等待），由整机内存压力引起，本次未处理。

2026-10-25 服务器重启后修复启动依赖环：MySQL/Redis 的 `After=default.target` 与依赖它们的采集服务形成循环，systemd 在 14:48:49（UTC+8）取消了存储启动任务。已备份并仅删除两个已安装 unit 中该顺序配置，同步修正仓库模板；default.target 验证不再有循环诊断。14:57:39 启动 MySQL/Redis，14:58:18 恢复 writer、collector、Across、Keeper；加上 ClickHouse，七个服务均 active/running、enabled、NRestarts=0。15:08 只读核验 15:05 分钟的 337 路 CEX 及 2,878 个 Deribit 成员全部 60 秒有效，DEX/Keeper 有新提交。仍有新写入排队，Across Arbitrum 的最新运行锚点尚未推进到重启后，不能据进程运行宣称全部来源追平。Reserve/LST、Across 历史和永续验收继续 disabled，正式历史迁移未恢复；[修复、数据验证与边界](../research/2026-10-25-reboot-recovery/report.md)。

2026-10-24 04:09:37（UTC+8）已将 Keeper 切换为 `dynamic-40-10-v1`：每轮最多 50 个不同订单、40 个重点与 10 个探索交错准入。原七表保留，新增六张状态、费率和调度审计专表；v1 runtime 完整认证转换为 v3，转换不消耗 RPC 日预算。实际进程、数据入库与调度验收见[动态实现与运行](justlend-keeper-dynamic-selection-implementation.md)及[验证记录](superpowers/validation/2026-10-24-keeper-dynamic-selection.md)。原 producer/代次、来源和预算保留。全历史仍在后台补采，发现集合不代表已独立证明全市场完整覆盖；此条替代此前“机会探测尚未部署”的状态。

2026-10-24 00:25:22（UTC+8）已部署 Across 网络故障恢复修复并重启实时服务。此前一次 Arbitrum DNS 错误被程序永久锁存，退出码 2 又被 unit 禁止自动重启，导致自 10 月 19 日 18:49:54 停采。现在来源网络故障保留失败证据，经共享 quota 按 5/10/20/40 秒冷却后重试；两个已安装 unit 均取消退出码 2 的重启例外，实时失败重启间隔仍为 15 秒。运行版本 `across-network-recovery-20261024`，实际程序为 `var/storage/bootstrap-preparation/20261013/bin/across-data`。00:32 的数据库核验确认 Base、Arbitrum 都有重启后 `complete/committed/canonical` 的新日志批次，证据哈希与区块范围一致；这些样本的最终性仍为 `head`，不代表四天缺口或费用已补齐。历史服务仍 inactive/disabled，其他服务未重启。构建、测试边界和部署证据见[修复记录](../research/2026-10-24-across-network-recovery/report.md)。下文较早网络停采规则均为历史行为。

2026-10-19 21:16/21:34（UTC+8）磁盘尖峰修正：Redis 自动 AOF 重写改为最小 256 MiB、增长 400%，每秒同步与交付确认保留，配置文件和在线值一致。三张来源登记 `_v2` 扩展采用内部自增主键集中追加、唯一哈希索引核验身份，新旧历史联合读取并保留冲突与首次知晓语义。21:34:35 主 collector/writer 已切换并重启，PID=1545384/1545376，writer `--source-storage-v2` 与 collector `MYSQL_SOURCE_STORAGE_V2=true` 已启用；原质量存储开关继续生效。旧表、队列与 producer 代次保持，正式 MySQL/Redis 未重启，Reserve/LST 仍停用。隔离实库 race/回归及相同输入布局对照通过，登记组件读盘/写入分别减少约 94.85%/85.96%，但新增索引与新旧联合读取有空间/查询成本，不能当作整盘降幅。重启初期交易所目录 HTTP 超时后自动恢复，21:40 已通过新旧实际发布/来源摘要只读核验；启动期缺失不补造。21:41–21:46 五分钟平均磁盘忙碌率 15.88%、最高 86.06%，Redis 重写 1 次、同期延迟同步计数无新增。最终 634 批全部提交、待写/旧待写/阻塞为零；每分钟 2722 个期权成员持续落库，但 UTC 13:43 的位图仍缺 1920 个成员秒，来源缺失如实保留。没有扩大 buffer pool 或强制 swapoff，采样工具已补齐内存/换页/AOF 计数。完整设计、验收及回退限制见[本轮记录](redis-mysql-migration/io-spike-fix-20261019.md)。

2026-10-19 20:55–20:57（UTC+8）用户再次采样后的只读复查：整盘平均忙碌率 45.18%、约一秒区间最高 93.61%，MySQL 平均写入 4.19 MiB/s，旧质量表写入仍为零，质量改造持续生效。剩余尖峰伴随本项目来源登记表慢随机读和分钟集中提交；Redis 在 20:55:14、20:56:05、20:57:05 连续自动重写 AOF，后两次与分钟封口重叠。最高一组还叠加 Codex 缺页读取和系统换入，不能将全部尖峰归于 AOF。旧待写始终为零，分钟待写在约 10–20 秒后清零；采样前另有来源恢复缺口，队列清零不代表全秒数据完整。ClickHouse 写入为零，Reserve/LST 保持停用。本轮未改服务或配置；[证据、归因边界及后续优先级](redis-mysql-migration/io-spikes-after-quality-20261019.md)。

2026-10-19 19:43 重启主 collector/storage-writer，19:52:26（UTC+8）补齐任务客户端能力继承后再次仅重启 writer；19:53 已确认质量存储降写入版本正式生效。三张独立扩展表用于按分钟集中保存盘口质量、共享目录质量 profile 和成员引用；旧历史兼容读取，未重写历史、清队列或改变 producer generation/holder。MySQL doublewrite 保持 ON、提交同步保持 1，正式 MySQL/Redis 未重启；Reserve/LST 保持停用。生产同口径隔离测试的 60 个逻辑分钟、111,410 个成员全部精确读回，质量组件的数据页/redo/doublewrite 合计减少 52.35%，提交尾延迟和整批读回未退步；该比例不能当作整盘降幅。新版写入开关为 `--quality-storage-v2`，回退写入时仍须保留兼容新表的新版二进制。上线后 19:53–19:56 三分钟样本的整盘平均忙碌率为 40.18%、MySQL 写 4.64 MiB/s；五个完整分钟每分钟 2,714 个成员持续落库，450 批全部提交且待写/旧待写/阻塞为零。短时观察不代替全天验收，综合历史健康查询仍有超时。[实施、验证与上线记录](redis-mysql-migration/quality-storage-implementation-20261019.md)。

2026-10-19 doublewrite 写入放大核验：线上 MySQL 8.4.11 的页大小为 16 KiB，质量热点表和提交登记表实际为 8 KiB 压缩页；对应版本官方源码确认 doublewrite 将压缩页置于 16 KiB 槽位并写出槽位区间。17:09–17:11 样本的保护写入可精确换算成 48,497 个槽位，与数据文件写次数基本吻合，解释了主库约 3.27 MiB/s、保护约 6.29 MiB/s 的比例。只读回读保持 doublewrite=ON、2 GiB buffer pool、256 MiB redo、I/O capacity=10000/20000。本轮仅核验并写方案；[减少刷页与 DETECT_ONLY 取舍](redis-mysql-migration/doublewrite-reduction-plan-20261019.md)明确区分完整恢复保护与降低保护的路线，未改生产设置。

2026-10-19 17:09–17:11（UTC+8）用户处理合并后的复查：ClickHouse 12 次合并查询均为空、进程写计数为零。整盘平均忙碌率仍为 66.30%，写请求平均完成时间已从上一轮 19.29 ms 降至 0.94 ms，平均未完成 I/O 从 15.12 降至 3.91。当前主要写入来自本项目 MySQL/Redis（约 11.03/1.47 MiB/s）；MySQL doublewrite 文件约 6.29 MiB/s，主库业务文件约 3.27 MiB/s，质量两表占其中 50.8%。登记/来源表的 8 KiB 小读取仍平均耗时 37–47 ms，十秒高负荷片段读耗时约 155 ms，需要同时处理写入成本和慢读。相邻两分钟交付 291 批、近期待写从 103 降至零，旧待写始终为零；Reserve/LST 文件写入为零。本轮未改服务或数据库；[对照、文件归因及后续方向](redis-mysql-migration/io-after-merge-fix-20261019.md)。

2026-10-19 16:19–16:21（UTC+8）宿主机采样归因：整盘平均忙碌率 65.33%，一段 8.10 秒达 94.10%，该段读取主要来自共享 ClickHouse。日志确认另一项目 `crypto_grid_trading.trades_raw` 正反复执行同一个合并，在旧分片 `202609_1277496_1299305_389/rule_id.size.bin` 报 `UNKNOWN_CODEC` 后重试；10 月 14 日曾暂停的该表合并已恢复，具体恢复原因尚未确认。本项目 MySQL/Redis 同期写计数约 5.82/1.16 MiB/s，质量表仍是主库业务表写入重点，Reserve/LST 文件写入为零；writer 旧待处理始终为零。此次只分析采样和现有日志，未再次暂停合并或改服务；[完整证据、计数边界与处理顺序](redis-mysql-migration/io-attribution-20261019.md)。

2026-10-19 凌晨积压回溯（01:00–02:15）：输入保持约 95 批/分钟，01:00–02:00 完成交付降至 69 批/分钟，待处理增加 1,520 批。历史 sysstat 证实换页和 I/O 等待加重；代码及日志还定位到 collector 元数据排他事务会阻塞分钟写入、Redis 候选读取和 ACK 慢响应会占用调度及提交路径。883 秒 AOF 重写约 01:56 才开始，是加重因素而非最初起因。02:37:32 旧待处理首次重新归零，不作为故障已根治的证据。没有改程序或服务；[时间线、耗时及阻塞路径](redis-mysql-migration/backlog-cause-20261019.md)。

2026-10-19 02:14（UTC+8）只读磁盘复查：Reserve、LST 保持 inactive/disabled、PID=0，两库观察期间没有新增文件写入。02:06:54–02:10:53 整盘平均忙碌率 71.98%，MySQL 写入约 7.06 MiB/s；主库占项目业务表写入约 99.2%，重点仍为期权盘口及质量记录。MySQL/Redis 主进程分别约有 512/95 MiB 内存位于 swap；Redis 一次 AOF 重写耗时 883 秒，期间出现读取及持久化超时。当前为今天新形成的期权积压，重写结束后有所缓解，但 02:13:49 仍有 1,514 批旧待写、最早逻辑分钟 01:43，不能沿用 10 月 16 日的“旧积压为零”结论。本轮没有重启、改参数或清队列；[实测、归因及后续方向](redis-mysql-migration/io-recheck-20261019.md)。

2026-10-19 01:50:32（UTC+8）：按用户要求，复核收益后已结束当前 LST 两条赎回路线实验，执行 `systemctl --user disable --now crypto-market-info-lst.service`。实际为 inactive/dead、MainPID=0、disabled，开机启动链接已移除；Ubuntu 重启不再通过现有启动路径拉起 LST。历史数据、程序和恢复状态保留，本轮仅操作 LST；Reserve 已由并行任务另行停用，其余采集器及共享存储保持。不要依据下文历史健康检查重新启动 LST；[决策、复算与停用验收](../research/2026-10-19-lst-retirement/report.md)。

2026-10-19 01:48（UTC+8）：按用户要求复核四项目评估及报告后的新数据，结束当前DFX的Reserve采集试验。原评估7,268次成功联合模拟全部负毛利；报告截止后新增20次成功模拟也全部为负，当前没有活动拍卖。已执行`systemctl --user disable --now crypto-market-info-reserve.service`，实际为inactive/dead、MainPID=0、disabled，默认目标启动链接已移除，无残留Reserve采集进程。Ubuntu重启后应保持停用；日常维护不要把它当故障自动启用。本轮只操作Reserve，历史数据、队列和恢复状态保留。停用前后立即对照其他服务未变；01:50复核时LST已同期另行停用，本轮未恢复，其他服务仍与停止前一致。结论限于当前DFX路线，不代表所有Reserve市场没有机会；[复核与停采记录](../research/2026-10-19-reserve-retirement/report.md)包含具体口径、证据及验收。下文较早的Reserve运行状态均为历史记录。

2026-10-16 21:52（UTC+8）只读复查：六服务保持原 PID、active，五分钟旧待写/阻塞为零，整盘平均忙碌率 31.51%，MySQL/Redis 进程写入约 8.37/1.50 MiB/s。控制面显示目录齐全，但 SQL 位图发现部分期权分钟缺失可回放盘口；13:48 UTC 最重，13:49 已大幅恢复但仍缺 78 个成员秒，不能据目录就绪宣称全秒采集正常。新增候选为 Redis 期权交付/预览值压缩及压缩后评估 AOF 重写频率；本轮未改程序或参数。实测、缺口与取舍见[二次复查](redis-mysql-migration/io-reduction-followup-20261016.md)。

2026-10-16 21:24（UTC+8）最终复核：collector `53068b5c…`、writer `bfb279a6…` 已于 20:31:59 正常重启，上线 spool v2 无损压缩、Redis 已提交残留回收和成员 pipeline。六服务 active、零重启，原队列、generation、holder、epoch 和 checkpoint 保留。5/30/5 分钟 MySQL 刷盘对照结束，候选值未证明稳定收益，已恢复 `10000/20000` 且未固化；本轮不能宣称整盘写入明显降低。最终待写/旧积压为零，UTC 13:21 的 337 条 CEX 和 2,958 条期权全 60 秒有效；观察期间仍有来源失败和短暂重同步。质量字段共享仅完成只读原型，未重建生产表。完整版本、验证、性能边界和 v2 回退限制见[本轮实施记录](redis-mysql-migration/io-reduction-20261016.md)。

2026-10-16 19:06:48（UTC+8）：collector 已补充部署为 `db43d2db…`，LST 为 18:21:54 部署的 `8ab85729…`。修复收益重采冲突/精度、期权规则刷新阻塞分钟写入、OKX 重连退避重置、LST 范围日志追赶，以及本轮跨周期复核新发现的目录失败重复通知导致分片溢出。writer 和其余采集保持运行，原队列、状态断点和 producer generation 保留，历史迁移仍暂停。首版完整输入窗口磁盘平均忙碌率 36.91%、十秒峰值 75.98%，两个周期规则批次刷新期间持续入库，40 分钟内 writer 锁超时及旧待写为零；18:59 后的期权缺数区间已排除，不能据此宣称物理写入明显下降或所有来源全秒正常。真实数据库回归、相关 race、最终版本状态、测试环境限制及未解决项见[修复记录](redis-mysql-migration/realtime-repair-20261016.md)。

2026-10-16 17:47（UTC+8）磁盘负载回升诊断：17:42:33—17:45:33 三分钟只读采样平均忙碌率 35.39%，十秒区间最高 72.78%，整机读/写 0.90/15.73 MiB/s；MySQL 进程读/写 0.37/5.84 MiB/s。六服务仍为原 PID、active、零重启，各次队列快照均为零，未出现旧待写持续增长。回查前一小时，writer 有 40 次 MySQL 1205 锁超时，集中于 17:30—17:40；同期期权交易规则刷新每批 822/936/1,148 条，事务外准备最长 26.61 秒，writePreparedTradingRules 事务主体最长 22.79 秒。代码确认目录状态约 30 分钟刷新，规则写事务对 collector 的 producer_generation 行持排他锁，分钟提交对同一行持共享锁，长规则事务会阻塞实时 writer；此时间对应关系和代码路径明确，但未捕获用户所见峰值当时的逐秒磁盘数据，不能据此分摊整个历史峰值。当前采样期间 writer 无告警，Redis 有一次 AOF 重写且延迟 fsync 计数未增加，MySQL 无新增 buffer pool/redo 等待或容器内存限额事件。此前五分钟低负载样本不能证明所有刷新周期稳定；后续应优先减少规则刷新重复读取和长事务占锁，并跨完整刷新周期验证。本轮仅诊断，未更改配置、数据或服务。证据：`var/storage/io-recurrence/20261016T0942/summary.json`。

2026-10-16 16:48（UTC+8）实时数据健康复核：不能将写入队列排空等同于全部来源正常。MySQL 中 16:44 的 CEX 分钟有 337 条，其中 317 条全 60 秒有效、20 条 OKX 盘口部分秒无效；16:45 同组预期 337 条中仅查到 323 条，其中 317 条全秒有效、6 条不完整，14 条无该分钟记录。日志显示 OKX 第 0 个连接组的 20 个目标反复超时/重同步，16:46:50—16:47:20 已恢复 332 个配对目标就绪；此前缺失不能据此视为补齐。16:44、16:45 的期权各 2,944 个成员均全 60 秒有效，16:44 的 sampled/stream/replay/market-known/open 位图均完整。近十五分钟有 11 个收益来源反复在 write 阶段失败，7 个为 immutable storage content conflict，4 个为小数精度超过 18 位；部分 Aave/Marinade/Jito/Kamino/OKX 路线最新已存观测仍在 10 月 12—13 日，JustLend 在 10 月 14 日。LST 的 16:42 市场批次已提交但为 partial，来源对冲盘口超时、标记价冷却导致当轮可用 entry=0，日志回补仍落后约 4,347 块；主 DEX、Reserve、Keeper 可见新提交/推进，Across 仍有来源额度及既有缺口。16:47:53 Redis/spool 待写均为零。此轮只读诊断，未修复这些数据完整性问题，证据在 `var/storage/realtime-health/20261016T0843/`。

2026-10-16 16:38（UTC+8）只读复测：16:32:05—16:37:05 连续五分钟磁盘平均忙碌率 26.74%，30 个十秒区间最高 55.84%，物理读/写约 0.53/19.92 MiB/s；MySQL 进程写约 7.87 MiB/s、Redis 约 1.16 MiB/s。同期 writer 新增约 97.2 批/分钟、成功提交约 97.0 批/分钟，十秒指标采样的待写峰值 51 批，旧待写与阻塞均为零；完整观察到的四个逻辑分钟均提交 337 条 CEX 盘口和 2,944 个期权成员，期权最后一批在封口后 12.23—15.91 秒提交。16:37:47 复核 Redis 待写与 spool 均自动排空，六服务 active、零重启，实际二进制哈希全部匹配部署清单。本轮 writer 无告警或锁超时，Redis 延迟 fsync 计数未增长；此前约 57 分钟也未形成持续待写积压。按主键抽查 16:30 完整分钟的 97 条 MySQL 提交登记，批次身份和成员数均与日志相符。仍有来源接口、规则校验和收益内容冲突等采集告警；上述结果属于正常生产负载观察，不能代表所有来源完整或额外压力、24 小时验收通过。本轮未修改服务、配置或数据，证据位于 `var/storage/runtime-state-recheck/20261016T0832/summary.json`。

2026-10-16 15:20:50（UTC+8）：按用户要求停止六个服务，清空主采集器待写队列后，已上线运行状态改造版本。五个领域均完成生产状态转换及最终二进制核验；六个实际进程 SHA-256 与构建清单一致，恢复世代文件位于 `var/storage/recovery-epochs/`，五个采集 unit 新增 `zzz-runtime-state.conf`。本次从队列移除 52 个去重交付 ID，保留 MySQL 已提交事实、原采集起点、限流/冷却与历史导入暂停状态。Across 转换补充兼容四条已审计的历史存储撤回登记，仍排除其有效覆盖；相关完整回归与 race 检查通过。部署、转换、版本、停机时间和上线后观察证据位于 `var/storage/runtime-state-deployment/20261016T0650/`，汇总为 `deployment-result.json`。

15:31:42—15:33:42 的两分钟完整范围观察：337 条 CEX 盘口、2,968 个期权成员，每分钟新增和成功提交均约 98 批，窗口前后去重积压均为零；对应两个逻辑分钟的期权最后一批分别在封口后 45.55 秒、33.74 秒提交。同期 120 秒磁盘平均忙碌率 46.45%，物理读/写约 0.89/22.08 MiB/s，其中 MySQL 进程写约 9.46 MiB/s、Redis 约 1.42 MiB/s。六个服务均 active、零重启。仍观察到两次 writer 锁等待超时后重试成功，以及 DEX/元数据超时和部分来源校验错误；不能把该短窗口当作全来源健康、60 分钟容量或 24 小时稳定性验收通过。下文为各次操作的历史记录。

2026-10-16 10:26（UTC+8）资本规则扩展已安装：隔离 MySQL 的安装、新旧格式读取、故障回滚和稳定重试验证通过，生产主库新增三张 8 KiB 压缩页专项表及 schema 登记。既有 schema 与历史迁移断点逐字不变，三条生产旧观测的内容、来源及发布时间核验一致。管理和只读核验工具已更新；常驻采集器未替换，新写入开关仍关闭，采集、writer、Redis 和历史迁移继续停止。详见[执行记录](../research/2026-10-16-capital-versions/report.md)。

2026-10-16（UTC+8）压缩转换完成：137 张项目 MySQL 表（125 业务表、12 控制表）已验证为原生 `COMPRESSED`、8 KiB 压缩页；实际分配空间 54.51 → 21.92 GiB。列、索引、分区、schema_version 和历史迁移断点保持，采集与历史迁移继续停止，binlog 保持关闭。详见[压缩执行记录](../research/2026-10-16-mysql-compression/report.md)。

2026-10-16（UTC+8）压缩转换进行中：按用户“改成压缩”，后台任务 `crypto-market-info-mysql-compression-20261016.service` 正将明确范围内 125 张业务表及 12 张控制表逐表重建为 `ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8`，随后核验行数、逻辑样本、索引及分区。采集器、writer 和历史迁移保持停止，binlog 保持关闭；压缩完成不会恢复这些服务。动态进度在 `var/storage/mysql-compression/20261016/status.json`，边界及验证见[执行记录](../research/2026-10-16-mysql-compression/report.md)。

2026-10-16（UTC+8）最新用户指令：已通过 `deploy/storage/mysql.cnf` 的 `skip-log-bin` 关闭生产 MySQL binlog，并正常重启核验 `@@log_bin=0`；原实例 UUID、redo 提交同步和 doublewrite 保持，旧 binlog 文件保留。历史迁移 run `1955f202-b4be-4775-ba6b-61bea9ae47ce` 已按用户要求暂停，不自动恢复；数据库重启前后全部迁移断点一致。采集器、四个低频服务、writer 和 Redis 保持停止，生产 MySQL 与共享 ClickHouse 运行。证据在 `var/storage/history-migration/20261016/binlog-disabled/`，迁移当前状态见[执行记录](../research/2026-10-16-history-migration/report.md)。以下为此前阶段记录。

2026-10-16（UTC+8）：采集运行状态源码已接入 Across、Reserve、LST、Keeper 和主 DEX，并新增显式转换、核验、回退及有限维护交接。用户已停止项目，本轮保持 collector、四个低频服务、writer、生产 MySQL/Redis 停止；只读核对发现原 ClickHouse 服务仍运行，本轮未操作它。未转换生产状态、未替换生产二进制，历史导入仍暂停。测试、工具产物和未完成的联合容量验收见[实施记录](../research/2026-10-15-runtime-state-improvement/report.md)，命令与恢复边界见[恢复协议](redis-mysql-migration/runtime-state-operations.md)。

2026-10-15 16:13:45（UTC+8）：writer `e3b2a94e…` 已部署共用事务超时错误处理修复，Keeper 为 `df29ff71…`；单元、vet 及真实自动回滚/原身份重试回归通过。准备队列扩展已撤回且从未上线，保持双提交、双准备和原连接/内存预算。隔离测试库已于 16:16:26 正常退出并保留数据。按用户最新边界停止追加性能优化，转入设计讨论；容量、排空和最终稳定性没有通过，历史导入继续暂停。

2026-10-15 15:54:49（UTC+8）：Keeper 已更新为 `df29ff71…`，修复事务超时被 `sql.ErrTxDone` 掩盖后错误退出；真实自动回滚、无部分事实和原身份重试回归通过。writer 仍为 `348a184b…`，吞吐约 103 批/分钟、积压约一万批，容量、排空及最终稳定性未通过。设计逐项状态见[实施记录](../research/2026-10-14-write-backlog-improvement/report.md)顶部核对表。

2026-10-15 15:23:48（UTC+8）：writer `348a184b…` 已部署期权分钟事实主键页预取，仅在原只读准备池执行三张表的有界主键读取；最终事实、冲突和发布检查保持。单元、vet 及新增“预取不持代次锁/随后事实变化仍拒绝/同一身份恢复”隔离回归通过。collector `cff3066…` 继续采集，正在重建完整队列索引并测量下一段吞吐；容量、排空与 24 小时仍待通过。

2026-10-15 14:47:44（UTC+8）：writer `380f2c33…` 已部署，六张唯一分钟事实表改用普通 INSERT 检查新键，冲突须完整回滚后按原协议重试；周期 schema 校验每池一次核对全部 33 项。新增冲突/代次回归、八个真实进程中断边界、发布不变量及双副本 writer 回归全部通过。collector `cff3066…` 继续运行，writer 的双提交/双准备、连接预算和原队列保留。当前重建完整队列索引并等待周期校验；容量、排空、P99 和最终 24 小时仍未通过，正式历史导入暂停。

2026-10-15 12:52（UTC+8）：collector `cff3066…` 已部署发现事实事务外预取和同批 instrument 批量读取，四项隔离事务回归通过（68.260 s）；writer `7d111d…` 保持运行。独占发布事务、首次已知意图、数据/代次/资源均保留。当前进入无构建负载观察；来源预热、积压排空、完整范围容量和最终 24 小时仍待验收，正式历史导入暂停。

2026-10-15 12:33（UTC+8）：采集器 `7628e2…`、writer `7d111d…` 已部署，原代次、队列、表结构与资源保留。补充事务外 CEX 发布索引预取、发布完整性复核、Redis 超时落盘时间预算及健康连接复用；生产交付入口双 worker / 7 批实际双副本回归通过（14.296 s）。直接 Redis 测试的环境超时保留，容量、排空延迟和最终 24 小时尚未通过，继续执行原方案，正式历史导入仍停止。

2026-10-15 08:46（UTC+8）当前 writer 为 `a07869…`（文件头预读建议及 PID 指标），collector `bdd00c…`、Keeper `56595f…`、Reserve `6808e2…`；双提交 worker、64 MiB 准备 / 32 MiB 缓存，已关闭不合格窗口的额外帧复制。生产 MySQL 为 2 GiB buffer / 3.5 GiB 容器、binlog 缓存 4 MiB；隔离 MySQL 512 MiB / 1.5 GiB 已停止且保留定义、数据。观察器 v8 新增实际 PID 绑定，旧截止不自动放行。容量仍未通过，正式历史导入和容量控制器停止；已按用户要求继续执行原方案，继续优化与验收，表结构和资源范围保留。下文为历史记录，最新证据见[实施记录](../research/2026-10-14-write-backlog-improvement/report.md)。

2026-10-15 08:25（UTC+8）当前 writer 为 `1995c5…`（缓存诊断版），collector `bdd00c…`、Keeper `56595f…`；双提交 worker、原代次保留。生产 MySQL 已结束 2.5 GiB 试验，恢复 2 GiB buffer / 3.5 GiB 容器，binlog 事务缓存 4 MiB。隔离 MySQL 保留原数据和 UUID，独立 512 MiB buffer / 1.5 GiB 容器。现无活动容量控制器，正式历史导入暂停；观察器 v8 的旧截止不等于通过。正验证 Reserve 组合游标重复扫描修复，尚未宣称积压排空、1.5×R 或 24 小时验收完成。此前窗口与阶段在下文保留为历史记录，最新证据见[实施记录](../research/2026-10-14-write-backlog-improvement/report.md)。

2026-10-15 05:30（UTC+8）最新部署：collector `a2174bcff5061d642c7013b4a31545628c07b8605104d2ea88c8f1690ac129df`、writer `c88ac624372078d277ce922d48769f15142c9c335de7bb35acb7aaa9505c15e8`，代次 1、双提交 worker。单项身份缓存和事务时长汇总已部署，binlog 事务缓存 4 MiB 已固化。生产 MySQL 正在试验 1.5 GiB 缓存 / 3 GiB 容器，隔离仍 1 GiB / 2.5 GiB；刷盘与 CPU 不变，试验未固化。Docker 本地协商超时已用经验证的 API 1.45 避开，当前数据库没有重启。新原始窗口 05:33—07:03，有限控制器 `cmi-backlog-streaming-gap-validation.service`，只读观察 `cmi-backlog-observation-final-v8.service`。实际吞吐、完整覆盖和持续稳定性仍未通过，不恢复正式历史导入。下文是先前阶段；最新状态与回退入口见[实施记录](../research/2026-10-14-write-backlog-improvement/report.md)。

2026-10-15 04:37（UTC+8）已补充部署 DEX 流式缺口查询，当前 collector SHA-256 `69c4d2e0571330b620245ad1014f94cde8ff028ee3421181fe78f5d133b8cc9e`，04:37:11 启动；writer 保持下段版本。14.2 万高度的只读对照为旧 102 ms、新 28 ms，结果均为无内部缺口，新查询无排序或临时表；旧查询本次也未产生落盘临时表，所以该项不能解释全部 I/O。04:23、04:24 的全范围分钟最后分片仍达到 183、294 秒，运行验收未通过。`cmi-backlog-dex-index-validation.service` 已停止且旧帧保留，新的最终容量窗口待本轮瓶颈修复后确定；旧观察 deadline 不能当作本轮自动通过时间。正式历史导入继续暂停。

2026-10-15 04:15（UTC+8）最新部署：collector `dc68416cfbbe918a6e7b0d9f0dba40bfe60ce7f318b418598f22810bbf0d43ce`、writer `d832574cf7fdf52525f7aadb548d67588ad18820d9555b5c3de14e3f425644ff`，启动分别为 04:15:23 与 04:15:49。已部署不可变规则与计划的事务外准备、DEX 批量写入和 UInt8 成交量确认标志修复。代次 1、双提交 worker、连接预算 14/16、MySQL 1 GiB 缓存与 2.5 GiB 容器上限保留。原件及回滚包在 `var/storage/write-backlog-improvement/`。新真实样本窗口为 04:25—05:55；预热缺口、小计划和排空后的短暂正常均不算验收通过。容量、P99 与 24 小时运行检查仍在执行，正式历史导入保持暂停。下文为较早阶段记录，最新证据见[实施记录](../research/2026-10-14-write-backlog-improvement/report.md)。

2026-10-15 03:41（UTC+8）现场：主采集器已部署 DEX 索引使用修复，实际 SHA-256 `2ff3d61aef62113d4aab013fde18cd1059b0ccfc389cf39f181bc0d89f85d49b`；writer 仍用 `531107de1b45b570e018db15a150c5af4ffb58fc5d9e404bb1c0662d1fc7ceb8`、双提交 worker，实时连接预算 14/16。生产/隔离 MySQL 均已固化 1 GiB 缓存和 2.5 GiB 容器限额，CPU 与持久化设置保留。四个 DEX 非唯一索引实测共约 72.5 MiB，主采集器启动检查索引形态，维护查询显式使用索引。实际进程切换为 03:19:11—03:19:12，之后预热另计。

仍未通过运行验收：期权目录已加载 2,806 个成员，但新计划只发布到 38 个，日志反复出现计划超时和边界过期。已定位计划事务内通过同一双连接池加载不可变引用，正在验证事务外准备修复；DEX 批量写入修复也在本轮代码中，尚未部署。该段旧队列下降不能当作完整输入吞吐通过。旧容量控制器已停止且原始样本保留，新冻结窗口待修复部署后确定。正式历史导入持续暂停。最新结果见[实施记录](../research/2026-10-14-write-backlog-improvement/report.md)，下文较早阶段保留为历史证据。

2026-10-15 02:28 已继续部署写入积压改进：writer 增加 CEX 直接解码、同一事务的大批次分块准备及分配前数组边界保护；真实进程 SIGKILL、隔离 Redis 重启、分块故障回滚和完整回放已测试。主采集器补修复 OKX 目录刷新因临时存储错误退出整组采集的问题，原采样代继续运行，只对同一元数据操作退避。发现元数据发布去掉重复查询，来源证明改为有界批量写。主采集器与 writer 最后于 02:27:41 重启，来源重新预热时间另计；producer generation 保持 1。writer SHA-256 为 `531107de1b45b570e018db15a150c5af4ffb58fc5d9e404bb1c0662d1fc7ceb8`，collector 为 `1cdd92a04e648857812e08780f94594f8002bf64a01e4e49527c7b18ed6e1523`。MySQL 缓存扩容试验未见收益且接近容器内存上限，已退回原 512 MiB；提交持久化设置保持。

02:38:13 起进行第二次资源试验：生产与隔离 MySQL 均为 1 GiB 缓存、2.5 GiB 容器内存，配置暂未固化；持久化设置保留。当前 writer 仍为双 worker，候选第三 worker 在隔离验证中。02:43 后测试负载引起积压反弹，已终止该轮测试并降低后续测试并行度，详情见实施记录。

02:08 和 02:12 快照中旧积压为零，但本轮仍出现反弹，**尚未通过稳定性验收**。修复前有缺口的样本保留；新的有限原始捕获窗口为 02:44—04:14（UTC+8），unit 为 `cmi-backlog-post-retry-validation.service`，状态在 `var/storage/write-backlog-improvement/post-retry-validation/validation-status.json`。最新只读观察 `cmi-backlog-observation-final-v6.service` 到 2026-10-16 02:27:41 自动结束。完整输入覆盖、60 分钟 1.5 倍回放、排空后 P99 和 24 小时稳定性仍须验收。缺分片或整分钟缺失不会被低延迟统计掩盖。正式历史导入继续暂停；[实施记录](../research/2026-10-14-write-backlog-improvement/report.md)列出部署、真实故障测试、试验回退和未完成项。

2026-10-14 已部署[写入积压改进](redis-mysql-migration/write-backlog-improvement.md)：Keeper 使用服务端 SELECT 15 秒上限、runner 退避、完整窗口 keyset 分页和 frontier 索引；writer 使用统一 Redis/spool 调度、两个提交 worker、两个准备任务、32 MiB 不可变缓存、64 MiB 准备预算及不可变分钟共享代次锁。主采集器在该阶段更新了元数据批量路径，后续补充部署见上文；producer generation 保持 1；实时账户连接预算为 14/16。写入器诊断在 `var/storage/writer.log`，上限 64 MiB 加一份轮转，源码和实际二进制回滚包在 `var/storage/write-backlog-improvement/`。21:50 首次完整索引观测到旧积压为零，22:34 一次 Redis/spool 同时为空；此后仍有延迟波动和积压反弹，**尚未通过稳定性验收**。90 分钟冻结真实样本和隔离验证工具在同一私有目录及 `tools/storage/backlog-validation/`，持续观察辅助 unit 为 `cmi-backlog-observation.service`，到部署满 24 小时自动结束。历史导入仍暂停，不能把短窗口排空视作正式迁移放行。阶段结果见[实施记录](../research/2026-10-14-write-backlog-improvement/report.md)。

2026-10-14 17:22:58（北京时间），按用户要求，仅对共享 ClickHouse 的 `crypto_grid_trading.trades_raw` 和 `crypto_grid_trading.quality_events_raw` 执行 `SYSTEM STOP MERGES`，暂停两张表因 `UNKNOWN_CODEC` 反复失败的后台合并。两条命令均成功，随后合并列表为空；暂停前后两张表的活动分片数量、行数和磁盘字节数一致。共享数据库继续运行，采集服务未因本次操作启停。该措施暂停重试，尚未修复旧数据读取错误；执行与负荷对照记录见[本次暂停目录](../research/2026-10-14-disk-write-audit/pause-abnormal-merges-092157Z/)，恢复命令保存在该目录的 `resume-merges.sql`，应在旧数据问题处理后择机执行。数据库重启或表重新挂载后应复核暂停状态。

17:23:24–17:25:24 两分钟复测，25 次合并检查均为空，ClickHouse 进程写入计数增量为 0，暂停后未再出现这两张表的合并错误。Ubuntu 虚拟盘写入从暂停前 30 秒样本的 8.54 MiB/s 降至 6.47 MiB/s，忙碌率从 79.73% 降至 75.88%，剩余负荷仍高。后一个窗口 MySQL/Redis 实际块设备写入分别约 1.97/1.01 MiB/s，MySQL 读取约 2.49 MiB/s；两窗口其他负荷并不相同，不能将差额当作固定优化收益，也不能直接换算宿主机磁盘忙碌率。[对照统计](../research/2026-10-14-disk-write-audit/pause-abnormal-merges-092157Z/comparison.json)和[最终核验](../research/2026-10-14-disk-write-audit/pause-abnormal-merges-092157Z/verification.json)保留原始边界。

2026-10-14 已按用户授权停止旧五个采集进程，启动 MySQL 新版 collector、Across、Reserve、LST、Keeper 及新 writer；实际二进制、后端和正式代次均核验。固定启动尾部逐字段及实际恢复通过，新 CEX/期权分钟回放通过。**运行验收仍未完全通过**：OKX 配对全体预热失败、writer/spool 积压增长、Keeper 间歇 MySQL 超时自动重启。现有采集保持运行并保存实际 partial/无效状态；本次未迁移其余历史，完整历史查询关闭。详情及停采边界见[切换报告](../research/2026-10-14-cutover/report.md)。下面较早日期记录为历史证据，现场状态以私有 `var/storage/cutover/20261014/` 记录和实时检查为准。


2026-10-14 纠正切换前复制范围：原 59 表整表预置任务已正常停止，不再要求它全部完成才能上线；旧服务继续运行。按[新版实际启动调用](redis-mysql-migration/startup-dependencies.md)缩减范围，封存快照的 DEX 恢复只需 76 批、4408 条报价，已复制并独立核验，余下旧发布/报价历史在切换后迁移。真实启动复测又定位并修复期权计划无查询索引造成的超时，已在线安装两个非唯一索引（合计 128 KiB）。用户已授权条件满足后直接切换，最终尾部同步和实际恢复验证尚待完成；未启动新采集或激活正式 producer。

Redis/MySQL 的安装与编程准备已完成，检查路径见[实现交付与检查入口](redis-mysql-migration/implementation-status.md)。2026-10-13 按用户限定授权开始导入固定快照中的 59 张启动依赖表、验证恢复并暂存新服务配置；正式 MySQL 已含导入事实，执行结果见私有目录 `var/storage/bootstrap-preparation/20261013/`。两个新数据库用户 unit 已运行；新 writer 保持 inactive。原五个采集服务继续使用原二进制、原配置和 ClickHouse，没有切换或激活正式 MySQL producer。完整盘口历史尚未迁移，正式切换前仍须补齐快照后的增量并刷新恢复状态。

最近核实：2026-10-13（Asia/Shanghai；含源历史全量读取、Reserve 源坏块定点恢复与后续历史成员恢复，其他既有记录保留各自日期）。本文记录 `/home/ubuntu/crypto-market-info` 所在机器的实际部署，不是另一套部署方案。路径、版本和启用配置变更后同步更新本文；运行状态仍以现场检查为准，不保存固定 PID。

2026-10-13 17:43:37（北京时间）完成后续源历史恢复：补回原始 116 条 DEX 报价、154 条 DEX 日志、1 条 Keeper 探测和 14,114 条 Keeper 索引页。全部经原提交成员 SHA-256 或原文件校验和认证、隔离与源表回读复核，原提交声明和来源时间保持。原 27 个 DEX 问题批次中 21 批恢复完整，剩余 6 批缺 290 条报价/13 条收据；Keeper 全量重核 289,048 个已提交批次，探测和成功分页缺口消除，仅剩 2 条原费用观测缺失。CEX 活动坏 part 中定位到一个含连续 64 KiB 清零的压缩块，未找到原始副本，原 31 个文件保全且哈希未改。当前剩余缺口需要原始备份或其他原始证据，未补造数据或接受历史缺口。正式 MySQL 125 张业务表精确计数仍全部为零；原五个采集服务、ClickHouse 及迁移写入器 PID/重启计数与此前审核一致，写入器仍 inactive，没有迁移、切换服务、撤回提交、删除或隔离原件。本轮为定点恢复，未重新全量扫描 125 张源表；[恢复报告](../research/2026-10-13-source-recovery/report.md)保留证据和边界，下文全库核查为修复前快照。

2026-10-13 14:46:52（北京时间）完成 125 张迁移源表全字段全量读取，124 张通过；CEX `order_book_second_delta/202609_90466_116798_41` 活动分片报校验和错误，指定 part 再查确认，同表其余约 9,349 万行读取通过。业务核查另确认 DEX 27 个已提交批次缺成员、Keeper 3 个已提交批次各缺 1 条事实；它们会阻断迁移读取或领域校验。Keeper 14,114 个历史成功分页仍缺索引页，普通兼容校验允许页为空，不等于分页完整。Across/LST 全量已提交成员核查通过。新定位的旧损坏不表示刚刚发生；不能以分片总行数当作损失数。此次未修复、隔离、删除源数据或切换服务，原服务 PID/重启计数与 Reserve 修复后记录相同，新 writer 仍 inactive。共享实例其他项目的旧活动坏块仍在；详细边界见[源完整性报告](../research/2026-10-13-source-integrity/report.md)。

2026-10-13 13:44:35（北京时间）已人工修复 Reserve `reserve_folio_state` 的 `202610_7483_7487_2` 读取阻塞。从 `/lost+found` 的错配文件内部找回 5 条原始状态，48 个压缩流的校验和全部通过；其中 3 条与原已提交 capture 的完整成员 SHA-256 逐一吻合，隔离库写入回读的 Native 字节也完全一致。恢复源表并再次验证后，仅将原坏 part 移到 detached，原件与取证副本的文件哈希保持。没有撤回 capture、修改原时间或编造值；另 2 条没有存续提交声明，保留为未发布事实。修复后状态表全字段读取、10,355 个快照状态成员及全部 62,502 个已提交 Reserve 批次的业务校验通过。各原采集服务和 ClickHouse 继续运行，未启停服务，新 writer 仍 inactive，未执行 MySQL 迁移。独立审计库 `cmi_reserve_recovery_20261013` 不属于正式迁移范围；[修复证据](../research/2026-10-13-reserve-recovery/resolution-summary.json)与[交付记录](redis-mysql-migration/implementation-status.md)保留恢复过程和边界。该结果不等于其他库的全部历史已完成全量校验。

2026-10-10 重启后恢复：18:31:55正常启动，当前initramfs检查日志报告恢复journal及孤立inode后clean；EXT4错误计数保持0。当前启动参数没有force fsck，未获得先前一次性强制检查的完整输出，不宣称全盘完整性已证明。原ClickHouse已恢复且停止崩溃；核验后将41个实际为普通文件的part路径原样移至`var/recovery/rootfs-20261010-quarantine/`，另单独DETACH Keeper校验失败的14116行索引页part，均保留原件且未绕过校验。ClickHouse另自动保留700个broken-on-start parts，含旧合并覆盖块，数量不能当作损失行数。

先找回2份原始证据，再全量核验100457个已提交日志/收据capture，剩余2个缺fill成员、2个缺紧凑证据；对这4个损坏批次追加存储撤回修订并保留来源身份、时间、成员承诺和canonical。原来源定点重采Base 52416516..52416526，补回3条fill并保存完整finalized新批次；Arbitrum 513465719..513465802原始扫描及新证据完成，但历史implementation调用被原来源拒绝，保持partial。两次有界backfill在日志提交后因可选旧收据补查达到120秒上限，不能将命令退出124当作日志未提交；新成员及证据均另行核验。原watch已恢复，r2二进制及来源/额度不改。

主collector、Reserve、Keeper已持续落库，19:19选定分钟334条OKX配对流和3002个衍生成员全部60秒实际回放通过；新现货/期货5档，2960个期权10档、42个交割期货5档。仍如实保留短暂无效秒，不能承诺所有分钟永远全秒有效。CEX实际可见最后旧完整分钟16:51，五条基础流到18:50恢复，全部337条到18:51恢复；中间实时盘口未补造。LST的Blockpi gate及两份日志游标被文件系统损坏，原件保留后从同来源/原流已核验备份恢复，其他host禁用/冷却和所有原请求限制不改。原RPC暂时521后已恢复；两次已调度市场报价四腿ok/fresh及资金费新批次核验通过。游标由原watch从保留Start和数据库连续覆盖重新计算，19:39:42已补入新的完整日志范围并继续推进；仍有旧积压，不能把旧备份Next当作覆盖证明。

本轮六个原unit保持enabled；所有`90-rootfs-maintenance.conf`和维护flag已清理，一次性GRUB条目已按内容匹配清理，普通`grub.cfg`验证未改。Reserve安装启动错误修复，SHA-256为`16828f47175de67a06262045ce50fbe07e22a246be5a55578dde31ab977f5a95`，数据库不可用时返回原错误而非nil Client panic，隔离race/vet和真实失败探针通过。历史损坏和Keeper分页缺口仍保留，不宣称全部历史完整；其他项目旧UNKNOWN_CODEC分片未改。完整操作与验收见[恢复记录](../research/2026-10-10-reboot-check/report.md)及[最终只读状态](../research/2026-10-10-reboot-check/final-health.json)。

2026-10-10 18:01 修复准备：已停止本项目六个采集/数据库unit，并为它们添加 `90-rootfs-maintenance.conf`；以 `var/recovery/rootfs-maintenance-20261010.flag` 阻止在核验前自动启动，原enabled状态、原unit和I/O限速drop-in保持不变。维护保护清单在[maintenance.json](../research/2026-10-10-reboot-check/maintenance.json)。已修复并安装Reserve启动时的错误变量遮蔽：数据库Open失败正确返回原错误，避免nil Client初始化schema而panic；安装SHA-256为 `16828f47175de67a06262045ce50fbe07e22a246be5a55578dde31ab977f5a95`，race、vet及真实关闭端口失败探针通过。同源隔离构建与原程序备份在 `var/deployments/reserve-startup-20261010/`。通过现有Docker管理权限准备**一次性**GRUB入口 `crypto-rootfs-repair-20261010`，下次启动当前内核时设置 `ro fsck.mode=force fsck.repair=preen`；普通 `grub.cfg` 未改，原环境块已备份。只自动处理无需人工决策的文件系统修复；严重不一致可停在启动修复界面，不能认为磁盘已修复。当时尚未发起重启；用户18:03确认后已安排正常重启，当前恢复结果见上文；[启动计划与回滚记录](../research/2026-10-10-reboot-check/boot-plan.json)。启动返回后先核验fsck结果和内核EXT4错误，再在维护保护下恢复数据库并检查part/历史完整性，最后移除已记录的维护保护、恢复原采集服务；不要在重启后直接另起writer或清空游标。

2026-10-10 17:39 重启后检查：本机于17:01:29启动，根文件系统 `/dev/sda2`（EXT4，挂载于 `/`）从17:01:44起记录错误，17:39:57内核计数已达41,680。内核报告目录校验和错误及 deleted inode referenced；ClickHouse 在加载 DEX、Across、Reserve 的历史 part 时遇到 `Structure needs cleaning` 并以134崩溃，HTTP ping及只读查询无法完成。Across、LST、Reserve、Keeper循环重启；主collector虽active，但CEX初始化和DEX写入持续因数据库连接拒绝/重置失败，不能视为采集正常。主collector日志中最后确认写入的是16:52分钟（5条原始流和332条新增流），本次启动后的日志没有分钟写入、DEX落库、收益写入或期权计划发布成功记录；该日志结论不代替数据库完整性检查。Across安装二进制仍为r2且SHA-256与原部署一致。Reserve启动还出现nil Client的schema初始化panic。当前无法核验历史是否完整或量化损失；应先在停止使用该文件系统后进行离线文件系统检查/修复，再恢复数据库并校验数据。此次仅只读诊断，未启停服务、改配置或删除/隔离数据；[现场证据](../research/2026-10-10-reboot-check/check.json)。

2026-10-10 16:08:41 已部署 Across `across-exit-recovery-20261010-r2`，安装和运行 SHA-256 均为 `d2481fdad64bad398e3255cc055316d312e3cdb17af7ee41f2e31b5e4e4dee66`。内部请求或 worker 的 `context.Canceled` 不再当作正常退出；只有 signal context 已取消才正常退出，明确 `network_issue_stop` 仍以 2 停止待处理。第一版于15:57恢复，15:58:33真实内部取消已以1退出并由systemd自动重启；原始23:08退出的触发错误没有日志，不能唯一还原。最终版启动恢复及每分钟收据核查按512个capture读取、逐批完整校验并释放事实，不保留全历史事实副本；跨批收据优先、unknown费用单次重试、canonical/revision及原采集起点保持。另记录发起worker错误后才取消其他worker，便于区分原始错误与连带取消。当前两链继续落库，停采旧孔洞仍在回补，不能用最新高度宣称历史已完整。三包race和vet通过，实际成员回读、内存与写入观测见[本轮修复记录](../research/2026-10-10-across-recovery/report.md)。原RPC、quota、unit及其他采集服务保持；共享数据库未主动重启。旧表失败合并仅作90秒暂停对比并已恢复，未修改或删除其数据；整体I/O及期权采样迟滞尚未证明彻底解决。程序备份及隔离构建在 `var/deployments/across-exit-recovery-20261010/`。

2026-10-09 16:18共享ClickHouse自动重启，16:18:38重新active，watchdog `NRestarts=2`，退出根因尚未确定。本轮没有修改数据库unit。Reserve16:18:26因本机数据库连接重置退出，自动重启后继续写入13条报价快照；Across、keeper同期重启，目前均active，主collector与LST进程保持。该事件发生在下段16:17检查之后，不能用先前重启计数0宣称始终稳定。16:27读库，预测和确认分钟成交量仍覆盖167币种，16:26全337条五档行情各60秒有效；期权计划3,082成员/97分片提交齐全但仍有927个无效成员秒。16:25普通CEX与期权也保留真实采样缺失。修复覆盖范围和未解决事项见[本轮修复记录](../research/2026-10-09-collector-recovery-fix/report.md)。

2026-10-09 16:08:57 补充部署独立发现事实采集，当前主 collector SHA-256 `74aa673c4849ed44ade339f03a67699d1e4fb07ee18ceb4af456afc5394dfb1c`；Reserve 仍为下段版本。资金费预测、上一完整分钟成交量及参考规则分别严格验证/发布；分钟未确认或费率组未知不再丢弃已完整预测，预测接口失败也不再阻断完整成交量。市场事实先采集，规则失败不推进其刷新时间，也不生成费用或资本许可。八组 race、go vet、单独 HTTPS 部分失败回归通过。仅重启主 collector，其他服务及 unit 保持；备份与源码快照另存 `var/deployments/collector-recovery-20261009-v2/`。16:17预测与确认分钟成交量均覆盖167币种，USDC也有真实发布，费用未知保持；16:11–16:16全337条盘口各60秒有效，16:12全部OKX20,040秒实际回放通过。同分钟期权按实际计划核齐3,082成员/97分片，184,920秒全部回放有效（3,040期权10档、42期货5档）。预测覆盖不等于所有输入和账户条件齐全；16:15期权仍有170个无效成员秒，不能宣布稳定性彻底解决。详细截止范围、失败证据与剩余问题见[本轮修复记录](../research/2026-10-09-collector-recovery-fix/report.md)。

2026-10-09 15:45:57 已部署 OKX 配对目录与 Reserve 异常恢复修复。主 collector SHA-256 `d19ab0ce5b8356a39e0f731ec0e89a1b743c789702b821170ff2737b65f13936`，Reserve SHA-256 `896623908f2f0c1318cbcdbca549ae3aca60911eff5749baf8fd39ab99e37b25`。OKX 非 live 条目允许缺少交易分类，但身份和重复检查、live 条目的严格完整性校验保留；目录错误日志明确是否仍有活动分支。原 45 秒写入积压终止保护保留，恢复分支现在可重新解析真实目录。15:48 全337条 CEX 盘口各60秒有效，167对/334条 OKX 五档盘口的20,040秒实际回放通过；15:51一次真实WS恢复缺口后，15:52重新达到337条全秒有效。期权15:49全3,138成员/99分片的188,280秒实际回放通过，3,096期权为10档、42交割期货为5档；后续仍记录真实连接关闭和采样迟滞，不承诺逐秒永无缺口。Reserve 区分 null 区块、链重组和永久错误，重组先刷新最终性、修订旧分支并回退日志游标，未提交结果不入库。六组 race 测试和 go vet 通过。只重启主 collector 和 Reserve，unit/drop-in 不变，其他服务 PID 未因本次部署改变；原程序与源码快照在 `var/deployments/collector-recovery-20261009/`，详情见[修复记录](../research/2026-10-09-collector-recovery-fix/report.md)。停采及重启预热形成的旧行情缺口没有补造。

2026-10-09 13:18:30–13:18:41（北京时间），用户同意后已安装 `/home/ubuntu/.config/systemd/user/crypto-market-info-clickhouse.service.d/merge-io.conf` 并重启共享ClickHouse，生产回读 `max_merges_bandwidth_for_server=16777216`（合并读取16 MiB/s）、`max_mutations_bandwidth_for_server=8388608`（mutation读取8 MiB/s）。重启命令耗时10.95秒，HTTP探针观察到不可用约9.73秒；原unit、background_pool_size16、warning/console=false日志及六表fsync/512 MiB合并上限保持。Across、Reserve、keeper因断连各自动重启一次，13:21七个项目unit已全部active/running，四个低频专项库均有恢复后可见观测；主collector及LST未重启。13:19:59–13:24:58五分钟独立观测，虚拟盘平均8.56%、P95为17.74%、最高36.11%，主库单分区part最高13→12，无明显积压。13:19–13:24六分钟计划/提交全部完整，13:20保留一次真实Deribit异常EOF造成的642个无效秒，其余五分钟全秒有效；13:21的188,280个期权/期货成员秒及原五路CEX300秒实际回放通过。此前OKX新增332流来源目录错误仍存在。限速控制合并/mutation读取，短窗口未覆盖GB级大合并，也不承诺整机I/O或Windows物理盘不再满载。配置、部署、观测和回滚见[合并I/O排查](../research/2026-10-09-clickhouse-merge-io/report.md)。该drop-in覆盖ExecStart，以后修改基础unit启动命令时必须同步此覆盖配置；当前备份在 `var/deployments/clickhouse-merge-limits-20261009T051828Z/`。

2026-10-09 12:53:42–12:53:43（北京时间）已对主库六张热表设置 `max_bytes_to_merge_at_max_space_in_pool=536870912`（512 MiB）：三个 `derivative_book_*` 表、`order_book_second_delta`、`options_catalog_live_minute_commit` 和 `options_catalog_quality_evidence_minute`。只改变后续后台合并的输入大小上限，原 DDL 其他部分及三项 fsync 完全相同；六个生产服务未重启，其他项目表设置保持。12:54期权3,138成员/99分片的188,280个成员秒，以及原五条行情流300秒实际回放通过。另发现新增OKX配对流最后分钟为11:56，调优前已有缺口，来源目录持续报 `invalid SPOT catalog identity`；不能将五条原始流通过表述为167对全覆盖恢复。全局16/8 MiB/s合并/mutation读取限速已在隔离同版本实例验证，但**未安装、未应用**；当前版本需重启且实例含其他项目库，需要协调连接中断。磁盘短窗口不能证明宿主机100%峰值已消除。应用清单、独立观测、查询验收、限制与回滚见[合并I/O排查](../research/2026-10-09-clickhouse-merge-io/report.md)。

2026-10-09 03:53:19 已部署期权目录优先恢复与心跳发送调度修复，主collector SHA-256 `62cd86c7a21ea6c7ec8803bedb69746b8883c7ab60d37a61a89b1cca3d208f68`。完整目录刷新使用独立有界优先队列，真实观测时间及成员证据年龄决定刷新，不被逐币恢复队列或延迟落库掩盖；状态屏障和歧义证据重试保留。WS接收时间采用socket读完时刻，控制限速期间收到心跳后优先使用已预留时隙回复。现货/期货5档、期权10档、自动新增/到期退出保持。现场33分钟观察已跨过真实30分钟刷新，六类目录04:23:32–04:23:41全部成功；3,118成员的29个全覆盖分钟无缺成员/分片或元数据过期，28分钟全秒有效，04:02保留一次272–320毫秒的单秒采样迟滞。03:56及刷新后04:24两个分钟各187,080秒实际回放全部有效；已有OKX167对现货/永续回放也通过。观察期间无期权连接/写入告警，不能据此承诺未来永无网络或调度缺口。源快照、程序备份与真实失败记录见[本轮修复记录](../research/2026-10-09-options-stability-fix/report.md)。本次仅重启主collector，其他服务及unit内容保持。

2026-10-09 03:15:38（北京时间）已部署 ClickHouse 日志降级，生产 user unit 显式设置 `logger.level=warning`、`logger.console=false`、`logger.size=100M`、`logger.count=3`；`system.server_settings`已回读确认。数据库重启约7秒，主collector/LST/Reserve未重启，Across与Keeper因数据库断开各自动重启一次并恢复写入。主collector新增OKX分支因写入积压自动重建，03:18/03:19只有原五条流，03:20/03:21全337条流60/60有效恢复；期权有队列溢出与真实过渡缺失，继续保留其不稳定记录，不宣称全量恢复。命令行logger覆盖以system.server_settings为实际口径。ClickHouse自己的warning/error文件保留，collector期权诊断继续保存。syslog原由控制台→journald（ForwardToSyslog=yes）→rsyslog重复存储大量Trace/Debug；降级后15秒仅增加2.6KiB。用户在Ubuntu终端完成管理员认证后，03:22:08已执行受限维护命令 [cleanup-clickhouse-syslog.py](../tools/maintenance/cleanup-clickhouse-syslog.py)：原位截断syslog并删除四个旧syslog文件，实测释放349.4GiB，03:25根分区可用约757GiB。专用root syslog轮转timer已enabled/active，每15分钟检查daily/maxsize100M/rotate3策略，下一检查03:30；其他系统日志规则保持。syslog复测15秒仅增加2061字节，当前约30KiB。完整执行记录见[日志维护](../research/2026-10-09-clickhouse-log-audit/execution.md)。原unit、有限故障样本和实际部署记录保留在 `var/deployments/clickhouse-logging-20261009/`。

2026-10-09 00:50:34 增加期权连接的数字诊断版本，当前主 collector SHA-256 为 `f879ada4fca5cf2569db77993c29d676eddbd166a4d83d8461c4e027043406f6`。此前写入与采样修复通过全量回放，但仍周期发生ACK/心跳连接关闭；本版在连接错误中记录ACK方法、控制门限等待、帧排队、回调时延及读写计数，不记录原始响应。当前完整分钟回放通过（01:01共3094成员、185640合约秒全部有效）；此前周期连接故障未唯一归因，继续保留诊断，不能据短窗口承诺长期网络稳定。

2026-10-09 00:33:58 已部署期权恢复修复版，该次主 collector SHA-256 为 `fd8626cdba0662e6cd7437ffc798ec415f8efca2cfc0a246f212a0eb2cb62dae`。暂时写库失败保留同批身份重试，40秒执行预算从取得写入槽位后开始；分钟缓存使用独立128MiB默认预算，压缩在后台限四路并发，采样线程只交接完成分钟。新建盘口连接错开、订阅每批最多64频道；期权10档及其他五档深度、目录自动发现和现有 unit 配置保持。原程序备份 `var/deployments/options-recovery-20261009/collector.before`，切换与逐秒验收见[修复记录](../research/2026-10-09-options-recovery-fix/report.md)。

2026-10-08 22:17:50 已部署新版主 collector；22:27:30 在保留交易所冷却时间后，补充部署公开规则接口的逐端点、逐物理请求限速修复。该次部署二进制 SHA-256 为 `87bb9b1d0d16425afd5fa403c1e02a60dbc42bdd3fcdff98ce6ecb144a751ff2`。已安装 unit 启用 `MARKET_BOOK_DEPTH=5`、`OKX_PAIRED_ENABLED=true`、`OKX_PAIRED_MAX_PAIRS=256`、`OKX_PAIRED_REFRESH=30m`，其他来源配置保持原值。OKX 当前公开配对目录为 167 对、334 条盘口，含原有 BTC 两腿，实际新增 332 条；期权保持每侧10档，现货、永续及交割期货每侧5档。新增找币数据表已在正式库创建，完整覆盖、时间和验收边界见[部署记录](../research/2026-10-08-okx-five-level-deployment/report.md)。原二进制和 unit 位于 `var/deployments/okx-five-level-20261008/`。本次只更新公开采集服务，未启动交易程序。

2026-10-07 02:14:10（Asia/Shanghai）已部署主collector紧凑期权辅助存储版本，现行SHA-256为 `ccec72f25771ad48fb275e2b80675f9d3a3f59f1073b8512a18953add0bd51fa`。五张辅助表迁移18,504,278条逻辑记录，全部原字段逐批精确核验；相同数据物理占用10.75 GB→4.49 GB，净减少6.26 GB。历史2,862合约/171,720合约秒迁移前后实际回放完全相同；新02:17分钟2,866合约/90分片/171,960有效合约秒实际回放及原摘要核验通过后，已删除五个旧展开副本。02:19分钟仍完整提交，五路CEX各60有效秒，服务active/running、NRestarts0，原文目录未重建。主采集器切换暂停约56秒，启动过渡按真实缺失/无效记录；不填造停机盘口。精确微秒、序号和档数仍可恢复，未删除唯一事实或降低深度、频率、覆盖；fsync设置已继承。测量不是每日增长，实际范围、测试、查询兼容及边界见[本轮清理记录](../research/2026-10-07-options-redundancy-prune/report.md)。

2026-10-06 21:16（Asia/Shanghai）已部署主 collector 无响应原文归档版本，SHA-256 `19f48c87e8db988230ae563b8e8745039e719fe29f23a486d834ad466f0f0d6a`。主 Ethereum DEX 使用 `Archive.HashOnly`，期权公开目录及生命周期在内存核验 payload hash 后直接写定类型表；旧 `DEX_EVIDENCE_DIR`、`OPTIONS_EVIDENCE_DIR` 仅兼容接受，运行程序不再读写两目录。来源摘要、UTC时间、价格数量编码、链锚点、质量表与批次提交语义不变。清理仅涉及确认不再使用的运行响应原文，独立研究样本、配置、续采状态和锁保留；Across compact capture/finality 仍被历史报告读取，不可整目录删除。LST 按已审清单清理前，先停服持原锁并复用 `FlushPending` 正常排空待写批次，禁止手工删除 pending。部署验证、精确删除统计和历史报告比较见[本轮清理记录](../research/2026-10-06-redundant-data-cleanup/report.md)。不要用旧版本回滚恢复原文依赖。

本轮实际删除 2,569,434 份冗余文件及 258 个空目录，测得释放分配空间 42.0224 GB；Across 另删除的 342,004 字节仅统计压缩文件长度，未混入分配空间合计。最终验收又复现期权旧分组恢复缺陷：计划异步入库期间新增预热成员，使下一轮超过单 run 32 个上限并反复发布失败。22:53:39 部署修复版，该版主 collector SHA-256 为 `be1b072b19b1c414206959f82d73b2543e28f71d13b7bd7d0edf44ff88680fcc`：每组最多 32 个，溢出成员继续分配，旧预热列表不抢回其他组已接管的成员。源码、回归及独立审核证明此错误不是原文读取依赖；清理期间磁盘负载可能加重数据库超时，不能宣称全程采集无中断。各次恢复与最终数据核验以本轮记录为准。

22:58 最终核验：22:57 期权2,862成员、90个run、171,720个成员秒实际回放全部有效；五路CEX同分钟各60有效秒。主collector、Across、LST、Reserve、keeper和ClickHouse均运行，两处主原文目录不存在且未重新生成。另记录Across旧的内部取消可能被当作成功退出、从而未触发自动重启的问题；本次已恢复采集，但该独立退出缺陷尚未修复，见清理记录中的诊断。

2026-10-05 11:23（Asia/Shanghai）重启后再次核验：七个项目unit均active/running，五路盘口最新分钟60/60；期权11:20、11:21两个完整分钟均3,104成员、每成员60秒有效。已给五个生产库59张MergeTree表显式启用插入、目录及合并fsync，51张本次有新数据的表各选一个有界part校验，全部通过；不涉及其他项目的表设置。Across已部署`across-reboot-head-repair-20261005`（SHA256 `34753e49bfba8965975d34e0acbd78ae5239880788240f869cbc8dc767f97015`），长积压时当前链头与旧缺口补采由已有worker分别推进，两链当前事实已恢复完整解码；旧放弃标记仍为993个partial。原始扫描与完整事实必须分开核验，最新高度不能证明历史连续；Arbitrum停机原始缺口仍在补，旧历史状态不可用仍partial。启动时的空文件隔离块和既有坏历史不视为已恢复；块号包含关系不能证明行完整。具体修改、真实验证与限制见[重启检查记录](../research/2026-10-05-reboot-check/report.md)。

## 1. 实际使用的本机服务

| 服务 | 实际运行方式 | 连接与用途 |
|---|---|---|
| ClickHouse | 用户级 systemd unit `crypto-market-info-clickhouse.service`；宿主机原生二进制以前台模式运行并带 ClickHouse watchdog；核实版本 `26.8.1.1825` | HTTP `127.0.0.1:8123`；native `127.0.0.1:9000`；数据库 `crypto_market_info` |
| collector | 用户级 systemd unit `crypto-market-info-collector.service`；直接运行编译后的单个二进制 | 当前使用 MySQL 后端，盘口封口批次持久交付到 Redis/spool，独立 writer 入库；其余定类型数据按各自事务写入；没有独立 HTTP 服务端口 |
| MySQL | 用户级 systemd unit `crypto-market-info-mysql.service`；无网络 Docker 容器 | 当前新事实与提交证明；私有 Unix socket；ClickHouse 保留源历史 |
| Redis | 用户级 systemd unit `crypto-market-info-redis.service`；无网络 Docker 容器 | 盘口预览与持久待写批次，AOF、noeviction；spool 为磁盘持久副本 |
| storage writer | 用户级 systemd unit `crypto-market-info-storage-writer.service` | 同一进程内两个提交 worker，统一处理 Redis/spool；不自动恢复正式历史导入 |
| 全量永续验收 collector | 用户级 systemd unit `crypto-market-info-perp-soak.service`；独立二进制与数据库 | `crypto_market_info_perp_soak_20260928`；仅三家共有 USDT 永续盘口和资金费率，不重复采集现货与收益 |
| 桌面状态指示器 | 图形会话用户级 systemd unit `crypto-market-info-status-indicator.service`；Python/Gtk AppIndicator | 每 30 秒只读检查生产 unit、五路盘口及最新收益写入；不访问交易所、不写数据库 |

2026-10-14 切换后使用本项目专用 MySQL、Redis 和 spool；下文较早的 ClickHouse 部署记录仅作为历史。基础 CEX 分支保持 BTC 显式列表及现有收益配置，新增 OKX 独立配对分支覆盖可公开借贷 USDT 现货/永续完整交集；Deribit期权auto已部署R5持续全量发现。永续全量验收服务当前停用。具体构建版本用下文 `go version -m` 查询，不以当前仓库 HEAD 推断正在运行的二进制版本。

AVAX 第二阶段已于 2026-08-27 部署。collector unit 保持 `AVAX_YIELD_ENABLED=true`，新增 BENQI sAVAX、Ankr ankrAVAX、BENQI AVAX 借贷三条 Runner；启动时已对生产 `yield_observation` 幂等补齐两列，并成功写入三条首批同区块观测。

2026-09-21 已在现有collector启用 Deribit 期权采集，unit 设置 `OPTIONS_ENABLED=true`、`OPTIONS_SYMBOLS=auto`，没有新增期权专用服务。当时run `5ec3ced8-4d3a-44e9-bf1f-62386d0c505b` 自动固定选择24个期权、4个同到期期货及四个指数。启动过渡分钟如实保留无效锚点；下一完整分钟 `2026-09-20T19:28:00Z` 的28个盘口和四个指数均为60/60有效秒，批次hash为 `ace2270262f3a7bdec6524cd70bcaea13abf9b7d3a5064c102e43531c1a2a779`。详情见[期权实时采集说明](arbitrage/strategies/arb-0009-options-live.md)。

2026-10-04 15:43（Asia/Shanghai）已完成Deribit R5部署：四族全部未到期期权及交割期货随新挂牌加入、到期退出。部署前独立库全量真实DB验收通过，并由同一Agent复审批量写入修复。常驻collector SHA-256为 `3a39581b13a8abe0dcf03383d1d4ae8337a29bf350bb11aec44f5787f98a0ee4`，生产已创建五张R5表；15:46、15:47两个分钟全部3108成员均60/60有效，15:46全部秒实际回放通过。原文持久目录及备份见[部署记录](arbitrage/strategies/arb-0009-options-lifecycle-deployment.md)。检查另发现9月期权质量表旧分片UNKNOWN_CODEC，未执行数据修复；上述成功仅针对新分钟及现场范围。上段28合约为R4历史。

2026-10-05 02:40（Asia/Shanghai）已按用户批准并要求继续的方案，完成九月旧历史坏块清理：生产期权质量表的`202609_1_5549_21`及停用旧验收库`crypto_market_info_perp_soak.order_book_second_delta`的7个坏parts，共8块。均有原始列全部或局部全零及实际读失败证据，未找到完整可用恢复副本，已逐块`DETACH PART`后`DROP DETACHED PART`，删除约1.97 GB。324个文件的哈希、UTC范围及当前最终状态保存于`var/recovery/history-codec-cleanup-2026-10-05.json`。剩余九月期权质量8块、旧永续差量2块均通过逐块校验和全列实际读取，健康块与删除前内容哈希相同。02:34分钟全部3,094合约及五路CEX全秒实际回放通过，02:36—02:38全量分钟均60/60；02:32有30个合约各1秒采样延迟，已按无效秒记录且随后恢复。服务未重启。删除不是历史恢复，旧验收库仍有明确缺口；具体清单、原始异常及验证边界见[清理完成记录](../research/2026-10-05-history-codec-cleanup/report.md)，[初次隔离记录](../research/2026-10-05-history-codec-isolation/report.md)保留当时中间状态。

2026-09-22 已在同一个生产 collector 启用 Ethereum DEX 采集，unit 设置 `DEX_ENABLED=true`、`DEX_ETH_RPC_URL=https://ethereum-rpc.publicnode.com`，证据写入持久目录。部署后二进制 SHA-256 为 `cf49b0795aad53a8b5ae00f623d66f867836aac07154761c8c13097f61aaf7a4`。验收区间 `26027452` 至 `26027477` 共26个连续高度无缺口，其中25个区块达到58/58完整报价；`26027460` 因一次RPC/Sky状态读取不完整保留为 `partial/unknown`，后续区块自动恢复。首份24高度机会报告没有毛正窗口。DEX分支不包含钱包、签名或交易发送。

2026-09-23 排查发现 DEX 公共 RPC 的响应读取超时集中出现在最终性校验阶段，旧版在校验失败后额外等待5秒，并在下一轮立即重复昂贵的校验。现版将 RPC 响应读取超时、截断与超过16 MiB分别记录；最终性校验把 `finalized`、`safe` 和原 checkpoint 合为一个 RPC 批次，每轮最多校验20个高度、最多占用6秒，失败后30秒再重试。实时新区块采集与补采、最终性、日志和回执维护现已独立运行，维护任务不会在调度上阻塞实时轮询；共享 RPC 仍可能使实时请求变慢。实时请求失败后按正常2秒轮询间隔重试。Binance 实际资金费率历史中部分 `fundingTime` 比计划整点晚数毫秒，现版查询并匹配整点后1秒内唯一的来源记录，保留其原始毫秒时间，启动补查窗口扩大到72小时；重试耗尽而来源尚无记录时会明确告警。2026-09-23 12:39 CST重启时，OKX启动元数据请求超时曾导致整个collector首次启动失败，此类启动缺口只能补回区块元数据，不能补回当时的实时报价。现已将DEX与CEX等其他来源拆成独立启动和重试分支，并分别使用数据库连接；CEX元数据超时不会停止已运行的DEX分支。12:52 CST重启验收时，DEX先于CEX元数据初始化完成写入新区块。2026-09-28 部署时二进制 SHA-256 为 `dd55a3a80fbba9bec3b61a18401f41b1ba436322d4de8012f4889fdca6859a21`。更新未增加外部域名。

Bybit USDT 线性永续已于 2026-09-05 部署，collector unit 设置 `BYBIT_PERP_SYMBOLS=BTCUSDT`。启动时已幂等增加 `instrument.venue_contract_version`：迁移前 Binance、OKX 永续 ID 2、4 保留，新版本分别登记为 ID 5、6，Bybit `BTCUSDT` 登记为 ID 7。首个完整生产分钟的五个当前行情流均有 60 个有效秒；Bybit 公共 ticker 实测产生了指向下一结算时刻的完整资金费率估算。

仓库的 [compose.yaml](../compose.yaml) 只是可选的独立开发环境，固定镜像为 `clickhouse:26.3.17.56-jammy`，与当前原生服务不是同一个实例。核实时 `docker compose ps -a` 为空，但原生 ClickHouse 正常响应。两套环境默认占用相同端口，不能直接同时启动，也不能混用数据目录。

两个 unit 已启用到 `ubuntu` 用户的 `default.target`，且 `loginctl show-user ubuntu -p Linger` 为 `Linger=yes`。因此用户服务管理器会在机器启动时运行，无需交互登录。ClickHouse 失败后等待 5 秒重启；collector 失败或正常退出后等待 30 秒重启。

## 2. 路径、启动方式与配置

| 项目 | 当前路径 |
|---|---|
| 仓库 | `/home/ubuntu/crypto-market-info` |
| ClickHouse 二进制（也提供 client 子命令） | `/home/ubuntu/.local/share/crypto-market-info-clickhouse-bin/clickhouse` |
| ClickHouse 数据目录 | `/home/ubuntu/.local/share/crypto-market-info-clickhouse-data/` |
| ClickHouse 普通日志 | `/home/ubuntu/.local/share/crypto-market-info-clickhouse-logs/server.log` |
| ClickHouse 错误日志 | `/home/ubuntu/.local/share/crypto-market-info-clickhouse-logs/error.log` |
| ClickHouse PID 文件 | `/run/user/1000/crypto-market-info-clickhouse/clickhouse.pid`（由 systemd 运行目录创建，重启后重建） |
| collector 二进制 | `/home/ubuntu/.local/share/crypto-market-info-collector/collector` |
| collector 日志 | `/home/ubuntu/.local/share/crypto-market-info-collector/logs/collector.log` |
| 期权旧原文目录（现行 collector 不再读写） | `/home/ubuntu/.local/share/crypto-market-info-options/evidence/` |
| 主 DEX 旧原文目录（现行 collector 不再读写） | `/home/ubuntu/.local/share/crypto-market-info-dex/evidence/` |
| 全量验收二进制 | `/home/ubuntu/.local/share/crypto-market-info-perp-soak/collector` |
| 全量验收日志 | `journalctl --user -u crypto-market-info-perp-soak.service` |
| 桌面指示器程序 | `/home/ubuntu/crypto-market-info/tools/desktop-status/crypto_market_status.py` |
| unit 仓库源文件 | `/home/ubuntu/crypto-market-info/deploy/systemd/` |
| 实际安装的 unit | `/home/ubuntu/.config/systemd/user/crypto-market-info-*.service` |

unit 的仓库源文件是启动参数和采集环境变量的维护入口，安装副本由它们生成。每个采集数据库只允许一个 collector 写入；不得绕过 systemd 对同一库手工启动第二份采集器。全量验收服务共享现有 ClickHouse 实例，但必须使用上表独立库，不另起数据库进程。

安装或更新 unit 后执行：

```bash
install -d -m 0755 /home/ubuntu/.config/systemd/user
install -m 0644 deploy/systemd/crypto-market-info-clickhouse.service \
  /home/ubuntu/.config/systemd/user/crypto-market-info-clickhouse.service
install -m 0644 deploy/systemd/crypto-market-info-collector.service \
  /home/ubuntu/.config/systemd/user/crypto-market-info-collector.service
systemd-analyze --user verify deploy/systemd/*.service
systemctl --user daemon-reload
systemctl --user enable --now \
  crypto-market-info-clickhouse.service \
  crypto-market-info-collector.service
```

ClickHouse unit 以前台模式运行数据库并在启动阶段轮询 `/ping`；collector unit 通过 `Wants`/`After` 启动并等待它，在执行二进制前再次检查 `/ping`。两个服务分别重试，避免数据库一次启动失败让 collector 永久留在 stopped。collector 的标准输出和错误仍追加到原日志文件，systemd 启停事件可通过用户 journal 查看。

数据库连接使用默认的 `127.0.0.1:9000`、`crypto_market_info` 和本地 `default` 用户。当前本机查询不需要密码；不得把无认证端口开放到公网，也不要把任何凭据或 `.env` 写入仓库。完整可配置项见 [README](../README.md#开发时运行采集程序)与 [internal/config/config.go](../internal/config/config.go)。

## 3. 已启用的采集与外部接口

| 分支 | 当前启用范围 | 采集频率 |
|---|---|---|
| Binance 盘口 | BTC/USDT 现货及线性永续，两条交易流，每侧5档 | 每秒采样，结束的分钟批量写入 |
| OKX 盘口 | 可公开借贷 USDT 现货/永续配对，部署时167对、334条交易流，每侧5档 | 每秒采样，结束的分钟批量写入；目录每30分钟复核 |
| 永续资金费率 | 已启用的 Binance、Bybit、OKX 永续合约 | 公共 WebSocket 估算及既有整点保存；REST 确认实际结算值 |
| OKX 找币辅助数据 | 独立资金费预测、标记/指数、完整分钟成交量、公开费用及资本规则 | 各币独立轮询，来源时间、有效期限和发布证据入库；实时账户条件由交易项目复核 |
| Bybit USDT 线性永续 | `BTCUSDT`，已部署启用 | 每秒盘口采样；公共 ticker 估算并由 REST 确认实际资金费率 |
| Deribit 期权 | BTC/ETH币本位和USDC线性四族；全部未到期期权及交割期货，四个指数 | 期权每秒前10档、交割期货前5档及指数；生命周期推送，目录每30分钟及重连后复核 |
| Ethereum DEX | 主网4个策略池、2个成本参考池及Sky相关状态；每块固定58条状态报价 | 每2秒检查新区块；按区块hash采集并跟踪最终性 |
| JustLend | 四条固定 TRX 路线 | 每小时 |
| TRON 原生质押 | 前 127 名 SR | 每 6 小时 |
| SOL 固定收益 | 下表九条路线，各自独立 Runner | 每 6 小时 |
| SOL 单验证者原生质押 | 未启用：`SOL_VALIDATOR_VOTE_ACCOUNTS=-`；这不影响 Marinade Native | 配置白名单后才采集 |
| AVAX 第一阶段 | OKX AVAX 公开出借 APR、Aave V3/V4 WAVAX 基础存款历史 APY | 每小时 |
| AVAX 第二阶段 | BENQI sAVAX、Ankr ankrAVAX 兑换率；BENQI AVAX 基础借贷 APR | 每小时 |

收益 Runner 启动即首采，失败后等待 10 分钟重试；数据库写入失败会重试原批次，不会用旧利率伪造新时间的观测。历史接口会重复抓取短历史窗口，逻辑去重由现有收益表完成。

SOL 的九条固定路线及对应日志 `source`：

| 产品 | 数据库 `provider / product_code` | 日志 `source` |
|---|---|---|
| bSOL | `BlazeStake / bsol` | `solana-stakepool-bsol` |
| JitoSOL | `Jito / jitosol` | `jitosol` |
| mSOL | `Marinade / msol` | `marinade-msol` |
| Marinade Native | `Marinade / marinade-native` | `marinade-native` |
| laineSOL | `Laine / lainesol` | `solana-stakepool-lainesol` |
| JupSOL | `Jupiter / jupsol` | `solana-stakepool-jupsol` |
| hSOL | `Helius / hsol` | `solana-stakepool-hsol` |
| Kamino Main SOL | `Kamino / main-sol` | `kamino-main-sol` |
| Save Main SOL | `Save / main-sol` | `save-main-sol` |

当前使用的公共接口 base URL 如下；具体路径和校验规则见各采集设计及代码。Bybit 默认域名可能按访问地区返回 403，只有响应体明确包含 `access too frequent` 的 403 才按限频冷却，其他 403 会立即失败并要求运维配置合规可用的 `BYBIT_REST_URL`。除 Binance spot REST 显式覆盖外，其余已启用来源使用配置默认值：

| 来源 | 当前接口 |
|---|---|
| Binance REST | 现货 `https://data-api.binance.vision`；永续 `https://fapi.binance.com` |
| Binance WebSocket | 现货 `wss://stream.binance.com:443/ws`；永续盘口 `wss://fstream.binance.com/public/ws`；资金费率 `wss://fstream.binance.com/market/ws` |
| OKX | REST `https://www.okx.com`；WebSocket `wss://ws.okx.com:443/ws/v5/public`（2026-10-27 起 8443 端口被 OKX 以 64010 通知退役） |
| Bybit | REST `https://api.bybit.com`；线性合约 WebSocket `wss://stream.bybit.com/v5/public/linear` |
| JustLend / TRON | `https://openapi.just.network` / `https://api.trongrid.io` |
| Solana RPC | `https://api.mainnet.solana.com`；用于链上池状态、身份和区块锚点校验 |
| Jito | `https://kobe.mainnet.jito.network` |
| Marinade | `https://apy.marinade.finance`；验证者接口 `https://validators-api.marinade.finance` 目前因白名单为空未调用 |
| Kamino / Save | `https://api.kamino.finance` / `https://api.solend.fi` |
| Ethereum RPC | `https://ethereum-rpc.publicnode.com`；仅使用公开只读 JSON-RPC 方法 |

这些接口只用于读取公开数据，不涉及钱包、签名、账户操作或下单；收益不混入永续资金费率。

## 4. 只读检查

在**宿主机终端**执行。若使用隔离沙箱，沙箱的 PID 列表和 `127.0.0.1` 可能不是宿主机的视图；看不到进程或连不上时，应申请宿主机只读检查权限，不能直接判定服务停止，更不能据此启动另一份。

GNOME 顶栏右侧的桌面指示器提供同一套轻量检查：绿色表示生产 collector
运行、五路盘口均为最新完整分钟且收益写入不超过两小时；黄色表示盘口仍在写入但
存在无效秒、延迟或收益检查降级；红色表示 collector、ClickHouse 或盘口更新中断。
点击圆点可查看五路详情、立即刷新或打开生产日志。它随图形会话启动，退出桌面后
停止，不影响开机后由 linger 维持的采集服务。实现与安装说明见
[桌面指示器 README](../tools/desktop-status/README.md)。

先检查开机启用状态、运行状态、端口、HTTP 和采集进程：

```bash
loginctl show-user ubuntu -p Linger
systemctl --user is-enabled \
  crypto-market-info-clickhouse.service \
  crypto-market-info-collector.service
systemctl --user status \
  crypto-market-info-clickhouse.service \
  crypto-market-info-collector.service
ss -ltnp 'sport = :8123 or sport = :9000'
curl --connect-timeout 3 --max-time 10 --fail --silent --show-error \
  'http://127.0.0.1:8123/ping'
pgrep -af '^/home/ubuntu/\.local/share/crypto-market-info-collector/collector$'
```

正常情况下 linger 为 `yes`，上面两个生产 unit 都是 `enabled` 和 `active (running)`，HTTP 返回 `Ok.`，并且该 `pgrep` 路径下只有一个生产 collector 进程。独立验收服务使用另一二进制和数据库，允许另一个进程；不要把两者误判为同库重复写入。collector 的 30 秒重启间隔内可能暂时没有进程，需要结合 unit 状态和日志判断。

查询实际数据库版本和表：

```bash
/home/ubuntu/.local/share/crypto-market-info-clickhouse-bin/clickhouse client \
  --host 127.0.0.1 --port 9000 --database crypto_market_info --multiquery \
  --query 'SELECT version(), currentDatabase(); SHOW TABLES;'
```

进入交互查询：

```bash
/home/ubuntu/.local/share/crypto-market-info-clickhouse-bin/clickhouse client \
  --host 127.0.0.1 --port 9000 --database crypto_market_info
```

自动 universe 版本连接后执行[架构文档的健康检查 SQL](architecture.md#6-运行状态判断)，使用当前进程启动日志的 `run_id` 核实当前成员；不能用全部已登记 instrument 猜测正在采集的范围。当前生产盘口仍是 BTC 显式模式，并同时启用期权、收益和DEX专项表；盘口用下列只读查询检查，其他来源按各自专项表核验。进程存在或 `/ping` 成功都不能单独证明各来源在正常采集。

```sql
SELECT i.exchange, i.market_type, i.exchange_symbol,
       b.latest_minute, b.valid_seconds
FROM
(
    SELECT exchange, market_type, exchange_symbol, max(instrument_id) AS instrument_id
    FROM instrument FINAL
    WHERE (exchange = 'Binance' AND exchange_symbol = 'BTCUSDT')
       OR (exchange = 'OKX' AND exchange_symbol IN ('BTC-USDT', 'BTC-USDT-SWAP'))
       OR (exchange = 'Bybit' AND exchange_symbol = 'BTCUSDT')
    GROUP BY exchange, market_type, exchange_symbol
) AS i
LEFT JOIN
(
    SELECT instrument_id, max(minute_time) AS latest_minute,
           bitCount(argMax(valid_bitmap, minute_time)) AS valid_seconds
    FROM order_book_minute FINAL
    GROUP BY instrument_id
) AS b USING (instrument_id)
ORDER BY i.exchange, i.market_type;
```

查看近期日志与二进制构建来源：

```bash
tail -n 80 /home/ubuntu/.local/share/crypto-market-info-collector/logs/collector.log
tail -n 80 /home/ubuntu/.local/share/crypto-market-info-clickhouse-logs/error.log
journalctl --user -u crypto-market-info-clickhouse.service \
  -u crypto-market-info-collector.service -n 80 --no-pager
go version -m /home/ubuntu/.local/share/crypto-market-info-collector/collector
```

数据表时间字段显式使用 UTC。当前主机日志带 `+08:00`，ClickHouse 默认服务器时区也是 `Asia/Shanghai`；不要把日志的本地显示时间直接当成表内 UTC 值，手写时间字面量时要明确时区。

## 5. 维护注意事项与已知缺口

- 更新代码不等于更新运行程序。先测试并构建新二进制，确认构建版本；再将它安装到 collector 同目录的临时文件并原子替换目标，避免直接覆盖正在执行的文件；最后执行 `systemctl --user restart crypto-market-info-collector.service` 并检查状态和最新数据。
- 首次部署包含 Bybit 支持的版本时，`InitSchema` 会幂等增加 `instrument.venue_contract_version`。既有 Binance、OKX 永续会登记有版本的新 `instrument_id`，旧事实不会迁移或删除；部署验收必须确认健康查询只看每个交易所代码的最新 ID，并确认最近 24 小时迁移前旧 ID 的待确认资金费率仍可由启动补查完成。启用 Bybit 前还应按专项设计做真实公共端点的 metadata、1000 档序列与 funding history 边界验证。
- 修改采集环境变量时先更新仓库中的 collector unit，再复制到用户 unit 目录、执行 `systemctl --user daemon-reload` 和重启 collector。不要只修改当前终端环境，也不要直接编辑安装副本而遗漏仓库源文件。
- 临时启停使用 `systemctl --user stop|start`；取消开机启动使用 `systemctl --user disable --now`。不要用宽泛名称批量杀进程。`Linger=yes` 是无需登录即可开机启动的必要条件，不要随意关闭。
- 单个来源失败先查日志和最近成功批次，不随意重启 ClickHouse。原生数据库的数据目录与 Docker volume 完全独立；不要清理数据目录、执行 `docker compose down -v` 或停掉其他项目的服务来排障。
- 2026-08-26 核实时，SOL 第一、第二阶段九条固定路线均已有成功写入。日志同时显示 Binance 实际资金费率 REST 曾返回 HTTP `451`（来源的地区限制）；这表示相应数据可能有缺口，不能将 SOL 成功写入概括为全部数据源无故障。保持缺口并按来源错误排查，不绕过地区限制。
- collector 日志目前继续追加到单个文件，尚未配置独立日志轮转；systemd journal 只记录 unit 启停和未重定向的控制进程输出。
- DEX上线69个区块时，gzip证据为2,101个文件、逻辑大小39,251,056字节、磁盘分配约43 MiB。按该短样本线性外推约4.1 GB和21.9万个文件/日，仅用于容量预警，不是24小时实测。上线时根分区尚余约695 GiB、约6,600万inode；在建立实测保留/归档策略前持续监控两项余量，不能让证据写满磁盘。

### 10 档版本的上线与回退

2026-09-21：每侧 10 档版本已先后部署到独立全量验收服务和生产 collector。旧 50 档历史保持原样，新分钟以 `stored_depth=10` 写入；运行进程仍应以实际安装版本和数据库字段为准，不能只看仓库常量判断线上是否生效。更新前二进制分别保存在 `/home/ubuntu/.local/share/crypto-market-info-perp-soak/collector.rollback-20260920-legacy50` 和 `/home/ubuntu/.local/share/crypto-market-info-collector/collector.rollback-20260921-legacy50`。

此次同时修复了全量验收中暴露的采样超时故障：取前 N 档不再对内存中全部保留价位完整排序；单个采样周期超过截止时间时，已采样来源保持有效，其余来源在 `valid_bitmap` 中明确标为无效，并由后续秒继续采样，不再退出整个 collector。部署后全量服务连续观察超过 10 分钟无重启、无采样超时，采样 p99 约 46 ms；生产首个完整新分钟的五路 BTC 行情均为 60 个有效秒、`stored_depth=10`，且第 11 至 50 档均为零。

升级时先完成测试与构建，再按上面的原子替换步骤逐个更新对应 collector。生产的 BTC 显式列表、独立全量验收的自动列表保持各自配置；不能因档位升级顺带把全量 universe 推广生产。新 collector 的 `InitSchema` 只为盘口添加 `stored_depth UInt8 DEFAULT 50`，保留原 50 档物理列和旧历史。新分钟显式写 `10`，第 11 至 50 档写零；分钟切换时建立独立起点，不改写旧分钟或已有差量。

升级后的只读检查（分别连接生产库和验收库）：

```sql
SELECT stored_depth, count(), min(minute_time), max(minute_time)
FROM order_book_minute FINAL
WHERE minute_time >= now() - INTERVAL 10 MINUTE
GROUP BY stored_depth;

SELECT instrument_id, minute_time, stored_depth, bitCount(valid_bitmap) AS valid_seconds
FROM order_book_minute FINAL
WHERE minute_time >= now() - INTERVAL 3 MINUTE
ORDER BY instrument_id, minute_time DESC
LIMIT 1 BY instrument_id;
```

新进程完成初始化后的分钟应为 `stored_depth=10`，旧分钟仍为 `50`。用新查询程序分别回放切换前后的有效秒，核对 `StoredDepth` 与返回档数；稀疏盘口可少于上限。回退旧 collector 时保留新列及默认值 `50`，旧 writer 省略该列仍会正确标记其 50 档数据；查询继续使用支持 `stored_depth` 的新版程序。不要为回退删除新列，否则已写的 10 档分钟会丢失深度语义。

改变深度只降低后续增长。压缩对照方法和样本结果见 [10 档容量对照](book-depth-10.md)，不应直接把旧日占用乘以 `10/50`，也不自动转换或删除旧历史。

## 6. 三家共有永续全量验收

验收采用 [crypto-market-info-perp-soak.service](../deploy/systemd/crypto-market-info-perp-soak.service)，生产服务不变。三家 `*_PERP_SYMBOLS=auto`，现货和收益均禁用，资金费率启用；任意两家共有的规范化 USDT 永续产品在所有符合条件的场内分别采集。字典及其单位依据见 [perpetual-asset-aliases.md](../config/perpetual-asset-aliases.md)。

2026-09-28 旧验收库 `crypto_market_info_perp_soak` 的秒级差量表有 22 个无法完整读取的数据分片（约 21.94 GiB）。逐一隔离并重启 ClickHouse 后，该表可读取的剩余差量为 33,432,284 行。另在本项目生产库和旧验收库中检查了 802 个由 ClickHouse 自动标为 `broken-on-start` 的历史分片：593 个存在空目录、缺失元数据、空数据文件或已证实无法解压的全零压缩头；39 个完整解压时报错；170 个有损坏元数据，其中 18 个逐个在隔离恢复表装载失败。824 个确认不能完整恢复的分片已按用户要求删除，总计约 22.45 GiB；清单和验证结果保存在 `var/recovery/*-2026-09-28.json`。普通 `detached` 分片没有被判定为损坏，仍保留。旧库保留作不完整历史检查；由于缺失秒级差量，不能将其盘口分钟当成完整可回放数据，也不能把新 run 继续写入旧库。

验收服务配置已切换到 `crypto_market_info_perp_soak_20260928` 新库，2026-09-28 13:19:54 UTC 开始的新 run 已写入部分有效分钟，但交易所 WebSocket 重连量很高，并与生产 Binance、Bybit 盘口断流同时出现。为优先保障生产行情，独立验收服务已停止并禁用开机自启；重启验收后必须从新 run 重新计算连续运行时间，并先确认生产五路盘口仍有完整 60/60 分钟。

同日现场直连 Bybit 公共 `orderbook.1000.BTCUSDT` 时，快照在订阅后约 8 秒到达，订阅确认约 13 秒才到；原采集器的 10 秒等待会在收到有效盘口后仍报 `subscribe acknowledgements timed out` 并断开。生产 Bybit 盘口订阅等待已调为 30 秒，继续按 request ID 严格验证确认，并使用现有有界缓冲在确认前保留消息。该修复于 21:42 +08:00 部署，旧二进制保存在 `/home/ubuntu/.local/share/crypto-market-info-collector/collector.rollback-20260928-pre-bybit-ack`。

Binance 永续当时持续报 `Binance snapshot bridge timeout`，生产仍将该路标为无效。公网 1000 档快照实测耗时约 16 秒，原快照请求等待为 20 秒、快照后的差量桥接等待仅 5 秒；现分别调到 35 秒和 30 秒，序列断档仍强制重新取快照，不能因等待变长而沿用旧盘口。2026-09-28 21:54 +08:00 项目状态指示器检查生产五路 BTC 盘口均为最新完整分钟 60/60，生产 collector 零次重启；随后 Binance、Bybit WebSocket 仍出现 TCP 读超时，21:57 +08:00 状态为黄色且有无效秒。网络波动期间继续保留无效标记，不能把这次短时绿色状态写成持续稳定验收。

2026-09-10 的完整目录预检选出 635 组规范化产品、1,529 条场内交易流：Binance 477、Bybit 630、OKX 422，合计 60 条盘口/资金费率 WebSocket。这是该次目录的结果，不是固定白名单，启动时重新计算。独立验收显式设置 `BYBIT_PERP_MAX_INSTRUMENTS=700`、`PERP_MAX_TOTAL_INSTRUMENTS=2000` 和 `MARKET_DATA_MAX_SAMPLE_SOURCES=2100`；程序默认保护上限仍是单家 500、总数 1000、source 1100，不能把验收覆盖直接当作生产容量结论。unit 设置 `MemoryMax=6G` 和 `LimitNOFILE=65536`，异常退出 30 秒后重启。

修复实盘发现的 Binance 单请求字节预算、错误帧解析和 funding 静默 topic 隔离后，全量采集于 2026-09-10 00:24:29 +08:00 重新启动；提交的 run 为 `aa8dceab-bf35-4d99-bc50-c7cf0a40a967`，`started_at=2026-09-09 16:24:36.609 UTC`。该 UUID 仅记录此次核验，重启后检查必须从新日志取得当前 run。此次使用的 mapping revision 为 `1f287a7cfe9fce6090da2a625f2e770afdf88eb8702c8bce2ecca4c7208f888a`。启动空间基线（2026-09-09 16:24:29 UTC，包含先前失败 run metadata）：活跃 part 的 `data_compressed_bytes=83,642`，`bytes_on_disk=109,746`。

小规模独立库 `crypto_market_info_perp_validation` 已完成 BTC、ETH、PEPE 三家共 9 条盘口检查，连续完整分钟均有 60 个有效秒，各家活跃/低活跃代表合约各 100 次只读随机秒回放通过。小规模临时 unit 设置 600 秒运行期限，结束后不继续采集，已写数据保留；资金费率按小时落库，未跨小时不能宣称资金费率落库验收完成。

2026-09-10 的全量验收还发现价格编码前置问题，不仅是运行时长不足。当日 00:40:36 +08:00 核实：Bybit 630/630、OKX 422/422、Binance 457/477 盘口 ready；共 1,509 条已有分钟数据，20 条 Binance 流仍在重同步。独立受限诊断已确认其中 SOLUSDT 的原因：`exchangeInfo.tickSize=0.01`，但三次 1000 档快照都含 `105.002`，约在卖方第 173–175 档，不能按当前步长精确编码。其余 19 条尚未逐一验证原因。

[Binance 官方调整公告](https://www.binance.com/en/support/announcement/detail/8bfe2e9b86734cd08f3cbcdb82b79f51) 明确 SOLUSDT 从 `0.001` 改为 `0.01` 后，既有订单仍按原步长匹配。因此当前把新订单价格步长同时用于全部盘口整数编码的语义不足以覆盖旧挂单，不能假定等待即可解决。程序仍严格拒绝无法整除的价格、保持相应盘口无效；已有效的其余交易流继续写入，不丢档、不取整。要支持此情况需另行确认盘口编码单位与下单步长分离的模型变更，本次未实施该变更，不能认定全量采集完成或推广生产。

当前部署版本的全项目 `go test -race ./...`、`go vet ./...`、10/50 档混合历史 ClickHouse 集成检查均通过。2026-09-21 部署后的新全量 run 为 `4add8a8d-bda7-4989-a0ee-5e2cde0da8c4`；三家活跃与低活跃代表各 100 次随机秒回放共 600/600 次通过，无回放失败。连续观察超过 10 分钟时服务无重启、无采样超时、无待写积压，采样 p99 约 46 ms；这些仍是短时运行观测，不是 24 小时容量结论。

全量的 24 小时持续验收尚未完成。新 run 启动初期 Binance 合约仍会受 REST 限频而分批完成初始化，部分安静的 Bybit ticker 尚未形成完整资金费率估算；程序会将其保持为未就绪或缺失，不会伪造有效盘口或费率。启动失败或修改代码后的新 run 必须重新计算连续运行时间，不得用旧 run 拼接得到“连续 24 小时”。生产仍只采集显式配置的五路 BTC 行情；验收程序不会自动把全量 universe 推广生产。

只读查看服务与最近健康报告：

```bash
systemctl --user show crypto-market-info-perp-soak.service \
  -p ActiveState -p MainPID -p NRestarts -p ActiveEnterTimestamp \
  -p MemoryCurrent -p MemoryPeak -p CPUUsageNSec
journalctl --user -u crypto-market-info-perp-soak.service -n 100 --no-pager -o cat
```

`perpetual_universe_started` 包含当前 `run_id`、mapping/selection revision 和预算。每分钟的 `perpetual_runtime_health` 分别列出 book、funding 和 sampler：检查 ready/expected、重连/断档/队列溢出、采样 p99、写入 p99 与待写积压。Binance 初始化 REST 快照至少需要所选 Binance instrument 数量乘以 1 秒，等待期间相应源保持无效，不能把这一阶段算成完整覆盖。

sampler 的 `SampleOverruns` 表示进程启动后跨过秒级截止时间的次数，`MissedSamples` 表示这些秒中被明确标无效的 instrument 样本数；两者增长需要调查 CPU、GC 和盘口快照耗时，但单次增长不会再终止 collector。`LastOverrunAt`/`LastOverrunID` 用于定位最近一次边界。分钟第 0 秒未及时取得锚点的 instrument 不写该分钟，不能用第 1 秒状态伪造第 0 秒快照。

运行数据库与回放只读验收（替换为当前进程日志的 UUID）：

```bash
go run ./cmd/perp-check \
  -database crypto_market_info_perp_soak_20260928 \
  -run-id '<current-run-id>' -min-duration 24h
```

快速阶段检查可改为 `-min-duration 0`。JSON 的 `pending` 表示时长、写入或代表样本尚不足，`failed` 表示执行或不变量错误，`passed_checks` 只表示该命令的数据库与回放检查通过；退出码依次为 2、1、0。命令不建库、不写表、不访问交易所，但会执行查询，不应高频轮询。`system.parts` 空间是该独立库所有活跃物理 part 的实际压缩占用（包含之前失败 run 和 metadata），不是严格的单次 run 新增量；24 小时空间增长应记录两次同口径读数相减，不能把启动前几分钟线性外推当作实测日占用。

## Ethereum DEX 实验分支（2026-09-22）

已完成代码与 `crypto_market_info` 内五张 `dex_*` 表的创建，并于2026-09-22 01:44:23 +08:00在现有生产collector启用。unit只增加 `DEX_ENABLED=true`、`DEX_ETH_RPC_URL=https://ethereum-rpc.publicnode.com`、`DEX_EVIDENCE_DIR=/home/ubuntu/.local/share/crypto-market-info-dex/evidence`，保持同库单writer。应用配置默认值仍为关闭，只有部署unit显式开启。证据目录必须持久可写并与数据库共同备份。不要把有限 `dex-check --sample-block` 研究命令当作持续采集。

建表命令、查询、成本情景及验收记录见 [DEX 实现说明](dex-arbitrage-implementation.md)。`dex-check`数据库连接启用服务端readonly；可选成本RPC补报只保存报告文件。DEX RPC失败单独退避，不取消CEX、收益或期权分支；既有finalized区块hash出现冲突时仅暂停DEX并记录错误，排查来源后通过现有collector生命周期恢复。

部署验收时连续高度 `26027452..26027477` 已落库，25/26区块为完整58/58报价，最近完整区块的58条状态全为 `ok`；完整区块单次采集耗时约3.9至7.3秒。继续观察到69个连续区块时服务仍为零重启，早期区块已由 `head` 追加修订为 `safe`，证明最终性重检链路在运行；当时Ethereum的 `finalized` 锚点尚未推进到本次启动后的高度。五路生产CEX盘口同期写到最新完整分钟且各有60个有效秒。首份只读报告位于 `var/dex-reports/initial-live/`，使用 `--include-head` 仅为启动验收；日常机会结论继续使用默认finalized口径。

## Reserve r5 研究采集（2026-10-02）

本节早期操作记录保留日期；现行七表、直接入库和响应清理政策见文末「Reserve 2026-10-04 存储修正」。旧原文已迁移并删除，不再按早期五表/证据目录方式恢复。

新增独立 user unit `crypto-market-info-reserve.service`，二进制 `/home/ubuntu/crypto-market-info/var/reserve/reserve-data`，工作目录仓库根，隔离库 `crypto_market_info_reserve`，证据 `/home/ubuntu/crypto-market-info/var/reserve/evidence`。默认 HTTPS POST 到 ethereum-rpc.publicnode.com，路由沿用用户配置。

```bash
systemctl --user status crypto-market-info-reserve.service
journalctl --user -u crypto-market-info-reserve.service -n 30 --no-pager
/home/ubuntu/crypto-market-info/var/reserve/reserve-data report
```

report 默认只读finalized；启动验收可显式 `--finalized-only=false`。后台watch与backfill共享文件锁，回补前停止这一独立unit，完成后重启。当前首个白名单为DFX完整六成分。免费RPC不保证30天历史，拒绝范围保留为missing；完整数据模型、命令与限制见 [Reserve实现](reserve-data-implementation.md)。

Watch主循环对明确的RPC传输/读取超时、截断及HTTP429/5xx保留游标后重新poll/复核，区块头复核每请求最多五个成员。非法响应、证据/DB失败、finalized hash冲突仍退出；启动预检失败仍会由systemd重启。2026-10-02 11:29 UTC部署这一运行修正，短时验收保持active、继续写入，没有自动重启；此前版本曾因公共节点读取超时重启，不能把这次短时观察当成长期无缺口证明。实网验收文件在 `research/2026-10-02-reserve-implementation`。

2026-10-03补齐的[Reserve操作说明](reserve-data-implementation.md)含当前manifest的只读SQL、报告计数口径、RPC环境覆盖、固定高度回补和备份恢复要求。前台export不会修改已运行的systemd环境；回补前停止这一unit，固定from/to及chunk，结束后恢复watch。日志完整而收据未齐时可能已经推进日志游标，Watch没有独立收据重试队列，应通过显式范围回补核验。备份须同时保留五表和原始证据；当前没有自动清理或已验收的自动恢复流程。


## Across 独立研究采集器（2026-10-03 修复）

2026-10-09 排查确认 Base 公共 RPC 的 `-32011` 文本为 `request limit reached`，单块日志同样被拒绝；充分冷却90秒后仍拒绝。已修正限流分类和跨成功请求的退避等级，常驻发现会跳过冷却来源且每链网络工作最多60秒，执行中遇到新增冷却立即让行；拒绝保留为 error/partial，不推进未成功的区间。独立核验链身份、实现代码与日志锚点后，实时 unit 显式设置 `ACROSS_BASE_RPC_URL=https://base-rpc.publicnode.com`；Arbitrum 保留原来源。maintenance 从最新保留孔洞的尾部逐片向前补，旧归档权限限制不会挡住较新孔洞；失败仍保留。原 Base 端点的 quota、数据库和旧缺口标记保留，历史服务继续 disabled。只更新 Across 二进制与该实时 unit，不设置自动来源切换。部署及实际两链落库结果见[本轮记录](../research/2026-10-09-across-repair/report.md)。

当前运行实时 `crypto-market-info-across.service`，磁盘 binary 为 `var/storage/bootstrap-preparation/20261013/bin/across-data`，由已安装的 `zz-redis-mysql-cutover.conf` 指定；`zzz-runtime-state.conf` 指定现有状态目录与恢复世代。工作目录为仓库根，使用 MySQL 实时库 `crypto_market_info_across`、证据 `var/across/evidence` 和状态 `var/across/runtime`。只有一个采集写者，report 只读；不要手工启动同库第二个 writer。user linger 已开启。固定历史 unit 模板仍保留，但已 disabled，其独立库和归档已按用户要求删除。

2026-10-09 同轮收据接口验收发现 PublicNode 要求个人归档授权，包含约一分钟前的交易；不能仅凭日志恢复交付全部费用。实时unit进一步显式设置 `ACROSS_BASE_RECEIPT_RPC_URL=https://mainnet.base.org`，只将Base收据及相应费用/区块核验留在已实测可用的官方端点。两个来源分别保留host gate和真实source_id，默认CLI及没有设置覆盖的其他链保持原样；无自动来源回退。

2026-10-03用户明确收窄采集范围：首次启动前的数据不再回补。已停止并禁用 `crypto-market-info-across-history.service`（inactive/dead、MainPID=0、disabled）。核对所有history事实均早于实时首次已保存区块（Base 52077937、Arbitrum 510991304），没有可补实时缺口的数据；已删除独立库 `crypto_market_info_across_history`、`var/across/history-evidence`（75,702个归档文件）和导出的历史报告数据，保留修复说明及少量验证统计。实时库、实时证据、RPC冷却状态均保留。实时unit继续active/running、enabled：首次无保存游标时从当时链头开始，以后从已保存位置续采；只修补其已有采集范围内的缺块、partial和缺收据，并继续最终性核验。重启实时服务不重置首次采集起点。

此前历史unit使用一条 `history --range 8453:50783591:52079591 --range 42161:500994686:511000892`，固定窗口约对应2026-09-02至2026-10-02，每链一轮64块、独立退避与补解码；现在已停用，不再执行该窗口。2026-10-24修正后，网络故障在进程内按共享quota冷却后重试，不再锁存为永久停止。实时进程失败仍由systemd在15秒后重启，两个unit均不再配置`RestartPreventExitStatus=2`；遗留网络停止错误以普通失败码1退出。

两进程共同使用 `var/across/rpc-quota` 的host gate，默认400ms/成员、每批最多3成员，所有批次共同等待上一批额度后准入，限流共享冷却。仅fresh live baseline及准时followup获准后有固定最多4秒优先窗口，不续租，届满按实际额度可用时间让行一个成员间隔；restart/open_check没有优先额度，且可被新live到达取消其网络工作。此预算不证明源端额度，也不约束不使用此gate的应用。不要删quota文件清冷却；不会自动改路由/代理。默认RPC仍为 `https://mainnet.base.org` 和 `https://arb1.arbitrum.io/rpc`，行情为用户指定 `https://api.binance.com`。对明确限流与网络错误分别记录并退避重试，持续故障需报告来源与失败类别；不得把排障期间的操作约束固化为生产采集器永久禁止恢复。

```bash
systemctl --user status crypto-market-info-across.service crypto-market-info-across-history.service
journalctl --user -u crypto-market-info-across.service -n 30 --no-pager
journalctl --user -u crypto-market-info-across-history.service -n 30 --no-pager
var/storage/bootstrap-preparation/20261013/bin/across-data version
```

`report` 必须显式使用与实时服务一致的 MySQL backend、只读连接配置和证据目录；普通 shell 不会继承 unit 环境，直接沿用旧命令可能查询 ClickHouse。当前完整历史查询仍受迁移完整性检查约束。

升级时先构建候选版本并测试，再停止 Across 实时 unit、确认历史 unit 保持停止，保存旧 binary 和 unit；核对实际 `ExecStart` 后原子替换对应 binary、复制 unit、保留现有 drop-in 并 daemon-reload，只启动实时 unit。历史 unit 保持 disabled，除非用户重新要求历史回补。部署后以 `systemctl show ... -p MainPID` 取得实时 PID，对磁盘和 `/proc/PID/exe` 做 sha256sum，验证没有 deleted 旧映像；本地状态恢复后启动日志含构建版本，还须核验新批次实际入库。只管理 Across 服务，不停止其他项目 collector。本次网络恢复版本的构建、备份与验证见[修复记录](../research/2026-10-24-across-network-recovery/report.md)；此前双服务修复见[旧验收](../research/2026-10-03-across-repair/validation.md)，其运行快照先于范围收窄。

健康检查使用[Across只读SQL](across-data-health.sql)：每链最近capture时间、fixed窗口raw/decoded union、finalized待核验量、最终性晋级新增revision、费用完整性去重和probe准时/取消要分开。原始coverage推进不能替代完整事实；pending积压要测趋势。成功返回finalized头不等于整尾已核验；只认实际存储的已证明revision。report performance明确SQL和归档耗时，finality策略明确包括head，不把报告存在当作盈利成立。

2026-10-04按用户要求改为内存解析后直接落库，不归档RPC/API响应。实时RPC及Binance适配器只计算payload hash；不再依赖response-retention.json或每批删文件。新增`across_receipt_transfers`保存完整原生USDC Transfer数组，退款查询读数据库。旧数据先用`var/across/bin/across-data migrate-transfers`本地迁移（需暂停同库writer，命令不联网），随后恢复实时服务；`prune-responses`可并发只读核验并清理旧response，包括未被引用的旧请求。未完整替代的旧日志、未迁移退款及两分钟内的请求暂留，compact capture/finality元数据和quota保持。history继续disabled；不重新补首次启动前的历史。修改及实际验收见[本轮记录](../research/2026-10-04-across-no-raw/validation.md)。

2026-10-05用户明确放弃已核验的Arbitrum旧历史状态解码缺口。30段共35,257块所涉及的993条partial日志capture追加新revision与reason标记`repair_abandoned=user_requested_historical_state_unavailable`，自动partial补采跳过，重启后仍生效；其他缺采、新块、收据和最终性继续按原逻辑处理。状态仍partial，原失败原因及事实/成员/锚点/最终性保留，覆盖统计仍显示缺口。02:15:25北京时间部署跳过逻辑，磁盘与进程SHA256为`042ea8b0e6b13937575d1d81d295a95c6df7b520802074858d94883d45311ed2`，两链随后实际落库，history保持disabled。修改和验证见[记录](../research/2026-10-05-across-abandon/validation.md)。

备份和恢复须同时保留每库七表、相应完整evidence目录、manifest及其引用的verified实现文件。恢复前停止对应unit并验证成员/归档摘要，不删除源错误证据或重写旧capture。启动时旧partial/费用unknown安排新capture尝试补齐；暂缺历史状态/未知实现仍保持unknown，换RPC应由用户配置并重新验证，不能静默回退到latest。当前无需私钥或真实账户凭据。

## LST 独立研究采集（2026-10-02 UTC）

**当前状态（2026-10-19）：已结束实验并停止采集，disabled，Ubuntu 重启不自启。** 当前两条路线的 9,367 条 finalized 可用报价没有正链上毛利，继续原样采集无新增研究必要。报价、日志、资金费、后续询价和维护请求随专属进程一起停止，保留数据库和恢复状态。下面启动/健康检查为历史记录和将来经明确决定恢复时的技术参考，不是要求服务保持 active。[停用记录](../research/2026-10-19-lst-retirement/report.md)。

**当前恢复验收（2026-10-05 11:22 北京时间）：** 已部署mvp11，active/running、enabled、NRestarts0。机器10:42重启后LST自动启动，但日志积压且游标曾领先实际DB覆盖8块；本轮人工补1852块，修改恢复为从保留Start重建完整连续前缀，自动补回断口并验证重启后跳过已补大段。Next26123313与DB连续覆盖一致，11:19、11:21两轮四腿ok/fresh/canonical；48个本次boot后批次校验和原文清理通过。旧2296块缺口仍低速补，停机报价不能重建，资金费实际结算已补回。程序SHA256 `77b5545b6ac6d74f5e3b9730b5d5a247ba5b28c75dfb29fdb4f6c4abb2536a59`，原unit/路由/端点/限速不改，不建旧程序备份。见[修复记录](../research/2026-10-05-lst-reboot-check/report.md)。

**最近健康检查（2026-10-04 22:27 北京时间）：** 同一mvp10进程持续运行、NRestarts0、enabled；最近15分钟7轮调度报价四腿完整及时、canonical，25个近期批次回读校验与原文清理通过。新段22:24:52追上当时finalized，旧缺口尚未补齐。此前容量不足及零星失败保留原状态，孤立后台数据库超时后落库和最终性继续，没有停止服务或改路由。具体窗口、失败分母与覆盖核验见[晚间检查](../research/2026-10-04-lst-evening-check/report.md)。下段为启动时点记录。

**当前状态（2026-10-04 14:39 北京时间）：** 按用户指令于14:35:16启动现有mvp10，active/running、enabled、NRestarts0；已确认新的market、funding、logs实际落库，4个新批次成员摘要和结构验证通过，成功落库后临时原文已清理。首轮25万USDT链上三腿ok、十档对冲容量不足，正确保持不可用；日志从原游标连续推进16块，未宣称全历史完整。程序SHA256 `24c812ec0a0f5cd7f3d1f7a38f4327f68f32661b224f65f26855dbbe541c0b6b`，原unit、端点、路由、额度和持久状态未改。13:31:19是此前助手手动停止记录，三轮初始化各一次超时不证明持续网络中断；程序本来会退避重试。此次短时恢复及原文清理核验见[启动记录](../research/2026-10-04-lst-restart/report.md)，此前代码与清理测试见[修改记录](../research/2026-10-04-lst-response-retention/report.md)。

**mvp9恢复验收（2026-10-03 17:13 UTC，历史记录）：** 16:52:36 UTC（北京时间2026-10-04 00:52:36）按用户“恢复”部署mvp9，唯一MainPID2196152、active/running、enabled、NRestarts0。BlockPI继续当前报价，MEV Blocker负责赎回日志与历史queue实现状态；固定新段Start26113044每分钟至多8块，原live-logs.gob只依据真实落库补旧缺口，每5分钟至多32块且让实时任务优先。每host20RPC/min、响应后2秒、日志8次/5分钟；不清旧预约或cooldown、不改路由。一笔真实request与claim已通过有限双来源读取落库；随后固定窗口5轮四腿完整及时、9片新日志连续覆盖65块，后台旧缺口已真实推进Next26105776；新Next26113109追上当时finalized26113108。一次Binance预检超时自动恢复，旧整体缺口仍不完整。原始证据、输入/binary/unit SHA及后续验收见[本轮记录](../research/2026-10-04-lst-logs-restore/report.md)。

已创建 `crypto_market_info_lst` 七张专项表及 instrument，程序 `var/lst/lst-data`，工作目录仓库根。状态与限速冷却在 `var/lst/state`，原始公开证据在 `var/lst/evidence`。2026-10-03 02:36:08（北京时间）按用户要求安装并启动用户级 `crypto-market-info-lst.service`，启用开机启动；用户管理器已有 `Linger=yes`。unit 源文件为 [crypto-market-info-lst.service](../deploy/systemd/crypto-market-info-lst.service)，安装在 `/home/ubuntu/.config/systemd/user/`，只运行 `watch`，不启动 30/60 日历史回补。补采仅用于断线后的增量日志缺口，旧状态目录和持久限速沿用。失败后等待 60 秒重启，日志进入 journal。

2026-10-03 18:09:36 UTC（北京时间2026-10-04 02:09:36）完成用户授权的一次性空间清理：删除36,823份已落库旧证据原文及7份旧程序备份，实际释放约292 MiB；未保留原文副本。保留数据库、当前程序、配置/游标/冷却/待写状态、近期或未证明已提交的数据和必要核验/错误样本。清理期间服务未停止，MainPID2196152、NRestarts0、程序SHA不变；清理后历史report仍通过成员摘要校验，且新market批次继续提交。已删除原文不能再按hash回读；本次没有启用自动清理，详情见[清理记录](../research/2026-10-04-lst-cleanup/report.md)。

初次启动核验（旧版本，非本轮验收）已连续写入三轮市场批次（24 条报价观测，其中四条完整且及时）和六条资金费观测。首轮本地发送预约过期被标 unknown，后两轮协议状态恢复 ok；服务未重启。实时日志补缺仍被 dRPC 拒绝，保留 failed、不推进覆盖。有限核验详见[常驻启动记录](../research/2026-10-03-lst-drpc/service-startup.md)。

2026-10-03（北京时间）按用户选择将此 LST CLI 的默认 RPC 改为 `https://eth.drpc.org`，无需注册或 API key。`LST_RPC_URL` 仍可覆盖；共享状态目录、排他锁、启动节奏和回补限速沿用，验证见[切换记录](../research/2026-10-03-lst-drpc/validation.md)。

旧版 dRPC 的一次实时 `watch --once` 已落库协议状态和报价，但该免费端点按高度查询日志返回 HTTP 400 / code 35，512 块历史回补未通过。程序保存错误与 failed 范围，不把拒绝当作成功空范围，不据这条不一致的范围提示反复拆分重试；未运行完整 30/60 日回补。

2026-10-03 06:16:46 UTC 已部署 `lst-mvp-2`，同一 `eth.drpc.org`、原状态目录和冷却，unit 显式 `LST_LOG_MODE=receipts`；非空区块 26106948 的全部收据与 blockHash 日志已有限核验。每轮轮转一个金额的 A/B 两条路线，六条未调度观测保持空值。启动预检在 06:19:28 UTC 记录 `code_curve: transport_http_timeout`；按用户有网络故障即停止、不改路由的要求停止服务，确认 `inactive/dead`、`MainPID=0`、`NRestarts=0`。本次没有新增市场/日志批次，持久 Next 仍为 26105616；连续生产验收未通过。修复、测试、只读数据库核验和归档证据见[本轮记录](../research/2026-10-03-lst-repair/report.md)。这是此前停止状态，后续同机复查见[网络复核](../research/2026-10-03-lst-network-recheck/report.md)。

用户要求恢复后，07:03:54 UTC 启动，首次日志覆盖成功推进至 26105623；随后过去 60 秒共 41 次 RPC 的轮次出现真实 429，来源进入原持久冷却。07:38:20 UTC 部署 `lst-mvp-3` 并启动，该次启动 active/running、enabled、无自动重启；二进制 SHA-256 `375fb00eee7975cda359d51bfc025b2fe35bf3dfc7e5ac94e94a03e46d1cbf82`。unit 显式 32 RPC/滚动分钟、2m 市场周期、一条路线/轮，另一分钟串行尝试最多两段八块 receipts；额度与冷却跨重启保留。限速是本程序保守配置，不等于供应商保证额度，也不约束同出口其他进程。服务继续使用 eth.drpc.org，未修改路由。07:50:19.743042..07:50:24.747729 UTC 的 Binance 十档请求发生真实 `transport_http_timeout`，本地观察在07:50:25.843994 UTC按用户约束停止服务。当前 inactive/dead、MainPID=0、NRestarts=0，保留enabled；未继续外部探测。已提交连续日志26105624..26105687，共64块，含一条请求与一条领取；失败26105688..26105695未覆盖，Next=26105688。另有轮前额度等待源码 `lst-mvp-4` 经本地测试和审核，尚未替换已安装二进制，恢复前须先部署。最新落库、剩余报价问题、完整证据见[启动修复记录](../research/2026-10-03-lst-live-restart/report.md)。

2026-10-03 10:35:52 UTC（北京时间18:35:52）已安装并启动最终 `lst-mvp-7`，MainPID2111383、active/running、enabled、NRestarts=0，二进制SHA256 `b8aa795efe98378f4379ae4a10727c7c8c859753f044e990804e3a1b01d7b5b7`。沿原eth.drpc.org/fapi.binance.com与原state/cursor，不修改路由。此前mvp6最后429的持久冷却10:33:54自然到期；本版Protocol13视图合为一条已pin运行时代码的Multicall3只读请求、核验执行高度/时间；unit20RPC/滚动分钟、响应后2s、启动gap10s、市场2m单路线。初始化20s head窗口对冷却作明确诊断，60s再试。receipts只在原始queue日志非空的真实块核验历史实现；空日志覆盖不声明历史implementation。live发送前额度不足可收束在完全核验的真实前缀，再canonical/Commit/按实际To推进；任何真实source/解析/身份失败整片零事实/Next不动。已通过最终全包race78.257s、vet及独立审核；实际预检/持续采集验收见[本轮记录](../research/2026-10-03-lst-running/report.md)，不能仅凭active认定正常。


以上是历史停止记录。用户要求再次恢复后，08:18:11 UTC启动mvp4，预检16RPC后收到HTTP429，原gate冷却至08:48:51 UTC。08:47:06 UTC已安装并启动mvp5，二进制SHA256 `af121d3bb626f934d38896beaa4b8471efbc647eeea65a37482fa721ea8cf654`，MainPID2081638，active/running、enabled、NRestarts=0，冷却未清除。unit增加 `LST_RPC_STARTUP_GAP=10s`，报价轮前等待持久NextAt和额度，CEX时序及陈旧原因诊断已修复，第二段receipts按剩余额度缩片。最终race49.502秒、vet及独立审核通过；真实预检和持续落库验收进行中，不能仅凭active认定数据正常，详见[本轮运行修复](../research/2026-10-03-lst-running/report.md)。

```bash
systemctl --user status crypto-market-info-lst.service
journalctl --user-unit=crypto-market-info-lst.service -n 30 --no-pager
var/lst/lst-data report --out var/lst-reports/latest
```

不要手工启动第二份 `watch`；同库采集由这个 unit 管理。手动补缺或 probe 前先 `systemctl --user stop crypto-market-info-lst.service`，完成后用 `start` 恢复。`--once` 只有一份市场观测，不触发后台维护或历史回补。来源 URL、严格限速、恢复、只读报告与边界见 [LST 实现说明](lst-redemption-data-implementation.md)，初版数据验证见[记录](../research/2026-10-02-lst-implementation/validation.md)。

数据健康检查使用[只读 SQL](lst-data-health.sql)：最近 15 分钟 market 应持续产生观测，协议/报价可用性按成员状态检查，`partial` 不代表所有成员无效。增量日志覆盖只认 complete/canonical/committed；失败范围与资金费重叠采样不作为新增覆盖或重复现金流。head 观测不会立即进入历史报告。升级、配置覆盖和状态备份见[维护说明](lst-redemption-data-implementation.md#维护配置与恢复)，不要删除状态目录来绕过冷却。




## JustLend keeper 独立研究采集（2026-10-03）

2026-10-08 22:58:37（北京时间）已部署本地状态写入优化，binary SHA256 `1ea617be1680aeda37c4b5fab5f51893f8df85d571746b23e36d12608d0c6a17`。v2 将常写进度和较大候选缓存分开压缩保存，跳过未变化状态；原请求预算、冷却、采集频率和冻结重试的同步持久化保持不变。旧单文件状态原位迁移，正常目录只保留主文件及其引用缓存，原响应归档仍不生成。备份须停服保存完整状态目录；旧 binary 不能直接读取 v2，回滚需先转换当前状态，不能用旧备份重置预算。修改和实测见[本轮记录](../research/2026-10-08-keeper-state-io/report.md)。

历史版本（2026-10-04 02:09:06北京时间启动）已去除请求/响应/summary文件归档：正常采集内存严格解析后直接落库，后台补查及export/report只读DB。七表包括新增jl_keeper_index_page，3244旧成功页进度已本地迁移，旧六表行数/内容指纹不变；state预算/游标保留。实际binary SHA256为480b8d916a21bf2bf013c55a2b954444fbf2d85ed2cac1865dba0c58295a7b47，原unit/路由/限速不改，自启动保留。旧evidence及其硬链接备份已清理，正常目录不存在时采集/后台/30天导出均验证通过。详细范围及持续运行记录见[本轮验证](../research/2026-10-04-keeper-no-raw/validation.md)和[独立审核](../discuss/0019-justlend-keeper-no-raw-review.md)。

按用户要求已启用常驻用户服务 `crypto-market-info-justlend-keeper.service`，2026-10-03 03:02:31 Asia/Shanghai 首次启动；本次边界与报告修复后13:43:36恢复，修复验证版本14:13:14启动，14:33:36曾因两次transport_error停止；该停止判定过于敏感，用户明确要求恢复后，14:55:43（北京时间）已启动并恢复自启动，当前active/running、UnitFileState=enabled。独立库 `crypto_market_info_justlend_keeper` 初版五表已建（当前七表），不接入主盘口 collector。该次二进制 SHA-256 为 `c264480c1eb67456fe242ce1e2368c26f0dd21f9269e3e660bc473eaa9d0b182`（最终报告优化版已安装并实际运行）；同目录其他模块同时维修，使用已提交依赖加keeper补丁的独立快照构建，只安装keeper程序。先前四条无法跨进程核验的报价提交保留原撤回状态，本次不改旧事实／摘要。实际服务、游标和验证证据见[修复记录](../research/2026-10-03-keeper-repair/validation.md)。

二进制在仓库 `var/justlend-keeper/bin/justlend-keeper-data`，配置 `config/justlend-keeper-tron.json`；状态在 `var/justlend-keeper/state/`；旧原始归档已按2026-10-04策略清理，正常运行不依赖evidence目录。仓库 unit 已复制到用户服务目录并 enable，已有 Linger=yes 使退出登录后继续运行。每次七天时限结束或失败后等60秒恢复，进度与预算保留。

```bash
systemctl --user status crypto-market-info-justlend-keeper.service
journalctl --user -u crypto-market-info-justlend-keeper.service -n 30 --no-pager
var/justlend-keeper/bin/justlend-keeper-data report --days 30
```

PublicNode `tron-rpc.publicnode.com` 承担只读节点数据，事件分页仍来自 `api.trongrid.io`，TRXUSDT来自 `api.binance.com`。第一请求等五秒，前五分钟全局至少五秒/次，此后至少一秒/次；TronGrid及后台请求仍至少五秒/次，单请求在途，每日总上限40,000次。初始化 Rent/Return 最多60页和固定50样本；unit显式启用 `--history-days 30`，生命周期事件追上后按一天一个窗口后台回补，冻结区间不随重启滚动。429保存来源冷却，401/403保存停用；不通过重启清掉状态，不自动改路由。

本轮两笔非停服时段的约10秒transport_error不足以证明持续网络故障；按用户后续明确指令已恢复采集及自启动。偶发传输失败保存证据并按现有机制重试，不因两次零星失败停止服务或取消自启动；持续采集不可用时再报告。未更换端点或修改路由。不要再手动启动一份采集器。恢复来源前先停止此unit，完成后重新start；report只读可以同时执行。历史backfill与watch共用状态时禁止带另一模式的待办切换，单纯停止unit不能排空watch队列，当前没有自动排空命令。模式切换限制、配置、备份及[数据健康SQL](justlend-keeper-data-implementation.md#数据健康检查与排查)见[实现说明](justlend-keeper-data-implementation.md)，独立[代码审核](../discuss/0017-justlend-keeper-data-code-review.md)与[真实验收](../research/2026-10-03-keeper-implementation/validation.md)。

04:07首批实际验收已有9条事件、9份收据、5条只读模拟、45条报价/费用观测，正式独立报告成员及原始证据核验通过。模拟均为TVM revert，source请求成功不被当作合约执行成功。启动五分钟51次已回读请求均HTTP200，最小间隔5.54603秒；其余固定样本继续经统一gate恢复核验。

### Reserve 2026-10-03 修复运行

实时 user unit 保留 PublicNode 来源，drop-in `~/.config/systemd/user/crypto-market-info-reserve.service.d/repair.conf` 指定 `watch --simulate-every=5m --from=26109689 --reconcile-manifest=0x98cf5cc757bbbe69d9cc8219210cad0490ae70e6a19a4531f6f6aa74afc3a4a9`，`RestartPreventExitStatus=4`。明确网络故障停止后先看归档诊断并联系用户，不改路由；普通来源限流由程序冷却，冷却元数据在 `var/rpc-state`，重启/换库不得删除它。

本轮独立临时历史 unit 使用 MEV Blocker、隔离历史库/证据。任务名与完整验收结果记录在 [修复记录](../research/2026-10-03-reserve-repair/report.md)，不要依赖临时 unit 在成功退出后仍可重启。固定窗口续跑命令见 [实现说明](reserve-data-implementation.md)。历史与实时不共享 writer.lock，但必须协调同 hostname 的来源额度。

旧 binary 已备份 `var/reserve/reserve-data.v1-20261003`；只替换/restart Reserve，其他会话正在改的 Across/LST/keeper 服务不属于本次部署。回滚时保留新六表、证据和配额目录，停止服务后原子替换二进制；回滚旧版会重新引入密集复核/限流问题，不能把旧参数当现行参数。联合模拟表随六表一起备份；它的 canonical/finality 必须通过 quote/capture 关联读取。

2026-10-03 16:57:13（北京时间，08:57:13 UTC）完成源采集/核验拆分升级并启动；active/running、enabled、NRestarts=0。新二进制SHA256为0749ecd085f5b187150bc88c424cd7a7896e0adc8a79b74a79e50342cb60009c。专用库新增jl_keeper_indexed_event及Capture兼容列，共六表；源页提交独立推进，后台父子核验不阻塞抓取。export及兼容report只输出数据CSV与metadata，不运行获利分析。原state/五表/证据/旧binary已备份，预算与来源状态保留、路由和unit参数不改。实际持续游标、非空索引、父子批次、导出及请求间隔见[本轮验收](../research/2026-10-03-keeper-collection-split/validation.md)。

### Reserve 2026-10-04 存储修正

现行 `reserve-r5-mvp-3`，DFX manifest `0xfb3914d4f2dbc4a357a45ca7aa7876cc78e82feca991a8a77d46746acc8ba332`。只在内存接收、解析和校验RPC；事实写七张定类型表，正文不归档。回执的完整调用参数（calldata）和全部日志在 `reserve_receipt_data`，查询/补采从数据库读取；源哈希、链锚点、旧成员摘要和首次可见时间保留。静态版本配置/ABI/模拟产物在 `/home/ubuntu/crypto-market-info/var/reserve/rules`，约232KiB；四个旧 evidence 目录均只剩 writer.lock。`var/rpc-state` 持久配额/冷却保留。

2026-10-04 02:17:07 CST仅停止 Reserve 服务，四库981旧回执迁移后于02:23:48 CST启动新版。二进制实际SHA256 `4a770096440460a0580a0d475df42084bb4d85ffa6423d152dcf420816a0bfa8`，已核对 `/proc/<pid>/exe` 与安装文件。drop-in 现为 `watch --simulate-every=5m --from=26113531 --reconcile-manifest=0x04af64a255a41d2e2182eb44cc8148338ee62e7fbc5e37a43eac86a73ef93abc`；保留 `RestartPreventExitStatus=4`，路由/RPC来源不改。停机区间日志已自动补齐；旧 v2 尾部正常复核最终性。

本轮删除209,324份运行归档gzip，文件正文压缩后合计7,144,459,564字节，已保留独立静态规则；不备份原文。历史研究目录的既有排障记录未批量删除。删档后从只读数据库逐一验证982回执（981迁移+1实网新抓），33,331条完整回执日志；30日原窗口报告一致。验收检查时 active/running、enabled，本次启动NRestarts=0，11/11完整状态及报价、61/61完整日志范围、128连续高度、零响应文件。该状态为短时现场核验，不代表长期稳定率。

迁移命令需要同目录 writer.lock，不能在运行中的watch旁绕过锁。日常核查使用 readonly `report`；每个旧 manifest 也可用 `--manifest-hash` 检查。备份七表、静态rules、公开配置、构建和unit参数；不要回滚到依赖原文的v1/v2。明确网络故障仍停下通知用户；429仍按来源冷却。修改、实网验证及测试限制见[验收记录](../research/2026-10-04-reserve-storage/report.md)。

交付前02:47:12 CST再次只读核对：Reserve同一进程保持active/running、NRestarts=0，17/17轮完整状态和报价、93/93完整日志范围，167连续高度，四目录零响应文件；见[最后核验](../research/2026-10-04-reserve-storage/last-check.json)。

### Reserve 2026-10-04 服务器重启核查

服务器12:33:55 CST重启，ClickHouse12:34:26、Reserve及其他采集服务12:35:04自动恢复；active/running、enabled、NRestarts=0，Reserve运行binary SHA保持不变。12:47抽查重启后9/9完整状态/报价采样，每批13报价；最新12:46:35落库。旧982回执逐笔校验及215,066高度/1,946日志/957回执的原30日历史报告通过，四目录仍零响应文件。

日志不完全正常：PublicNode `eth_getLogs`反复403/-32602，明确 `rpc_archive_auth_required`；积压在重启前已存在，不是整体网络不通。本次恢复后已推进465高度至26116373，已完成并集内部连续，但相对最新报价仍落后314块、约63分钟；现有watch继续回补。不要把服务running或报价完整当成日志已追平。本轮只读检查，未重启服务、额外回补、修改来源或路由；完整证据见[重启核验](../research/2026-10-04-reserve-reboot/report.md)。

### Reserve 2026-10-05 请求优化

01:39:04 CST部署请求优化，PID223487，安装和运行SHA256同为`8a5604db1a0bd055a2822fdc3b93ab8b2e0aaf3db7dd9dc296ae363260a39978`。保留原manifest、RPC、unit及报价/模拟采样规则，响应正文仍不保留。普通head轮询只读latest，safe/finalized按原分钟复核及链冲突需要读取；失败范围不因授权/限流而缩片，成功后恢复512批量，授权拒绝每5分钟复测且不跳游标。仅Reserve主动重启一次，路由未改。

前两片实际完整补入1024块、2日志/1新回执，第三片512块仍遇来源archive授权限制，保留missing等待冷却；不能仅凭批量恢复就宣称当前已追平。原报价节奏持续落库、全20笔回执数据库摘要检查通过。后续运行状态及严格连续覆盖验收见[本轮记录](../research/2026-10-05-reserve-cadence/report.md)。

01:46:16现场：同一PID、NRestarts0；部署后6/6完整snapshot（78报价）、4联合模拟。01:45:10在约312秒后重试原512块范围仍为archive授权拒绝，没有缩片或跳游标。成功区间连续到26119712，相对最新完整报价26120573尚差861块、2小时52分48秒；来源限制尚未解除，不能把已补1024块说成全部补齐。

### 期权2026-10-04持续运行修复

23:59:51 CST原子替换主collector为SHA-256 `41059f71a06f0064dbb05db7ecbea9380aec350fd2871c3da9e6c8bb42265c0c`，PID201072。部署仅包含期权修复，沿用已安装unit，其他独立采集服务未重启。生命周期合批、丢失屏障主动确认、创建/open及epoch排序、目录状态证据刷新和退订ACK均通过新一轮独立复审。孤立真实公开候选3,090成员全部60秒回放通过；生产跨35分钟持续验证结果见[修复记录](arbitrage/strategies/arb-0009-options-sustained-repair.md)，不能把运行状态当作数据健康。

2026-10-05 00:39:51完成40分钟真实生产核验：37个新计划生效分钟均提交全部预期成员/分片，最新3,094成员、97分片、pending=0；真实新C/P自动开始采集。00:30状态引用实际更新，00:38全量185,640秒回放通过。窗口有效成员秒99.64%，8次连接退役和两次采样迟滞的24,151无效成员秒如实保存并随后恢复；无状态证据、retry容量或writer错误。最终CEX5流各60秒，DEX继续推进，其他独立服务PID不变；实网瞬断根因及下一次真实到期的未观察边界保留在修复记录。

### 2026-10-05 10:42服务器重启后核验

10:50–11:08 CST检查六个数据库/采集服务均active/running、enabled并持续落库，自动重启计数0。Reserve运行SHA仍为`8a5604db1a0bd055a2822fdc3b93ab8b2e0aaf3db7dd9dc296ae363260a39978`；11:07严格只读报告确认26113531..26123372共9842高度日志/所需收据连续无缺口，停机后自动补入1986高度，无需手工补采。启动后17轮状态/报价完整、每轮13报价，24收据完整明细摘要通过，响应正文仍零留存。

本次启动隔离457个全部文件0字节的空parts。452个有现存合并块覆盖，24个对应健康块逐块校验全部通过；另外5个probe尾块以全量capture承诺数量核对，Across/keeper已提交成员缺失均0。空块继续隔离，没有强挂或删除。停机前3094、重启后3104个期权及五路CEX完整分钟全部实际60秒回放通过；10:50有一秒采样迟滞如实标无效，11:01–11:04全量分钟已恢复60/60。未发现需人工重建的本次已提交数据缺失；不撤销已有九月坏块/历史缺口记录。

主DEX旧区块仍在自动补采，Across Arbitrum仍有既有历史状态来源限制；不能据六个服务running宣称全部历史完整。停机期间的实时盘口及报价不可事后还原。本轮没有主动重启、改来源/路由、另起writer；同期LST维护改变PID，11:06已恢复新市场批次。详细时间、查询、空块清单和验证边界见[本次核验](../research/2026-10-05-reserve-reboot/report.md)。

11:22 CST 增补检查与修复：keeper 虽无已提交成员数量缺失，扫描覆盖仍发现停机前的三个 30 秒断口。已正常停止原服务、取得原锁后用原 Collector 定点补采，三片无事件且分页穷尽，来源哈希、成员摘要及窗口连续性核验通过；11:21:16 已恢复 enabled 的常驻服务，实时游标和额度保留。同期 LST 人工补入1852块，新版恢复逻辑又自动补回8块持久游标领先数据库的断口，实际续采与成员验证通过。七个服务均 active/running、enabled；五路 CEX 最新分钟各60/60，期权97分片/3104成员，停机前后完整分钟均已实际回放。未改路由/来源或取消自启动。

本次 CEX 04:29–10:44 CST 共376个分钟缺失，无法精确事后补回；不要填入重启后的盘口冒充历史。共享 ClickHouse 的另一个项目表 `crypto_grid_trading.quality_events_raw` 仍有旧分片 `UNKNOWN_CODEC`，本轮只定位并记录，没有删改该项目数据。Across 的部分 Arbitrum 历史状态及旧 LST 缺口仍按原限制保留。操作、实际查询、补采程序和完整 race 回归见[重启核查与修复](../research/2026-10-05-reboot-recovery/report.md)。

### 2026-10-07：5档与 OKX 配对版本交付时状态

用户选择“期权10档、现货/永续/交割期货5档，补齐 OKX 可借贷 USDT 现货/永续配对”。代码与仓库中的 systemd 模板已加入 `MARKET_BOOK_DEPTH=5`、`OKX_PAIRED_ENABLED=true`、`OKX_PAIRED_MAX_PAIRS=256`、`OKX_PAIRED_REFRESH=30m`。本轮为编程和审核，**尚未覆盖安装二进制、安装 unit 或重启生产采集器**；线上实际深度仍以数据库 `stored_depth` 与运行二进制为准。新版本细节与验证见 [实现说明](okx-paired-five-level.md)。

该段记录 10 月 7 日交付时状态；10 月 8 日已实际部署，当前版本与验收见本文顶部及部署记录。

部署时先备份实际二进制和 unit，停止原采集器后安装已验证版本，保持现有其他来源环境配置。新现货/期货从新分钟写5档；期权继续10档。核验新配对目录、相应两腿有效秒、资金费率及来源政策时间。回退时设 `MARKET_BOOK_DEPTH=10`、`OKX_PAIRED_ENABLED=false`，用支持5/10/50回放的新查询程序读取混合历史，不删除或重写已有历史。

Keeper 的 runtime-convert / runtime-verify / runtime-rollback / runtime-handoff 和原 State/Ops 扩展已接入源码并完成领域恢复回归，尚未用于生产；不要仅凭命令已存在切换服务。新模式的 watch、preflight、resume-source 需要同一已核验运行目录与外部恢复世代；维护、转换和回退步骤见 [运行状态操作说明](redis-mysql-migration/runtime-state-operations.md)。历史导入继续暂停。

主 DEX 的本地运行状态代码仍在验收，生产尚未转换。新模式使用 `DEX_RUNTIME_STATE_DIR`、`DEX_RUNTIME_EPOCH_FILE`；显式转换/核验/回退命令为 `dex-runtime-state`。它只处理 DEX 分支，主 collector 的真实停机与缺失区间必须纳入转换记录；正常启动不默认重建全历史。详见 [操作说明](redis-mysql-migration/runtime-state-operations.md)与实施记录。
