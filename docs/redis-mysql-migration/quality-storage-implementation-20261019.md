# 2026-10-19 质量存储降写入实施

按用户“根据设计改造并重启程序”的授权，实施[保留完整恢复保护的方案](doublewrite-reduction-plan-20261019.md)。doublewrite 保持 `ON`。最终版本已于 19:52:26（UTC+8）生效，19:57 完成短时上线验收。

## 当前改动

- 三张独立运行时表：按分钟排列的盘口质量表、目录证据 profile 表、目录成员引用表。旧表、冻结迁移清单、交付身份和内容摘要保持。
- profile 使用原定类型 RLE 字段；其版本化 SHA-256 覆盖 UTC 分钟和完整字段编码。分钟成员保留原 evidence hash，读取时精确还原后核验；不共享盘口质量中每合约不同的字段。
- 新旧历史统一读取，重复成员和悬空引用失败。重试及关闭新写入开关时保留已有批次的物理格式，避免重复写入两种格式。
- 共享 profile、成员、盘口与原提交清单均在同一 MySQL 事务内发布。没有事务外“已保存”缓存，也没有吞掉冲突的 upsert。
- 混合历史读取使用单条 UNION 查询，同时取得旧格式和新格式；共享字段在同一语句快照中按 profile 校验一次，成员仍逐个核验原摘要。缺 profile 的 LEFT JOIN 结果因非空字段解码失败，不能静默少读。
- 新表分区扩展纳入显式 `extend-partitions` 管理。新增两个查询索引的键宽及实际空间成本计入隔离对照；它们用于保持原按 instrument/minute 的查询。

## 操作入口

管理安装（先隔离验证，生产安装仅创建空表和 schema 登记）：

```text
storage-migrate install-quality-storage --socket <private-socket> --user <ddl-user> --password-file <private-file> --family crypto_market_info --target <explicit-database>
```

writer 的 `--quality-storage-v2` 开启新事实写入。采集器直接写入路径对应 `MYSQL_QUALITY_STORAGE_V2=true`，默认关闭。新版客户端启动时发现完整安装的扩展会读取两种格式，读取能力不依赖写入开关；未完整安装或 schema 摘要不符会拒绝启用。主分钟 writer 两个池均校验扩展表。

回退使用同一新版二进制并关闭新写入开关；已写入新格式后，不可换回只认识旧表的旧二进制。目录共享不会恢复已缺失的历史，不代替来源完整性检查。

## 已完成验证

离线存储/writer、采样、回放及相关命令回归通过，静态检查通过。新增测试覆盖每秒 NULL/时间/UUID 的精确恢复、profile 与成员身份分离、分钟绑定、冻结映射和按分钟布局。

隔离 MySQL 的真实发布回归及 race 检查通过：正常分钟回放、关闭写开关后读取及幂等重试、旧物理证据兼容、profile/成员写入后故障回滚、悬空 profile 拒绝。另以屏障让两个事务同时发现同一 profile 不存在，确认首次插入的竞争失败完整回滚、按原身份重试后成功；已有 profile 内容被篡改时，写入和读取均拒绝。公共 profile 读取不持排他锁，避免将两个提交 worker 串行化。`integration-final.log`、`producer-integration.log`、`regression-final.log` 和 `producer-regression.log` 已保留在私有证据目录。

## 隔离性能结果

`TestQualityStoragePhysicalComparison` 使用原封存 `frozen-real-publication-prefetch-90m` 的 60 个逻辑分钟，在固定 UUID 的无网络隔离 MySQL 中顺序比较：原结构、按分钟布局、按分钟布局加目录共享。比较数据文件、redo、doublewrite 总字节、充分刷盘后的文件增量、提交 P95/P99 及逐批精确读回。该测试没有按实时节奏施加全采集链路负荷，不作为持续容量验收。

最终生产同口径（binlog 关闭、doublewrite=ON、提交同步开启）三组全部精确读回通过，60 个逻辑分钟共 3,495 批、111,410 个成员：

| 结构 | 数据页/redo/doublewrite 合计 | 提交 P95 / P99 | 整批精确读回耗时 |
| --- | ---: | ---: | ---: |
| 原结构 | 3,963,916,288 B | 30.89 / 130.90 ms | 69.06 s |
| 按分钟布局 | 2,115,239,936 B | 24.41 / 36.18 ms | 64.37 s |
| 布局加目录共享 | 1,888,882,688 B | 25.18 / 42.02 ms | 65.98 s |

最终结构合计减少 **52.35%**。其中数据页 661,135,360 B、redo 531,968,000 B、doublewrite 695,779,328 B；111,410 个成员引用 2,008 个 profile。三组索引成本均已计入。新增分配量分别为 310,247,424 / 322,813,952 / 281,862,144 B；仅调整布局因辅助索引会增加空间，最终共享结构将该成本抵消。逐批精确读回的总耗时包括解码和摘要校验，不等于单次 SQL 的 P99 或持续容量。首版混合读取曾多出 SQL 往返而慢约 10%，已在部署前改为单条混合查询；最终读回比原结构略快。

结果目录为 `var/storage/quality-storage-20261019/`，`comparison.json`、`comparison-final.log`、`validation-complete.json` 和 `build-manifest.json` 记录实测及候选摘要。另一次 binlog 关闭的相同写入结构对照合计下降 57.13%，保存在 `comparison-before-read-optimization.json`；刷页调度波动会影响绝对字节，因此不承诺固定比例。质量组件下降比例不能套用于整个 MySQL、Redis 或整盘忙碌率。

## 上线

2026-10-19 19:43:11（UTC+8）已完成生产安装和重启。只创建三张独立空表及其 schema 登记，停止主 collector 后停止 writer，原子替换二进制并启动 writer、collector。新 writer 显式启用 `--quality-storage-v2`；新版 collector 自动识别完整扩展并兼容读取。未清队列，producer generation/holder 及旧表 schema 摘要逐项对照未变。MySQL/Redis 没有重启；Reserve/LST 保持停用，历史迁移仍暂停。

运行二进制 SHA-256：collector `531c890b7a369f41d68953be575fed9f7d489583f5e28f252d1d270b2cdfa625`；writer 最终为 `d8cccbe6da544f6efb2b36c6cc93fb172458a05a9cd616cd1cfa7d235e6c6f4c`。安装前文件与 unit 已备份至私有 `deploy-20261019T114235Z/`。首轮进程级验收未直接判定完成：发现 `WithProducer` 为每个 writer 任务构造客户端时漏传质量存储能力，导致首版仍正常写入旧表、未获得收益。已补齐能力继承，并将真实数据库回归改为通过任务客户端准备、提交和读取，确认旧质量表不写入、新表及提交登记一致。19:52:26 只原子替换并重启 writer，collector 持续运行，队列/代次/旧 schema 均保持。最终版本见 `writer-followup-manifest.json` 和 `writer-binding-20261019T115226Z/deployment.json`；初次构建与验证记录保留原样，不冒充最终二进制。隔离测试库已正常关闭（ExitCode=0），配置恢复到原副本，并保持任务开始前的 stopped 状态。

首轮隔离实例保留旧 binlog 开启设置，三组全部读回通过，目标质量存储的数据页/redo/doublewrite 合计从 5,852,023,808 字节降至 1,903,428,096 字节，约减少 67.5%；该轮不作为与生产一致口径的正式结论。已备份隔离配置并正常重启，仅对隔离实例关闭 binlog，使用同一 SHA-256 校验过的夹具复验；正式 MySQL 未重启。首轮文件保留为 `comparison-binlog-enabled.json`，最终对照另存 `comparison.json`。样本为 60 个逻辑分钟的 111,410 个合约分钟成员，包含有变化及无变化/缺锚点等记录，不代表所有来源全秒有效或当时完整采集范围。

验收限制：通用 `storage-check` 在带历史数据的综合健康查询中触发 MySQL 3024（语句执行时间上限）；不能把本轮记为通用健康命令通过。后续验收使用有分钟范围的新表查询、实际提交/回放读取及 writer 队列指标，不为绕过超时提高生产执行上限。重启初期存在 Bybit API TLS 超时、部分来源采集/发现告警；存储升级不修复或补造这些来源的缺失秒。

19:53:25 新格式正式验证：UTC 11:52 分钟的盘口质量与目录成员各 2,714 条，目录 profile 仅 6 组，近六分钟悬空引用为零。新版查询接口读取旧 UTC 09:09 分钟和新 UTC 11:52 分钟均核验原提交摘要成功；新批次抽查的 32 个成员全部 60 秒有效。全量输入的 60 个历史逻辑分钟已在隔离对照精确读回，上线抽查不能代替所有来源持续完整性验收。隔离实例在补充回归后再次正常关闭，正式磁盘样本不包含其刷盘。

19:55:24 复核：UTC 11:52、11:53、11:54 三分钟均为 2,714 条质量记录及 2,714 条成员，每分钟 6 个 profile；全部近期待查引用无缺失。writer 本轮 observed=committed=270，待写、旧待写、blocked 均为 0，两个进程无自动重启。OKX 配对采集的 `completed_minute_written` 仍输出 332 条，基础 5 条流并列运行；期权目录 entries=2,714 且 unknown/stale 为 0。上述是运行规模和存储验收，不将通用健康查询的超时或现有来源告警抹去。

回退操作：保持当前新版二进制，先停止 `crypto-market-info-storage-writer.service`，将 `/home/ubuntu/.config/systemd/user/crypto-market-info-storage-writer.service.d/90-quality-storage.conf` 备份移出 drop-in 目录，执行 `systemctl --user daemon-reload` 后启动同一 writer。原有 ExecStart 的其余参数继续生效，新写入恢复旧格式；已存在的新格式批次仍按原格式幂等重试。collector 可继续采集并把分钟放入原持久队列。不可恢复只识别旧表的旧 writer/collector，不删除三张新表、profile 或积压。

## 上线后的磁盘观测

19:53:53–19:56:53（UTC+8）隔离库保持停止、无测试/构建任务。180.53 秒宿主机样本：整盘忙碌率 **40.18%**，读/写 2.316 / **7.382 MiB/s**，平均读/写完成时间 28.59 / 0.47 ms，平均未完成 I/O 为 2.03。正式 MySQL 进程写 **4.644 MiB/s**、Redis 1.557 MiB/s；ClickHouse 同期读 1.868 MiB/s，未对此其他项目的读取作变更。进程计数不能机械相加当作物理盘归因。

七次 MySQL 文件快照均成功：主库其他业务文件 1.105 MiB/s，新三张质量表合计 **0.177 MiB/s**，旧两张质量表为 **0**，doublewrite **2.492 MiB/s**，undo 0.172 MiB/s。redo 文件轮换，因此本次文件名差值不当作 redo 总字节。17:09 的旧样本 MySQL 写入约 11.03 MiB/s、doublewrite 6.29 MiB/s；本次值明显更低，但采集时刻和其他负荷不同，不能将差额全部算作固定优化收益。对照试验的 52.35% 仅指相同输入的质量组件。

19:57:37 最终状态：UTC 11:52–11:56 五个完整分钟，每分钟质量/成员各 2,714 条、6 个 profile，近六分钟无悬空引用；writer observed=committed=450，待写/旧待写/blocked 全为零。collector/writer 的实际进程摘要匹配最终部署记录，无自动重启。writer 无新增 MySQL 错误，存在一次已提交部分副本的 Redis 延迟清理告警，后续队列为零。全部结果属于短时生产观察，不能宣称所有来源全秒有效、所有慢读已解决或已完成 24 小时容量验收；综合健康查询的执行上限问题仍保留。

证据：`var/storage/io-attribution/20261019T115353Z/{summary,mysql,quality-accounting}.json`；`var/storage/quality-storage-20261019/health-20261019T115740Z.json`、`legacy-options-read.json`、`new-options-read.json`。
