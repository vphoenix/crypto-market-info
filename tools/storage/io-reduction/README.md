# 有限刷盘对照

`live-attribution.py --seconds 120` 是宿主机只读归因工具，保存进程/整盘 I/O、MySQL 文件计数和 ClickHouse 合并快照。2026-10-19 起增加 Redis AOF 重写次数、文件大小、延迟同步累计计数，以及系统可用内存、换入/换出和关键进程 RSS/swap；它只读配置，不改变参数。用区间差值判断事件，不能把累计延迟同步次数或已有 swap 总量当作本次新增故障。

`deploy-source-storage.py` 是本次明确授权的来源登记扩展部署入口，要求固定候选摘要、通过的实库回归与相同输入布局对照。仅安装空扩展表并替换/重启主 collector 和 writer，保留既有质量存储开关、队列及生产代次。它不重启 MySQL/Redis，也不启动停用服务。执行后不能以不认识新表的旧二进制回退，见[修正及回退记录](../../../docs/redis-mysql-migration/io-spike-fix-20261019.md)。

本工具仅针对本项目已固定 UUID 的生产 MySQL。连接和凭据读取复用 `../backlog-validation/mysql_client.py`，不支持任意目标。每次运行须使用新证据目录。

```sh
python3 tools/storage/io-reduction/trial.py --output /home/ubuntu/crypto-market-info/var/storage/io-reduction/<新的试验目录>
python3 tools/storage/io-reduction/summarize.py <试验目录>
python3 tools/storage/io-reduction/commit-counts.py <试验目录>
```

`trial.py` 等待完整输入，执行 5 分钟原值、30 分钟候选值、5 分钟恢复原值；候选期异常退出及正常终止信号都会尝试恢复。只有 `complete.json`、`restore-settings.json` 和恢复后观察数组 `restored.json` 均存在，且实际配置读回为原值，才表示完整对照结束。输出 `accepted=false` 是刻意设计：工具不自行固化设置。不要强杀正在更改参数的进程；无法连接数据库或检测到他人修改参数时必须人工读回状态。

配置限于 `innodb_io_capacity=1000/10000` 与 `innodb_io_capacity_max=2000/20000`；检查提交 fsync、doublewrite、flush_sync、binlog 状态但不修改。队列、历史、内存预算和服务保持。证据仅含计数、公开流身份与运行状态，不记录凭据。

`observe.py` 只读本项目 Redis socket 和 spool 目录，兼容 v1/v2 暂存头部；不依赖被忽略的本机研究目录。它读取计数和少量头部，不解压 payload、不变更队列。

`summarize.py` 的进程字节是操作系统进程计数，不能当作准确的整盘物理归因。writer 成员计数是每十秒日志快照；最终用 `commit-counts.py` 的提交时间范围核验。该读数使用逻辑时间索引并回看三分钟，只适用于本试验无历史待写的条件；有历史积压的窗口必须另行核算，不能套用。跨所有阶段比较输入密度、采集完整性、待刷盘债务与短期队列，不能只选低谷。

`full` / `incomplete_source_samples` 只检查目录、连接就绪和健康日志时效，不会检查 MySQL 中每个成员的每秒有效性。它们不能证明全部盘口完整；验收必须另查实际 `valid_bitmap` / `replay_valid_bitmap`，提交成员总数也包含记录无效状态的质量成员。[二次复查](../../../docs/redis-mysql-migration/io-reduction-followup-20261016.md)保留了控制面齐全但期权回放缺秒的实例。

实施、源码、回退限制和质量共享原型见 [实施记录](../../../docs/redis-mysql-migration/io-reduction-20261016.md)。
