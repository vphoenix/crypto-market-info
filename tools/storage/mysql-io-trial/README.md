# MySQL 后台 I/O 对照

仅操作本项目已经固定 UUID 与 Unix socket 的生产/隔离实例。`main.go` 的 `inspect` 只读；`apply` 要求原参数仍是 10000/20000，保存原值后在线设为 200/400；`rollback` 仅接受试验原值/目标值并恢复原值。所有操作均核对 flush-at-commit=1、sync-binlog=1、doublewrite=ON、flush-sync=1，不改变事实、schema、generation 或提交持久化，也不重启数据库。

依据是本机实际磁盘压力、低脏页比例与低 checkpoint age；不能只凭设备报告 rotational 就推断真实硬件的 IOPS。参考 [MySQL 8.4 官方配置说明](https://dev.mysql.com/doc/refman/8.4/en/innodb-configuring-io-capacity.html)。200/400 是需验证的试验参数，不是普遍推荐值或吞吐承诺。

以低并发构建后将二进制放到私有证据目录 `var/storage/write-backlog-improvement/mysql-io-capacity-200/mysql-io-trial`。先 inspect，再 apply；`watch.py --until UTC_ISO_TIME` 有限监测，checkpoint age 连续三次超过 redo 容量的 75%、新增日志等待或连续三次检查失败时尝试 rollback。原件、采样与结果写入该目录，检查结束不会自动通过容量验收。回滚操作失败时保留错误状态，由操作者继续核验，不声称已经恢复。参数当前只在运行时调整，持久配置需在对照后另外同步。

保护器每 30 秒检查一次，单次工具上限 40 秒；保留同步 checkpoint 刷盘作为数据库本身的保护。该工具没有对 SQL 前台 I/O 做硬限速。比较完整输入的成员/类型/字节、净积压、最后分片延迟、实际读写与 checkpoint，不能把后台写盘变少单独当作成功。
