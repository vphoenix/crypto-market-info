# Binlog 事务缓存对照

只作用于代码中固定 UUID 和 socket 的生产/隔离 MySQL。`--action inspect` 只读计量；`apply` 要求两端原值均为 32 KiB，保存 `binlog-cache-4m/before.json` 后在线设为 4 MiB；`rollback` 按已保存的 UUID 与原值恢复。普通采集器不获得管理员权限。

更改不关闭 binlog，也不改变 flush-at-commit=1、sync-binlog=1、doublewrite=ON 或批次事实。4 MiB 按每个客户端估算，24 条最大连接约 96 MiB。配置是否固化需结合实际延迟、溢出与内存对照，工具本身不自动改配置。现有池最长十分钟自然更换连接，另留一分钟事务时间；不为使参数立即生效而重启所有采集器。

[MySQL 8.4 官方说明](https://dev.mysql.com/doc/refman/8.4/en/replication-options-binary-log.html#sysvar_binlog_cache_size)说明缓存溢出使用临时文件，可用 `Binlog_cache_use` / `Binlog_cache_disk_use` 计量；该全局变量对新会话生效。私有输出在 `var/storage/write-backlog-improvement/binlog-cache-4m/`，不输出凭据。

`python3 tools/storage/binlog-cache-trial/config.py keep` 在核对两端在线值和连接轮换后固化 `deploy/storage/mysql.cnf`，保存原文件及前后摘要。回退在线值用私有目录中的 `binlog-cache-trial --action rollback`；再执行 `config.py rollback` 恢复配置，外部修改过的文件会被拒绝覆盖。
