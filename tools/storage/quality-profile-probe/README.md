# 质量证据存储验证

`main.go` 保留原只读 profile 字段共享原型。实际扩展表安装入口为 `cmd/storage-migrate install-quality-storage`，写入开关为 `storage-writer --quality-storage-v2`。

物理对照在 MySQL 包的显式选择测试 `TestQualityStoragePhysicalComparison`。必须设置 `MYSQL_INTEGRATION=1`、原有 `CMI_MYSQL_TEST_*` 私有连接参数，以及 `CMI_QUALITY_BENCH_FRAMES`、`CMI_QUALITY_BENCH_OUTPUT`。程序只接受固定 UUID 的隔离 socket，创建随机 `cmi_quality_bench_*` 库；拒绝生产 socket。输出目录必须位于私有 `var/storage/quality-storage-*` 下。

测试读取封存批次，不修改原帧，生成压缩质量字段夹具后，顺序比较三种布局的相同 60 个逻辑分钟。结束阶段先偿还脏页，再计量数据文件、redo、doublewrite。分配文件增量的每日估算是 60 分钟样本外推，不是完整自然日实测；未按实时节奏运行，不证明采集系统容量。对每个原始成员进行摘要及原定类型字段的精确读回检查。

实施、上线和回退限制见 [实施记录](../../../docs/redis-mysql-migration/quality-storage-implementation-20261019.md)。

`deploy.py` 用于本次已授权的固定服务切换。它要求 `comparison.json`、通过的 `validation-complete.json` 和明确的 collector/writer/admin SHA-256；先备份原文件和 unit，再安装独立扩展表，停止主 collector/writer，原子替换并启动，核验实际进程摘要、generation/holder 和旧 schema 摘要。它不清队列、不变更 MySQL 保护参数，也不启动停用的 Reserve/LST。已写新格式后，回退必须使用兼容新表读取的新版程序，关闭写入开关；不能直接恢复备份旧程序。
