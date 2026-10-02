# Across 采集设计验证记录

2026-10-02。范围是设计与数据库草案，不是采集器实现或收益验证。

- 本地官方 ClickHouse local **26.8.1.1825** 使用独立 `/tmp/across-design-ddl-validation` 路径，完整执行 `docs/across-stablecoin-data-schema.sql`，退出码0。
- 系统表返回全部七个预期表：across_capture、across_deposit、across_deposit_update、across_fill、across_order_probe、across_refund、across_tx_receipt。
- 没有连接生产 ClickHouse，没有执行生产迁移，没有变更采集进程。
- 独立 Agent 核对源码、结构与报告边界，提出三项P1和一项P2；作者修订后，Agent重新读取文件复审，全部关闭。详见 [审核记录](../../discuss/0012-across-stablecoin-data-design-review.md)。
- 六份固定官方源码／部署文件已独立按SHA-256复核，与 [source-manifest.json](source-manifest.json) 一致。当前部署实现、RPC行为、真实日志/费用与持续资源量仍需实现阶段核验。

可重复的本地语法验证方式：把DDL原文复制到一个临时SQL文件，末尾加入如下查询，再执行 ClickHouse local（不要同时使用 `--query` 和 `--queries-file`）：

```sql
SELECT name FROM system.tables
WHERE database = currentDatabase()
ORDER BY name FORMAT TSV;
```

```text
/home/ubuntu/.local/share/crypto-market-info-clickhouse-bin/clickhouse local \
  --path /tmp/across-design-ddl-validation \
  --multiquery --queries-file /tmp/across-design-ddl-validation.sql
```

这只验证真实引擎接受DDL，不替代程序对ABI、完整性、重组、退款归属、费用和首次可见性的验收。
