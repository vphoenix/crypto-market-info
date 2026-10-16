# 按新版代码确定启动依赖

切换顺序是：复制实际启动和继续采集所需的记录，停止旧服务并补齐最后增量，启动新服务，之后迁移其余历史。迁移期间不开放历史查询；整库历史复制和全历史领域校验不是新采集上线条件。

2026-10-14 复查发现，此前的 59 表、22,824,530 行清单是保守整表预置清单，不能称为最小启动依赖。对应后台整表任务已正常停止，事务检查点和已复制事实保留，临时 MySQL buffer pool 已请求恢复到原 512 MiB；原采集服务继续运行。

| 新版入口及实际调用 | 切换前需要 | 其余历史 |
| --- | --- | --- |
| `internal/app/perpetual_runtime.go:prepareMarkets`、MySQL `RegisterInstruments` | 原全部 instrument 身份、已有 derivative contract spec，保持 ID 和完整定义；新号由已有最大 ID 继续 | 旧盘口、分钟发布、交易规则观测不因盘口启动而整表复制。盘口从交易所重新获取快照并校验序列 |
| `internal/app/app.go:runMarketSources`、`LoadPendingFundingConfirmations` | 最近 72 小时的实际值、待确认值，防止 estimate 覆盖 actual；不改变原补采窗口 | 更早资金费历史后迁 |
| `internal/optionslive/catalog_runtime.go:runCatalogClient`、`LatestOptionsPlan` | 最新计划、直接前序计划；保留引用身份和新计划接续所需内容。新 session 重新发现、获取行情 | 旧期权盘口、全部旧 run/catalog/lifecycle 并不是启动时遍历的数据 |
| MySQL `InitYieldRegistry`、`WriteYieldBatch` | 原 yield_route 身份；已有 route ID 和定义不得重编 | yield_observation 历史后迁 |
| `internal/app/discovery_runner.go`、MySQL `discoveryIntents` | 正常退出前排空的事实；如仍有待重试的原内容，保留其 ID、首次 known_from 和证据。新 API 观测生成新的 UUID | 全部 `source_batch_status`、`source_publication_intents`、`public_observation_provenance` 无启动遍历；不能要求整表搬完。以后迁移仍保留重复发布和歧义 |
| `internal/app/dex_runtime.go:runDEX`、`reconcile`、`retryLogs`、`retryReceipts` | 完整 block 覆盖声明用于 gap 检查；最新 canonical、最新 finalized 锚点；从 finalized 起的尾部、所有未最终确认及所有待补日志/收据批次的完整成员。待补集合要完整，不能只取接口的首个 LIMIT 100 页 | 旧 finalized 且无需修改的报价历史后迁；原坏批次保持无效 |
| Across `PrepareRepair`、`refreshRepairReceipts`、订单恢复 | 原 manifest 下的 capture、连续覆盖和恢复订单/收据所用成员。代码确实会重读已提交事件批次来重建待办 | 已复制的 8 表可保留，不能仅拿游标替代实际恢复；不扩大到其他 Across 历史库 |
| Reserve `Watch`、`Reconcile`、`RepairReceipts` | 当前及 related manifest 的覆盖声明、未最终确认尾部、待补收据及其日志、共享收据；已有 state/pending 原样保留 | 已复制 7 表可保留，不把全部历史报价当作所有服务的上线条件 |
| LST `restoreFollowups`、日志游标恢复、`finalizePending` | 覆盖声明和成员、31 天 followup 窗口、未最终确认批次、pending 文件及原配置身份 | 已复制 8 表可保留，继续采集不丢原种子或重新设置起点 |
| Keeper `LoadState`、`restoreEnrichment`、`KeeperEvidenceFrontier` | 原持久 state（扫描、来源冷却、预算、候选、pending）、未完成 enrichment 父批次及索引页链 | 已复制 7 表可保留；Keeper 报告对全部旧批次的读取不应混入启动验证 |

对封存快照实际只读核对：DEX 最新 head 为 26182994，finalized 为 26182919；恢复集合为 76 批、4408 条报价，其中 75 批未最终确认、无待补日志/收据。整张报价表原有 8,314,648 行。76 批是这个固定快照的结果，不是写死的条数或范围；正式切换时必须从旧端停止后的数据重新计算集合。

范围审计与证据见 [启动依赖复核](../../research/2026-10-14-startup-dependencies/report.md)。`tools/storage/prepare-startup-dex.py` 对该恢复集合建立独立小快照，使用原迁移器逐字段复制和独立重读核验，不修改原源表或原 59 表封存清单。恢复检查不应将未迁历史标为 verified，也不执行 finalize。
