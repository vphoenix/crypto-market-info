# JustLend keeper：公开数据采集设计

2026-10-03 按用户要求调整为纯数据采集。获利、年化、机会 episode 和盈亏判断由其他程序完成；当前命令不执行这些分析。实现见[实现说明](justlend-keeper-data-implementation.md)，独立 Agent 审核见[审核记录](../discuss/0017-justlend-keeper-data-code-review.md)，本次实际验收见[采集拆分记录](../research/2026-10-03-keeper-collection-split/validation.md)。此前研究报告留在 research，不能当作当前采集器输出。

## 1. 范围与来源

只读取 TRON 主网能源租赁代理 `TU2MJ5Veik1LRAgjeSzEdvmDYx7mefJZvd` 的 `RentResource`、`ReturnResource`、`Liquidate`，目标为 `resourceType=1`。金额使用 UInt256 sun，时间 UTC 微秒；价格、数量使用 Decimal(38,18)。不执行交易、签名或私钥操作。

| HTTPS 来源 | 用途 |
|---|---|
| https://api.trongrid.io | 三类合约事件的分页索引 |
| https://tron-rpc.publicnode.com | 区块、收据、交易体、公开链参数和只读模拟 |
| https://api.binance.com | TRXUSDT BBO 报价及容量 |

均为中心化接口，不使用 P2P。沿用用户已配置的端点和路由，不自动切换节点或修改路由。

## 2. 一个循环，两种进度

```text
事件索引页 -> 内存严格解析 + 计算来源哈希 -> 索引观测及分页进度落库 -> 推进抓取游标
                                           |
                                           v
                                  后台补区块/收据/交易体
                                           |
                                           v
                                  已核验事实独立落库
```

源页只需要一次 TronGrid 请求，不先取 PublicNode 固化头，不等待后台核验。实时页优先；已经尝试的模拟完成后置头；历史源页随后；其他辅助及核验请求低优先。继续单进程、单请求在途，不增加消息队列或任务平台。

源页校验合约、事件名、交易 hash、块高、时间、目标资源、ABI 字段及分页链接；`event_index` 缺失或 NULL 不能当作0。查询使用秒包络 `[floor(from),ceil(to))`，合法边缘记录按逻辑半开窗口裁剪并记录排除数。资源类型0不进入能源观测表。请求/响应原文不写文件，不写通用JSON表；解析结果及来源哈希落库后释放内存响应。目标事件全部存索引表，不因不在固定样本中丢弃；页内 ordinal 保留供应商重复下标的独立行。

`event_index/complete` 表示一页解析并提交完整，`pagination_exhausted` 另外表示最后页。空页也有成员摘要。窗口必须全部页提交后才推进 `EventCursors`；写入失败保持冻结批次身份重试，HTTP/解析失败不推进。

后台一次只恢复一个源页，待办从已提交源页读取，不把每个源事件塞入持久任务队列。子批次通过 `parent_capture_id` 指向源页，核对父批次身份、窗口和 typed 成员摘要，直接从索引表读取事件，不读取原始响应文件。保留原收据/日志一致性核验：Liquidate 全部核验，Rent/Return 仅核验当前固定样本，覆盖范围明确记录。失败子批次保留，至少30分钟后重试、最多3次；核验失败不阻塞抓取。

`EvidenceCursors` 是当前选中样本的连续生命周期证据进度，不表示全量索引行均已核验。每个实时窗口必须具有从空 token 到终页的完整源页链，且每个父页都有成功子批次；后续窗口先完成不能跳过前面缺口。空样本子批次表示该页没有选中的核验对象。周期推进处理已经全部完成但尚未晋级的窗口。历史页独立标记 `history`，不推高实时样本证据进度；新 probe 仍以证据进度判断生命周期是否过时。

## 3. 七张专项表

| 表 | 语义 |
|---|---|
| `jl_keeper_capture` | 最后写入的提交标记，成员数/摘要、源窗口、状态及父批次身份 |
| `jl_keeper_index_page` | 一页的扫描ID、进出分页token、实际来源时间及响应哈希；没有JSON正文 |
| `jl_keeper_indexed_event` | 全量目标能源索引观测，键 `(capture_id,row_ordinal)` |
| `jl_keeper_rental_event` | 已核验固化事件、块 hash、交易序号与收据日志位置 |
| `jl_keeper_tx_receipt` | 核验交易/收据、真实资源及费用；未知费用 NULL |
| `jl_keeper_probe` | 当前固定样本的 latest 只读模拟原始观测 |
| `jl_keeper_cost_observation` | 公开链参数、身份及 TRXUSDT 报价 |

索引观测保存块高及链时间，`block_hash=NULL`、`position_status=indexed_only`、`finality=provider_claimed_confirmed`；供应商宣称确认不冒充独立固化核验。核验后写独立事实，不覆盖或改写原观测。新后台事实的 `available_at` 为核验完成时间，原索引观测保存实际响应可用时刻；完整上下文可用时间取 `max(row.available_at,capture.available_at)`，不可用链时间替代。

Capture 追加 `indexed_rows DEFAULT 0`、`indexed_digest Nullable DEFAULT NULL`、`parent_capture_id Nullable DEFAULT NULL`，保持旧 gob Frozen/旧表默认值兼容。旧四类事实及摘要不改。全部成员先写，capture 最后写；读端即使期望0行也读取所有成员表并核验。DDL 见[七表定义](justlend-keeper-data-schema.sql)。

## 4. 限速与恢复

沿用所有预算和冷却：第一次请求至少等待5秒；每次启动前5分钟间隔至少5秒，此后全局至少1秒，TronGrid 和后台请求另受5秒间隔约束。所有来源合计每日最多40,000次实际尝试，重试也计数。不积攒额度、不集中补发错过的 probe。401/403仅停用相应来源，429/418保存冷却。零星超时按原机制重试；持续采集不可用时报告用户，不自行修改路由。服务保留自启动。

旧 Frozen 批次原样完成数据库重试。旧未冻结首页使用state中已解析的全部发现记录；尚未解析时按原限速请求；旧分页前缀或页间停机状态从原窗口空 token 重索引，已存旧事实保留。旧 Frozen 非末页完成后同样重索引完整源窗口；成功末页对应的旧已核验证据进度可继续使用。预算、冷却和原游标不清空。

## 5. 数据导出与验收

`init-schema` 显式升级七表；`watch` 常驻采集，可用 `--history-days 30` 逐日回补源页；`backfill` 显式采源历史。后台核验由 watch 继续处理。`export` 只读数据库，无外部 HTTP；兼容命令 `report` 与 export 相同。

导出七张 typed CSV（含index_pages） 和 `metadata.json`，按 `capture_started_at` 的 `[from,to)` 选择。captures.csv 是审计清单，保留撤回/未提交及不同配置的记录；成员文件只输出已提交、当前配置且通过数据库成员计数/摘要校验的批次。费用未知为 unknown，金额整数完整输出，地址/hash 转 hex。不做利润统计、成本假设或胜率分析。输出目录必须不存在，失败不留下完整导出标记，不覆盖旧结果。

验证包含真实失败页重放、全部非样本事件落库、重复下标与空页、分页链缺页、源写入失败冻结重试、核验逆序补齐、旧 gob/旧表迁移、页间停机、超过256页窗口、时间可用性和限速。实际服务和落库结果以本次验收记录为准，不把短期样本外推为全天覆盖。

## 2026-10-04 去除响应归档

正常请求只在内存中解析并计算SHA-256；来源哈希、时间、区块信息及解析字段进入数据库。后台补查、分页链恢复、export/report均只读取数据库，不依赖归档目录。`evidence_manifest_hash`兼容列保留原值；新批次只计算内存摘要，不保存summary正文，不代表存在原文文件。

`jl_keeper_index_page`是分页进度，键capture_id，按capture_started_at月分区；每页八个定类型字段，完整空页也保存进出token与来源hash。它与源事件先写，capture最后提交；冻结重试的进度内容不可改变。没有新增队列或盈利分析。

旧部署先停止keeper（保留enabled），init-schema后执行migrate-pages，一次性读取旧manifest并与DB成员交叉核验，将分页进度写入新表；不重新请求、不改旧capture/事实/摘要/游标/预算。验证迁移和无归档导出后才清理旧归档。migrate-pages为显式本地维护命令，不在watch自动运行。

没有完整原文后不能离线重新解析供应商当时的JSON；保留的hash是来源指纹，不冒充仍持有原文。真实部署和清理结果见[本轮验证](../research/2026-10-04-keeper-no-raw/validation.md)。
