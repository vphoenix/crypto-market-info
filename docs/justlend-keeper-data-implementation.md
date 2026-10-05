# JustLend keeper 公开数据采集实现

2026-10-04：已实现 Go 命令，独立研究库 `crypto_market_info_justlend_keeper` 的七张表。设计见[最小方案](justlend-keeper-data-mvp-design.md)，代码独立审核见[本轮审核](../discuss/0019-justlend-keeper-no-raw-review.md)，实际验收见[验证记录](../research/2026-10-04-keeper-no-raw/validation.md)。

2026-10-05 11:22 CST 重启后复核：常驻服务 active/running、enabled，停机大窗口已自动补采。另发现停机前本地游标与现存数据库覆盖之间各少一片：Liquidate/RentResource 的 20:27:06–20:27:36 UTC、ReturnResource 的 20:27:13–20:27:43 UTC。11:20–11:21 CST 使用原 Collector、单进程锁和持久预算定点补采，三片均无事件、一页且分页穷尽，完整写入来源哈希和分页表；实时游标未回退，恢复后来源/明细游标继续推进。按完整扫描窗口并集验证停机前至当前无内部断口，实际成员摘要导出通过。未保留响应正文、未修改路由或自启动。完整操作、SQL、验证及不能补回的盘口缺失见[重启修复记录](../research/2026-10-05-reboot-recovery/report.md)。

## 2026-10-03 采集与核验分离

本轮以准确获取公开数据为边界。TronGrid 页经内存严格解析/计算SHA及 typed 索引观测提交后，即推进抓取进度；PublicNode 区块、收据、交易体作为独立父子批次后台核验。源页不再预取固化头，也不等待核验请求。所有目标能源事件进入新增 jl_keeper_indexed_event，不受固定样本限制；未知块hash为NULL、finality明确是供应商宣称确认。

实时索引页优先，历史索引页与核验低优先，现有限速/冷却/单请求在途不变。源 EventCursors 与当前选中样本的 EvidenceCursors 分开；后者要求完整分页链及每页成功子批次，不认证全量租单。旧 Frozen 批次不重写，旧分页前缀从原窗口首页重索引；capture 的三个新字段默认0/NULL，兼容旧 gob 与表。详细设计见[当前方案](justlend-keeper-data-mvp-design.md)，实际部署核验见[本轮记录](../research/2026-10-03-keeper-collection-split/validation.md)。

## 运行与来源

程序入口为 `cmd/justlend-keeper-data`，配置为 `config/justlend-keeper-tron.json`。单进程循环调度 HTTP 工作项；请求/响应只在内存中解析及计算SHA，不保存正文文件。解析字段、时间、区块锚点、来源哈希和分页进度存入专项表。没有队列平台、全链索引或交易执行。

| 来源 | 地址 | 读取内容 |
|---|---|---|
| PublicNode | https://tron-rpc.publicnode.com | TRON 原生 wallet/solidity 只读接口、块头/指定事件块、账户、合约、收据、交易及清理模拟 |
| TronGrid | https://api.trongrid.io | 目标合约三种事件的确认后分页索引 |
| Binance | https://api.binance.com | TRXUSDT 买卖报价及数量 |

三者均为中心化 HTTPS。PublicNode 没有可用的 v1 合约事件索引，所以事件仍访问 TronGrid。没有自动换节点或绕过访问阻断。可选 TronGrid key 只从 `JUSTLEND_KEEPER_TRONGRID_API_KEY` 读取，不归档请求头。配置不保存凭据。

实际 Go 客户端已通过 PublicNode 六请求限速预检。复制的默认 Transport 曾遗留 h2 ALPN，与仅 HTTP/1 的设置冲突；现明确只协商 HTTP/1.1，关闭连接复用、重定向及透明重发，并有本地 TLS 回归测试。每次 HTTP 尝试均经过发送 gate。

## 命令与常驻服务

```bash
var/justlend-keeper/bin/justlend-keeper-data init-schema
var/justlend-keeper/bin/justlend-keeper-data preflight --duration 90s
var/justlend-keeper/bin/justlend-keeper-data watch --duration 168h
var/justlend-keeper/bin/justlend-keeper-data export --days 30
```

`init-schema` 是唯一显式建库建表命令。采集连接只打开已有库；`report` 使用只读连接，不发外部 HTTP，不执行 DDL。采集、建表与来源恢复命令使用同一 UID 的全局文件锁；换数据库或状态目录不能同时启动另一份采集器。`report` 不占用此锁，可以在服务运行时执行；并行报告应使用不同输出目录。

用户级服务源文件为 `deploy/systemd/crypto-market-info-justlend-keeper.service`。服务每次运行上限七天，退出后等待 60 秒恢复，所有进度和限速状态保留。退出登录后由已启用 Linger 的用户服务管理器继续运行。实际状态以以下命令为准：

```bash
systemctl --user status crypto-market-info-justlend-keeper.service
journalctl --user -u crypto-market-info-justlend-keeper.service -n 30 --no-pager
```

持久路径：二进制 `var/justlend-keeper/bin/justlend-keeper-data`、状态 `var/justlend-keeper/state/state.gob`；旧归档 `var/justlend-keeper/evidence/` 仅供一次性migrate-pages使用，正常采集/导出不依赖该目录；数据导出 `var/justlend-keeper/exports/<UTC时间戳>/`。不要删状态以恢复运行；状态校验损坏、配置或数据库身份不符会停止。

### 构建与配置

在仓库根目录使用 `go.mod` 指定的 Go 版本构建；更新运行中二进制前先停止 unit，安装新文件后恢复。以下只构建候选文件，不改变正在运行的服务：

```bash
go build -buildvcs=false -o /tmp/justlend-keeper-data.next ./cmd/justlend-keeper-data
```

| 参数 | 默认值／口径 |
|---|---|
| `--config` | `config/justlend-keeper-tron.json` |
| `--clickhouse` / `--database` | `127.0.0.1:9000` / `crypto_market_info_justlend_keeper`；库名必须以该研究库前缀开头 |
| `--state` | `var/justlend-keeper/state`，必须保留进度/预算和未完成解析结果 |
| `--evidence` | 兼容旧unit参数；仅migrate-pages读取旧归档，watch/export忽略 |
| `--duration` | 命令默认24小时；实际 unit 使用168小时；到时保留待办，不表示历史窗口已完成 |
| `--days` | 默认30，允许1..30；backfill 为首次历史窗口，export 为查询区间；export 不抓取缺失历史 |
| `--from` / `--to` | RFC3339 时间，含起点、不含终点，建议显式 `Z`；backfill 终点至少落后当前120秒，范围不超过30日 |
| `--history-days` | 仅watch；0默认不新增历史任务，1..30冻结已满足120秒延迟的完整UTC日窗口；现有未完成历史继续恢复 |
| `--max-pages` | backfill 本次最多再提交多少页，默认0表示继续处理整个冻结窗口；分页上限退出不代表完整覆盖 |
| `--out` | `var/justlend-keeper/exports/<UTC时间戳>`，仅用于数据导出 |
| `--source` | resume-source 要恢复的 `publicnode`、`trongrid` 或 `binance` |

配置文件只有三个公开 HTTPS origin、两个公共 caller 和 `daily_budget`（1..40000）。链、合约、implementation/code 指纹、ABI 与调度间隔在代码中固定。ClickHouse 用户与密码分别来自 `JUSTLEND_KEEPER_CLICKHOUSE_USER`、`JUSTLEND_KEEPER_CLICKHOUSE_PASSWORD`；TronGrid 的可选 key 来自 `JUSTLEND_KEEPER_TRONGRID_API_KEY`，均不写入仓库或证据。

状态绑定 ClickHouse 地址、库名与配置 SHA。修改来源 URL、caller 或每日预算后不能直接沿用原状态；当前没有自动配置迁移命令。仅调整域名网络路由时无需改 JSON。迁移需保留原限速、冷却和当日预算，不能通过新状态目录重置这些信息。

## 限速与恢复

第一次请求至少等五秒；每次启动前五分钟，所有来源的相邻请求至少五秒。此后全局至少一秒，始终只有一个请求在途。TronGrid 和所有后台请求分别再受五秒间隔约束。启动检查、分页、交易/收据/区块补证据以及每次重试均受限。上限是第一分钟 12 次、前五分钟 60 次；实际数量会因响应耗时更低。闲置不积攒额度，错过的 probe 不补发。

发送前保存额度和期限；收到响应headers或传输失败结束时再向后延长期限，使用该次预留时的间隔。这样持久化耗时波动不会把实际间隔压短，跨过第五分钟也不会提前改用一秒。所有期限取max，冷却与预算不重置。

每日所有来源合计最多 40,000 次实际尝试，按 UTC 日期持久计数。超时/5xx 最多三次尝试，等待五秒、十五秒后经 gate 重排。429/418 冷却从五分钟递增，最高一小时；更长 Retry-After 优先。收到 headers 即保存冷却，响应 body 随后损坏也不会遗失它。401/403 停用该来源，重启仍停用；其他来源继续调度。显式恢复只取消停用，保留预算和冷却：

```bash
systemctl --user stop crypto-market-info-justlend-keeper.service
var/justlend-keeper/bin/justlend-keeper-data resume-source --source trongrid
systemctl --user start crypto-market-info-justlend-keeper.service
```

## 收集范围与数据身份

watch 缓速初始化六个 Rent/Return 年龄窗口，每个最多十页，共最多六十页。原始完整页和全部目标能源索引观测保留；只把选中租单的日志、收据、交易及指定块核验后写为事实。替补缓存最多 1,500 个候选，按固定 hash 每年龄层保留最多 500 个；它不是总体数量估计。固定样本目标 50 个，年龄层配额 15/15/20，缺样本不伪造补足。新租单不会挤出已选成员；固化事件证实关闭后才在同层替换，manifest 记录生命周期和替换原因。

初始化分页完成后，三类事件按确认延迟120秒、半开窗口增量抓取。新 watch 窗口按秒对齐；积压时窗口最多30分钟，每页仍最多200条、先提交索引观测再后台核验，发送速率不增加。实际部署启用 `--history-days 30`，先追赶租单生命周期，然后一次只补一个完整UTC日的 Liquidate；历史起止首次冻结，重启不滚动。实时 Liquidate 与历史窗口分别调度，不因历史未完成而停止实时游标。UTC零点后的前120秒不启动刚结束一天的重查。

事件 HTTP 查询使用 `[floor(from,秒),ceil(to,秒))` 包络，供应商返回值必须位于包络内，地址／种类／ABI／交易身份仍严格核验；包络边缘合法事件按原逻辑 `[from,to)` 裁剪，保留完整原页和排除数。分页链接必须保持同一个包络。升级时未冻结的小数 watch 待办以 `legacy_fractional_window_replaced` 提交原失败记录；新扫描从原游标所在秒重扫，最多重叠一秒，收据位置与 canonical 身份去重。旧 Frozen 数据库重试保持原样，原预算、冷却、游标不清空。失败窗口保留原分页 token，1／2／4／8／16／30分钟退避，成功后才推进游标；每天重查失败也按此恢复。

冷启动前五分钟不新增probe；已有模拟观测仍完成后置头。probe 一次只调度一个样本，最早每6秒轮转一次，最多5个／30秒，不积攒轮次。Rent/Return 样本证据游标落后当前超过5分钟时暂停新 probe，待事件追上再恢复。未验证样本仍如实 skipped；30秒期限仅拒绝尚未发出模拟的过时待办，一旦模拟尝试已经发出就保留观测并补完后置块头，实际前后范围全部记录。链参数与身份检查每五分钟，TRXUSDT 报价每分钟。

watch 默认不做三十日奖励历史回补；当前unit显式加 `--history-days 30`，可与原watch待办一起恢复，无需删除队列或切换命令。独立 backfill 可用 `--max-pages`、`--duration` 分段；首次冻结起止时间，恢复仍用原窗口和分页游标，不能用新的默认 now 再开一套滚动历史。以下示例只适用于当前状态没有 watch 待办的新部署：

```bash
var/justlend-keeper/bin/justlend-keeper-data backfill --days 30 --max-pages 20 --duration 30m
```

现有常驻 watch 停止后通常仍有待办，因此**仅停服务不保证能切换 backfill**。`pending_watch_tasks_resume_with_watch_before_backfill` 要求继续原 watch；`pending_backfill_resume_with_backfill_before_watch` 要求先续原 backfill。当前没有排空模式的命令，不能删除队列或换 state 绕过限制。历史回补能否切换需另行处理，不能把这条示例当作已运行库的维护步骤。返回码0可能只是到达 duration/max-pages，完整性仍要检查 coverage 与完整 UTC 日数量。

固化事件保留块高/hash、块内交易序号、供应商事件下标与 receipt 日志下标。同高度 hash 冲突会停止写入；报告也拒绝冲突数据。完全相同日志被分页拆散时保守记 partial，不能把后页重新匹配到第一条日志。只有具备唯一日志定位和成功收据的事件进入历史奖励统计。

真实 Rent/Return 使用扩展事件：非 indexed 数据分别六/七个 uint256 word，比官方参考页的旧四/六个多 `securityDeposit` 和 `rentIndex`。按精确 topic 和 word 数兼容两版；扩展版两字段严格核验，保存为 `security_deposit_sun`、`rent_index` Nullable(UInt256)，旧版 NULL，ABI revision 区分版本。`init-schema` 对既有表幂等增加这两列。索引和收据的解析字段及来源hash分别落库，不从未认证 ABI 推导清理截止时间。

已选且未验证的样本可恢复核验：一次只排一个恢复 seed，失败后至少等30分钟，每个解析版本最多三次；次数、下次时间、解析版本保存在状态和 cohort manifest。解析版本改变可重新核验旧失败，预算和来源冷却不重置。到达上限仍保留未验证，不标关闭、不自动换样本。初始化结束后，恢复任务不阻塞已验证样本的 probe 或增量事件。

最新只读模拟为 `node_latest_unpinned`；前后块头给出观测范围，不能当作某个固化历史块的执行结果。API true 与 TVM 成功分别解析，公共 caller 必须为正常 EOA 且不同于 renter/receiver。

## 七表与写入

七表为 `jl_keeper_index_page`、`jl_keeper_capture`、`jl_keeper_indexed_event`、`jl_keeper_rental_event`、`jl_keeper_tx_receipt`、`jl_keeper_probe`、`jl_keeper_cost_observation`，DDL 见[表结构](justlend-keeper-data-schema.sql)。UTC 微秒；金额 UInt256 sun，价格/数量 Decimal(38,18)，hash/address 是二进制 FixedString。缺失 fee 或资源信息保留 NULL。

响应在内存解析及计算哈希，冻结解析成员与摘要后写事件/分页进度等成员，最后写capture提交标记。重试复用 capture_id、起始时间和原成员，跨月不会换分区。writer 检查部分写入未被变更，已提交批次回读核验摘要。导出只验证已提交当前配置的数据库成员计数/摘要、分页来源关系及父capture；索引观测、已核验事实、整笔交易费用各自保留，由下游程序按身份组合。

成员摘要采用 `jl-keeper-fact-v1` 固定二进制编码：域标签、固定字段顺序/名称、长度前缀、UTC UnixMicro、整数/大整数及18位Decimal；NULL与零不同，空列表与数据库空列表等值。每行SHA-256排序后再哈希，manifest显式记录编码标识。gob仅用于本地状态和冻结容器，不用于持久成员摘要。不同进程及不同gob类型注册顺序的回归均核验一致。

组合chain_resource观测的available_at等于参数、proxy与前后头全部组装完成的capture可用时刻；source_time/received_at仍是参数响应时刻。旧组合行保持原值，报告取max(row.available_at,capture.available_at)作为保守有效时刻，下游需据这两个时间计算有效可用时刻，避免使用未来元数据。

首批实际验收发现早期gob摘要不能跨进程认证，四条报价错误提交已完整备份后撤回提交标记；事实值、原摘要、身份和时间保留；原始归档按本次策略清理。它们仍在coverage中显示uncommitted，分析排除，详见验证记录。其他十九个空事实批次逐一认证通过；没有把撤回数据当零值。链参数按TRON signed int64严格解析，无关参数合法的-1不影响两项必须为正的资源费率。

## 纯数据导出

`export` 与兼容命令 `report` 只导出 captures/indexed_events/verified_events/receipts/probes/cost_observations/index_pages 七张 CSV 和 metadata.json；不生成利润、年化、episode、集中度或成本情景报告。`--from/--to` 按 capture_started_at 半开窗口过滤，`--days` 只查询、不回补。目录必须不存在，默认使用 UTC 时间戳；先在临时目录写完、认证成功后整体发布。

captures.csv 保留窗口内撤回/未提交/其他配置的审计记录；这些记录的成员不进入认证输出。其他六份 CSV 仅输出 committed=true、当前 config_hash 的成员，每256个capture批读所有成员表。即使期望0行也读取并校验；子批次还核对父capture身份、窗口和发现数；分页记录核对来源hash和时间。金额完整输出整数，Nullable 用 unknown，hash/地址输出 hex，嵌套内部转账作为无损 JSON 单元格。metadata明确原响应不保留、不能重新解析核验原JSON；导出不读取文件归档。

索引观测与固化事实分开读取。索引页 complete 不证明整个窗口分页完成，更不证明已收据核验。Rent/Return 后台核验仅覆盖固定样本；complete 且 selected_candidates=0 的子批次不代表父页所有索引行已核验。后台事实 available_at 为核验完成时刻，完整上下文有效可用时间为 max(row.available_at,capture.available_at)；block_time 是链时间。失败子批次最多尝试3次、至少间隔30分钟，父子状态可查询，不自动标为核验完整。

模拟仍保留真实 TVM/API 状态和 NULL。`revert` 只表示明确 REVERT，其他明确 TVM 失败为 tvm_failure，不能视作网络错误；成功奖励 fixture 未认证，禁止写 success_reward。这是结果分类与完整性校验，不做机会判断。历史研究 Report 函数保留作旧测试兼容，命令行不调用它。

## 数据健康检查与排查

在仓库根目录读取unit与journal，再用本机客户端执行下面SQL；索引与父子状态完整查询见[健康SQL](justlend-keeper-data-health.sql)。SQL仅用于看数据进度，完整数据库成员校验使用export/report，不再重放原文。`active`只说明进程存在；资源/BBO可用时刻、实际probe和事件窗口也需要持续推进。初始化、来源冷却或当日预算耗尽时允许延迟，按journal和coverage确认原因，不能用旧值填新时间。

```bash
systemctl --user show crypto-market-info-justlend-keeper.service -p ActiveState -p SubState -p NRestarts
journalctl --user -u crypto-market-info-justlend-keeper.service -n 30 --no-pager
/home/ubuntu/.local/share/crypto-market-info-clickhouse-bin/clickhouse client --database crypto_market_info_justlend_keeper
```

```sql
SELECT capture_kind, capture_mode, max(available_at) AS latest_batch_available_utc,
       countIf(status = 'complete') AS complete_batches,
       countIf(status != 'complete') AS other_batches
FROM jl_keeper_capture FINAL
WHERE committed AND capture_started_at >= now('UTC') - INTERVAL 1 HOUR
GROUP BY capture_kind, capture_mode
ORDER BY capture_kind, capture_mode;

SELECT observation_kind, source_id, status, count() AS rows,
       max(available_at) AS latest_available_utc
FROM jl_keeper_cost_observation FINAL
WHERE capture_id IN (SELECT capture_id FROM jl_keeper_capture FINAL WHERE committed)
GROUP BY observation_kind, source_id, status;

SELECT status, count() AS observations, max(available_at) AS latest_available_utc
FROM jl_keeper_probe FINAL
WHERE capture_id IN (SELECT capture_id FROM jl_keeper_capture FINAL WHERE committed)
  AND available_at >= now('UTC') - INTERVAL 1 HOUR
GROUP BY status;
```

| 现象／错误 | 当前处理方式 |
|---|---|
| `caller_conflict_or_unverified_candidate`、`stale_probe_round` | 如实skipped；看state中的cohort verified、seed_attempts和seed_next_at，不把空probe表当作已经观测到零机会 |
| 429／418，或当日已达daily_budget | 等待持久来源冷却或下一UTC日；不通过重启清除 |
| 401／403 | 来源保持blocked，核实路由／鉴权后按上面的resume-source流程显式恢复 |
| `state_checksum_failed`／`state_version_target_config_mismatch` | 保留现场，检查状态备份、配置和目标身份；不用删除state启动新额度 |
| `solid_hash_conflict`、`stored_solid_hash_conflict`、`report_solid_hash_conflict` | 保留冲突批次和state，核对区块锚点及来源；重启不能认证冲突数据 |
| `capture_members_mismatch`、`partial_capture_retry_mutated` | 查数据库成员和冻结状态；先备份再纠正，不重算旧摘要让错误记录冒充通过 |
| `pending_watch_tasks_resume_with_watch_before_backfill` | 恢复原watch；当前没有自动排空后切换模式的实现 |

## 备份与后续验收

恢复运行需要同一时点的七表数据、`state.gob`及对应配置／二进制版本；CSV报告可以重建，不能代替源数据。备份时暂停keeper unit，确认进程已退出，再保存该研究库七表及DDL、状态；完成后恢复unit，其他采集器无需停止。恢复只针对keeper研究库，沿用当日预算／来源阻断，不覆盖主行情库。正常恢复与导出不需要manifest/raw文件；完整分页进度与数据库成员摘要必须通过校验。

已有失败修复的Native快照、原状态和SHA清单在[验收目录](../research/2026-10-03-keeper-implementation/)，这不是自动备份服务。不要仅恢复数据库或仅恢复较旧state后宣称恢复完整；需核对原批次身份、冻结待办及已用预算。当前没有自动状态迁移、备份或队列排空命令。

继续采集后的文档只追加实际证据，待办如下：

| 验收项 | 补齐条件 |
|---|---|
| 24小时稳定性、请求量与空间 | 同一观测期的journal、UTC日预算、七表活跃part和state待办；原响应目录不再增长；重启／缺口保留，不能把首批线性外推为全天 |
| 查询耗时 | 在同一数据量下记录报告墙钟耗时，以及代表性活跃／低活动身份查询耗时；报告大量revert也属于有效观测，不能只选择成功样本 |
| 完整30日历史 | 显式回补且首尾分页链覆盖所需完整UTC日，报告列出未知窗口；常驻watch的bootstrap不替代奖励历史 |
| 奖励成功模拟字段 | 一份可核验真实成功响应或可复现TVM fixture，加上implementation/code身份、return/log/transfer核验；目前仅有失败模拟验收 |
| 获利评估 | 资源实际采购报价、失败尝试成本、兑换容量／费用、库存占用，以及机会观测覆盖；当前采集并未认证日净200 USDT或年化3% |

维护测试命令（本地HTTP mock会监听临时端口；第二条仅使用UUID命名的临时ClickHouse库，测试后删除该测试库）：

```bash
go test -race ./internal/justlendkeeper ./cmd/justlend-keeper-data
KEEPER_DB_TEST=1 go test -race ./internal/storage/clickhouse -run Keeper -count=1
```

测试使用research目录中的公开raw fixture及provenance，移动代码时需一并保留。文档改动核验相对链接、SQL和`git diff --check`即可，不需要重复外部采集测试。

export/report按每256个capture批读六张成员/分页表（含indexed_event与index_page）；包括预期0行表，验证摘要和分页来源字段，核对子批次的父capture。保留旧记录的原始status，不派生盈亏或重分类分析；TVM原字段同时导出。只读本地数据库，不发外部HTTP。

2026-10-03 边界、调度、历史与报告修复的实际验证见[修复记录](../research/2026-10-03-keeper-repair/validation.md)。网络或来源访问失败由运行记录明确报告，不自动修改路由。

## 2026-10-04 旧归档迁移与清理

旧分页token只在manifest，因此先将必要分页进度迁入jl_keeper_index_page。旧Capture和事实成员的值/摘要不修改，旧Frozen继续完成原批次，只为成功索引页补独立进度记录。该表只保存capture身份/起始时间、scan_id、fingerprint_in/out、请求/可用时间、payload_hash八列。

```bash
systemctl --user stop crypto-market-info-justlend-keeper.service
var/justlend-keeper/bin/justlend-keeper-data init-schema
var/justlend-keeper/bin/justlend-keeper-data migrate-pages
systemctl --user start crypto-market-info-justlend-keeper.service
```

migrate-pages持全局锁，仅访问本地旧manifest与数据库，批量交叉核验索引成员来源hash/时间后写分页进度，不发HTTP。重复执行已迁移记录时不需旧归档。维护不能disable服务或重置state。归档清理必须在分页迁移及无归档恢复/导出验证完成后进行；新的正常采集不产生原始请求、响应或summary文件。已删除原文后旧归档版binary不能直接回滚；数据库/state不删除。

独立审核见[0019](../discuss/0019-justlend-keeper-no-raw-review.md)，真实验证、清理范围及二进制hash见[本轮记录](../research/2026-10-04-keeper-no-raw/validation.md)。历史研究Report函数只作原fixture测试兼容，CLI不调用，不能用它读取清理后的数据。
