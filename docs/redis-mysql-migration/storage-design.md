# 存储与交付协议

## 2026-10-16 数据精简设计（先改代码，暂不操作数据库）

前一阶段先实现资本规则无损去重与公共折扣响应共享，在 MySQL 独立压缩维护期间仅修改代码和设计。2026-10-16 压缩完成后，已通过隔离 MySQL 真实事务验证并显式安装以下三张生产压缩扩展表；旧 125 表 `mapping.json`、既有 schema 登记及历史迁移断点保持。采集与历史迁移继续暂停，`MYSQL_CAPITAL_RULE_VERSIONS` 仍关闭。安装、只读核验及启用边界见[执行记录](../../research/2026-10-16-capital-versions/report.md)。

### 资本规则：内容与观测分离

每币每次复核仍产生独立观测，但完整档位只按内容版本保存一次。新增三张专项表，不使用通用 JSON 表：

| 表 | 身份与内容 |
| --- | --- |
| `okx_discount_rule_version` | 完整 SHA-256、OKX、币种、金额单位、规则解释版本、定类型折扣档位数组；USDT 曲线可跨币种复用 |
| `okx_risk_rule_version` | 完整 SHA-256、instrument_id、SWAP/MARGIN 与 cross 作用域、基础币单位、规范化版本、定类型风险档位数组 |
| `okx_capital_observation_v2` | 原 observation_id、instrument_id、四个规则版本引用、原观察/来源/生效/到期时间、base、借币杠杆、model_hash、完整逻辑观测摘要 |

每个档位数组采用一个 `Array(Tuple(... Decimal(38,18)...))` 定类型 BLOB，避免为同一曲线的每个属性分别创建 BLOB。版本 ID 排除观测 UUID、来源响应 hash 和观察时间，包含作用域、币/合约身份、金额单位、规则解释版本和全部精确档位。相同版本键但不同内容必须冲突；不同币、SWAP/MARGIN 或合约版本不能误合并。

`CapitalParameters` 与 `LoadDiscoveryCapital` 的业务结果保持不变；完整逻辑 `DiscoveryHash(Body())` 仍覆盖原全部字段。发布 kind 继续是 `okx_public_capital_parameters`，新物理表示使用 `discovery-capital-versions-v2` codec 明确指向新表。读者在同一发布序列中按原观察时间/ID 选最新，再按 codec 读取旧完整行或新引用；不得因新观测损坏、过期或引用缺失回退旧观测。旧读者拒绝新 codec，因此上线前必须更新读取端。

规则版本、轻量观测、全部来源和 complete 发布在同一事务提交；first-knowledge intent 仍单独耐久保存。重试遇到既有 intent 时沿用其 codec、UUID、全部时间、完整内容及首次 known 时间，不能因开关变化换表示或刷新身份。读者重建全部档位、核对版本摘要及 instrument 作用域、原事实摘要、来源 hash/数量/时间，最后复核 publication；版本早已存在不代表本次观测提前可知。

扩展目录与旧迁移目录分离。`MYSQL_CAPITAL_RULE_VERSIONS` 默认 false；true 只在预安装的主库扩展 schema 验证通过后采用新写法。启动和查询只核验、不建表。显式安装命令只创建三张新表和相应 schema_version 记录，不修改旧事实表、旧映射、分区、压缩或迁移状态。历史不会自动重写。回退新写法可关闭开关，但已有 v2 观测仍需支持 v2 的读取器。

### 公共请求共享与失败重试

全市场折扣接口按客户端共享最多 60 秒的成功响应，租期从真实发送时间计算，不能按命中或解析时间续期。并发缺失只发一个受原 gate/retry 约束的请求；每个调用返回独立数据副本，保留原 URL、payload hash、source/observed 时间。缓存过期后请求失败不返回旧缓存，取消不能触发额外请求。各币风险档仍独立采集和校验，某币缺失不会伪造该币规则。

采集返回和冻结 pending 前先校验全部金额、比例、杠杆符合 `Decimal(38,18)`，超精度、超范围或不支持的来源不进入写入重试，允许下一次重新采集恢复。资本写入失败后，runner 保留原完整观测在内存 pending，后续先按同 UUID/时间/来源重试；成功才推进复核周期。pending 不阻断资金费预测和成交量来源。此机制不承诺进程退出后的未提交观测重建：既有 intent 不含完整载荷，重启后原未完成记录仍不得变成 complete；本轮不引入第二套持久队列。

### 期权诊断的后续契约

精确 captured_times、每秒末 change_id 和冗余档数可设计为独立短留明细，永久保留采样规则版本、准时位图、异常原因、断档/恢复及来源/接收/知悉时间。但是旧成员摘要、250ms 回放验证和待写 Redis/spool 都依赖这些值；本轮继续原格式，不删列、不设置 TTL。实施短留需新的质量与摘要版本、旧格式解码、已提交边界、混合 5/10/50 档回放与缺成员拒绝测试，并明确失去精确历史排障能力。不能把旧质量表当作可直接清理的诊断表。

### 验证与上线边界

离线验证必须覆盖：定点精度与作用域隔离、重复内容共享但观察时间不合并、旧/新表示重建同一完整逻辑摘要、缺版本/错币/篡改拒绝、首次知悉与重试 codec 稳定、共享请求并发/过期/失败/取消，以及单币失败不影响其他事实。数据库集成用独立显式测试环境，默认跳过；集成测试不得连接生产 socket。完整生产空间收益和冷读耗时须独立同范围实测后报告，不能将现存表体量当作可释放量。

代码提供 `storage-migrate install-capital-versions --family crypto_market_info --target <目标库> --socket <专用socket> --user <DDL用户> --password-file <凭据文件>` 独立安装入口，2026-10-16 已在生产主库执行。该命令要求已有 `cmi_storage.schema_version`；安装中断留下未登记的同名表时会拒绝继续，需要核验实际定义后处理，不能自动认定兼容。压缩维护、隔离验证和扩展安装已完成；后续恢复采集前须部署支持 v2 的采集器和所有 MySQL 读取端，再开启 `MYSQL_CAPITAL_RULE_VERSIONS=true`。相邻交易项目当前仍是旧 ClickHouse 读取器，尚未适配 MySQL v2，不得将本次安装视为交易读取切换完成。关闭开关只影响没有既有 intent 的新观测。

离线体积对照使用 48 次相同规则观测、每组 90 个风险档位的合成 fixture：旧定类型行载荷 738,672 字节，新内容版本加观测 42,000 字节。该结果不含来源表、数据库页、索引或压缩，不能作为生产磁盘节省比例。真实 MySQL 安装、事务回滚、新旧表示和重试已验证；生产没有 v2 观测，完整新格式性能与空间收益仍未实测。

前一阶段模型、OKX、runner、MySQL、backend 和迁移命令相关离线测试、`go test -race` 与 `go vet` 已通过。Agent 只读复核发现的超精度观测重试阻塞已修复，并覆盖六类无效数值后恢复的测试。后续真实集成验证还覆盖了缺表时拒绝启用、重复安装、压缩格式、provenance 写入故障后的三表完整回滚及首次知悉时间不变。生产安装前后通过只读 Go 入口核验三条旧观测，完整逻辑摘要、来源证明及发布时间逐项一致。

## 保持的模型不变量

时间统一 UTC。价格使用 `int64 price_tick`，数量使用 `uint64 qty_lot`；费率、金额和比例走精确十进制定点路径。身份、UTC 时间、有效位、来源证据、区块高度、区块哈希、最终性和修订版本必须可无损往返。

盘口仍是第 0 秒完整起点，加第 1–59 秒相对上一有效采样的价格差量。`qty=0` 删除价格；无变化秒与无效秒由有效位区分。无效区间恢复后不凭旧盘口伪造有效样本。历史 50 档起点不能截短后再应用差量。

现有 minute ID 计算保持不变：`(UTC Unix 秒 / 60) << 32 | instrument_id`。高 32 位是 Unix **分钟数**，不是 Unix 秒；不得重新分配历史 instrument ID。

## Redis 的内容与键

使用独立实例和版本前缀 `cmi:v1`。示意键如下，实际编码由一个包统一管理，不让业务代码自行拼接。

| 键的用途 | 内容 |
| --- | --- |
| `book:<db>:<family>:<producer_epoch>:<scope>:<minute>:<instrument>` | Hash；第 0 秒起点、各秒差量、已采样位与有效位、每秒可获得的质量证据 |
| `aux:<db>:<batch_id>` | 分钟封口后的计划、成员、指数、证据、各事实数量和摘要 |
| `batch:<db>:<batch_id>` | schema/codec 版本、分钟、生产者、成员键、内容摘要、sealed/committed 状态 |
| `ready:<db>` | 有序集合，只登记封口且具备完整成员的 batch ID，不复制 payload |
| `lease:<db>:<batch_id>` | writer 租约、所有者与重试调度；过期仅释放消费权，不删除事实 |

`scope` 区分现货/衍生品及 run/shard，避免同一 instrument/minute 的不同批次互相覆盖。指数须保留 `index_id + minute + batch_id` 身份；期权提交须保留 run、plan、batch 的原有关联。

Hash 字段按起点、秒、质量种类组织，字段值是版本化定类型二进制；不为每个价位建键，也不存字符串价格或原始 WebSocket JSON。无变化秒不造空差量，但必须推进已采样位与有效位。

当前分钟还未产生的证据不能提前填写。封口时使用已有分钟模型重新校验，最终有效位、成员与摘要以封口批次为准。实时预览必须返回 `provisional`、最后已采样秒及有效位；不能把 Redis 当前分钟当成 MySQL 已提交的完整分钟。

## 写入与保留状态

```text
采样中的分钟 → sealed → 可重试消费 → MySQL committed → 窗口外清理
                   ↓ Redis 不可用或内存受限
              持久待写文件 → 可重试消费 → MySQL committed → 文件清理
```

采样线程只把不可变的秒样本或封口对象交给有界队列，Redis 网络请求、压缩和 fsync 均在后台执行。秒样本按来源顺序批量 pipeline，每秒集中发送；队列中未接纳或丢失的采样须在质量记录中如实表现，不能阻塞 L2 收包后仍声称行情连续有效。

封口流程：

1. 校验完整模型，冻结现有 batch 身份、成员摘要和首次观测时间。CEX 批次增加确定性的交付 ID，但不改变 minute ID 或原发现事实哈希。
2. 写齐成员及辅助记录；原子脚本验证生产者、状态、计数和预期字段后发布 `sealed` 与 ready 条目。同一 ID 同摘要重试可成功，不同摘要立即报冲突；脚本不处理大 payload 或计算整个批次哈希。
3. 使用同一专用 Redis 连接对封口写入执行本地 AOF 持久化屏障，确认成功后才能释放采集器拥有的封口副本。收到 `sealed` 命令响应本身不足以证明已持久化。
4. writer 获取短租约，重新验证批次，写入 MySQL。成功标准是目标提交记录存在且内容摘要相同，不是网络调用曾返回成功。
5. MySQL 提交后，再标记 Redis committed 并移除 ready 项。此时崩溃造成的重复消费，通过目标提交记录和摘要消除。
6. 保留当前分钟及上一分钟用于短期读取。已 committed 且早于上一分钟的键用有界 `UNLINK` 清理；维护索引同步删除。

`collecting`、`sealed`、ready 和任何未确认批次均不能设置无条件 60 秒 TTL。只有已经得到 MySQL 提交确认的缓存才可设置保险性 TTL；租约 TTL 不承载数据保留策略。异常退出留下的未封口分钟在恢复时单独判定，不能仅因时间过去就发布完整提交。

后台定期核对 sealed 清单与 ready 索引，修复“payload 已封口而 ready 未登记”等中断状态。旧生产者留下且无法满足封口条件的 collecting 数据，先将现有样本和缺失原因持久保存为非完整诊断，再标记 abandoned 并回收预览键；不能永久泄漏内存，也不能丢掉已接纳的完整批次。ready 清单与每条实际成员的摘要仍由 writer 复核，Lua 的原子执行不等于发生错误时自动回滚所有写入。

## 持久化与容量控制

Redis 初始使用 `appendonly yes`、`appendfsync everysec`、`maxmemory 512mb`、`maxmemory-policy noeviction`。不配置每分钟 RDB 快照；AOF 重写仍需要测试并计入 CPU、磁盘和内存预算。AOF 每秒策略存在尚未同步的近期写入窗口，不承诺当前分钟在突然断电后完全保留。[Redis 持久化文档](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/)

封口确认采用 `WAITAOF 1 0 <timeout_ms>`，使用有限正超时并检查返回的本地确认数为 1。它必须在封口写入之后、同一连接上执行，不能放进 Lua 或事务后把立即返回当作完成。超时或连接中断时保留原批次；重连后重新执行幂等封口写入及屏障，或改走持久待写文件。重连后的幂等操作必须在新连接产生一个实际写入的确认令牌，不能用纯读取或无写入的 no-op 配合 WAITAOF 来证明另一条连接的数据已同步；令牌不参与业务内容摘要。实现须在选定 Redis 版本验证这些行为。[WAITAOF 文档](https://redis.io/docs/latest/commands/waitaof/)

内存和积压策略：

- 监控 `used_memory`、RSS、AOF 状态、最老未确认分钟及当前写入拒绝数。超过两个完整分钟仍未交付即告警，不能靠延长缓存无限吸收故障。
- 初始软门限 320 MiB，硬门限 512 MiB；软门限以上先清理窗口外 committed 缓存，再把未交付封口批次转存磁盘。接纳时还须预留当前活跃分钟和客户端缓冲的预算，不能只看键数量。
- 待写目录初始总量上限 20 GiB，文件使用版本、来源库、批次类型、稳定 ID、长度、校验和及定类型 payload。临时文件写完后 fsync 文件、rename、fsync 目录，才可承认它接管批次。
- 先确保文件持久，再从 Redis 移除对应 payload；崩溃造成两份副本时允许重复交付，数据库幂等处理。ready 中仅留文件引用或由独立磁盘索引发现，不能依赖 Redis 存活才能恢复文件。
- 磁盘队列也满时，明确记录采集故障并停止接纳受影响流的新批次，保留已接纳数据。新缺口不得补造；数据库恢复后从原有待写队列重放。
- 1 GiB 是 Redis 初始进程预算，不是已证明足够的强制 OOM 限额。实际应测 AOF rewrite 与高周转同时发生时的 RSS，再设置 systemd/cgroup 上限。

磁盘待写只在交付异常时承担持久排队，不为所有已入库历史维护永久副本。既有 LST 等低频 pending 文件继续由其原有恢复协议管理，不能在数据库确认前删除证据或推进游标。

## MySQL 的表与编码

保持原数据库命名空间及各语义表，不合成一个全项目 payload 大表。源字段以[逐字段清单](source-columns.json)为依据；每张表必须有显式映射和校验器，出现不支持的类型立即失败。

| 原字段类型 | MySQL 目标与约束 |
| --- | --- |
| UInt8/16/32/64 | TINYINT/SMALLINT/INT/BIGINT UNSIGNED；Bool 限定 0/1 |
| Int32/64 | 有符号 INT/BIGINT，保留边界值 |
| Decimal(38,18) | DECIMAL(38,18)，禁止先转 float；超精度或超范围拒绝，不能静默四舍五入 |
| UInt256 | BINARY(32)，固定无符号大端；Go 使用 big.Int，NULL 与零分开 |
| DateTime/DateTime64 | UTC DATETIME 或 DATETIME(3/6)，保持原精度；会话时区固定 UTC |
| UUID | BINARY(16)，保持 UUID 字节语义，不进行隐式字节重排 |
| 原始 FixedString(N) | BINARY(N)，包括尾零；原十六进制文本摘要按其原语义保存，不能一律当原始 hash 解码 |
| String/LowCardinality(String) | 按模型拆分文本与任意字节；任意字节用 BLOB/VARBINARY，文本身份使用大小写敏感比较 |
| Enum8 | 明确映射代码与允许值，不依赖 MySQL ENUM 的位置推导 |
| Nullable(T) | 对应 SQL NULL；数组元素另有显式 NULL 标记 |
| Array 与 Array(Tuple) | 原专项列对应的版本化定类型 BLOB；保留元素顺序、NULL、空数组、嵌套边界和整数宽度 |

MySQL DECIMAL 最多 65 位，因此不能用 `DECIMAL(78,0)` 保存 UInt256。[MySQL 精确数值类型](https://dev.mysql.com/doc/refman/8.4/en/fixed-point-types.html)

二进制数组编码统一定义为版本字节、列类型标识、元素数、元素数据；整数规定字节序，十进制规定 scale，变长值规定长度，NULL 显式编码。读取设最大元素数和解码字节数，拒绝截断、额外尾字节及错误版本。大端 UInt256 与 ClickHouse RowBinary 原编码的转换必须显式执行。

压测脚本的 RowBinary BLOB 可作为对照 fixture，但不能直接作为未定义版本的生产格式。2026-10-16 按用户要求改用 InnoDB 原生 `ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8`，由数据库压缩物理页，原 BLOB 字节及 codec 标识保持。若以后在应用层增加 BLOB 压缩，必须另行定义 codec 标识，不对同一数据无依据重复压缩。现有 RLE/compact 字段的精确还原逻辑必须复用，不能重新展开后丢失微秒、NULL 或证据身份。

压缩是独立的物理存储策略：历史 `mapping.json` 的映射 DDL/摘要及已有 `schema_version`、迁移块摘要保持，避免仅因页格式变化使已封存断点失效。管理员初始化将压缩策略应用于新表；`schema.sql` 输出实际安装格式。已有表由显式工具逐表原子重建，核对精确总行数、首尾及内部键边界的完整列摘要和每个真实分区的压缩页；不改变发布可见性，也不推进历史完成状态。

原 `selection_config_json` 等既有元数据文本原样保留，用于兼容既有哈希；这不授权创建通用 JSON 事实表。原始二进制日志 data、收据数据也不能被误当 UTF-8 文本截断。

## 键与分区

核心查询键保持：

| 逻辑表 | 唯一身份与查询路径 |
| --- | --- |
| `order_book_minute` | `(instrument_id, minute_time)`；首版可保留原 50 档物理列及 stored_depth，避免同时改写起点编码 |
| `order_book_second_delta` | `(minute_id, second_offset)` |
| `derivative_book_minute`、quality | `(instrument_id, minute_time, batch_id)`，compact 列与逻辑 batch ID 显式转换 |
| `derivative_book_second_delta` | `(minute_id, second_offset, batch_id)` |
| 期权 commit | `(run_id, minute_time)`，不可变内容冲突拒绝 |
| `funding_rate_hourly` | `(instrument_id, hour_time)`，actual 不得被 estimate 覆盖 |
| 其他表 | 以逐表清单的逻辑键和既有 reader 的修订规则共同确定，不能只复制排序键 |

热盘口表按 UTC 月分区。含 `minute_time` 的表可用该时间分区；差量表用 `minute_id` 的月起止数值进行 RANGE 分区，因此唯一键仍包含 minute_id，不另引入会削弱唯一性的日期。分区预创建，写入前检查；保留兜底分区或等价的受控创建路径，避免跨月停写。冷表首版可不分区。

MySQL 要求分区列属于每个唯一键；不得为了分区改变 batch 的全球唯一身份。没有合适共同分区列的提交与身份表保持不分区。[MySQL 分区键限制](https://dev.mysql.com/doc/refman/8.4/en/partitioning-limitations-partitioning-keys-unique-keys.html)

除现有查询所需键外，不添加按价格的历史辅助索引。长字符串键必须核实最大长度、collation 和索引字节限制；若用摘要辅助寻址，碰撞时仍逐字节检查完整业务键。不得以字符串前缀唯一索引替代真实身份。

## 事务与可见性

MySQL writer 按现有 CEX 100 instrument 分组及期权计划分片调度，SQL 每批初始最多 500 行并设字节上限。不能对每条行情消息或每个价位开事务，也不能把所有交易流整个分钟强行塞进一个超大事务。

小型完整批次在同一事务内写事实、质量、证据及提交标记。大型批次允许先分块写入不可变成员，最后事务发布提交标记；所有读取入口必须通过该标记和完整性验证后才能暴露事实。CEX 若分块写，最后事务才发布 minute 可见行与对应提交状态。不能只改 writer 而保留会直接读到未提交成员的 reader。

新增内部控制表，使用定类型列，不替代领域事实：

- `ingest_batch`：批次类型、ID、逻辑时间、codec、摘要、成员数、状态、持久提交时间。
- `producer_generation`：服务、当前生产代次、持有者；新旧 writer 的代次在事务中核验，防止仅凭过期 Redis 锁形成双写。
- `migration_run` / `migration_chunk`：冻结源范围、表映射版本、块键、计数、摘要、导入/校验状态。
- `migration_conflict` / `history_coverage`：发生冲突的键、两端摘要、裁决类型，及每个历史范围 verified/unavailable/failed 状态。

新增控制表的归属、DDL 和主键在实现时固定到迁移管理库，不能依赖 Redis 来证明数据库已提交。所有新控制行使用稳定身份；低频 writer 成功返回仍表示 MySQL 已提交，不能偷偷改成“刚入 Redis 队列”。

领域提交的语义保持：先解析最新 revision，再判断 canonical、committed、finality，并校验成员数量和摘要。导入日期、Redis 接纳时间和 MySQL commit 时间不能替换原 `known_from`、`published_ms`、source/received/observed 时间。

MySQL 保持 `innodb_flush_log_at_trx_commit=1`、doublewrite 开启；2026-10-16 按用户明确指令通过 `skip-log-bin` 关闭 binlog，保留已有日志文件。`sync_binlog=1` 仅在以后显式重新启用 binlog 时生效。超时后重新查询 batch ID 与摘要再决定重试。[MySQL 提交持久化参数](https://dev.mysql.com/doc/refman/8.4/en/innodb-parameters.html#sysvar_innodb_flush_log_at_trx_commit)

## 冲突与故障处理

| 情况 | 必须执行的行为 |
| --- | --- |
| 同 ID 同内容重复消费 | 校验既有提交完整性后确认成功，不新增逻辑事实 |
| 同 ID 不同不可变内容 | 停止该批，记录两端摘要，禁止最后写入者覆盖 |
| 更高 revision 或明确更新版本 | 在事务中比较原版本再更新；低版本不得覆盖高版本 |
| 资金费 estimate/actual 重叠 | actual 优先；同优先级按原 writer 规则处理，历史导入不得无版本盲覆盖 |
| MySQL 提交后确认丢失 | 读取同一提交记录后幂等确认；不能重建 batch ID |
| Redis 重启/AOF 恢复 | 重新扫描 sealed/ready，核对 MySQL 已提交项；未封口项保持非完整 |
| writer 租约过期 | 允许另一个消费者尝试，最终由 MySQL 身份与代次决定唯一结果 |
| 部分成员已写入 | 无完整提交标记不得对外可见；按同一批次重试补齐 |
| 迁移中的旧数据晚于新数据到达 | 比较领域版本和摘要，不比较到达顺序 |

普通 MergeTree 表存在物理重复时，必须保留其重数及歧义诊断；既有 discovery reader 会拒绝重复发布，不能因为目标新增 UNIQUE 就把它“修复”为合法发布。具体导入策略见切换与迁移文档。
