# ARB-0009 R5 新挂牌发现和到期退出设计审核

日期：2026-10-04。审核者：用户指定的唯一独立审核 Agent。对象：[R5 设计](../docs/arbitrage/strategies/arb-0009-options-lifecycle-design.md)。本记录仅审核方案，不表示代码已修改、测试已通过或线上采集已恢复。

## 第一轮结论

初审版本 SHA-256：`23cd9100134159c6b30620f32a4afef78cf185ea782268dc8f7455fabbc4660f`。

方向正确：持续目录和生命周期发现代替启动一次选约；所有四族的新到期日、新行权价独立准入；到期自动退出；分析配对不再作为采集条件；32成员逻辑分片、全量计划、提交可见性与R4历史hash兼容均已考虑。

结论为**需要修订后复审**。没有发现P1；以下六项P2阻断方案通过。它们是设计细节和现有实现之间的冲突，可以局部修订，无需推翻整体方案。

| 编号 | 位置 | 问题和具体修订建议 |
| --- | --- | --- |
| R1 / P2 | 第6节成员切换步骤及唯一owner段落 | T第0秒启动与“旧run写队列排空后才能启动重复成员继任者”冲突。现有writer允许40秒写入、45秒排空，上一分钟慢写会使继任run错过T锚点。以分钟范围隔离所有权：旧writer仅允许提交`minute<T`，T边界立即移交采样器，旧写后台排空；仅相同分钟重试或跨进程交接需要writer完全退出。旧写失败应标旧分钟缺失，不能污染新分钟。 |
| R2 / P2 | 第5、8、10节分片写入隔离 | 现有`WriteOptionsMinute`、定义注册及规则写入均在整个数据库查询/重试期间持有全局`derivativeMu`，单分片40秒卡顿会阻塞其他分片和目录。R5需明确采用每run/minute不可变提交锁、可取消且公平的有界数据库并发预算；定义注册单独串行，不能跨网络请求持有共享采集锁。数据库整体故障与单分片故障分别报告。增加慢分片与其他分片/目录持续推进的测试。 |
| R3 / P2 | 第7节计划键及第8节重启恢复 | `(session_id,revision)`仅定义会话内顺序，未规定不同session之间如何选择该分钟唯一计划，也未限制后来发布的计划重写已生效范围。补全局前驱plan ID/hash链及严格递增的生效分钟；新session续接已发布计划，同一分钟只能一个已发布计划。集中新增在发布前合并，发布后新增推迟至下一未预定分钟。读写双方拒绝分叉、逆序或回溯覆盖。 |
| R4 / P2 | 第4节平台初始状态及第7节平台证据 | 初版只要求未来适配器验证“公共查询/初始推送”，缺少可落地的基线接口和字段。官方`public/status`可给锁定基线，但不提供maintenance字段；`locked=false`不能被改写为`maintenance=false`，也不能因为maintenance未知而让所有盘口永久无效。明确字符串`true/partial/false`、`locked_indices`、partial缺列表、旧bool响应的严格处理及基线与并行推送的冲突规则。maintenance作为独立观察事实，未观察不得伪造其值；有效性条件明确绑定锁定基线、已知维护事件、连接和单合约状态。 |
| R5 / P2 | 第3节证据持久化顺序与第4节立即失效 | 若所有生命周期事件串行经过归档、数据库写入后才发布到Ingress，已接收的locked/halted/maintenance在慢写期间仍可能被采样成open。规定负向、未知及冲突事件先在有序入口保守失效，正向open/恢复必须等证据持久化后发布。证据未落盘时使用NULL和未知质量，不能提交引用不存在的证据。原始归档及持久化队列有界，失败可观测。 |
| R6 / P2 | 第5节增量订阅代次 | Deribit book消息没有本地subscribe request ID或订阅代次，仅给本地消息套“当前代次”无法隔离同连接同symbol的旧消息。明确单reader接纳序号、unsubscribe ACK栅栏、停止旧路由和新代次完整snapshot要求；若不能证明旧消息不越过栅栏，或经济定义改变，则只将受影响channel移到新物理epoch的恢复连接。不能将任意同symbol snapshot认作新经济身份。增加迟到旧snapshot、ACK不完整及定义变更测试。 |

## 核对依据

读取了根目录`AGENTS.md`、`internal/options/live.go`和`selection.go`、`internal/optionslive/runner.go`、`engine.go`和`ingress.go`、Deribit metadata/stream适配器、ClickHouse run/spec/metadata/minute写入及查询实现、derivative定义注册和分钟采样器。

现有经济定义的`Version()`包含native ID、creation时间和definition hash，支持新经济版本注册；无需覆盖旧规格。旧LiveRun为JSON hash，保持字段和编码不变即可保留旧摘要，catalog_v2仍须单独准入及验证入口，不能把旧配对检查整体放宽。

协议核对仅使用官方文档：[creation](https://docs.deribit.com/subscriptions/market-data/instrumentcreationkindcurrency)说明创建通知一次发送完整定义；[get_instrument](https://docs.deribit.com/api-reference/market-data/public-get_instrument)区分书可见与open；[platform_state](https://docs.deribit.com/subscriptions/platform/platform_state)给锁定和维护事件；[public/status](https://docs.deribit.com/api-reference/supporting/public-status)与[官方OpenAPI](https://docs.deribit.com/specifications/deribit_openapi.json)给锁定基线，但当前文档示例与字段类型不一致，解析须按实测严格处理；[book](https://docs.deribit.com/subscriptions/orderbook/bookinstrument_nameinterval)定义完整snapshot和change_id连续性；[unsubscribe](https://docs.deribit.com/api-reference/subscription-management/public-unsubscribe)说明ACK只列出本次成功取消的频道。

## 复审

由同一审核 Agent 完成第二轮复审，没有另派Agent。复审提交版本 SHA-256：`38fd32c6e7560346b364bdc9d96d89948c6e07526a171427c6f860813401f37e`（此时头部仍标“设计待审核”；改为审核通过不改变已审方案语义）。

定稿SHA-256：`79f75dad0eee91874f73b338d7097ef29cf9eb668c7ece4641cf6bff29ee53d2`。作者已核验将头部状态还原为“设计待审核”后，字节摘要精确等于上述复审版本；定稿未改动已审方案内容。

| 第一轮问题 | 修订及复审结果 |
| --- | --- |
| R1 | 第6节明确T边界立即移交采样，旧writer仅可提交T之前的冻结分钟，晚写或失败不阻塞新锚点。已闭合。 |
| R2 | 第8节明确R5不沿用持有到网络重试结束的全局`derivativeMu`，改为按run/minute冲突锁及公平可取消的有界数据库并发额度，控制事实单独保留机会；区分共同数据库故障。已闭合。 |
| R3 | 第6、7节增加全局前驱plan ID/hash、严格递增生效分钟、不可撤回或回溯覆盖、跨session续接和本机进程独占锁。读写双方按该链唯一解释分钟owner。已闭合。 |
| R4 | 第4、7节明确同epoch订阅后调用`public/status`、字符串锁定模式及partial严格列表、在途通知冲突重查；maintenance独立三态，初始unknown不会伪造成false，也不会永久挡住已确认未锁定的连续open盘口。已闭合。 |
| R5 | 第3节增加负向接纳先失效、正向落盘后发布、NULL证据与有界后台队列；第9节增加故意阻塞持久化及队列溢出验证。已闭合。 |
| R6 | 第5节增加单reader帧序号、取消ACK栅栏和旧代次Ingress排空、新ACK/full snapshot条件；不能证明隔离或定义变更时仅迁移受影响channel至新物理epoch。第9节增加迟到旧channel消息验证。已闭合。 |

同时核对了第5节公开目录初筛证据和存储字典第10.4节。3106个初筛成员与其“尚需严格规范化”的限制一致；共享物理连接、控制请求预算和全链容量测量均明确列为实现要求。存储字典明确标为拟实现，未把新表描述为已建或线上事实。

**最终结论：设计审核通过；没有遗留P1/P2阻断。** 实施必须按第9节完成真实动态订阅与栅栏验证、慢分片写入隔离、同分钟和跨session所有权回归、旧R4及10/50档混合回放，以及完整四族加20%余量的容量测量。第一轮问题已在设计上解决，本次没有执行这些尚未实现的运行测试，也没有修改生产代码、数据库或服务；审核通过不能被解释为线上bug已经修复。

## R5 实现代码审核

同一独立审核 Agent 继续审查实现，没有另派 Agent。初轮实现复审时间为 `2026-10-03 18:27 UTC`，最终实现复审为 `2026-10-03 19:15 UTC`。范围限于期权目录、生命周期适配器、共享订阅、运行器、分钟计划及证据存储、查询和相关 CLI；工作区其他采集项目的修改不属于本次审核。

**最终代码审核结论：通过，没有遗留 P1/P2 阻断。** 初轮真实公开探测发现全量分钟有效盘口为零，曾暂缓通过；修复控制证据排队、订阅 ACK 计时及心跳调度后，最终真实公开探测验证四族共 3106 个成员在同一完整分钟的全部 60 秒可回放。实现尚未部署到常驻服务。

代码初审及多次复审发现的问题已经修正，主要核验结果如下：

| 审核问题 | 已核验的修订 |
| --- | --- |
| 旧平台或合约事实越过负向失效屏障 | 源接纳阶段建立 epoch、platform generation 和逐 symbol 序号屏障；所有全局及单 worker 的 gate/state 发布统一检查，prewarm 和 planned 不再绕过。gate map 在发布前复制。 |
| 旧 maintenance 重试、不可解析平台通知造成错误 open | 维护接纳和持久化序号独立跟踪；未知平台通知单调推进待确认序号，公开锁定基线不能解封维护不确定状态。旧合法维护在新坏帧之后落盘也不能消除新失效。 |
| 缓存目录解除状态冲突或未知状态 | symbol 冲突与 scope 协议失效分开保存；fresh get_instrument/完整 creation 受相同源序号屏障约束，cached bulk 不能解封未确认状态。被后续事件抢先的确认会再次获取来源。 |
| 旧目录重试退回新定义或规则 | 正向发布拒绝早于当前规则观测的结果；bulk 的版本变化要求单合约确认；creation/单合约确认后的一分钟缓存窗口得到保护。错 symbol 的单合约响应记为 parse_error，保留原始 hash，但不携带准入事实。 |
| 分片失败等待旧 writer 排空才替换 | failed 在收到错误时立即排除分配；后台 writer 仅排空已冻结分钟，done 后回收 fanout 和路由。T 边界采样所有权独立切换。 |
| 计划提交成功但回复和读取均失败后永久分叉 | 未解决的候选保留完整 plan ID/hash/groups，以相同身份解决歧义；在明确不存在前不生成新身份。迟到发布不回填过去的有效秒。 |
| 容量拒绝永久占住 inflight、重试或旧成员无限增长 | 队列与重试共享字节预算；重试身份稳定、数量有界；拒绝时释放匹配 inflight，并保留重新确认机会。scope 恢复使用稳定 generation、跳过已确认成员；到期及部分成员退役清理 byID。 |
| 部分物理连接恢复仍逐合约退订 | 按物理连接聚合 unsubscribe；整条连接失效只开启一次新 epoch；同 channel 不在旧 epoch 重用。 |
| 分钟末全量回放校验挤占下个采样边界 | R5 采样器冻结分钟后交给有界 writer，移除采样热路径的重复全分钟验证；写入器保留完整 envelope、run/plan 所有权、回放及来源证据校验。旧 R4 验证路径保留。 |

独立执行的本地验证（末轮四包 race 与 CLI 编译于上述最终复审再次通过）：

- `go test -race ./internal/options ./internal/exchange/deribit ./internal/optionslive ./internal/storage/clickhouse` 全部通过。容量测试显式使用 `!race`，避免将 race 仪器开销当作部署吞吐。
- 正常测试覆盖上述四包，并编译 `cmd/options-check` 与 `cmd/collector`，全部通过。
- `go test ./internal/orderbook ./internal/sampler ./internal/replay` 通过，核验盘口断档、差量、质量位图及历史回放基础路径。
- 3728 盘口、117 分片的非 race 容量测试通过；本次独立实测最慢同步一秒为 `69.949866ms`，一分钟模拟总耗时约 `1.290s`，测试时 heap 为 `130,207,232` 字节。场景每本书 20 档/侧、20% 流每秒更新；该数字不代表最大 L2 深度或生产网络下的整机容量。
- `/tmp` Go overlay 独立构造的四条故障回归全部通过：旧维护重试、提交成功后回复及第一次查询丢失、未知平台帧被锁定基线解封、旧维护在后续坏平台帧之后持久化。关键交错已纳入正式测试。

真实 ClickHouse 的 `TestCatalogClickHouseCommitRetryOwnershipAndReplay` 由实现作者在孤立数据库执行并报告通过；该项不冒充本审核 Agent 的本地数据库实测。本审核使用的存储包普通/race 测试仍按环境门控跳过真实数据库集成。完整四族真实公开订阅及存储压缩/查询测量由作者执行并归档，审核 Agent 独立读取日志、核对测试统计和原始证据；这些结果与本地独立运行的测试分别列明。

审核 Agent 没有修改实现、生产数据库或服务，仅写本记录和 `/tmp` 临时测试。初轮核心文件摘要清单 `/tmp/codex-options-review-core-manifest.json` 的 SHA-256 为 `ac35c7a5733f4d8845be6c076889d2dcfdf6b0cff319128d9d65d5e28345755a`，已被后续修订替代。最终 29 个核心文件摘要清单位于 `/tmp/codex-options-review-final-core-manifest.json`，清单本体 SHA-256 为 `59c5f1d48dacba4f2261ef0c7daa9f6316505dc23a3c0242e0a8bbf293414455`；不将同文件内其他项目的修改纳入期权审核结论。

### 真实公开覆盖复核

作者随后归档的 `research/2026-10-04-options-lifecycle-implementation/public-probe.log` 显示 `max_plan_books=3106`、完整分钟 `minute_books=3106`，但 `minute_valid` 为空。原公开探测断言只检查成员及提交数，不能证明有效盘口覆盖，因此撤回实现的通过标记。随后按质量原因及公开协议原文定位并复审控制调度问题。此前设计审核结论不变；以下末轮结果闭合这一实现阻断。


### 真实运行修订与最终复审

| 真实探测及末轮问题 | 修订和复审结果 |
| --- | --- |
| 初始 `platform_state` 对约 155 个公开指数各推送一次锁状态；每条触发 status 请求使待 ACK 预算耗尽，控制证据又与六个慢 REST 目录共用队列 | 非采集四族的合法纯锁消息仍归档，但不推进采集代次；status 单飞保持到证据持久化，旧代次 ACK 重查最新基线。独立有界 lifecycle 持久化队列与 REST 队列隔离，共用字节上限。正式突发、旧 ACK、坏 status 和丢弃后重试回归通过。 |
| 小订阅批次排队却按已发送计算 ACK 超时 | 每物理连接在初始 ready 后、已有订阅 ACK 完成后才发送新的合并批次；排队 90 秒与实际发送后的 ACK 15 秒分别计时；ACK 早于 sent 通知也回收 queued。 |
| socket writer 等待订阅 gate，heartbeat test 被阻塞导致交易所 `heartbeat close` | 有界 preparer 等待 SubscriptionGate，单 socket writer 优先处理 heartbeat，只受 ControlGate 约束；取消等待 reader/writer/preparer 收束。正式模拟故意占用 5 秒订阅额度，heartbeat 仍先响应。最新四包 race 通过；两次后续真实全量探测无此错误。 |
| 单合约请求失败误使同 scope 健康成员一起未知 | get_instrument 的源失败和容量拒绝仅标该 symbol；新请求退避为 5、15、60 秒，之后维持 60 秒；DB 歧义重试继续使用原身份。完整目录失败仍按 scope 保守失效。 |
| P2：完整目录失效后，另一个单合约先落盘即可清整个 scope，旧成员随后被重新发布为已知 | 独立 overlay 反例先 FAIL：`TestIndependentSingleSuccessCannotReleaseUnconfirmedScopeMember`。修订后仅完整 catalog 可清非协议 scope 失效，单合约仅确认自己的代次；同一反例 PASS，正式 `TestCatalogSingleSuccessCannotReleaseFailedDirectoryScope` 及最新 race 通过。已闭合。 |

核对 [最终公开探测日志](../research/2026-10-04-options-lifecycle-implementation/public-final.log)：在 `2026-10-03 18:59 UTC` 同一完整分钟，BTC 为 1015、ETH 为 849、BTC_USDC 为 671、ETH_USDC 为 571，总 3106。每一族总数、第 59 秒可回放数和全部 60 秒可回放数完全相等，`valid_seconds_histogram` 仅有 `60:3106`。探测历时约 215.62 秒，无重连或 heartbeat 错误；内存 sink 对每份 CatalogEnvelope 执行严格校验。这是作者执行的真实公开来源探测，未写生产数据库，不等于长期生产运行证明。公开测试的断言现检查完整计划及每族第 59 秒至少 95% 有效，日志额外报告整分钟 60 秒统计，不再只检查提交数量。

另独立解压作者保留的 `/tmp/options-lifecycle-public-heartbeat` 162 份公开响应，所有文件名 SHA-256 均与原始字节一致：155 条 platform_state，其中采集四指数锁通知为 4 条，1 条 public/status 的 locked 为字符串 `"false"`；这一连接没有 maintenance 通知，不将其推导为已确认 maintenance=false。

核对作者归档的真实 ClickHouse [回归日志](../research/2026-10-04-options-lifecycle-implementation/clickhouse-regression.log)、[R5 窗口测量](../research/2026-10-04-options-lifecycle-implementation/catalog-measurement.log)和[整日基础测量](../research/2026-10-04-options-lifecycle-implementation/day-measurement.log)：

- R5 身份稳定重试、计划所有权、证据读取与回放，旧 R4 提交与引用，旧 50 档/新 10 档混合历史均通过。
- R5 合成 12 分钟、3 盘口样本：活跃/静默分别为 41,873/30,467 压缩字节；完整读取、证据验证及回放 P95 为 63.94/65.10ms。每日单盘口外推为 1,674,920/1,218,680 字节，明确仅是窗口外推，排除静态目录/定义，不能作为全量历史或磁盘容量证明。
- 1440 分钟单流基础合成测试的活跃/静默压缩总数分别为 2,282,942/1,120,977 字节；100 个分散分钟的完整读取及回放 P95 为 24.22/22.73ms。此项不包含 R5 额外生命周期证据和指数副本。

最后再次独立执行四核心包 race、两 CLI 编译，以及 orderbook/sampler/replay 基础回归，全部通过。Prepare 仅接受 explicit 固定清单的保护条件已核对；旧真实公开测试改为选择当前完整 C/P/同到期期货组合，R5 自动目录另用全量探测验证。没有修改实现或生产状态。尚未部署、短窗口探测与合成容量的限制保留，不用这些结果证明未来挂牌时间、源快照延迟或长期无缺口运行。


末轮还核对将 `Client.FrameBudget` 在任何 Session 创建前指向 supervisor 的同一 budget，使 WS 读取与所有 book ingress 共同受 `OPTIONS_MAX_INGRESS_BYTES` 约束；不是运行中并发重配。独立 optionslive race 再次通过。作者对该修订另跑[共享预算最终公开探测](../research/2026-10-04-options-lifecycle-implementation/public-final-budget.log)：`2026-10-03 19:13 UTC` 四族仍为 BTC 1015、ETH 849、BTC_USDC 671、ETH_USDC 571，全部 3106 书均 60 秒可回放；197.34 秒过程无重连/心跳告警。最终核心摘要包括这一修订。

作者将最终公开响应保存到项目 [public-evidence hash 清单](../research/2026-10-04-options-lifecycle-implementation/public-evidence/manifest.json)。审核 Agent 独立按清单逐份解压 162 份响应，重算 SHA-256 及 raw_bytes 均一致：155 份平台、6 份目录、1 份字符串 locked=false 基线，避免仅依赖临时目录。

## 2026-10-04 部署前真实全量数据库验收修复

用户授权部署后，在现有ClickHouse实例的独立验收库发现逐合约定义/规则读写导致BTC/ETH option完整目录超过30秒处理期限。未在生产运行该候选。改为完整批次校验、批量读取既有定义与规则、批量插入新定义及规则；分钟引用查询只读取本批instrument，保持每秒owner、known_from、effective_from校验。FIRST定义证据同时覆盖同批重复和已存重复；规则重试沿用捕获时间及内容ID，不刷新来源身份。

复用同一独立Agent `options_design_review` 复审。结论无P1/P2；独立optionslive/clickhouse race和新增两项真实ClickHouse回归通过。1100定义、刷新、1100规则及重复写入实测约326ms；主Agent同项实测219ms。回归还覆盖非法后项/缺定义整批拒绝、缺规则、跨instrument规则、早于known_from和缺spec引用拒绝，以及原R4/提交重试/全秒回放兼容。五个审核文件与隔离构建源SHA一致，见[复审摘要清单](../research/2026-10-04-options-lifecycle-deployment/reviewer-batch-manifest.json)。此小批回归不代替真实全量持续写入验收；部署结果另存运行记录。
