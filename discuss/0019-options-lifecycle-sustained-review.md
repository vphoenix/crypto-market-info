# R5 期权生命周期持续运行故障独立审核

初轮诊断时间：2026-10-04 15:07 UTC；最终代码复审：2026-10-04 15:58 UTC；生产持续验收独立复核完成：2026-10-04 16:44 UTC。沿用用户要求的唯一独立审核 Agent `options_design_review`，没有增加其他 Agent。审核只读实现、构造 `/tmp` 反例及写本记录；真实数据库测试仅创建并清理孤立测试库，没有修改实现、生产表、服务或其他分支。

**最终结论：代码审核通过；40分钟真实生产持续验收已完成并独立复核，没有遗留 P1/P2 阻断，关闭本轮“持续验收待完成”项目。** 本记录保留旧部署故障、反例及修订验证。37个已生效分钟均提交全部计划成员与分片；30分钟目录刷新后仍全量运行，最后一分钟3094本全部60秒严格读取/回放通过。窗口有效秒覆盖率99.64181%，存在并保留真实重连、采样迟滞及缺第0秒起点的缺口；瞬断根因未确定，下一次真实到期和多日运行尚未验证。

## 审核范围与先前验证的边界

范围为 `catalog_runtime.go` 的源序号屏障、生命周期持久化、状态发布及恢复；`catalog_engine.go` 与 `options_catalog_minute.go` 的证据新鲜度；`catalog_hub.go` 的成员退役和恢复控制容量。其他采集来源的生产问题和工作区其他任务修改不纳入本审核。

此前 [0018 审核记录](0018-options-lifecycle-design-review.md) 中的四包 race、合成容量、短期真实公开全量有效分钟和批量定义/规则数据库回归是真实结果，但没有覆盖“到期状态突发、生命周期写入变慢或拒绝、同秒创建和开市交错、30 分钟目录刷新、35 分钟状态证据过期、整分片替换控制请求”这一持续运行组合场景。公开测试约 3–4 分钟，内存 sink 的 envelope 校验也没有真实 writer 对目录 StateID 的 35 分钟引用校验，不能替代该场景。

部署独立源码 `/tmp/options-r5-deploy-source-20261004` 与本轮所读工作区四份关键实现逐字节相同：

| 文件 | SHA-256 |
| --- | --- |
| `internal/optionslive/catalog_runtime.go` | `4721bc031d92ba376ff00a2b9dd4e417e9523cb8d4ea26e488eea700acfcde83` |
| `internal/optionslive/catalog_engine.go` | `f07c749de7cf8f1113b4196e39a550f4d55b1974eeaadd850dd2224d61de4752` |
| `internal/optionslive/catalog_hub.go` | `39cce5e61339e29a4a9af1efbe257b998dc4d353e301ead1aa94045cc5b2ffb5` |
| `internal/storage/clickhouse/options_catalog_minute.go` | `3fd361d170e86b5780c2ec8eee9a64d4ff1e7d54f589498571ac4f7c4b3a0c26` |

行号均针对以上部署版本；作者修订后会变化。

## 生产证据

基础记录见 [生产状态报告](../research/2026-10-04-postdeploy-status-2224/report.md)、[错误时间线](../research/2026-10-04-postdeploy-status-2224/options-error-timeline.json)、[逐 symbol 状态分布](../research/2026-10-04-postdeploy-status-2224/options-state-stream-distribution.jsonl)和[公开目录对照](../research/2026-10-04-postdeploy-status-2224/options-public-comparison.json)。

- 北京时间 15:46–15:59 完整有效；16:00 到期与状态通知期间出现 13,397 次 `options retry capacity exceeded`，16:01 有效秒归零。
- 最近完整小时实际有效成员秒 37,468 / 10,717,200，约 0.35%。22:40 已提交的 2,848 成员中，2,832 的 `market_known_bitmap=0`，另 16 个全部已知。说明长期退化主要是逐 symbol 屏障没有恢复，不能仅解释为全局 platform gate 持续未知。
- 16:39 起出现 `wrong catalog state evidence`。22:23 计划 2,977 成员、94 个 run，实际 2,848 成员；缺 129 成员、5 个 run。
- 公开规范化目录 3,090 个 open 成员，现有计划 2,977 个；113 个公开已 open 合约仍 `waiting_open`。现有计划成员均仍在公开目录，不是上游已经退市。
- 新 `ETH_USDC-6OCT26-2850-C/P` 于 20:41 出现；20:56/57 有整分钟 60 秒有效，21:14/15 后不再提交，与 StateKind=2 首次确认后约 35 分钟相符。

另独立用 ClickHouse 客户端 `--readonly=2` 查询以下公开源生命周期证据。两个新 ETH_USDC 合约的时间均为 UTC：

| symbol 后缀 | state | source_time | received_at | ingress_sequence |
| --- | --- | --- | --- | --- |
| `2850-C` | inactive | 12:41:00.000000 | 12:41:00.396810 | 18842 |
| `2850-P` | inactive | 12:41:00.000000 | 12:41:00.397989 | 18843 |
| `2850-C` | open | 12:41:00.155000 | 12:41:00.405301 | 18845 |
| `2850-P` | open | 12:41:00.155000 | 12:41:00.406859 | 18846 |

`BTC-8OCT26-75000-C` 当前 `waiting_open`，其 creation 在 08:00:13.134845 为 inactive；08:00:31.793224 的完整目录已经 open，之后每 30 分钟直到 14:30:59.501560 都有持久化的 open 完整目录。该 symbol 的生命周期表没有任何行，也没有单合约确认观测。不能由缺失历史行反推出某一帧必然到达，但结合 reader 先建立 pending、容量拒绝后不恢复的代码，可确定完整目录已知 open 仍无法推进状态的实际症状。

## 阻断问题及修复要求

### P1：容量拒绝丢弃生命周期工作，逐 symbol 屏障永久未完成

`catalog_runtime.go:286` 在源 reader 接纳状态时先更新 `pendingStates` 并立即失效；持久化队列仅 256，retry 仅 64，且生命周期仍逐帧数据库查询/插入。`scheduleRetry:1390` 达上限时丢掉 job，`dropRetry:1543` 只全局失效，没有保存这个 symbol 的恢复意图。`tick:970` 仅重查 scope 及已有 retry，不主动确认 pending 未 applied 的合约。bulk 不能覆盖当前 epoch 的 pending，因而正向失效状态可能一直留在屏障之后。

修复应批量持久化生命周期，按公开全量事件规模确定队列容量，并继续受共享字节预算约束。拒绝原始工作或资源不足时，须保留有界的逐 symbol 恢复意图；最新负向接纳仍立即失效，只有相同 epoch、匹配屏障的持久化事实或 fresh get_instrument 可解除。不能以提高队列上限作为唯一恢复机制。平台与合约恢复意图分开，单 symbol 资源拒绝不能反复失效全部健康成员。请求调度应公平且受退避限制，未 admitted 的新 open 候选也必须能被调度。

批量写入需保留每条 observation ID、raw hash、原始 sequence 和 epoch；同一批存在相同 ID 不同内容时整批拒绝；DB 结果歧义重试同一身份；查询失败不得当成不存在或已持久化。只发布已确认写入成功的项目，不伪造正向证据。

### P1：规则刷新伪装成状态刷新，35 分钟后整分片提交被拒绝

`catalog_runtime.go:865` 更新 RuleObservationID/ObservedAt；`:875` 在已有当前 epoch pending 时阻止 StateKind=2 的 StateID 刷新，即使这个 pending 已 applied。`catalog_engine.go:364` 只用新的 ObservedAt 检查 35 分钟，仍发出 `MarketKnown=true`。真实 writer 在 `options_catalog_minute.go:257` 按旧 StateID 的目录 observed_at 检查，严格拒绝整个分钟批次。

必须保存规则与状态各自的真实观测时间；采样器和 writer 用一致的来源时间、新鲜度和发布前提。已 applied 且没有较新 reader 屏障时，足够新的完整目录可刷新 StateKind=2 的 StateID；存在未完成事件或协议不确定时仍需 fresh 单合约确认。StateKind=1 的合法 WS 状态不能被缓存 bulk 倒退覆盖。不能取消 writer 的 35 分钟校验、用本地发布时间刷新旧事实或继续以新规则时间代替旧状态时间。

### P1：creation 借用后续序号，源时间与本地时间混用，使 open 回退或被忽略

`catalog_runtime.go:529` 在 supervisor 处理 creation 时读取 reader 已推进的 pending.sequence；`:875` 因而可能把旧 inactive creation 当成后续 open 的确认，替换已持久化生命周期状态。另 `:878` 把 REST/creation 的本地 ObservedAt 赋给 lastEvent，`:763` 却拿 WS SourceTime 与之比较；同秒的真实 open 比本地接收时间早，被消耗 appliedSequence 后无声忽略。

creation 只携带自己的源接纳序号及定义/规则证据，不能确认后来接纳的 state；当前 epoch 已有 state 或未完成 pending 时，不允许旧 creation 改市场状态。WS SourceTime 只与同一时钟域的 WS 源事件比较；REST 本地观测使用请求开始前捕获的 epoch/sequence 屏障判断是否足够新。同源时间冲突应失效并 fresh 确认，不能用本地毫秒差强行确定 market state。

### P2：到期成员缺失仍反复产生确认工作，放大控制队列压力

`catalog_runtime.go:923` 对完整目录缺失的每个旧成员继续请求 get_instrument，包括 expiry 已过及 terminal 成员。tick 只删除 terminal 且到期超过一小时的 entry；停在 locked/settlement 的过期项可能长期留存。新触发的 requestInstrument 也可能绕过已有 retry 的退避节奏。

到期或明确 terminal 后停止生成健康恢复请求，清理其待恢复状态及调度索引，保留已写历史和分钟所有权事实。所有新请求入口都应受同一逐 symbol 退避与 inflight 合并约束。到期清理不能误删同 native symbol 新定义版本的成员或撤销仍在排空的旧分钟。

### P2：成员移除和 owner 替换逐条退订，耗尽 Session 控制预算

`catalog_hub.go:99` 的 detach 每个成员立即排一个 unsubscribe；recoverPending 的合批未覆盖 remove/removeWorker/换 owner。Session 最多 64 pending、共享订阅 gate 约一秒一步。65 次移除即可填满 64 条命令并取消仍有健康成员的连接，与生产 `session request budget exceeded` 和 queued ACK timeout 的错误形态一致。这里独立证明存在控制放大路径，不声称每一次生产连接退休均由该路径造成。

把退役、owner 替换及部分恢复统一收敛为每个 physical 的有界待退订集合，由 maintain 合批；新订阅须保留 ACK/epoch 栅栏。重复或过时命令应合并；全部路由受影响时一次新 physical epoch。资源不足须保留待恢复路由，不能让健康路由随一批移除被反复重连。还应验证最坏部分迁移下 MaxConnections/ChannelsPerConnection 的容量与公平恢复，保持旧 epoch 同 channel 不复用约束。

## 独立反例与修订验收

在部署独立源码上执行 `/tmp/options-review-postdeploy-overlay/overlay-deployed.json`，没有修改项目测试文件：

```sh
GOCACHE=/tmp/options-review-go-cache go test \
  -overlay=/tmp/options-review-postdeploy-overlay/overlay-deployed.json \
  ./internal/optionslive -run '^TestReviewPostdeploy' -count=1 -v
```

六条反例全部 RED，结果与生产症状一致：

| 测试 | 实际失败 |
| --- | --- |
| `SourceClockCannotEatLaterOpen` | later-received open 被消耗，state 仍 inactive，无 fresh 确认 |
| `AppliedBarrierRefreshesStateEvidence` | 新 RuleObservationID 更新 ObservedAt，旧 StateID 仍 Known |
| `CreationCannotBorrowFutureStateBarrier` | inactive creation 替换已持久化 open，StateKind 变 2 |
| `DroppedLifecycleGetsConfirmation` | retry 满后丢弃状态工作，未 applied 屏障没有确认意图 |
| `ExpiredMissingMemberNoNewRequests` | 已到期且目录缺失仍生成 get_instrument |
| `RemovalBurstCoalescesControls` | 65 次移除生成 64 控制请求，并取消仍有 2 个路由的连接 |

修订后的最低验收条件：

1. 将上述六种交错纳入正式测试，并验证旧负向状态、同 timestamp 冲突、新 epoch、过期版本和旧 retry 不会恢复错误 open。
2. 模拟全量数千成员状态突发与新增成员、目录刷新、慢数据库和容量拒绝；在源恢复、数据库恢复后所有未到期的合法 open 成员可自行重新确认、入计划及取得新快照，不需要重启，健康成员保持隔离。
3. 真实孤立 ClickHouse 验证生命周期批量身份冲突、同身份重试、丢失 ACK、读取失败、缺引用、每秒 StateID/RuleObservationID 时间及整分片提交；模拟时间跨过 30/35 分钟和多次刷新，不能只检查批次数。
4. 订阅模拟在64 pending、全局 gate、部分连接容量和持续 owner 替换下，检查合批、heartbeat 优先、旧 epoch 拒绝、最终恢复和资源界限；数据源断档仍必须重新快照。
5. 回归 race、旧 50 档/新 10 档混合回放及变更相关集成。真实公开持续验收应跨越目录刷新及35分钟阈值，报告每族预期成员、实际提交成员、全部60秒有效成员、valid_seconds分布、pending原因及重试/重连错误。包括真实数据库路径；短内存 sink 探测只能作为其中一项。

以上为初轮修复要求。最终实现及验收进度见下一节；审核 Agent 不执行部署，部署授权来自用户和主任务，审核结论本身不替代授权。

## 修订闭合与最终代码复审

最终范围为主 Agent 指定的 13 个文件：`catalog_runtime.go`、`catalog_engine.go`、`catalog_hub.go`、`catalog_lifecycle_batch.go`、三个 sustained/runtime/engine 测试组及 capacity 测试、`options/catalog.go`、ClickHouse 的 `options_catalog.go`、`options_catalog_minute.go` 与 `options_lifecycle_batch_integration_test.go`。内部 clock hook 仅用于虚拟时间测试；生产默认仍为 `time.Now`，没有新增配置或外部重配路径。

| 原始问题 | 已核验的修订 |
| --- | --- |
| 生命周期突发工作掉队后永久 pending | lifecycle 专用批量持久化循环，最多128项一批、5ms收集窗口；队列及retry数量按 `2*MaxBooks+512` 有界，仍受共享32MiB工作预算约束。reader pending key同样有上限，超过上限保守失效并重连。容量拒绝只标有关symbol，tick主动确认当前pending未完成、未知、旧epoch及未admitted成员。 |
| 无entry的新合约恢复意图无人处理 | tick同时处理当前epoch尚无entry的pending map，以get_instrument建立定义和状态，不必等30分钟scope刷新。新请求合并inflight，受到退避约束。 |
| 新规则观测遮住过期StateID | 内存状态增加实际 `StateObservedAt`，StateKind=2 的采样器新鲜度与writer共用35分钟常量。已确认当前屏障后完整目录可以更新StateID；未完成屏障、scope generation或symbol不确定仍阻止正向发布。 |
| creation覆盖新state、源时间与本地时间混用 | creation不再捕获后续pending序号，也不覆盖当前epoch的WS state。lastEvent只保存WS源时间；single确认在HTTP请求真正开始前捕获屏障，既有result的DB重试不换身份。后落盘的同屏障WS副本不能撤销已经更新的HTTP确认。 |
| 到期成员继续占用恢复请求 | get_instrument、完整目录缺失处理及retry排队检查到期/terminal；到期清理不再依赖必须先见delivered，停止健康恢复请求。已冻结历史分钟仍按既有writer所有权排空。 |
| 未知terminal意图永久重查 | 已持久化terminal且无entry时，重新核对最新epoch/sequence后清pending/uncertain/orphan；旧terminal不能清后来的open屏障。 |
| remove/removeWorker/换owner逐条退订 | 全部路径聚合到每physical待退订集合，最多512 channel一批；subscribe/unsubscribe共用一个待ACK批次，新意图等待ACK而不继续堆命令。controlPending扫描所有requested未acked，包括已经detach的路由。 |
| 历史channel使用记录使预算满时永久nil | 不复用旧epoch channel；历史used阻止分配且连接预算已满时，每秒至多轮换一个稀疏physical，立即失效相关书并保留路由，待旧goroutine释放slot后建立新epoch。5秒恢复冷却期间也保留恢复意图。 |

本轮完整复审还发现两项新epoch风险，已修正并纳入正式测试及独立反例：

- 旧epoch的StateKind=1错误阻止新epoch完整目录重新确认，tick也没有确认已admitted的unknown成员。修订仅保护当前epoch的WS state，tick包含Known=false和state.Epoch不匹配。
- 新epoch序号从头计数，旧appliedSequence恰好等于新pending时错误清障。connected重置appliedSequence和WS lastEvent，confirmedBarrier的相等判断另外要求状态所属epoch相同。reader提前推进新pending后才处理connected的交错仍保持失效。

### 独立执行与证据核对

下列结果由审核 Agent 独立执行，不冒充作者日志：

- 最终四核心包 `go test -race` 全部通过。首轮Deribit模拟端口因沙箱禁止loopback监听未运行；为仅运行本地模拟测试提升权限后通过，最终变更再次通过。
- `orderbook`、`sampler`、`replay` 普通回归通过，`cmd/options-check` 与 `cmd/collector` 编译通过，包含旧50档/新10档及差量、删除、无效区间基础路径。
- `/tmp/options-review-sustained-final-overlay/overlay.json` 四条独立race反例通过：新epoch建立新basis、同序号不借旧applied、detach未ACK继续阻挡新控制、旧terminal不清新意图而当前terminal清理。
- `TestCatalogWholeUniverseLifecycleBurstIsBatched` 独立通过：7,456事件、59批次。该项使用内存batch sink及共享fixture raw，不代表7,456份不同公开响应的磁盘fsync吞吐。
- 非race全量加20%采样测试独立通过：3,728书、117分片，模拟一分钟约1.746s，最慢同步一秒82.44ms，heap121,109,536字节；保留既有合成L2/更新率限制，不当作整机最大网络负载证明。
- 真实孤立ClickHouse `TestCatalogSustainedRefreshAcross96MinutesClickHouse` 独立通过，约24.89s：实际supervisor发布、engine、数据库证据校验、分钟commit及读取/回放链路，用虚拟时间跨96分钟、4次目录观测更新，5,760秒全部有效。此项为单流合成数据，网络、心跳、REST请求及真实计时调度不是96分钟真实运行。
- 真实孤立ClickHouse `TestCatalogLifecycleBatchIdentityAndAmbiguousRetry` 独立通过，约2.14s：4,096条写入后丢失回复、同身份重试、同批相同ID去重、异内容冲突拒绝，冲突批次新行数为0。
- 真实孤立ClickHouse原 `TestCatalogClickHouseCommitRetryOwnershipAndReplay` 和 `TestDerivativeClickHouseRoundTripFailuresAndVisibility` 独立通过，覆盖原目录所有权、稳定重试、旧R4及存储引用/回放兼容。

作者另在隔离最终构建源重跑race、vet及两项新数据库回归，归档于 [修复证据目录](../research/2026-10-04-options-sustained-repair/)。审核 Agent 独立核对 [构建源码清单](../research/2026-10-04-options-sustained-repair/build-source-manifest.json)：全部13文件与工作区及 `/tmp/options-r5-sustained-deploy-source-20261004` 逐字节相同。候选二进制 `/tmp/options-r5-sustained-collector-20261004` SHA-256 独立重算为 `41059f71a06f0064dbb05db7ecbea9380aec350fd2871c3da9e6c8bb42265c0c`，与作者清单一致。

审核 Agent 另保存13文件摘要到 `/tmp/codex-options-review-sustained-final-manifest.json`，清单本体SHA-256为 `17897d9ac8ed7d26f65c320d5173e8e512a2f84645a0370f7ff2c5816380984f`；完成测试后再次检查摘要未变化。本审核不包含同目录其他采集项目的dirty修改。

### 部署前真实公开候选窗口与当时验证边界

作者执行的隔离候选使用真实公开源并写孤立数据库，见 [最终探测](../research/2026-10-04-options-sustained-repair/accept-final-probe.json)与 [全秒读取回放结果](../research/2026-10-04-options-sustained-repair/accept-all-seconds.json)：2026-10-04 15:52、15:53、15:54 UTC每分钟均3,090成员、97 runs、所有成员60秒有效、pending=0；15:52分钟严格读取及全秒回放185,400秒全部通过，用时19.83s。审核 Agent 独立读这些结果及来源证据，未声称亲自重跑此全量网络候选。另逐份解压 `accept-evidence` 的162份原始响应，重算文件名SHA-256均匹配；候选日志没有期权WARN/ERROR，其他永续alias警告不属于本次范围。

这个窗口证明最终候选能真实全量接纳、持久化及回放，仍只有数分钟，不能证明持续刷新或下一次真实到期事件。**此为15:58 UTC阶段结论：代码审核通过，真实持续验收当时尚未完成；该项目已由文末40分钟生产结果关闭。** 当时要求后续报告每族计划/实际成员、完整60秒覆盖、pending原因、目录观测和StateID更新、重试/重连/分片错误，保留实际完整读取回放证明；发现退化时重新开放审核问题，不能以本次通过结论覆盖新事实。


## 部署后独立补充：心跳预算及真实缺口（2026-10-04 16:38 UTC）

主任务报告已由用户授权部署候选，生产PID为201072。本 Agent 仍只读生产记录，没有部署、重启、断开或向该进程注入故障。另启动的模拟15条WebSocket只连接本机；公开心跳探测是独立的单条无认证连接，没有订阅盘口或调用任何账户接口。

### 共享心跳 gate 的核实

最初待核假设使用了 `deribit.NewClient` 的默认一秒ControlGate。实际R5生产入口 `RunCatalog` 在 `internal/optionslive/catalog_runtime.go:166`、创建任何Session之前，把它改为 `100*time.Millisecond`。工作区该文件SHA仍为 `1f54b40fb48c927972e1d818c7c476666e7a2617ffcc09a1be21881bc84572e3`，与部署隔离源码一致。另独立核对Session SHA `d39bdc67d242165b9ab8910f4654f99094b9e66f18893253ed7180bc8de0b3b6`、gate SHA `f2dadf0af1f69e1eb2bc946dec7221f323ed1f58c721598ad73e3171bf905f7e`，均与隔离源码一致。

Session的priority绕过订阅gate，但仍经过共享ControlGate；已经选中的普通请求等待ControlGate时，priority不能抢占。当前100ms配置下，“15连接每10秒约1.5次回复超过全局1次/秒”的容量推断不成立。100ms是客户端调度容量，不代表官方承诺公开IP一定享有每秒10次额度。

官方 [public/set_heartbeat](https://docs.deribit.com/api-reference/session-management/public-set_heartbeat) 和 [连接管理说明](https://docs.deribit.com/articles/connection-management-best-practices) 要求interval至少10秒，区别无需回复的heartbeat与必须通过public/test回复的test_request；未回复会关闭连接。这两份文档没有给出严格的“每10秒恰好一次test_request”数量或可据以认定超时的固定回复宽限。不能把本地ACK15秒预算或本测试10秒观察线当作上游宽限。

审核 Agent 独立执行 `/tmp/options-review-heartbeat-overlay/overlay.json`（只追加到临时测试副本，不改项目文件）：

```sh
GOCACHE=/tmp/options-review-go-cache go test -race \
  -overlay=/tmp/options-review-heartbeat-overlay/overlay.json \
  ./internal/exchange/deribit -run '^TestReviewHeartbeatSharedGateBurst$' -count=1 -v
```

15条Session全部完成初始ACK后，同步收到test_request。当前100ms gate最慢public/test到达本地服务器为1.499561881秒，超过10秒的为0；假设1秒gate最慢为14.999976464秒，6条超过10秒。两组race通过，总计约65.92秒。结果归档 `/tmp/options-review-heartbeat-overlay/result.log`。这个反例证明一秒配置具有排队风险，同时验证当前配置的同步心跳突发；没有服务器限流、网络卡顿、暂停及同时大量普通控制，所以不声称它证明所有生产调度延迟的上限，也不将其当作此次瞬断归因。

独立公开探测 `/tmp/options-review-public-heartbeat.log`：2026-10-04 16:36:18 UTC set_heartbeat(interval=10)成功，随后收到4个test_request，接收间隔约11.14、10.88、10.89秒；前三个public/test回复均收到合法版本结果，RTT约0.38秒。46秒本地读取截止时主动退出，最后一次回复没有在截止前被读到，结束的i/o timeout来自本地探测期限。只有这条短连接的观察，不能推广为官方频率承诺或证明生产连接正常。

**本轮没有由心跳预算新增P1/P2；保留代码审核通过。** 不需要按一秒假设修改生产gate。若要追查现有瞬断，最小后续证据是请求method/ID、原始frame到达时间、priority入队时间、ControlGate等待、socket写完时间、ACK时间与本机暂停/GC时间；当前退休日志只有错误文本，ACK id127没有method，不能分辨网络响应缺失、源reader处理延迟或写入排队。此项为诊断建议，不是假定存在阻断后再要求修复。

### 生产窗口中的已知缺口及恢复

独立读取 [生产进度](../research/2026-10-04-options-sustained-repair/production-progress.json)、[连续观察](../research/2026-10-04-options-sustained-repair/production-monitor.jsonl) 与 [16:20采样迟滞统计](../research/2026-10-04-options-sustained-repair/production-sample-lag-1620.json)，截至16:37:52 UTC、启动后约38.02分钟：

- 初始6次physical退休；16:04/05计划成员依次为3092/3093，16:06后为3094，每分钟97 runs。16:04–16:19各当时计划成员均60秒有效，不能把3094倒填到更早计划。
- 16:20:31有1440书reason6 (`DerivativeSamplingLag`)，该分钟有效184200/185640成员秒；16:21、16:22恢复全3094书60秒。代码对采样完成迟于边界250ms的状态判无效，不能将其改为有效，也没有证据将此迟滞与三分钟后的心跳关闭认定为同一原因。
- 16:23新增ACK id127超时和heartbeat close，512书出现短暂缺口：2582书full60，有效185106/185640成员秒。16:24有256书缺第0秒起点，整分钟不能有效回放：2838书full60，有效170280/185640成员秒。16:25起重新全3094书full60。按AGENTS.md不补造缺失第0秒快照；下一分钟新的起点恢复是正确语义。
- 16:32全部3094书各少1秒，有效182546/185640成员秒，观察中没有新增连接退休。此项需最终逐秒validator按实际reason归类，当前不指定成心跳、目录刷新或其他根因。16:33–16:36均全3094书60秒。
- 16:30:08–18 UTC六scope新完整目录观测全部成功，数量合计3094；当前计划pending=0，没有逐symbol永久unknown、retry容量、wrong state evidence或writer错误。服务MainPID201072、NRestarts=0。连接退休总数8：ACK超时3、heartbeat close4、unexpected EOF1。

以上为16:38 UTC阶段的独立补充，聚合提交和valid_seconds本身不是全量严格加载/回放证明。随后的最终结果在下一节区分37分钟聚合覆盖与末分钟严格回放，补充刷新前后StateID/观测时间及各族公开对照。保留全部短缺口，不能将“可自行恢复、没有永久退化”表述成“100%连续有效”或“瞬断根因已解决”。


## 最终真实生产持续验收闭合（2026-10-04 16:44 UTC）

本 Agent 独立读取主任务生成的最终JSON、日志和检查脚本，重新计算总数、集合差及有效率，解压重算六份公开原文哈希，并使用ClickHouse `--readonly=2`查询已提交的历史分钟补证。**没有亲自重跑生产全量网络采集或整套全量回放；下述全量读取/回放运行结果来自主任务，明确区分独立核对与独立执行。** 生产进程和数据没有被本 Agent 修改。

### 完整提交、覆盖和恢复

[40分钟窗口结果](../research/2026-10-04-options-sustained-repair/production-window-validation.json)及 [检查日志](../research/2026-10-04-options-sustained-repair/production-window-validation.log) 截止2026-10-04 16:39:51 UTC，运行40.002351分钟。16:02–16:38连续37个已生效分钟，均按当时计划提交全部预期成员和run；缺分钟、缺成员、缺分片及整分钟市场全未知为0。最初计划996本，随后3092/3093，16:06起3094本；没有用最新成员数倒填早期窗口。

独立逐分钟求和为预期6742500成员秒、有效6718349、无效24151，有效率99.6418094%，与结果一致。32个分钟全部有效，另外5分钟的真实缺口如下：

| UTC分钟 | 无效成员秒 | 已观察情形 |
| --- | --- | --- |
| 16:03 | 3723 | 初始连接退休和重建窗口；1227书未满60秒 |
| 16:20 | 1440 | 第31秒1440书采样迟滞，reason6 |
| 16:23 | 534 | ACK超时/heartbeat关闭后512书短暂缺口 |
| 16:24 | 15360 | 256书缺第0秒起点，整分钟明确无效 |
| 16:32 | 3094 | 第13秒3094书采样迟滞，reason6 |

上述无效合计24151。16:32原因由本 Agent独立只读查询确认；结果与唯一合法WS状态补证共同保存 `/tmp/options-review-production-final-direct-read.log`。8次physical退休均能恢复，没有服务重启、retry容量、状态证据或分钟writer错误；16:33–16:38连续6分钟3094本全部60秒有效。记录保留这些缺口，没有重写valid_bitmap或补造缺失快照。ACK/heartbeat瞬断的底层原因仍未确定，本轮只证明失效后可恢复，没有将其标为已经解决的根因。

### 超过35分钟后严格读取、证据刷新及公开成员一致

[最终全秒回放](../research/2026-10-04-options-sustained-repair/production-final-all-seconds.json)针对16:38 UTC分钟：计划3094本、97 runs，实际严格加载3094本，missing books/runs均0，3094本各60秒回放全部通过，共185640有效秒；加载25.0955秒、含回放总计33.3933秒。**37分钟整窗口为提交/质量聚合检查，严格加载和逐秒回放为最终16:38分钟；不声称全部6742500历史成员秒都重新回放过。**

[初始证据](../research/2026-10-04-options-sustained-repair/production-evidence-initial.json)中单合约首次确认时间为16:03:02.455440，35分钟阈值是16:38:02.455440；16:38整分钟有效覆盖跨过该阈值。独立比较 [最终证据](../research/2026-10-04-options-sustained-repair/production-evidence-final.json)：六个16:30新完整目录ID支撑3093本StateKind=2及3094本规则，旧目录/creation/single证据ID交集为0，末秒真实观测年龄521–531秒。剩余一条StateKind=1由独立只读查询确认是 `BTC_USDC-8OCT26-88500-P` 的合法open生命周期：source_time16:03:00.194000、received_at16:03:01.170423，所属WS epoch `165c521f-b66b-4bdd-bc95-119e7c503b1f`、sequence210。其StateKind=1语义不冒充30分钟REST刷新，规则引用仍已更新。

按 [刷新原文核验清单](../research/2026-10-04-options-sustained-repair/production-refreshed-raw-verification.json) 独立解压生产evidence目录六份raw并重算SHA-256，全部匹配，实际解压字节合计4776063。由此核对真实来源事实与刷新后StateID/规则引用，不能只以新本地发布时间替代旧证据。

[最终公开目录对照](../research/2026-10-04-options-sustained-repair/production-final-public-comparison.json)经主任务严格规范化：公开3094、计划3094，public_not_planned、planned_not_public、definition_mismatches和pending均0；四族期权/交割合约数量相符：BTC/USD 996/13、ETH/USD 836/13、BTC/USDC 656/9、ETH/USDC 562/9。

### 最终结论、版本与验证边界

截至本次独立复核，**代码审核和要求的真实超过35分钟、跨30分钟目录刷新后的持续验收均通过，本轮无待完成部署验收项，无新增或遗留P1/P2。** 此结论对应37分钟计划完整提交、真实新成员持续采集、引用刷新和最终全量严格回放；不是保证100%有效秒。短暂连接关闭及两次采样迟滞如实记录，源恢复后采集恢复，没有再次出现旧部署的逐symbol永久unknown或35分钟整分片写入失败。

未等待下一次真实到期，也未验证多日真实公开持续运行；全量生命周期突发、同秒creation/open交错、数据库歧义及跨96分钟刷新已有前述正式回归/真实孤立数据库证据，但不能冒充下一次真实到期事件验收。

再次独立检查13个已审文件SHA没有变化；候选二进制SHA仍为 `41059f71a06f0064dbb05db7ecbea9380aec350fd2871c3da9e6c8bb42265c0c`。最终结果文件独立摘要为：

| 文件 | SHA-256 |
| --- | --- |
| `production-window-validation.json` | `7887b28e05de2775a6ecbce123f69227ee427801e5f72a5905d35afafc5f3248` |
| `production-final-all-seconds.json` | `608392baf6089af1ffd974e6118bb7b4d8b481bbe7a0ed1294df50f9b0d9dad9` |
| `production-evidence-final.json` | `6c8acf26c274db76cee9bab962219a7e4feba8990a75e2bdfcf252602a0664dd` |
| `production-final-public-comparison.json` | `5fc260f0d126a87ddd50e57090f544d1835a2f594a82507ca3a5da14ba3796c3` |
