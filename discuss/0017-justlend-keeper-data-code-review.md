# JustLend keeper 公开数据采集代码审核

日期：2026-10-03。独立审核者：`/root/keeper_code_review`。

状态：最新“索引先存、后台补证据、纯数据导出”实现已完成代码复审及真实六表首批落库／独立导出验收，无剩余代码阻断。当前服务记录为active/running、enabled，PID2085678，NRestarts=0；之前的运行故障、停服及审核遗漏保留在末尾，不能代替本轮运行状态。审核者未向公开节点发请求、未启动服务、未改实现代码或正式数据库。当前通过范围仅为采集、恢复、认证与数据导出；获利判断由其他程序完成，长期覆盖仍需持续观测。

## 范围与检查方式

读取项目 AGENTS.md、已审核设计、五表 DDL，以及 `cmd/justlend-keeper-data`、`internal/justlendkeeper` 和 ClickHouse keeper writer 的完整实现。除作者反馈外，逐次重新读取修订代码，并独立运行本地解析、HTTP mock 和恢复测试。

独立执行 `GOCACHE=/tmp/keeper-review-go-cache go test -race ./internal/justlendkeeper ./cmd/justlend-keeper-data` 已通过。测试使用真实历史事件、收据和失败模拟归档，以及本地 httptest；没有对供应商压测。首次沙盒内 httptest 无监听权限，换至经审批的本地测试执行后通过，属于运行环境限制。

## 已修订并复查的主要问题

1. 实际 TronGrid 地址为 `0x` 加 40 位 hex；修正原先被 42 字符裸地址分支遮蔽的解析，真实 Liquidate fixture 可解码。
2. 收到 HTTP headers 后立即持久化 401/403 阻断和 429 冷却；body 读取或归档失败不能漏掉限频状态。GET 重定向禁用，使用独立、禁复用和 HTTP/2 的 Transport，避免透明重放绕过发送 gate。
3. 所有外部 HTTP 只有 Client.Send 出口，实际发送前统一 Reserve，单循环串行，无按事件扇出并发。每次启动第一次等待 5 秒，前 5 分钟相邻至少 5 秒，TronGrid 和补采各受 5 秒来源/后台间隔约束。重试重新排队，经相同 gate；闲置不积攒额度。
4. 本地额度、冷却、阻断、冻结历史窗口、分页游标及待写批次保存为带摘要、原子替换的状态。旧状态增加 map 的迁移保留原预算。显式 resume-source 只取消阻断，不清空预算和冷却。
5. backfill 默认终点延后 120 秒；恢复使用原冻结窗口，重跑默认命令不会追加另一套移动的 30 日窗口。watch 不隐式回补 30 日。
6. 同块候选缺执行顺序时标 Ambiguous；已核验事件按真实 block/transaction/log 顺序应用。固化关闭后同年龄层补位，固定 cohort 不受新单挤出，过时 probe 轮次跳过。manifest 保存 cohort、分层、轮转、生命周期和补位原因。
7. HTTP 503 后成功的 probe/cost 赋独立事实索引；之前失败尝试仍保存。收据 NULL 补全和已知链字段、转账、执行结果冲突检查从报告路径复查通过。
8. 固化同高度 hash 的冲突增加本地有限锚点缓存、writer 历史事实检查和 report 检查；不会将冲突分叉作为第二笔独立奖励。writer 对部分写入重试校验冻结行，已有 capture 也回读核验成员摘要。
9. API true、Transaction.Result.code 与 contractRet 的 TVM 结果分开；失败模拟不分类成功。所有金额保持整数/big.Int/Decimal，费字段省略保持 NULL；多事件交易费用只在 summary 扣一次。
10. 报告逐 capture 校验事实计数/摘要及原始证据，再 canonical 去重；分页首尾链完整后才给完整日零值。奖励毛量、混合调用、整笔 burn、未知费用、地址集中和成本情景分别表达。

## 测试证据与边界

已读并独立跑过的测试涵盖真实归档金额/地址/日志、uint256 上界、重复 JSON key、API 成功但 TVM 失败、冷启动及日预算、429 后状态恢复、headers 后读 body 失败、来源隔离、503 后事实索引、重定向禁止、冻结批次重试和损坏状态、过时 probe、同块候选歧义、分页过滤、完整日覆盖。

发送时序 mock 将 1,200 个 HTTP 工作项排队，观察前 70 秒以及长时间暂停后的发送，验证间隔和无积攒突发；这不是执行了完整 60 页 bootstrap 加所有真实证据查询。静态检查确认翻页、hydration 和前后链头都使用同一个发送出口。没有向外部服务发送压测。

## 最后复审结果

完全相同日志跨页保守拒绝精确定位，suppliedKeys 以 provider_event_index 去重；同页重复一个源下标不能膨胀匹配数量。已读拆页/重复页回归及完整集合一一匹配测试。新增报告 canonical 去重、NULL fee 和固化冲突测试也已读并独立执行。

独立执行以下命令通过，三包结果分别为 1.666 秒、无测试文件和 1.443 秒：

```bash
GOCACHE=/tmp/keeper-review-go-cache KEEPER_DB_TEST=1 go test -race ./internal/justlendkeeper ./cmd/justlend-keeper-data ./internal/storage/clickhouse -run 'TestKeeperFiveTableRoundTripAndFrozenRetry|Test(RealArchived|StrictNumbers|ArchivedAPI|LimiterWarmup|HeadersPersist|NoRedirect|CaptureDigest|StaleProbe|PaginationGuard|HTTPRetry|429|CollectorHydration|IdenticalLogs|StateMigration|CompleteCoverage|Client|Report)'
```

数据库测试建立随机名称的隔离库并清理，只访问本机 ClickHouse。五表接受 DDL，UInt256 上界、Nullable、二进制 hash/address、Decimal 和内部转账 Tuple 精确往返；已写事件事实但丢失 capture 后冻结重试不重复，冻结起始时间位于月末，修改起始批次内容被拒绝；只读 reader 拒绝 writer。不是在正式研究库注入故障。

作者真实 Go preflight 的六请求 PublicNode 证据已由审核者只读检查，capture 为 `477ea65a-9da5-4c6c-8a8b-49f99b87393d`，2026-10-03 02:55:14 上海时间完成。六请求均 HTTP 200，实际发送间隔均超过 5 秒；所有当时已归档 gzip 的内容 SHA-256 与文件名一致。实现 bytecode 重算 SHA-256 为 `c66f4e441f88f9bc0b2cf31be8417c5ea220596cfc49dd9abfebf85cf92c3782`，与固定 manifest 一致。

之前 transport_error 来自复制默认 Transport 遗留 h2 ALPN；实际代码现只协商 HTTP/1.1 并清空继承的 h2 handler，本地启用 HTTP/2 的 TLS 服务回归确认不会协商 h2。另独立执行完整 `GOCACHE=/tmp/keeper-review-go-cache go test -race ./internal/justlendkeeper ./cmd/justlend-keeper-data`，包含 NativeClient ALPN 测试，1.646 秒通过。已读新增测试、客户端及[实现说明](../docs/justlend-keeper-data-implementation.md)。运行说明已区分观测点、空 episode 表头、未启用优先队列与获利认证门槛。

复审范围指纹：共 20 文件，包括 `internal/justlendkeeper/*.go`、CLI、keeper writer/DDL/集成测试、配置及实现文档，按路径排序，将每个 `path + NUL + SHA256 + LF` 拼接后再 SHA-256，结果为 `9406b744823c04dc304ef0605de17468614a08acf4b98b79c19ee56c10cb1170`。作者后续服务启动与真实首批写入验收属于运行验证，不包含在此时的代码版本指纹中。

## 获利判断仍需的数据

成功 reward ABI fixture 尚未认证，程序禁止 success_reward：正的返回数据只保存为 unknown/uncertified 证据。短期优先采样、经认证的成功 episode、实际成交/失败尝试和净收益不应描述为已验证。

当前成本数值为最近已可用观测费率、400 字节带宽假设的条件情景，能源采购、兑换、失败成本和维护边界未知时不能视为净利润。单一报价档位需要容量检查，不用历史事件时间伪造本地 first_seen。

完整一天的请求数、压缩后空间与查询耗时，以及供应商在实际出口的稳定性，仍待持续采集后测量；单机 gate 不覆盖其他同出口进程，也不等于供应商免封承诺。

## 真实首批后的重新审核

首批运行暴露两个之前的测试未覆盖的缺陷；原通过结论不代替这次修复验收：

- ChainParameter.Value 的协议类型为 signed int64，不相关参数可能为 -1。全部参数用 *uint64 反序列化会让合法整页失败。修复应按 RawMessage 保留值，需要的两项费率严格校验正整数和范围，其他值按 signed integer 语义校验。
- SHA-256 成员摘要使用 gob，Decimal 的系数和指数也是表示的一部分。真实 Binance 八位小数读入后直接冻结，与 ClickHouse 固定十八位小数读回的金额相等但摘要不同。此前数据库 fixture 全部为十八位，未暴露这个问题。新观测应在输出时精确规范化到十八位；输入超过十八位不得有损舍入。

作者计划保留四条既有 quote capture、事实、原始响应和原摘要，不修改历史值。兼容校验只能在计数和其他三表摘要完全匹配、仅 cost digest 不符时，在副本中尝试原 Decimal 指数的等值重编码；每字段必须 Equal，所有其他字段保持原状，只有 SHA-256 精确命中旧摘要才可接受。无法命中仍拒绝。报告另由原始报价验证数值及原表示，不因兼容而放宽成员校验。修复代码与对应真实精度/错误金额回归正在复审。

进一步独立只读运行正式库 report 仍返回 capture_members_mismatch，证明单纯 Decimal 指数恢复不足。作者随后查明 gob 类型 ID 由进程中类型注册顺序分配，跨进程成员摘要不稳定；此前同进程数据库测试掩盖了这点。上段 Decimal 兼容建议已被此根因推翻，不作为最终修复。

需要以有版本、字段顺序固定的稳定事实编码替换 gob 摘要；gob 只保留本地冻结/状态用途。测试须跨子进程、改变 gob 类型遇到顺序，并另跑 CLI 读取实际库。初版无可靠摘要认证的已写批次允许在先备份全部原 capture/事实及证据身份后，仅撤回 committed 标记并标 error/明确原因；原事实、digest、capture_id/time 和 raw 保留。报告覆盖清单必须显示撤回，不能将撤回数据当完成窗口或零值。实际修复操作由主 Agent 执行，审核者未修改数据库。

### 修复后再次复审

已读 `canonical.go`、修改的解析/摘要/manifest/partial writer/报告以及新增测试。FactBytes 使用明确域 jl-keeper-fact-v1、固定字段名/顺序、长度前缀、UTC UnixMicro、NULL marker、整数/big.Int 和精确固定十八位 Decimal；不依赖 gob 类型 ID。空列表和数据库空列表等值，NULL fee 和明确 0 不同。失败的旧指数 fallback 已移除，不能旁路摘要认证。RawMessage 严格 signed int64 解析接受真实 getRemoveThePowerOfTheGr=-1，所需两项费率必须明确为正。

独立执行局部完整测试加跨子进程/不同 gob 注册顺序及隔离 ClickHouse 集成，`-race -count=1` 通过：keeper 3.773 秒，storage 2.010 秒。数据库测试现在使用 1、8、18 位来源报价；四类事实的不同子进程摘要一致，NULL/0 和等值 Decimal 表示有回归。此验证覆盖上次同进程、固定十八位 fixture 的遗漏。

独立新进程执行 `go run ./cmd/justlend-keeper-data report --days 30 --out /tmp/keeper-review-stable-digest-report` 成功。正式库 23 个旧批次，4 个错误旧报价提交在 coverage 明确列 uncommitted/invalid_legacy_gob_member_digest，19 个空事实批次可以验证；完整 UTC 日为 0，没有把撤回报价或未抓取历史当作完整市场零收益。

审核者只读校验修复前 Native 全备份、前后 TSV 及当前数据库。TSV 中仅指定 4 行的 committed/status/reason 改变；另外用 ClickHouse local 读取 Native 快照，与本机正式库按固定身份排序，对全部非修复 capture 字段的 typed tuple hash 比较，23 行一致。四条旧报价事实全部字段的 typed tuple hash 也与原 Native 快照一致；未改它们的值、原摘要、身份、时间或证据。原始 event/receipt/probe 备份为零行，修复不是删除事实或重算旧摘要。

最终复审范围仍按上述 path+NUL+SHA256+LF 算法，当前 23 文件聚合指纹 `934b20bb5828e34d9f3f310ae3ff91795b741c1305af05e3b112546f1ca54fd4`。部署二进制指纹由作者在验证记录维护。

结论：代码修复复审通过，可以恢复采集。交付前仍应核验修复后二进制实际产生的非空报价、链资源费率，并由另一个新进程重读报告；不能只依赖旧空事实批次重读成功。继续保留成功 reward ABI、净利润和完整一天实际容量未认证的边界。

### 实测发送间隔及组合时间的末次修订

作者真实 manifest 发现相邻 started_at 最短 4.962758 秒：旧 Reserve 在持久化前推进发送期限，fsync 耗时差可以压缩最终实际开始间隔。这不是原限速要求允许的余量；已修复，而非忽略小误差。

已复查 Limiter.LastGap/ExtendGap、Client.Send 及本地回归。Reserve 保存本次 gap，收到 headers 后或 transport 结束后再以观测时刻向后延长全局/来源/后台期限，所有期限取 max；headers 路径与 Status 一次持久化，transport 失败显式持久化。预算只在 Reserve 增一次，冷却和阻断不清零；跨启动五分钟边界仍使用本次保留的五秒间隔。单请求串行不改变，不借延时积攒额度。

chain_resource 是参数、代理资源比例与前后头的组合观测。Finish 使用一个 completedAt 同时赋 capture.available_at 和非 error 的资源行 available_at；received_at/source_time 保留参数收到时间，独立失败行保留真实失败完成时刻。旧资源行不重写，报告在摘要核验后仅用副本的 max(row.available_at, capture.available_at) 做有效时间，CSV 明确 effective_available_utc。

独立执行完整 keeper/CLI `go test -race -count=1`，3.664 秒通过，含模拟持久化/传输耗时、warmup 跨界、429 冷却保留、transport 失败期限持久化一次及组合 metadata 可用时间测试。最终 23 文件聚合指纹更新为 `b21fe80689faa80935406af5f574b18c7df5e6850fc60b4f6290f817af4a6cc7`。

末修复审通过；允许原子替换二进制并恢复采集，原记录和 Frozen 批次保持不变。作者仍应核对替换后真实 started_at 间隔、新非空报价/资源和新进程报告重读，再交付运行状态。

### 真实 Rent/Return 扩展事件与失败样本恢复

持续运行进一步暴露原审核的覆盖遗漏：真实 Liquidate fixture 可以通过，不代表 Rent/Return 也符合参考页 ABI。真实 seed 收据连续出现 event_receipt_log_mismatch；审核者只读核对归档后确认，部署的 RentResource/ReturnResource 非 indexed 数据分别为六/七个 uint256 word，旧解析只接受四/五个。多出的 securityDeposit、rentIndex 在索引响应中也有明确名称和值。旧失败没有 ApplyEvents，且初版只初始化一次 seed，缺少恢复路径会使没有新事件的成员一直未验证。

已复查两版精确 topic、完整 word 数、地址和金额映射。新两字段同时存在时严格解析为 UInt256，只存在其中一个时失败；两者皆无的旧版保留 NULL。EventEqual 纳入两新字段，RawFromRow 保留它们；真实六/七 word 日志不会与旧版匹配。两个 Nullable(UInt256) 列通过 init-schema 幂等添加，collector 不执行 DDL。生产历史事件事实为零，此次新增结构字段不影响旧空事件摘要；cost/probe/receipt 编码和摘要域没有改变。不对历史非空旧版事件摘要作未经验证的兼容承诺。

审核者独立核对 rent-v2、return-v2 的四份 raw SHA-256 和来源 seed manifest：receipt、block、body hash 均精确对应；归档索引页中的交易和 provider index 对应实际 receipt log 3。真实扩展版测试涵盖两新字段、RawFromRow、截断拒绝、缺单字段拒绝及完整 hydration。旧四/五 word 回归由真实 v2 fixture 删字段并换 topic 构造，只证明兼容解析，不称为真实旧版收据认证。

恢复计数、版本及下次时间进入原子状态和 cohort manifest；Run 在发送前保存调度结果。同一解析版本最多三次，失败完成后再等至少30分钟，一次仅有一个恢复 seed；解析版本更新可以重启旧失败的核验计数，不清空来源冷却或每日预算。达到上限仍为 unverified，不伪造关闭或自动换样本。ApplyEvents 按 block/tx/log 位置拒绝旧 seed 倒退；核验期间的新 Return/Liquidate 不会被旧 Rent 重新打开。初始化结束后，恢复任务不再阻止已验证成员 probe 和增量事件的调度。

独立完整 keeper/CLI `go test -race -count=1` 通过，4.324 秒；隔离本机 ClickHouse 五表回归通过，2.145 秒。此轮首次数据库 fixture 新两列均为 NULL，已要求作者补非空 UInt256 往返/冻结重试覆盖，并在真实首个非空事件后由新进程 report 验收；这两项证据完成前不声称真实事件最终运行验收通过。目前未发现阻止恢复采集的代码问题。

作者补充不同 provider index 的第二个 v2 Rent fixture，新 securityDeposit 为 UInt256 上界、rentIndex 为非空整数，旧 Liquidate 两列仍 NULL。独立再次运行隔离数据库 `-race -count=1` 通过，2.119 秒，冻结部分写入重试、整批字节精确往返和新 reader 摘要均通过。曾在作者编辑过程中编译到两个事件共用 provider index=0 的中间 fixture，ReplacingMergeTree 按同键去重导致成员不匹配；这是测试身份冲突，现已修为不同身份并完整重跑，没有放宽 writer 校验。

独立新进程 `go run ./cmd/justlend-keeper-data report --days 30 --out /tmp/keeper-review-event-v2-report` 成功，读取到真实非空 Rent/Return、Liquidate 以及成本事实，所有提交批次成员摘要与证据均可验证。报告截至 UTC 2026-10-02T20:02:34：251 个 capture、1 笔 Liquidate 历史事件、历史奖励毛量20 TRX、完整 UTC 日0；费未知仍列 unknown，不伪装为零或未来收入。

另独立只读选出实际 Rent capture b1e78646-ebdd-4d18-8a57-1a6f610aa3f1、Return capture 5832fa5b-8f0f-4c17-adbf-ce8b52783d86，并按各自页面 hash、manifest hash 打开归档，重新核对 SHA。两新列分别为 65340351/1051407644019492408 与 0/1054702608028912431，与对应索引具名值、receipt log 3 最后两个完整 uint256 word 精确相同；明确零未退化成 NULL。

本轮代码与非空正式事件跨进程验收通过，没有剩余采集阻断。实际成功模拟、竞争胜率、完整日容量与净获利结论仍未认证，持续采集才为后续研究提供输入。

本轮最终复审范围共24文件，按上述 path+NUL+SHA256+LF 算法聚合指纹为 `6db97260c57e7671c9f156c84a04aecf95e76ff6820b3777ac13a713c01eaa45`。

实际 probe 另只读抽查 capture `69454f65-dfa8-4bf7-bbbe-b944b75e4e40`：manifest/request/response SHA 与数据库完全一致，原 API result=true、TVM transaction.ret=FAILED、消息 REVERT opcode executed、energy_used=18173，定型状态准确为 revert、奖励 NULL；前后头 86766298/86766302 的高度、blockID 与原始头响应相同。节点最新状态仍为 unpinned 前后范围，没有将失败模拟当作成功收益。

### 数小时实际运行后的边界、调度与报告修复复审

本轮依据用户五项故障、原始响应及离线重放重新审核。原代码通过首批 fixture 和局部启动验收，未覆盖持续数小时的游标推进：Rent 下界20:54:03.378227、Return 下界00:34:39.138186，HTTP成功响应返回下界所在秒的.000事件，严格校验拒绝整页，同ID旧窗口随后反复重试。另有五个probe各需三个请求，30秒期限在已发模拟之后也会丢掉后置头；旧revert分类包含一般TVM失败。原审核覆盖遗漏在此保留，不以之前通过结论代替本轮验收。

已读并复审秒查询包络、逻辑半开裁剪、watch迁移及1/2/4/8/16/30分钟失败退避。保存的两份真实边界响应现在分别保留逻辑窗口内7条/0条，记录边缘排除2条/1条；从原游标所在秒重扫时接受9条/1条，不丢掉原页面。包络之外仍失败，下一页参数必须完全一致，排除边缘不跳过分页。旧未冻结待办保留capture身份及已有证据、明确标partial/legacy_fractional_window_replaced；不将旧token用于新包络。Frozen内容不改，完成后新扫描仍对游标秒对齐，最多重叠一秒。

失败的daily rescan及history按原窗口/token恢复，预算和来源状态不清空。可选watch --history-days冻结完整UTC日区间，一次只排一天；从rent/return追赶完成后开启历史补采。审核发现新history/catchup同kind活跃判断会抑制实时Liquidate，已要求修为仅watch前缀之间互斥，并独立验证history与实时同时调度。另要求UTC零点后的120秒不能提前认证刚结束的一天；EnsureHistory以now-120秒选择完整日终点，daily rescan等到延迟满足才排。历史窗口扩大或多个流继续运行不增加请求速率，原统一gate仍使用。

probe一次一个、最早每6秒轮转；生命周期游标落后超过5分钟不发新模拟。仅未尝试模拟的过时待办被跳过；已有观察或旧Evidence显示已发constant call时继续完成后置头。明确REVERT才标revert，其他已知执行失败标tvm_failure，API/传输错误独立。报告在成员及证据认证后仅在副本派生旧分类，保留observed_status，不改原事实或摘要。

报告每组最多256个capture读取四表，包含预期空成员表；逐capture继续检查计数/摘要。完整SHA缓存只保留本次已认证结果，不缓存失败；错误成员不能通过批量路径。未知费用改为NULL并列known_burn_transactions/burn_coverage，净利润仍unknown。已读隔离数据库新增的非空v2往返与预期空表被注入额外member后拒绝的测试；作者该测试2.496秒通过，审核者本轮没有修改测试库或正式库。

独立keeper专项完整-race -count=1通过6.841秒。共享仓库其他模块维修期间CLI依赖出现中间编译失败，未修改其代码；作者用git HEAD和当前keeper专项文件构造可复现快照。审核者逐文件SHA确认快照中keeper、CLI和keeper writer/集成测试与工作区一致，在该快照独立完整keeper+CLI -race通过5.237秒。首次两个回归fixture缺新鲜生命周期游标/有效probe身份及一个空逻辑窗口的断言问题均已修正后重跑，没有放宽Validate。

独立新进程正式只读report成功，输出/tmp/keeper-review-repair-bulk-report，8,906个capture、4,748条probe、9笔历史Liquidate/180 TRX毛奖励、完整UTC日1。全部9笔整笔burn仍unknown，known_whole_transaction_burn_trx=NULL、burn_coverage=none、net_profit_status=unknown；4,742条旧revert的原始证据仍支持REVERT，实际派生分类变更数0。审核者实测240.27秒，包含go run构建并处于并行维修负载；作者纯已建二进制测量76.88秒，条件不同，不合并或宣称固定耗时。

已读同步后的设计、实现和存储文档。本轮代码及离线报告复审通过，没有剩余代码阻断；实际部署SHA957021c8b11b51a9ac9c9ab94f49a7f69865078cc844d242fbfd58880a4007b1。只读正式库已见新Liquidate逻辑窗口complete；作者05:47:27 UTC状态记录Rent已从20:54:03推进至21:54:03，Return首30分钟窗口仍在核验选中事件的块/收据。Return持续推进、追平后的新probe前后头、完整30日历史及完整日容量仍需运行验收，不能由“服务active”或一次成功页替代。成功奖励fixture、连续episode、竞争胜率和净收益仍未认证。

本轮复审范围共25文件，沿用path+NUL+SHA256+LF聚合算法，当前指纹 `8ebc4e8c3c4788148086c61c7061ff7b19f779fd226f0743bca0071027aace4b`。

### 原生 API 编码的 VM exception 分类补审

作者06:01:25 UTC状态显示Rent/Return均已推进到05:58:59，历史补采及新single probe开始运行。进一步复核原OTHER_ERROR响应发现明确Program$OutOfTimeException（CPU JUMP/PUSH1超时），属于执行失败。第一版补丁只匹配显式API result=false；审核者只读抽查真实旧cap `3b315ed3-d132-45cd-95ce-7a1f2c84bdf7`，响应SHA `3f45d5a0d9dabf2492111b14cbbffd027bb76848bd5941ea974ec2ac31a708d4` 实际省略result.result，数据库api_success=NULL。合成false fixture不足以证明这份真实protobuf默认省略响应的分类正确，已要求补实际归档回归。

修订后在API未成功分支，仅OTHER_ERROR加明确OutOfTimeException/OutOfEnergyException两类VM异常归tvm_failure；ApiSuccess保留原nil或false，不从缺字段补造false。旧report也从认证的原响应派生，observed_status、原事实和摘要不改。明确的非REVERT contractRet失败优先于消息里的REVERT文字，不把OUT_OF_ENERGY误记为业务REVERT。

独立核对真实gzip fixture内容SHA与正式证据一致，并运行限定-race回归通过1.055秒，包含真实缺result.result、合成显式false、旧report派生且原行保持rpc_error，以及既有TVM失败/后置头回归。末项代码复审通过，无新增代码阻断；该修订的最终二进制SHA、正式报告中的实际分类计数及最新范围指纹由后续部署验收补充。完整30日、成功奖励fixture和净收益仍不在此结论范围内。

另复查冷启动准入：仅Schedule新probe要求now达到本次Limiter.Started+5分钟；已发旧模拟仍由Step按probeAttempted完成后置头，不修改已有观察或预算。独立限定-race回归（WarmupDoesNotAdmitProbeQueue、SingleProbeAdmissionAndFrozenHistoryWindow、ProbeCompletionAfterAdmissionDeadlineAndTVMClasses）通过1.043秒。此修订减少启动慢速gate下无望完成的排队轮次，不承诺固定实际采样频率。

审核者实际读取部署文件SHA为 `e7a0b6afe095da98b15ebfde7260ca99b3a2b2f4e519993bb2e07dce5054b495`。此前名为final-report的文件as_of为06:07:13 UTC、分类变更0，属于末项修订前的阶段，不能充当缺失result.result已修复的正式分类验收；新版报告仍需刷新确认。代码及限定回归已通过，历史费用、奖励及覆盖边界不变。


### 报告本地证据并行读取复审

最终报告再次超过六分钟，作者SIGQUIT堆栈定位到本地Archive.Get/gzip文件读取。此轮只加速离线证据读取，不增加公开API调用，也不更改事实、采集窗口或来源路由。每组最多256个capture，先最多8个worker逐一完整SHA及严格JSON认证manifest，并检查capture身份、版本与成员编码；再去重请求/响应SHA，由最多8个worker调用原Archive.Get完整解压及SHA认证。逐capture的成员计数／摘要和全高度固化hash冲突检查仍保留。

审核者已通读新report_evidence.go、readManifest及report调用。各worker只写独立下标，retained总字节数加锁；所有worker退出后才由主线程合并成功verified缓存，任一错误使整组失败且不会缓存该组新结果。已认证的来源SHA不能跳过另一capture的manifest身份核验。旧probe分类只在当组保留最多16MiB响应，超限后沿原顺序Archive.Get重新完整认证，不从缺缓存推导成功或缺失。16MiB仅为保留响应缓存上限，八路Archive.Get仍可各临时解压至原64MiB上限，另有manifest和事实内存，不能宣称整个报告内存仅16MiB。

独立定向-race -count=1（ParallelReportEvidenceFailsClosedAndRetainsResponses、APIExecutionExceptionAndLegacyReportClassification及既有Report回归）通过1.437秒；包含17个capture共享响应、缓存下错误capture绑定仍拒绝、预取消、损坏gzip整组失败且不写新缓存。代码复审通过，无报告认证或分类阻断。超缓存上限的本地fixture回归已建议补强，实际完整报告耗时和修订后真实分类计数仍待本次运行结果，不能沿用旧final-report的零分类变更。


### 最终只读报告与停服验收

已读正式并行报告及计时文件：滚动30日as_of=2026-10-03T06:31:21.150486Z，9,234个capture，99.33秒，实测max RSS 90,500KiB。原观察7条rpc_error在报告副本中成为5条tvm_failure与2条rpc_error；审核者读取CSV确认五条具体capture含此前真实缺result.result样本。另独立本机只读SQL确认这五条原行全部仍为rpc_error、api_success=NULL，没有重写旧状态或补造false。

停服后的固定完整UTC30日窗口为2026-09-03T00:00:00Z至2026-10-03T00:00:00Z，as_of=2026-10-03T06:35:12.891646Z：9,256个capture，28.89秒，实测max RSS 86,412KiB；已认证完整UTC日2，27笔历史Liquidate，毛奖励591.259864 TRX。全部27笔burn费用仍unknown，已核验reward transfer数0，burn_coverage=none、net_profit_status=unknown。此窗口排除10月3日probe，分类变更数2，与滚动窗口的5不同是查询时间边界差异，不是分类不一致。两次计时数据量、窗口、采集并发及文件缓存状态不同，不承诺固定运行耗时或精确加速倍数。

独立本机只读SQL核对06:13:14 UTC本次启动后70条新probe：70条均为revert，70条前后头的高度/hash/时间均非NULL；首个scheduled为06:18:16.492813，首个available为06:18:31.659415，满足warmup后准入。作者runtime-verification记录三类事件游标已到06:28:45／06:29:16、878次真实请求延续原预算10,682至11,560，新event_filter_mismatch为0；PublicNode仍有真实transport_error，不应将其改为链上失败或假成功。

新增3个不同6MiB响应的缓存边界测试已通读并独立限定-race通过2.692秒：3份SHA均认证、只缓存2份共12MiB，未保留的6MiB响应仍可顺序完整认证。部署二进制实际SHA为c264480c1eb67456fe242ce1e2368c26f0dd21f9269e3e660bc473eaa9d0b182，与保存的deployed-sha256相同。按用户要求服务在06:33:36 UTC停止；已读final-service记录inactive/dead、MainPID=0，路由未改，此验收不授权重新启动。

最终代码、真实游标恢复、新probe前后头、旧分类派生及新进程报告验收通过，没有剩余代码审核阻断。完整30日覆盖仅2日、成功奖励fixture未认证、连续episode和竞争胜率未测，未知费用与转账核验不足仍不能支持日赚200美元或年化3%的判断。当前复审范围共26文件，沿用path+NUL+SHA256+LF聚合算法，指纹15d1d98f1af683648670891e5f114b8f1b24af7fc41491b01d02df2bd889fa88；最终validation说明正在由作者补齐，不将未完成文件称为已读证据。


最终验证文档闭环已完成：审核者已读research/2026-10-03-keeper-repair/validation.md及其停止状态、最终请求核验、传输失败、probe调度、capture状态、构建／测试记录；文档所有20处本地相对链接均存在。固定与滚动报告窗口、2完整日、毛奖励及NULL成本、5条旧分类原行保持NULL、二进制安装但不重启的说明与实际记录一致。最终停止核验比前述运行快照多39次请求：917次真实请求，原预算10,682延续至11,599，350个capture／待办证据，游标Rent06:30:46、Return／Liquidate06:30:16 UTC。最终四笔transport_error明确区分两笔正常运行失败与两笔计划stop取消；无响应不归TVM失败。文档保留另1个未模拟stale和3个身份skipped，不宣称70条已完成probe代表全部计划均及时。

无进一步代码改动；最终26文件范围指纹重新计算仍为 `15d1d98f1af683648670891e5f114b8f1b24af7fc41491b01d02df2bd889fa88`。最终验证文档自身SHA256为 `681c5d67a7b22f3ce055b2f78566a8b6164d69ae515bc2bba6dbaf32348adc76`（未混入上述实现范围算法）。本轮独立审核及文档闭环完成，服务保持停止，净获利结论仍未认证。

最后状态补充已复核：final-service为UnitFileState=disabled、inactive/dead、MainPID=0，避免主机重启自动恢复请求；disable后ExecMain时间字段为空，06:33:36 UTC退出时间保留在final-journal和验证正文。validation及运行文档已同步；审核者未执行任何服务变更。上述最终验证文档SHA已更新为disable后的版本，实现26文件指纹保持不变。


### 索引先存、核验后补的简化方案设计审查（实现待审）

按新任务只读重新检查模型、分页解析、Finish冻结／提交、状态恢复及调度。新方案拟增加一张定类型jl_keeper_indexed_event表，使用Capture追加IndexedRows、可空IndexedDigest、可空ParentCaptureId作索引页提交记录；既有四种事实结构与成员摘要不改。每页保存全部目标事件和原页面序号，不先按cohort筛选；block hash及收据位置未核验时保持未知，索引页完成不升级为solid／receipt_verified，也不计入旧奖励认证。空页仍需要非空的稳定空成员摘要。

设计可行，但实现通过需验证以下边界：旧Capture增加字段的DB默认应与旧gob缺字段后的0／nil完全一致，Frozen批次不重Seal；writer对期望零行的indexed表也检查，拒绝额外成员。索引页严格解析、原始SHA归档与稳定冻结在DB写入之前，索引事实先写、Capture提交标记最后写；提交后同一次本地状态保存推进token／抓取cursor并记录补证据关系。DB已提交而本地保存失败时按原pageID／成员恢复，不能重新取页替换冻结内容。

旧未Frozen页不能直接使用按cohort筛选后的RawEvents迁移，应重新严格解析保存的原页或保留的全量Discoveries；原From／To／token及已发证据留存。provider event_index不是链上日志位置，字段缺失不能默认为真实0；写入身份使用page capture与原页面ordinal，防止ReplacingMergeTree按交易／provider index静默丢掉重复或冲突观测。实时watch catchup按实时角色设置priority/background，不因mode=catchup误作历史；页请求移除PublicNode前置head，来源冷却、单在途和日预算保持。

补证据最多一个独立child，通过已提交父页数量／摘要／原SHA认证后补链上证据，引用ParentCaptureId；失败只影响核验，不回退或阻塞抓取。child complete只认证该页目标成员，与父页pagination_continues分开。EvidenceCursors应依据连续父页token链、最终exhausted和每页完整child推进，不能用新的抓取游标或未核验bootstrap起点冒充核验。历史／实时进度分别处理。DB选择有界未完成父页，避免把全部历史事件塞入本地Ops。数据导出表达索引与核验差异，收益计算不参与采集流程。此段为方案审查，不将未完成实现、迁移或部署称为已通过。


索引简化实际代码第一轮读审已开始：Capture三个字段追加且Nullable默认NULL，新表按capture／原ordinal保存索引来源；索引页未知block hash保持NULL，provider_claimed_confirmed与receipt_verified事实分开。writer读写新增表并继续期望零行检查，capture提交标记仍在事实之后；旧四种事实结构未改。本轮只读检查尚未替代完整实现和迁移验收。

第一轮已反馈具体问题：初版只在刚完成child上推进EvidenceCursor会卡在逆序完成窗口，且新状态在bootstrap前复制空游标会无法开始核验；作者改为连续DB frontier及显式bootstrap增量baseline，待回归确认。DB frontier初版仅以存在终页及每个可见父页有成功child计算完整性，缺少分页token链证明，已要求完整链验证。旧Frozen末页成功后抓取／核验游标，以及旧Frozen非末页后接新索引页的缺前缀恢复，仍需专门处理与测试。迁移已取到response的历史页分支也应统一history模式；索引manifest不能套用旧空事实允许缺digest encoding的兼容，父response应绑定精确查询路径。作者正在补这些修订。当前阶段不宣称新采集方案通过或已部署。


简化方案完整读审期间，独立keeper＋CLI完整-race -count=1通过7.431秒；首次沙盒内httptest监听被环境禁止，改为仅本机mock端口权限后通过，不涉及公开请求。独立本机随机隔离库的旧成员冻结重试与新IndexFrontier／LegacyDefaults回归通过2.050秒，未改正式库。审核者直接检查旧gob字节，没有IndexedRows／IndexedDigest／ParentCaptureId／IndexedEvents／IndexingEnabled／EvidenceCursors字段；旧nullable默认往返有实际跨布局证据。

本轮进一步发现并已要求修订：无Operation的旧非末页扫描在升级时仍沿非空token进入新索引，缺少新第一页；初版256父页限制会截断单窗口或使全部已完成的后续窗口无触发地卡住；默认Export会因四条已撤回committed=false旧报价失败。作者已修首启旧scan前缀、按覆盖当前游标完整窗口查询及有界连续／空闲推进，并将captures审计行与已提交当前配置成员分开。父child在writer增加同配置／合约／事件／窗口及提交关系检查，Export认证父manifest身份／种类，不只核对任意原文SHA。新增边界回归仍在补齐，此阶段测试耗时不替代这些末改的最终验证。

另发现新异步核验的重要可用时间风险：Hydrate生成RentalEvent时仍继承父索引页AvailableAt，可能比child链上核验完成早数十分钟。已要求新enrichment事实可用时间提升为完整child完成时刻，索引行保留原来源观测时刻；旧Frozen及旧事实不改，导出说明历史组合数据的有效可用时刻需结合capture完成时间。此项修订和最终文档／部署尚待后续验收，暂不将简化实现判为最终通过。


### 采集简化最终代码复审通过（实际落库待验收）

最终末改已读并确认：全目标能源索引页先提交，不等待PublicNode前置固化头／收据；nullable block hash与provider_claimed_confirmed／indexed_only明确保留未核验语义。三新增Capture字段与旧gob／旧数据库NULL默认一致，旧四类FactBytes及Frozen成员不变；新索引空页也核验窗口、来源、种类与scope，仍有非NULL空摘要。原ordinal使重复供应商下标分别留存。

旧Frozen非末页先按原批次完成，再从原窗口空token重索引；末页只推进原已核验连续基线。首启旧页间停机而无Operation的扫描也重置前缀；未Frozen来源从完整归档重新严格解析，旧证据保留。source token／cursor仅在DB提交之后与本地状态一起保存，DB失败不推进，冻结身份及时间不变。实时catchup按watch角色优先，所有请求仍走原来源／后台／预算gate，没有外部并发。

核验一次仅恢复一个父页；父数量／摘要、原SHA和精确请求路径重验，writer另检查父提交身份、配置、合约、事件、时间窗及发现数。Rent/Return只核验当前选中样本，EvidenceCursors仅表示该样本连续生命周期证据。完整窗口需空token到终页的全manifest链，且每个父页有成功child；当前完整窗口不被256页截断，连续晋级每次最多32窗，无child时仍周期恢复，逆序先完成后续窗口不会掩盖早期缺口。失败次数最多3、至少间隔30分钟，失败父页关系可查询，不伪造完整。

新enrichment事实AvailableAt仅在Freeze之前提升为child完整核验完成时间，来源RequestStartedAt／payload hash不改；索引行仍是原响应可用时间。旧Frozen重试不重新定时，旧事实保持原值；导出metadata说明完整上下文应取max(row.available_at,capture.available_at)。

CLI export与兼容report现在只输出六份typed CSV＋metadata，没有机会／年化／利润／成本情景计算。撤回／未提交及不同配置cap作为审计行保留，其成员排除；正常成员逐批摘要及完整SHA认证，child父manifest绑定继续认证。金额与Decimal无损输出，地址／hash十六进制，NULL为unknown，输出先认证到临时目录后整体发布且拒绝覆盖。旧研究Report函数保留仅作历史测试兼容，不参与命令或采集。

在此前完整keeper＋CLI race7.431秒、隔离CH2.050秒基础上，独立末项定向race通过1.139秒，覆盖异步事实时刻、冻结重试、撤回cap导出、真实旧gob／页间停机和token链；273个已完成窗口在无新child时周期晋级回归另通过1.014秒。独立真实隔离CH IndexFrontierRetryAndLegacyDefaults末项通过4.430秒，包括257父页完整窗口、逆序进度、30分钟重试及旧nullable默认。候选/tmp/keeper-collection-split-build共28个keeper／CLI／writer源码逐文件相同，DDL两份字节相同。未修改正式库或服务，也未访问公开节点。

最终代码审核通过，没有剩余代码阻断；实际部署后的非空索引行、父子批次、来源SHA、分页游标与独立导出仍需运行验证。最新设计、implementation、storage及architecture已读；implementation末尾仍残留旧四表研究report描述／旧测试名，storage章节旧编号与五表口径已要求作者收尾，不将文档未闭环称为完成。当前实现范围30文件指纹为a6675e42213a6cf8175ae87c05d0ee5b13864860de938e491b62051ce1aed960，文档收尾后应重新计算最终指纹。


### 简化采集真实落库及纯数据导出验收

已读本轮[validation](../research/2026-10-03-keeper-collection-split/validation.md)、service-final、live-audit-final及两份导出metadata，独立抽查本地不可变归档与CSV。服务08:57:13 UTC启动；service-final为active/running、enabled、MainPID2085678、NRestarts=0。审核者实际读取部署二进制SHA为`0749ecd085f5b187150bc88c424cd7a7896e0adc8a79b74a79e50342cb60009c`，与构建记录一致。本轮只读验收，不向公开节点发请求或更改正式数据／服务。原五表、状态、二进制及归档备份保存在本轮before目录。

历史父33834175-8d08-4b3d-8352-ebc3dd70d266及child4b29cb00-c651-4db2-bb00-8830ab9eb024所引用59份唯一请求／响应原文，独立解压重算SHA全部一致；11条索引可按原页ordinal、交易ID与供应商下标追溯，未知block hash保持NULL。child保存11条已核验事件及11条收据，AvailableAt全部为09:06:08.365361 UTC完成时刻，父源观测时间单独保留，未提前声称链上核验可用。父页开始时间早于export-live的08:52下界，父capture及索引可在export-30d读取；子批次导出仍认证父manifest，不把时间窗外父页误作漏采。

Rent父26fcb7e5-689f-43fb-ad13-770ffbba1c4c及child3e5ad30c-539d-413d-b4e5-38ed2da4e6ec所引用8份唯一原文SHA也全部重算一致，父manifest身份／hash绑定正确。父页2条索引均保存，child发现2、选中1，实际1事件／1收据，AvailableAt等于child完成时刻。抽查块86781643、收据log2：原blockID与事实／收据hash一致，交易在原区块交易列表中，收据id、高度和合约地址相符；索引金额、security_deposit_sun、rent_index原值与已核验事实一致，receipt_complete／body_complete均为true。

Return父667d3ca9-3ee6-46ae-894e-34b27c2a038e与child5a2bdb07-4da7-43d5-bf82-b33a5488e5a8已抽查父子manifest和源响应SHA。3条索引按原ordinal保存；child发现3、选中0、事件／收据行数均0，complete仅表示当前选中范围完成，没有伪造3条收据认证。样本EvidenceCursors与源EventCursors分开，实际快照的证据进度晚于源抓取进度，文档明确该范围。

09:10:02.195828266 UTC的作者离线审计含135个源／子capture、172次归档请求；审核者以上小范围独立重放不冒充逐一重新执行全部审计。export-live实际metadata为187个审计capture、234索引、12事件／12收据、21probe、16成本观测；export-30d为10499个审计capture、137索引、257事件／257收据、5424probe、910成本观测。两目录仅六份typed CSV与metadata，没有获利分析文件；窗口均按capture_started_at半开选择，不将不同截点行数或样本核验范围合并成全量完备数据集。

本轮代码、首批真实索引／异步补证据、来源认证及独立纯数据导出验收通过，没有剩余审核阻断。通过范围是短期运行和已列样本，未验收长期供应商索引完整性、全天流量或后台全部积压；此限制不阻止准确抓取继续运行。最新实现及设计文档旧章节已清理，validation全部9处本地链接、两份设计／实现文档链接均存在。

最终30文件范围沿用path+NUL+SHA256+LF聚合算法，指纹为`1a046c3c41229c15a16b3a0a11f353310ec48f7be70e94da62cfa45fceb0a20d`。本轮validation自身SHA256为`de017dd5bbe5334f761d97a6a83170857ea3ff31f968105ee5227849ecf29f36`，未混入实现范围指纹。
