# JustLend keeper 公开数据采集代码审核

日期：2026-10-03。独立审核者：`/root/keeper_code_review`。

状态：真实首批问题修复后再次复审通过；没有剩余采集代码阻断，非空正式报价、资源费率及扩展 Rent/Return 事件已经独立进程报告验收。修复经过及原审核遗漏完整保留在末尾记录。审核者未向公开节点发请求、未启动服务、未改实现代码或正式数据库。通过范围是采集、恢复与条件报告，成功机会及净利润认证仍受下文数据门槛约束。

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
