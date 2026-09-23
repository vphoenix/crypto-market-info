# 0008：期权实时采集代码独立审核

日期：2026-09-21。审核者：独立 Agent `options_design_reviewer`。本次按 [R4 设计](../docs/arbitrage/strategies/arb-0009-options-collection-design.md) 审核现有 collector 内固定 C/P 与同到期期货清单、10 档盘口、元数据及所选指数。沿用 [0007 离线基础审核](0007-options-code-review.md)，但该历史通过结论不替代本次实时审核。

独立服务/数据库、七天准入、全链 BBO、原生 combo、Greeks/IV 和动态计划已被 R4 撤销或改为按需，不列入本次缺陷。审核者不修改实现文件；独立反例通过 `/tmp` Go overlay 运行。

## R1 实际审核范围

已读取 `internal/options/live.go`、`selection.go`，`internal/optionslive` 的 selection/runner/ingress/engine，Deribit REST/WS/index 适配器，实时 ClickHouse schema/run/minute 及共享盘口读写变更，以及 app/config 接入。重点检查来源时间、秒边界、重连失效、元数据发布、固定成员、指数缺失和提交后可见性。

### R1-01 [P2] 使用进入 sample 的时间冒充实际冻结完成时间

位置：`internal/optionslive/engine.go` 的 `sample`。

首版仅在入口用传入 `now` 判断是否超过 T+250ms，随后所有成员都将这个相同时间写入 CapturedAt。完整订单簿截取、遍历成员及其间调度暂停的时间未计入。入口准时而实际冻结超时的样本仍可能成为有效分钟锚，违反截止约束。

整改要求：逐成员冻结完成后读取可注入时钟，以实际完成时间判定截止；超时须明确无效，指数同样处理，不能以早先入口时间替代。

### R1-02 [P2] 控制请求冷却阻塞读取时旧连接可持续有效

位置：`internal/exchange/deribit/stream.go` 的同步 `send`/读取循环；`internal/optionslive/engine.go` 的连接有效性。

首版 `send(public/test)` 在 WS 读取 goroutine 中同步等待共享 ControlGate。一次 REST 429 的冷却可使此处等待一分钟；期间已设置的 read deadline 要等重新调用 ReadMessage 才会体现，disconnected 事件也尚未发出。engine 只检查 ready，不检查 confirmed 的实际年龄，旧书与 held 指数因此可超过连接确认期限继续有效。心跳按协议应响应 test_request；未响应时服务端可关闭连接，不能以本地尚未读到断开证明有效。[Deribit 心跳协议](https://docs.deribit.com/api-reference/session-management/public-set_heartbeat)

整改要求：在读循环之外执行独立 liveness 判断，超过确认期限即使受影响的盘口/指数失效并重建；冷门书真实源时间不能被连接心跳刷新。

### R1-03 [P2] 注册提交跨分钟会使固定 run 永远缺少首个采样边界

位置：`internal/optionslive/runner.go` 的 Prepare/Collect；`engine.go` 的初始 next；`ingress.go` 的初始边界。

首版 StartedAt 在 WriteOptionsRun 之前记录，engine.next 取其下一完整分钟；ingress.next 却从 Collect 开始的当前时间生成。若数据库确认或启动跨过该分钟，即使只在 xx:59.999 附近稍慢，入口不再产生 engine 等待的第 0 秒，首个边界就报 `missing options second boundary`，已登记的 run 立即重启。

整改要求：从实际进入采集与 run 起点两者较晚时间确定下一完整分钟，或显式记录错过边界；不能倒填有效秒，也不能依赖提交不跨分钟。

### R1-04 [P2] 元数据请求证据缺少原始范围数量与统一请求身份

位置：`deribit.ScopeResult`、`options.MetadataObservation` 和 `options_metadata_observation`。

R4 §5 要求保存所需请求范围的 URL、请求窗口、hash 及数量。首版保存前三者，但丢弃 DecodeInstruments 的 excluded 结果，未保存原始/接受/排除计数，且为同一响应中的每个成员单独生成 AttemptID。历史行不能直接说明一份 scope 响应包含多少对象、哪些选中成员属于同一复核请求，部分响应证据也无法按统一请求身份检查。

整改要求：在现有定类型模型中补充共同请求身份和 scope 计数或等价证据；成功解析时记录可核验数量，失败/解析未知不要伪造完整计数。无需引入 R3 的全链目录或动态计划框架。

## 首轮整改进度与独立验证

R1-01～03 已在审查过程中由作者修改，审核者重新读取实现并执行三个 `/tmp` 独立用例，均通过：

- 逐书与指数冻结后检查可注入时钟；完成于 T+251ms 的第 0 秒不能形成有效锚。
- sample 独立检查连接确认超过 30 秒，失效相关书/元数据或指数并请求重连，避免 ControlGate 等待保留旧有效状态。
- `newEngine` 使用实际 Collect 起点，启动跨过计划第 0 秒后顺延下一完整分钟；此前边界可忽略，不因缺首秒重启。

```sh
GOCACHE=/tmp/crypto-options-review-go-cache go test \
  -overlay=/tmp/options-live-review-overlay.json ./internal/optionslive \
  -run '^TestReview' -count=1
```

作者同时将失败/遗漏元数据的复核重试缩短为一分钟，避免单次失败等待原 30 分钟周期。上述记录仅关闭三个具体问题，不代表整体实时代码通过。

## R1 当时结论（历史记录）

**首轮审核当时尚未给出最终通过结论。** 当时代码与回归测试仍在完善，R1-04 待复审；真实数据库实时批次往返/缺行/失败重试、模拟完整 WS 链路和最终冻结版本仍待实际验证。后续整改及最终结论见下文；本记录不把作者报告当作审核者独立完成的测试。

## R2 最终独立复审

审核依据仍为 R4，代码审核时的设计文件 SHA-256：`741a862619a4f1b1c23e1b5dd64f859a007c483162ef92dcf40053f75cb16ebe`。重新检查最终实时实现、协议/恢复测试、存储测试、配置和只读 CLI；没有重新引入已撤销的 R3 范围。该摘要保留为历史审查依据，实施后文档同步另记如下。

R1 四项问题均关闭：

| 问题 | 最终证据 |
| --- | --- |
| R1-01 冻结完成截止 | 每个盘口和指数在冻结后读取可注入时钟，T+250ms 后不标有效；独立 T+251ms 锚点反例及仓库回归通过。 |
| R1-02 连接确认超时 | engine 按确认年龄独立失效超过 30 秒的连接，清理相应书/元数据或指数并重建；静默书不刷新真实源时间。独立反例及指数/元数据失败隔离回归通过。 |
| R1-03 启动跨分钟 | 引擎以 run 与实际 Collect 启动的较晚者选择下一完整 UTC 分钟，略过未采集的启动片段；独立跨分钟反例及仓库回归通过。 |
| R1-04 scope 证据 | 每个 ScopeResult 有共同 RequestID，成员观测复用该 AttemptID，并保存 raw/accepted/excluded 计数、scope_complete 和证据 hash。request/parse_error 必须不完整且无计数，missing/definition_changed 基于完整 scope，成员成功要求 accepted 至少为 1；模型反例、REST fixture 及真实数据库往返通过。 |

另核实了实际读写闭环：实时分钟绑定持久化 run 的精确成员/指数集合；run 读取重新加载并校验 spec hash 与 C/P/期货配对；市场状态读取所引用的已存元数据，校验实际状态、观测/发布时间及到期；元数据缺失不能回退成正常盘口。`options-check -profile live -instrument ...` 返回实际规格、所选秒的交易规则及指数，交易规则经过定类型读取与摘要校验。离线 origin、原表编码和旧 50 档回放路径保持兼容。

## 独立执行的验证

1. 全仓 `GOCACHE=/tmp/crypto-options-review-go-cache go test -race ./...` 通过。包含现有 app/config、10/50 档回放及其他采集来源回归；模拟 HTTP/WS 使用本机网络权限，未设置公共行情或数据库集成开关。
2. 最后补充规则查询、scope 状态校验和恢复测试后，对 options/optionslive/deribit/clickhouse/config/options-check 再执行相关 `-race` 测试，通过。实际执行完整/部分 ACK、test_request 响应、指数精确数值/错误字段、规则晚发布、闭市、缺首锚、断序重建、新旧 epoch、失败隔离、配置解析等用例。
3. 在自动创建并清理的本机 ClickHouse 临时库，执行最终 `TestOptionsLiveStorageCommitAndReferences`（1.46 秒）与 `TestOptionsLiveMissingMetadataIsIncomplete`（1.05 秒），均通过：固定成员拒绝缺腿、真实写入/读取、规则往返、提交应答丢失及缺指数/元数据检测。
4. 审核者额外用 `/tmp/options-live-final-review-overlay.json` 执行独立反例。对 live_delta、live_book、live_index、live_commit 四阶段分别注入超时：提交前不可见、提交后的应答丢失仍可读、同内容重试保持原 ID，全部通过（合计 3.92 秒）。混合 missing/disconnected/invalid 的 Nullable 指数往返保持摘要且不影响正常盘口；删除实际元数据后查询返回 incomplete，通过（1.14 秒）。scope 失败不能冒充完整成功，以及前三项时间反例也通过。
5. 最终 `git diff --check` 通过；记录摘要后再次核对，下列审查文件没有变化。审核者未修改实现、服务或生产数据。

## 公共实测的来源边界

作者实际执行并报告 `TestOptionsLivePublicCollection` 通过（131.06 秒）：公共 REST/WS → 真实 ClickHouse → 查询完整分钟，run=`4def5416-93ea-4dcb-a0fe-e93d579f84c2`，分钟 `2026-09-20T18:54:00Z`，28 个盘口各有 60 个有效秒，四个指数有值，临时库已清理。审核者检查了该测试实现，但没有重复发起公共抓取；独立数据库和模拟协议验证如上一节所列。该有限观测不是一整天容量测量或常驻服务状态证明，持续运行与实际空间统计由主任务记录。

作者随后报告第二次公共测试通过（145.99 秒）：run=`c5019724-96fa-4db2-8529-82a96d84dd1b`，分钟 `2026-09-20T19:04:00Z`，28 个盘口各有 60 个有效秒，四个指数有值。该分钟盘口、差量、质量、指数及提交共 48,729 压缩字节；100 次完整 28 成员读取、校验和逐秒回放的 P95 为 144.78ms。详情与测量边界见[实时采集说明](../docs/arbitrage/strategies/arb-0009-options-live.md#已完成验证)。这些公共观测与性能数字来自作者，审核者仅核对报告及对应测试实现；它们不替代整日实测，也不表示常驻采集已经启用。

## 实施后文档同步复核

再次读取同步后的 R4、实时采集说明及存储/运行说明。当前 R4 的 SHA-256 为 `0bc4926e20e09b70d14e651676a8ba32b72ea99c0faee2a58fca22327baa689d`；相对上述历史审查版本，文档同步了已实现状态、32 个盘口硬上限、未提交分钟的缺口语义、实际表和使用方式，没有改变本次审核的实施范围。未发现新增的 P1/P2 文档问题。此轮仅复核文档，未重跑代码测试；下面的实现摘要及最终通过结论保持不变。

## 审核版本与最终结论

本次实现、测试及 fixture 共 57 个文件，按仓库相对路径排序生成 `SHA256 + 两空格 + 路径 + 换行` 清单，其 SHA-256 为 `5e95777ab56b63f11333f3c0f489c44f525835b083bb93d0630f37d4077ef82f`。范围包括本次 app/config/collector 接入、options-check、Deribit、options/optionslive、专项模型/采样/回放及 derivative/options_live 存储文件；不含本审核文档、临时 overlay 或运行说明。

| 关键文件 | SHA-256 |
| --- | --- |
| `internal/options/live.go` | `e6d612571d558d1da48b3bb50d25d1df1ae7613c854a9402de49c406bb98ec24` |
| `internal/optionslive/engine.go` | `f094b75a8886f966bf447f831436dc13d8bf708812ec57c06a3fcc748c088aa6` |
| `internal/optionslive/ingress.go` | `4fa6b48ff96a29e279e625047aac71e7a017c1c86c4be972218e1797feb5e4e4` |
| `internal/optionslive/runner.go` | `a4a832082e7100a17b41cc454e34f884bff5fe9ea8cfa2083f2ac199d1254142` |
| `internal/exchange/deribit/client.go` | `7b725e033834960e5f71667ade7710e72b79143a3fa71e0a390ee0398bfc8806` |
| `internal/exchange/deribit/stream.go` | `70b5a911313d4cbdbf5ddf0bf65c9eae72b8859edd492184a7300ca498506169` |
| `internal/storage/clickhouse/options_live_minute.go` | `6d15c4561f4f6c34af7415609282e681a78595bbf21cf94b20befbf7306cc622` |
| `internal/storage/clickhouse/options_live_metadata.go` | `3575b6350c5566cc3b595d12431e409f23d66fea0a1d4dc6ff8b22c19978f87d` |
| `internal/storage/clickhouse/derivative_rule_query.go` | `69e4755bd6e5bbe335daa252e78080758f7034f6813a7ea125f8ae85b66161f1` |

**最终结论：R4 固定清单实时采集代码审核通过，当前没有未关闭的 P1/P2。** 实现符合在现有 collector 中持续采集公开数据、保存每秒 10 档和必要元数据/指数供以后分析的范围；不增加环境划分、固定运行天数或额外部署审批条件。
