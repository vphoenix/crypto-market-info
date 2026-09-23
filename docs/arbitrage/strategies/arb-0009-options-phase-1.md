# ARB-0009：期权离线数据基础实现记录

日期：2026-09-21。本文保留按[历史 R3 设计](../legacy/arb-0009-options-collection-design-r3.md)完成的离线代码及验证记录；下文能力边界描述该批交付时点。之后按[简化后的 R4 设计](arb-0009-options-collection-design.md)完成的公共实时采集见[实时实现记录](arb-0009-options-live.md)，不再执行原四阶段部署流程。离线代码审核记录见 [0007](../../../discuss/0007-options-code-review.md)。

## 本阶段可用能力

| 组件 | 实现与边界 |
| --- | --- |
| 合约与定点数 | `internal/options`：不可变经济规格、独立交易规则、固定 1e-8 tick/lot、native amount 与合约张数分离；指数记法直接进 Decimal，有长度、指数及整数范围限制 |
| Deribit 解码 | `internal/exchange/deribit`：单 scope 元数据及完整 `book.<instrument>.100ms` 消息；不启动 REST/WS；拒绝重复及大小写别名字段，不把 grouped 视图当连续增量 |
| 内存书 | `internal/orderbook/derivative.go`：完整价位状态、前驱校验、失效后新代次 snapshot、每侧最多20,000档；空书有效，只有 combo 可零价/负价；不截尾伪装完整 |
| 每秒与分钟 | `internal/sampler/derivative_buffer.go`：消费已冻结的连续60秒，验证截止/质量与盘口一致；分钟起点及按价格差量仅10档；无起点仍保留质量 |
| 回放 | `internal/replay/derivative.go`：完整校验位图、差量、质量和逐秒状态；原始 SourceTime 与 SampleTime 分开；缺差量报错 |
| 存储 | `internal/storage/clickhouse/derivative_*`：显式建表、规格/规则写入、离线分钟批次、提交后完整性读取；详见 [存储字典](../../market-data-storage.md#10-期权采集) |
| 只读检查 | `cmd/options-check`：复用已有只读连接，读取指定离线run、instrument、UTC秒；不会建库、建表或启动采集 |

`model.Instrument` 已兼容 `option/option_combo`，但旧 `BookSnapshot` 的正价和双边限制保持原状，旧10/50档模型与回放代码没有替换。

## 离线批次与实时设计的界限

本阶段 `BookEnvelope` 只允许 `origin=fixture/synthetic`，要求显式 run UUID、来源证据 hash、准备时间及已排序的完整成员集合。数据表先写，最后发布 `derivative_book_foundation_commit`；它不表示真实行情已采到。接实时来源时需增加实际run和分钟发布语义，明确来源与采集范围；不再把全链BBO或辅助估值覆盖作为盘口采集前提。

读取先按 `(run_id,minute_time)` 找提交标记，再核对所有成员的质量、锚点、差量数量、delta_bitmap和确定性内容摘要。缺锚点可以是已明确记录的状态；缺应存在的行是 `incomplete derivative batch`。孤立写入不可见；超时重试复用同一原始信封，同键不同内容拒绝覆盖。规格与规则引用在第一条市场数据写入前检查，规则知悉时间不能晚于其发布引用，生效时间不能晚于对应采样秒。

经济规格及组合腿本阶段以一条定类型规格记录保存，期权专用字段为Nullable、组合腿为等长定类型数组，保持规格单行完整；并未使用通用JSON大表。实时元数据获取与来源证据仍需接入；完整费表及全链目录按需增加。首次经济定义的来源hash保持不变，后续相同定义但不同响应hash不会重写首次证据。下单tick或最小量变化只新增交易规则。

当前写入API和registry要求单进程、单writer，进程内由互斥锁串行化；尚未提供可启动的实时options任务。现有 `app` 和原 `InitSchema` 均不调用 `InitDerivativeSchema`。

`DerivativeBook.Current()` 只返回当前状态，`DerivativeMinuteBuffer` 只消费调用方已经冻结的样本。实时接入仍需实现有序接纳和秒边界冻结，保证T之后接纳的行情/规则不倒填到T；R4优先采用单个有序处理入口，不要求预建R3的多套水位协调框架。直接轮询当前书不能证明满足实时截止。

## 已有测试与运行命令

本阶段固定样本含四族期权及配套期货共12个合约，每个一条原始snapshot和紧随change，共24条公开消息。样本来源、时间与限制见 [testdata说明](../../../internal/exchange/deribit/testdata/README.md)。combo使用明确标记的合成数据验证signed/empty编码，真实combo协议/数量仍未验收。

```bash
go test -race ./...

# 连接本机已有 ClickHouse，仅创建并清理自动命名的临时测试库。
CLICKHOUSE_INTEGRATION=1 go test ./internal/storage/clickhouse \
  -run '^TestDerivativeClickHouseRoundTripFailuresAndVisibility$' -count=1 -v

# 各生成一天10档合成数据，分别测活跃与静默输入，不访问交易所。
CLICKHOUSE_INTEGRATION=1 OPTIONS_DAY_MEASUREMENT=1 \
  go test ./internal/storage/clickhouse \
  -run '^TestDerivativeClickHouseDayMeasurement$' -count=1 -timeout 10m -v
```

如默认Go缓存目录只读，可加 `GOCACHE=/tmp/crypto-options-go-cache`。完整回归的本机httptest端口以及ClickHouse集成测试需要本机网络权限。

已存在的离线批次可这样检查；下例占位符必须替换为该批次的实际值，当前没有常驻期权测试库：

```bash
go run ./cmd/options-check -database '<existing_offline_database>' \
  -run '<run_uuid>' -instrument '<instrument_id>' -at '2026-09-20T00:00:12Z'
```

输出明确携带 `offline_book_foundation_v1`、origin、evidence hash及批次ID，不将离线fixture时间当当前行情。

## 本轮验证结果

2026-09-21 在本机完成以下检查；数据库测试只使用自动命名并清理的临时库。

| 检查 | 结果 |
| --- | --- |
| 全仓库 `go test -race ./...`、`go vet ./...` | 通过；最后一次共享引用校验调整后，对全部受影响包补跑测试与 race 检查，均通过 |
| 期权 ClickHouse 往返与故障恢复 | 通过；覆盖部分写入不可见、提交超时重试、冲突拒绝、缺差量检测、未来规则拒绝和只读连接 |
| `TestClickHouseBookDepthMigrationAndMixedHistory` | 通过；真实数据库验证旧50档、新10档及混合历史 |
| `options-check` | 从临时库成功读取合成分钟，输出离线来源，采样时间与原始行情时间分别保留 |
| 原设计审核 Agent 的独立代码复审 | 通过；首轮3项P2全部关闭，详见 [R2审核记录](../../../discuss/0007-options-code-review.md#r2-最终独立复审) |

### 整日合成容量与查询测量

活跃、静默各一条交易流、一天1440个分钟批次、买卖各10档，经真实writer逐分钟写入并完成合并后统计压缩数据字节。活跃输入每秒改变买一数量；静默输入分钟内不变，每分钟设置新的样本基准。质量和提交记录均计入，静态instrument/规格/规则行不计入。

| 数据 | 活跃：压缩字节 / 行数 | 静默：压缩字节 / 行数 |
| --- | ---: | ---: |
| 分钟快照 | 106,727 / 1,440 | 106,672 / 1,440 |
| 秒级差量 | 154,659 / 84,960 | 0 / 0 |
| 分钟质量 | 1,820,964 / 1,440 | 814,009 / 1,440 |
| 离线提交标记 | 200,434 / 1,440 | 200,393 / 1,440 |
| **合计** | **2,282,784（约2.28 MB/天/流）** | **1,121,074（约1.12 MB/天/流）** |

整日写入测试通过，用时590.68秒。以上为 `system.parts` 的压缩数据字节，不是完整磁盘分配量；质量字段是这组样本的主要占用。MB按1,000,000字节计算。

查询计时使用最终代码另行复测：在临时库批量预装相同的整日有效批次，抽取100个分散分钟，计时包含完整单成员信封读取、规格/规则引用校验及指定秒回放；批量预装不作为writer吞吐量测量。

| 合成输入 | P50 | P95 | P99 |
| --- | ---: | ---: | ---: |
| 活跃 | 21.40 ms | 31.49 ms | 34.61 ms |
| 静默 | 26.02 ms | 31.54 ms | 42.48 ms |

这些结果只代表本机离线单流样本，未覆盖全链BBO、多成员查询、真实市场活跃度及实时负载；不能直接按合约数外推实际运行容量或延迟。

## 后续实时接入状态

上述实时接入已按R4实现：固定C/P及同到期期货集合、所选元数据/规则获取、公共WS及恢复、有序秒采样、指数和真实分钟写入/回放。配置及实际验证见[实时实现记录](arb-0009-options-live.md)。

不新增options隔离数据库或专用验收服务，不要求先做动态集合预热、全链BBO、原生combo、完整费用/保证金公式，也不再以七天稳定性验收作为开跑前提。现有代码、测试及必要的数据校验保留。

原离线批次的测试结论保持不变。实时链路另有独立测试；本阶段完成时常驻服务尚未启用期权，当前启用状态见[实时实现记录](arb-0009-options-live.md#当前启用记录)。
