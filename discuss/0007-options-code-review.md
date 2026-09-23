# 0007：期权采集第一阶段代码独立审核

日期：2026-09-21。审核者：独立 Agent `options_design_reviewer`。本记录保留首轮问题及之后的实际代码复审；设计审核历史见 [0006](0006-options-collection-design-review.md)。

## 范围

依据已批准[历史 R3](../docs/arbitrage/legacy/arb-0009-options-collection-design-r3.md)的 §12 第一阶段，审查经济/定点模型、Deribit 离线协议解析、全深内存盘口、10 档分钟缓冲与回放、专项 ClickHouse 离线批次及查询、`options-check`。不把当时规划的后续 live discovery、跨连接截止屏障、quote/analytics/index Runner 尚未实现列作本轮缺陷；本阶段不等于实时期权采集已完成。

后续设计已按用户要求简化为[R4](../docs/arbitrage/strategies/arb-0009-options-collection-design.md)，本轮已审代码未因该设计调整而改动。本文保留原复审结论，不将它扩展为R4或未来实时实现的审核结论。

审核者没有修改实现代码。复现测试通过 `/tmp` 文件和 Go `-overlay` 注入，未覆盖工作树文件。原生产入口是否默认关闭、旧 50 档回放兼容、空书/有符号价、源序列、规则身份与时序、缺行检测和稳定重试均在检查范围。

## R1 结论

暂不通过。首轮发现以下 3 项 P2；整改后的结论在本记录末尾另行追加，不能仅以作者回复关闭。

### R1-01 [P2] 缺失分类字段会把永续合约接受为交割合约

位置：[metadata.go](../internal/exchange/deribit/metadata.go)，`DecodeInstruments` 的范围分类与 `normalizeMetadata`。

首轮实现只用 `settlement_period == "perpetual"` 排除永续，没有要求该字段存在且属于已知枚举；其余 `kind=future` 默认规范化为 `MarketDelivery`。用真实 BTC future fixture 的首行构造 `instrument_name=BTC-PERPETUAL`、远期 expiration，删除 `settlement_period`，解码返回一个 accepted delivery，无错误且无 excluded。这使缺失证据被解释成已知可采范围，违反严格分类要求。同一位置还丢弃 `is_active` 值，仅要求任意非空 state 字符串，会让后续状态冻结缺少真实证据。

整改要求：分类前严格验证决定分类的 settlement period；缺失/未知值必须失败。保留 active 事实并验证已知 state 域，不凭缺失字段补成可采合约。补 missing/unknown period、正常 perpetual 排除、false active 和未知 state 用例。

### R1-02 [P2] 规则引用只检查归属，能采用尚未知悉或尚未生效的规则

位置：[derivative_writer.go](../internal/storage/clickhouse/derivative_writer.go)，写入第一条市场数据前的规则引用校验；[spec.go](../internal/options/spec.go) 的 `TradingRule`。

首轮 writer 对所引用 `trading_rule_id` 只查询 `instrument_id`。批次本身只检查 `RulePublishedAt <= T`，因此可以把数据库中 `known_from > T` 或 `effective_from > T` 的规则配上伪造的较早 RulePublishedAt，仍提交成有效历史秒。该缺口不依赖尚未实现的 live sequencer：当前离线 writer 就公开接受独立构造的 envelope 与 rule。

此外首轮 TradingRule 未携带 effective-time basis，数据库写入却恒定填 `first_observed`，而校验允许 EffectiveFrom 与 ObservedAt 不同，会使该已落地列不忠实反映事实。

整改要求：所有引用在数据 insert 前同时校验归属、`known_from <= RulePublishedAt <= T` 和 `effective_from <= T`；保存并校验真实有效时间来源。增加 future-known、future-effective、伪造早发布及允许正常已生效引用的数据库测试。

### R1-03 [P2] 大小写别名绕过重复 JSON 字段检查并覆盖身份

位置：[json.go](../internal/exchange/deribit/json.go)，`jsonValue` 与后续 `json.Unmarshal`。

首轮预检查仅按原始字符串查重复键，但 Go 标准 JSON struct 解码会不区分大小写匹配字段。复现 `{"instrument_name":"BTC","Instrument_Name":"ETH"}`，`decode` 返回成功，最终 instrument 为 ETH。因此未知扩展字段并非总是被忽略，大小写别名可覆盖已验证字段；此问题同样涉及 change_id、时间、scope 元数据等，破坏严格身份/序列输入。

整改要求：对实际已知字段采用严格键名匹配，或至少拒绝会映射到同一字段的大小写冲突；同时阻止仅出现错误大小写字段时被当作必填字段。补顶层和嵌套 metadata/book 用例，仍允许真正不冲突的扩展字段。

## R1 验证记录

审核者独立执行以下相关包测试，首轮均通过（ClickHouse 集成测试未设置开关，不能据此声称数据库测试通过）：

```sh
GOCACHE=/tmp/crypto-options-review-go-cache go test ./internal/options ./internal/exchange/deribit ./internal/orderbook ./internal/sampler ./internal/replay ./internal/model ./internal/storage/clickhouse ./cmd/options-check
```

随后执行独立反例：

```sh
GOCACHE=/tmp/crypto-options-review-go-cache go test -overlay=/tmp/options-review-overlay.json ./internal/exchange/deribit -run '^TestReview' -count=1
```

`TestReviewMissingPeriodAdmitsPerpetual` 与 `TestReviewCaseAliasReplacesIdentity` 均失败，分别确认 R1-01 与 R1-03。R1-02 通过逐层检查现有公开写入路径确认，整改后需用真实 ClickHouse 再验证拒绝发生在第一条市场数据 insert 前。

## 已检查且未发现本轮阻塞项的部分

- 新实现独立于旧非空、正价格盘口模型；完整 L2 状态仅在采样时裁到 10 档，空边/空书、有符号组合价由专项模型与回放处理。
- 严格定点解析没有经二进制浮点数；序列断档和协议错误会使书无效，重建要求新订阅 epoch；`Current()` 和 buffer 明确不是实时 as-of 截止器。
- 分钟零秒缺锚时仍保留完整 60 槽质量并禁止回放；差量以价格为键、相对最后有效采样；`delta_bitmap`、成员 hash 和完整 envelope 查询可检测丢失差量，避免伪装无变化。
- `InitDerivativeSchema` 是显式调用入口，原 `InitSchema`/生产 app 未自动启用；foundation commit 限定 fixture/synthetic，与未来完整实时 OptionsCompletedMinute 分离。
- 先数据后 commit，查询只认确定 batch 的完整成员；提交超时后的相同重试身份、缺行、只读查询和真实压缩占用仍须以本轮正在增加的数据库测试实际结果为准。

## 后续复审

作者正在整改；本文件当前尚未给出代码通过结论。

## R2 最终独立复审

重新读取实际修复后的解析器、规则模型、writer/query 共享引用校验及二进制写入路径，并独立执行测试。以下为本次冻结代码的关键文件 SHA-256：

| 文件 | SHA-256 |
| --- | --- |
| `internal/exchange/deribit/json.go` | `eeb18bbd47b588ba4ec1577971cdf3c2ffee55c5272d32908c522a501f247339` |
| `internal/exchange/deribit/metadata.go` | `520dbf159602af78d3e89d2915ca3d99673af35446270dbb001294f26c638668` |
| `internal/options/spec.go` | `26799829d8f50c29f3d31d04f098fdad41f7ad9a2419e03ea5bed1ad315dcd31` |
| `internal/storage/clickhouse/derivative_writer.go` | `ecd4c386c74a9c197a996c275d937630a890015caaabc027777007edf560b9eb` |
| `internal/storage/clickhouse/derivative_query.go` | `345e51c7e919ef07bc6a6e9761583f701f9f3db05c2dbcbcfef71bb88859602b` |
| `internal/storage/clickhouse/derivative_spec.go` | `c8acd2c8f5b83e17dabc4e775db33920c886acb960fbda9ec2b9a814502b5958` |

本轮审查范围内代码及 fixture 共 33 个文件，按仓库相对路径排序，逐行生成 `SHA256 + 两空格 + 路径 + 换行` 后的清单 SHA-256 为 `59f5088e16125d69749ced2850d2d2bf40ba5feeb521644bfc09571e72811e1c`；此摘要不包含审核文档、临时 overlay 或正在收尾的阶段说明。

| 问题 | 复审证据与结论 |
| --- | --- |
| R1-01 范围分类 | 关闭。period 在分类前须属于 day/week/month/perpetual，缺失和未知值失败；active 采用必需 bool 并保留到结果，state 须为已知值。原独立缺失 period 反例已通过，新增状态用例通过。 |
| R1-02 规则时序 | 关闭。TradingRule 显式保存并校验 effective-time basis，first_observed 要求生效时间等于观测时间。写入与查询调用相同引用校验，验证 owner、known_from 不晚于发布时间、effective_from 不晚于对应秒；批次本身校验发布时间不晚于秒边界。真实数据库测试拒绝 future-known/future-effective，拒绝发生在第一条市场数据 insert 前。 |
| R1-03 JSON 别名 | 关闭。反射收集协议字段的确切键名；按 Unicode SimpleFold 规范键检查重复，已知字段错误大小写即使单独出现也失败。metadata 每行重新严格解析，避免 RawMessage 绕过字段校验；普通未知扩展仍可解析。原独立覆盖身份反例、单一错误大小写和 Unicode 别名用例通过。 |

独立测试结果：

- R1 的 `/tmp` 两个反例重新执行，均通过。
- 对 options、deribit、orderbook、sampler、replay、model、clickhouse 和 options-check 执行 `go test -race`，均通过；包含现有新 10 档及旧 50 档相关回归。options-check 编译通过，无独立测试文件。
- 在自动命名并清理的本机临时 ClickHouse 库，独立执行 `TestDerivativeClickHouseRoundTripFailuresAndVisibility`，通过（2.38 秒）：真实 DDL、写入/读取、故障后可见性、相同重试、冲突拒绝、缺差量和只读连接检查均执行。
- 额外以 `/tmp/options-review-storage-overlay.json` 注入审核专用数据库用例，通过（0.56 秒）：期权 strike=`12345.123456789012345678` 与 contract_size=`0.123456789012345678` 经 PrepareBatch 精确往返；future strike 保持 NULL，未补零；已提交规则的 known_from 在临时库人为改成未来后，读取返回 `incomplete derivative batch`，确认 query 侧共享校验确实生效。

数据库测试首次受沙箱本机网络限制而失败，在授权的本机网络执行权限下重新运行通过；仅操作测试自动创建的临时库，没有修改生产数据。审核专用反例没有写入生产实现或仓库测试文件。

最终结论：**第一阶段离线基础代码审核通过，当前没有未关闭的 P1/P2。** 新数据每秒保存 10 档、旧 50 档回放兼容及公开数据边界保留；生产入口仍默认关闭。

通过结论仅覆盖当前离线 foundation profile。每日合成压缩占用由作者单独运行并记录，不在本次独立复审中重复测量；合成量测与离线 fixture 均不能代替后续实时 ingress/control/lifecycle 截止、真实 combo 单位、全链容量及长期稳定性验收。
