# Reserve 数据采集代码独立审核

日期：2026-10-02。审核者为独立 `reserve_code_reviewer` Agent；作者负责代码、测试、建表和运行。审核者没有修改实现，也没有启动交易或生产服务。

状态：R2 与后文 R3 运行修正复审均已完成。对于当前单 DFX、固定 Uniswap v3 白名单、完整数据库事实和 JSON 摘要的首版范围，未发现尚未解决的阻断上线问题。审核通过指数据采集首版可以继续观察，不是套利可执行性或盈利认证。

## 审核范围

- `cmd/reserve-data/main.go`。
- `internal/reserve` 的固定 ABI、manifest、状态、整数费用、DEX 报价、三类路线、日志、轮询与报告。
- `internal/storage/clickhouse/reserve.go`、`reserve_schema.sql`，及复用的 RPC、日志、收据定义。
- 按项目 AGENTS.md、`docs/reserve-data-mvp-design.md` 和 `discuss/0011-reserve-data-design-review.md` 检查。

## R1 发现与修正要求

| ID | 级别 | 问题与影响 | 最小修正与验收 |
|---|---|---|---|
| R1-01 | P1 | 读取旧 capture 时从所有日志交易重建“当前可用收据”集合。另一批补齐收据后，旧 capture 会读到当时没有引用的新事实，计数/摘要失配。 | 控制表保存定类型实际 receipt 引用；严格按原引用读取共享 immutable receipt。旧 partial capture 在后续补齐后仍能按原摘要查询。已关闭：作者已加 `receipt_refs`，独立运行实 DB 集成测试通过，旧 partial capture 的零收据集合保持不变。 |
| R1-02 | P1 | 活动拍卖每金额档遍历全部有向篮子 pair，六资产即 30 pair；300 RPC 限额易被耗尽，申赎和后续金额档长期缺采。 | 已关闭：Snapshot 共享六个 pair 配额，批量容量探测、按块轮转，再对每个选中 pair 做三档报价；未覆盖计数保留。实际 AuctionQuotes 六 pair/容量/未知 RPC 测试通过。 |
| R1-03 | P1 | 重组虽使旧 capture 不可见，但 live 日志 cursor 不回退，重组替代高度不重新抓取。 | 已关闭：全部旧批失效后，从首个被撤销范围之前恢复 cursor，重抓替代分支日志；同高度不同 hash 亦触发复核。两成功一失败旧批全部撤销及游标回退测试通过。 |
| R1-04 | P2 | 零数量篮子成分仍调用 Quoter，零返回值使整条路线失败。 | 完整篮子向量仍保存，只为非零数量生成买卖腿，期望腿数按实际非零数计算。已关闭：实际批处理篮子测试通过。 |
| R1-05 | P2 | `isDeprecated` 被用于统一阻止全部路线；r5 仍允许赎回 deprecated 基金。 | 状态可用条件与 mint/bid 权限分开，赎回按实际合约权限处理。已关闭：实际批处理篮子测试通过。 |
| R1-06 | P2 | 有效候选之后出现零/无效候选会覆盖成功腿的状态，丢失已取得的有效报价。 | 只在未取得成功结果时更新失败状态；已有报价应继续参加最优值比较。已关闭：成功报价之后零候选的测试通过。 |
| R1-07 | P2 | 默认 report 包含 head，正样本没有 finality/capture_mode 字段；容易把未最终确认的数据用于默认统计。 | 已关闭：默认 finalized-only；显式包括 head 时逐候选保留最终性和采集模式。已复读 CLI/报告实现并编译通过。 |
| R1-08 | P2 | gas 使用向上档位的 USDC exact-output 成本正确，但报告未标该成本是保守上界。 | 已关闭：输出匹配/向上取档标签及所需 wei/采用档位；明确是假设 gas 场景，其余成本未知。GasBucket 向上取档、超最大档 unknown 测试通过。 |
| R1-09 | P2 | 跨批日志去重仅保留首次事件，未检查内容冲突；日志索引用 rune 拼 key。 | 已关闭：writer 和 report 按稳定链上身份比较 emitter/topics/data 等事实摘要，冲突报错；索引按数值编码。已复读两处实现并验证专项测试。 |

## 已核查的正确方向

- `eth_call` 使用同一个 blockHash 和 `requireCanonical=true`；RPC 缺失不会使用旧值回填。
- 固定 r5 的 `getBid` 方向、`toAssets(Ceil/Floor)` 与完整原始篮子顺序。
- mint 费用用大整数逐步复现 checked 乘加溢出；pending fee shares 不重复计入 totalSupply。
- 报价已包括池费，程序没有再次扣一次池费；共享池标记为 `indicative_overlap`。
- facts 先写、capture 提交行最后写；重试复用已有整批身份和时间。
- gas 参考来自购买 WETH 的 exact-output；不是把卖出 ETH 的收入用作 gas 成本。
- 本轮只做公开数据采集，没有交易签名、私钥或自动执行。

## 独立实网预检

预检证据在 `research/2026-10-02-reserve-implementation/reviewer-preflight`，不使用目录价格/NAV 作为报价。

- 官方目录的 Ethereum index DTF 有八个；固定同一 hash 查询 version，DFX、ixEdel、BED、SMEL 为 5.0.0，其余四个为 4.0.0。
- DFX `0x188D12Eb13a5Eadd0867074ce8354B1AD6f4790b` 的链上完整篮子为六个资产，share 与六资产 decimals 均为18；下一拍卖 ID 为18（不能据此推断当前活动）。
- 全六资产和 share 已从官方 Uniswap v3 factory 验证实际池；按最多两路径、最多两跳生成白名单建议。share 只有 WETH 两跳路径，常与篮子腿共用 WETH/USDC 池，因此不少完整独立报价应标 overlap。
- ixEdel 包含多种 vault receipt，当前单 Uniswap v3 场所覆盖可能不足；不得截断其篮子。
- 免费 PublicNode 在距 head 约10,000块的历史日志请求返回 `-32602 Archive requests require a personal token`；大量更早请求还有 HTTP403/429。30天回补不足属于实网能力缺口，不能用近期成功空数组声称30天零活动。

预检只是对象/路径/入口能力核验；不是该程序实网验收，也不是盈利结论。

## R1 独立测试结果

以下命令由审核者独立执行，退出码0；ClickHouse 集成用临时研究库并自动删除，不改生产表：

```text
GOCACHE=/tmp/crypto-market-info-go-cache CLICKHOUSE_INTEGRATION=1 go test -count=1 ./internal/reserve ./cmd/reserve-data ./internal/storage/clickhouse -run 'TestMintFee|TestCanonicalBinary|TestBatchedBasket|TestBestCandidate|TestReorg|TestAuctionRunning|TestReserveIntegration'
```

- Reserve 单元测试通过，覆盖实际 `BasketQuotes` 路径、固定 hash、完整向量、零成分、费用/预算、共享池、成功候选不被零候选覆盖、重组全部尝试与游标回退。
- 临时数据库测试通过：200bit以上整数/Nullable/Array(Tuple)精确回读；无 capture 的半批不可见；重复写入与旧 revision 不复活；共享 receipt 后补时旧 capture 引用/摘要不改变，首次 available_at 不被覆盖。
- CLI目前没有独立测试，编译通过。本节是 R1 当时结果；活动拍卖和报告修正的最终核验见 R2。
- 默认 sandbox 不允许 httptest 监听本机端口，网络授权执行后成功；这属于测试环境差异，不是程序失败。

## R2 最后复审补充

作者按反馈限定首版为“完整数据库事实 + JSON 摘要”，设计中的三份 CSV、候选窗口/下一块变化分析及历史权限重建明确延后；这必须同步设计/实施文档，不将缺采解释成无机会。

追加两个小型身份/错误解析问题：

- **R1-10 / P2：有效 manifest 身份必须包含 ABI 与规则版本。已修复。** `ManifestIdentity` 已包含 collector_version、原 manifest 文件 hash 和嵌入 ABI hash；白名单补了明确的资产/份额 decimals 及转账语义假设。不是仅凭路径存在就宣称已做完整转账模拟。
- **R1-11 / P2：同组 RPC 的混合错误不能互相归类。已关闭。** 仅当全部 error 都为明确合约 revert 才分类为 contract_revert；混合错误保留 unknown。解码失败且没有 RPC error 的 pair 也保留 unknown。独立 `TestMixedRPCErrorGroupRemainsUnknown` 及实际拍卖未知 RPC 测试通过。
- **R1-12 / P2：事件与待开始拍卖的采样触发。已关闭。** 篮子、费用、拍卖、投标权限及 unknown 事件触发立即采样；未来 startTime 但尚未 end 的有效拍卖逐块监控，真正报价仍要求已经 start；篮子毛正触发至少十块加速观察。已读取 Watch 实际调用路径，事件和未来拍卖测试通过。
- **R1-13 / P2：多 Folio 的报价身份冲突。已关闭。** QuoteId 原来仅包含 batch/route/budget；现在还包含 Folio。独立两 Folio 同路线/金额身份测试通过。

其余修订已实际读取：默认报告 finalized-only，显式 head 候选含最终性/模式；gas 匹配/保守上档标签及数量；日志在 writer 和 report 比较稳定事实摘要；当前 `BasketQuotes`/`AuctionQuotes` 是实际运行中的唯一实现；六 pair×三档有直接测试；同高度 hash 变化会触发重组复核、全部旧批撤销和日志 cursor 回退；未知历史版本和升级过渡块保留原始日志并标 unknown；完整 receipt 必须包含所选日志。

最终运行逻辑也已读取：日志失败保留失败 capture 并缩小分片重试，游标不跳过；head 不变但仍有欠采范围时继续抓取。Reconcile 对唯一高度批量读取，最终性未变时不写重复 revision。capture 的 received_at 取区块实际接收时间；最终性提升仍保留原事实 available_at。

审核者最后独立执行以下两组命令，均退出码0（网络授权用于本机测试 listener 和隔离 ClickHouse 测试库）：

```text
GOCACHE=/tmp/crypto-market-info-go-cache CLICKHOUSE_INTEGRATION=1 go test -race -count=1 ./internal/reserve ./internal/storage/clickhouse ./cmd/reserve-data -run 'TestMintFee|TestCanonicalBinary|TestBatchedBasket|TestBestCandidate|TestReorg|TestAuction|TestBatchedAuction|TestReserveIntegration|TestEvent|TestSourceError'
GOCACHE=/tmp/crypto-market-info-go-cache go test -race -count=1 ./internal/reserve
```

第一组涵盖实际篮子/拍卖路径、重组、事件/未来拍卖和两个实 DB 集成用例；第二组再运行全部 Reserve 测试，包含最新两 Folio 身份和混合错误修正。没有另写一套未被生产调用的数学函数来替代实际 collector 验证。没有重复执行作者全项目的测试；代码审核者没有独立认证 systemd 的持续运行情况。

### 首版的明确限制

- 30天历史日志仍缺少可用 archive RPC；失败区间有事实记录，当前不能计算可靠历史机会频率或日收益。
- 多腿共享池仍是独立报价拼接，须联合模拟才可判断净盈利；资产转账语义白名单是明确假设，尚未完成逐路线交易模拟。
- JSON 正值是毛正观测；假设 gas 不覆盖 USDT 换入、对冲、MEV、审批及真实打包成本，观测数量不能直接累加为每日利润。
- 窗口/CSV 和历史权限重建延后；本次通过以 `docs/reserve-data-implementation.md` 已公开说明的首版范围为准。

R2 当时检查的文件 SHA256（后续运行修正另见 R3，不表示当前所有文件仍为这些摘要）：

```text
e417a356b617e433d8d6a6acd14419261fb9cacb26f23a56d225731c6b94e2dc internal/reserve/quoter.go
e81ea3c88dcda69ef4b7a2f6d52320781b354ed3691a50ac26fa1d821321a146 internal/reserve/runner.go
1eca3ed6a2bd8f7711611c658f8d251ad8294251cb1b0b1953502c467fe1297e internal/reserve/collect.go
f8e6f6a96c2e64305f7fcc3d02a9d749c25865f199ff7a534bdcfe90d1c80abb internal/reserve/auction_batch.go
0611f5bfe6104d69ce8417c4cafc555ce35a67ade639bf3a30631cb642960fd4 internal/storage/clickhouse/reserve.go
f9dd951e55bdc6405b620be176799f267153cb88337bce1b0722e0900a4213a5 internal/storage/clickhouse/reserve_schema.sql
```

## R3：实网超时后的最小运行修正

作者实网部署发现 PublicNode `rpc_response_read_timeout`，此前 systemd 曾自动重启。R2 的代码通过不能被解释为“持续运行零重启”。本轮仅复审 header 分组、RPC 暂时错误分类和 Watch 重试，不认证服务可用率。

- **五成员 header 小批：通过。** Reserve 专用 `Collector.Headers` 每个请求最多五个 header，复用原 Client 的两 HTTP 并发限制。全部小批完成后，只要任一批失败就返回 nil/error；成功子集不会交给 Reconcile。Reconcile 在取得全部所需 header 前不会写最终性 revision；没有证据时不伪造 canonical/finalized。缩小分组降低单次响应大小，不保证公共节点永不超时。
- **主循环暂时错误重试：通过。** transport/read timeout、截断、HTTP429/5xx 等明确暂时错误保留当前游标，等待两秒后重新读取 latest/safe/finalized，并成功 Reconcile 后再进入采集分支。审核发现过“失败后 continue 回业务循环，绕过重新复核”的问题，作者已改为跳转 poll。最终性冲突、非法 envelope、证据写入、存储错误和任意 `rpc_method_error` 不被自动归入暂时错误。
- **批次提交：通过。** Header/Logs/Snapshot 返回错误时没有调用该批 WriteReserveBatch；cursor 只有在日志批已写成功且覆盖 complete 后推进。日志查询本身失败产生的明确缺采 capture 仍允许保存，游标仍不推进。写入器、实际批次 ID、receipt_refs 及 capture 最后提交的机制未修改。没有用旧报价填补本轮失败。
- **事件采样触发保持：已修复并复读。** `pendingSnapshot` 保存在 Watch 外层，成功写入日志后 OR 事件触发，Snapshot 暂时失败跳转 poll 时仍保留；仅在 Snapshot 批写入成功后按 StateCoverage 是否 complete 清除或继续保留。不会因日志 cursor 已推进而丢掉待采样状态。启动阶段的 HeaderTags/Preflight/Reconcile 仍可因错误退出并由 systemd 重启；“暂时错误不重启”仅适用于进入 Watch 主循环以后。网络失败仍可能延迟或丢失某个历史时刻的报价，不能声称无采样损失。

审核者独立执行下列命令，退出码0：

```text
GOCACHE=/tmp/crypto-market-info-go-cache go test -race -count=1 ./internal/reserve ./internal/dex/ethereum
```

其中失败 header 用真实 transport failure 触发，断言没有 revision、没有游标回退/最终性变更；另检查完整性错误不进入暂时重试。RPC 测试区分响应读取 timeout/truncated 与响应过大，读取失败不伪造成功 payload。对这一运行修正没有再次运行未变的 ClickHouse 专项，也未把测试通过当作实网无缺口证明。R3 没有新增数据安全阻断问题。
