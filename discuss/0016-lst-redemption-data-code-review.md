# LST 公开数据采集程序代码审核

状态：代码最终复审通过，已发现的 P1/P2 均已修复；结论限于公开数据采集 MVP。默认端点已按用户选择切换 dRPC，实时短跑通过，历史日志回补未通过；详见末尾专项记录。日期：2026-10-02 UTC（北京时间 2026-10-03）。独立审核者：`/root/lst_design_review`。

后续部署导航（不扩大本次独立审查范围）：服务随后已[安装并常驻启动](../research/2026-10-03-lst-drpc/service-startup.md)，实际操作与文档补齐见[运行说明](../docs/lst-redemption-data-implementation.md)。下文 SHA 与测试结果记录各轮当时被审版本，后续说明性文档更新不回写为新的独立复审结果。

审核依据：[采集设计](../docs/lst-redemption-data-mvp-design.md)、[DDL](../docs/lst-redemption-data-schema.sql) 与 [设计及限流专项审核](0014-lst-redemption-data-design-review.md)。本审核只评估实现、必要本地测试及设计一致性；不编写采集实现，不运行公共抓取或压力测试，不操作生产库、服务、账户或交易。

## 审核范围

- `internal/lst` 的模型、RPC/Lido ABI、协议状态与定额报价、日志/收据、CEX 资金费与价格、持久请求 gate、runner、离线报告。
- `internal/storage/clickhouse` 新增 LST 存储及 schema，和 `cmd/lst-data`、配置与本次实现文档；共享 Client/reader 中与 LST 可写/只读构造器相关的窄改动也在范围内。
- 排除本任务之前已有的盘口、Reserve、Across 等改动；不因未提交工作树把历史改动归入本审核。

## 初审发现

代码分批产出时已检查模型、批次摘要、ClickHouse 写入、RPC/Lido 身份与协议、报价、CEX 与持久 gate。以下是代码初审发现；修复状态只以实际复读为准，不能将设计通过标成代码通过。

| 级别 | 初审问题与影响 | 最小修正 / 当前状态 |
| --- | --- | --- |
| P1 | HTTP 返回的本地时间有纳秒，而模型要求 UTC 微秒；真实协议成功批次会无法 Seal。 | Response 三个时间已统一 UTC 微秒；已复读。 |
| P1 | RPC Response 的打印 hex hash 被直接写入 `Array(FixedString(32))`，成功报价/协议批次会无法 Seal。 | RPC.Call 已在适配器边界将 payload hash 解码为 raw 32 字节；CEX 保留打印 hash，在模型边界解码；已复读。 |
| P1 | 原来在 gate 预约后才写请求归档；归档延迟可使实际 HTTP 发送间隔短于约定。 | 已将归档前移，预约状态持久后才允许发送；持久化耗时达到间隔则放弃发送，响应/失败后再持久延长 NextAt。新进程首请求另等完整启动间隔，覆盖旧进程在途崩溃。已复读，慢响应与重启回归通过。 |
| P1 | 协议某个 view 失败时，前面已取得的数值没有保存已知 block/hash/time 和来源证据。 | Protocol 已改为具名返回并 defer 汇总真实证据，入口保存已知 head；部分失败保留已知锚点、证据与未知状态。失败路径回归通过。 |
| P1 | 对报价的 freshness 只计算来源时间到它自己的接收时间，忽略其后链上报价计算期间的老化。 | 已增加统一报价结束时间对 CEX 来源时间的 15 秒检验；未提高允许时效。 |
| P1 | 最后的 canonical 检查复用报价已到期的 context，会使完整的早期候选也失去 canonical 标记。 | 已分为 25 秒报价和末尾检查，共享总 30 秒上限；检查失败仍不可视为 canonical。 |
| P1 | `ApplyHedge` 成功写 complete，验证器和查询判断 ok，成功态绕过必要证据验证且无法成为完整候选。 | 已统一为 ok；成功状态枚举及必需证据由验证器检查，相关测试通过。 |
| P1 | CEX 网络失败只留下 RequestedAt；验证器要求三个时钟全 NULL 或全非 NULL，单个 depth 失败会让整批链上/独立 mark 结果无法落库。 | 已允许失败来源保留真实的部分时钟，校验已知时钟的依赖与顺序；独立 depth/mark 失败测试及 request-only 验证通过。 |
| P2 | Followup 经 ApplyHedge 从 q0/current lot 重新计算 qH，可能覆盖参考候选原来的 ETH 短仓量。 | 当前以原 qH 查询买回，校验原 instrument/lot 可表示性；不一致时 unknown，固定 q0/qH 报告回归通过。 |
| P2 | Binance IP 共享权重的 50% 阈值硬编码 1200，未依据本次公开 rateLimits。 | 已读取并持久化实际 REQUEST_WEIGHT/MINUTE 限额；取得之前按研究配额保守暂停，缺失则 Metadata 失败；已复读。 |
| P1 | 缺 live cursor 文件但库中已有提交范围时，只恢复 Next/Start、不设置 Manifest，写出的 cursor 导致下一次启动必然 manifest mismatch。 | 已恢复 manifest，并仅以最新、完整、canonical 提交的连续高度推进游标；backfill 在提交后游标写入前崩溃的恢复测试也通过。 |
| P2 | 历史 Header 强制解析 baseFee，二分实际探测到 London 前时会失败。初报把当前 60 日回补也判定为必然阻塞，影响描述过大。 | Header 已允许历史缺 baseFee 为 NULL，历史 fixture 通过。实际核验头 26105487 的首次二分中点已在 London 后；本项属于历史兼容性修复。 |
| P1 | withdrawals.csv 的 left-censored 行少一列，claim 时间、金额、状态和 gas 都与 header 错位。 | 已改为固定 21 列、显式赋值；已复读，csv 列数回归已补。 |
| P1 | 将报告 cutoff 减请求时间命名为“已观测队列年龄”，日志断档或采集已停止时会冒充等待下界。 | 已改名为 elapsed_since_request_at_report_cutoff_seconds，保留 coverage_unknown；不宣称这段时间均已观测。 |
| P2 | 失败部分时钟放开后，成功态只检查 RequestedAt，仍可能接受缺 Receive/Available 的“成功来源”。 | 已补协议、链上报价、depth 和 mark 的完整成功证据约束；成功缺证拒绝与失败部分证据接受回归通过。 |
| P2 | 固定 100k 优先会耗尽公开 RPC 的有效报价时间；仅轮换其余三档仍会饥饿。 | 实网发现后已将四档全部按分钟轮转首位，每档约四分钟一次优先机会；UTC 12:00—12:04 日种子窗口优先 100k。调度/种子回归通过，不加速或放宽时效，不合成跨分钟容量。 |
| P1 | Followup 排在全部八条 entry 之后，协议及 entry 调用可耗尽报价预算，同量未来退出长期 unknown。 | 当前 due followup 在 entry 之前；保留所有八条 entry 身份，超时仍 unknown。超过两分钟的 followup 标 missed，不补跑过期行情。 |
| P1 | 新 CLI 误用 OpenDEXReader 作为 collector writer。该构造器 readonly=1，且未设置 maxAttempts/writeTimeout；现有 retryWrite 在 maxAttempts=0 时不执行写入并返回 nil，pending 可被误删，形成“成功但零落库”的数据损失。 | collector 已使用 OpenLSTWriter，只打开已存在数据库并设置写入/重试默认值；LST 写入口拒绝 readonly、零 attempts 或零 timeout。报告仍 reader；已复读专用构造器和隔离写读测试，实际执行结果由根实现者记录。 |
| P2 | EnrichGas 已实现但未接入 runner，设计中的有限真实 gas 样本始终缺失。 | 已接入 watch 每五分钟的有界 quiet 窗口及显式 backfill 尾部。完整结束 UTC 日后每天每类两 tx、每轮最多十 tx；新 UUID 补证保留原证据根。选样、断档、失败零提交和不重复收据测试通过。 |
| P1 | 普通 Gob 会把 `*bool(false)`、`*uint64(0)` 等已知零值指针解码为 NULL；真实 pending 因行 hash 不符无法恢复。 | 新 pendingEnvelope 显式保存全部 nullable 存在位，解码后整批 Validate；known zero/NULL/raw bytes/重试身份回归通过。旧失败 pending 按原证据仅恢复两个 false，不重新 Seal，原行 hash 与 FactDigest 精确匹配，原字节与恢复审计保留。 |

另已提醒实现者：raw 地址/hash 是任意二进制字符串，不能直接 JSON Marshal 做 pending/seed 文件或行身份。当前 CanonicalHash 使用长度分隔的原始字节；pending 用保留 nullable 存在位的二进制 envelope，seed/progress 也不将 raw 字节经 JSON 改写。大整数、Decimal、NULL、无效 UTF-8 字节的身份回归通过。

## 必须纠正的前次设计审核结论

前次设计审核 `0014` 第 56 行把 `WithdrawalsFinalized` 源字段范围认作 `(from,to]`，该结论错误并撤回。根实现者重新检查已归档的官方 Lido v4.0.1 `WithdrawalQueueBase._finalize`，独立复核确认 [源码第 344 行](../research/2026-10-02-lst-design/lido-withdrawal-base.raw.sol) 计算 `firstRequestIdToFinalize = lastFinalizedRequestId + 1`，第 351—358 行 emit 直接使用这个首 ID。

所以源 event 范围是 **`[from,to]`**，`from==to` 是合法的单请求 finalization，`from>=1`；源 from 字段必须原样存储，不能偷偷减 1。最终复读确认设计、两份 DDL、解码器、验证器、报告和 fixture 已同步。`[id,id]`、首 ID/末 ID 命中和后一 ID 不命中的边界回归通过。源事件没有 `maxShareRate` 的结论仍成立。

## 复审与验证

独立审核者最终运行以下仅本地测试，无公共抓取、压力测试或生产操作：

```bash
CLICKHOUSE_INTEGRATION=0 GOPROXY=off GOCACHE=/tmp/lst-review-go-cache \
  go test -race ./internal/lst ./internal/storage/clickhouse ./cmd/lst-data -count=1
```

结果通过：`internal/lst` 3.531s、`internal/storage/clickhouse` 1.054s；CLI 编译通过，尚无独立 CLI 单元测试。所测版本已含最后的四档轮转、due followup 前移、gas/资金费/metadata 调度、deadline 不发送、writer 与 pending 修复。测试覆盖 gate 冷启动/稳态/滚动配额/持久冷却/重启/封禁/慢响应、严格整数/Decimal 与来源独立失败、空日志证据、inclusive finalization、重试冻结身份、恢复连续性、gas 选样补证、报告删失/coverage/资金费方向和 unknown 成本。该命令关闭 ClickHouse 集成测试，不能冒充实际数据库验收。

根实现者/存储实现者另在隔离 ClickHouse 运行了真实 UInt256/NULL/Decimal/raw FixedString 往返、同批重试/修订、事件收据补证和资金费冲突测试；根实现者运行全仓测试与 vet，并做有限实网 CLI 验证。这些执行结果由[实现验收记录](../research/2026-10-02-lst-implementation/validation.md)留档，独立审核者检查测试与恢复审计，不宣称亲自操作数据库或公共接口。

真实旧 pending 的[恢复审计](../research/2026-10-02-lst-implementation/pending-recovery.json)保留 capture `f162f073-eafb-468e-bbc0-b9bd64557b9f`、原文件 hash、归档来源 hash 与新文件 hash。恢复仅补 `queue_paused=false`、`bunker_active=false`，原 protocol row hash `0x7cbbb029b4143b33cef2ecccd4e8fba9b8e376c953aeddde5277c276f4b69647` 和 FactDigest `0x61dae9845392ae8041e8a82b465cf4759d2001693ccda5d9be0f6f3abb520e72` 不变；整批验证及新 envelope 回读匹配，不重新取得行情。

## 最终结论与剩余限制

本次复审未发现未解决的 P1/P2 阻塞，公开数据采集 MVP 通过。七张专项表及现有 instrument 身份、只读来源、单 CLI/状态目录、显式回补和离线 CSV 报告符合首版范围。

- 限速约束覆盖本 CLI；同出口其他进程及供应商不可见配额仍在范围外，不能据此保证不会封 IP。跨模式锁、持久冷却、单成员 RPC、串行来源、预算耗尽 partial 保持不变。
- 四档轮转给每档实际取得数据的机会，但不同分钟不构成同一时刻容量；due followup 优先时仍可能使部分 entry unknown。低速公共端点的长期覆盖和稳定吞吐尚未验收。
- head 到 finalized 的复核不改来源时间、报价结束时间或事实 hash；未确认、孤块、缺锚点和过期报价不提升为有效历史样本。短跑不能替代等待分布、完整日 gas 或长期重组恢复的实网观察。
- 根实现者最后的有限历史回补 `26104300..26104811` 在 queueIdentityAt 收到默认 PublicNode 的 `historical state ... is not available`（-32000）。[原始错误证据](../research/2026-10-02-lst-implementation/historical-provider-error.json)已归档；该范围保持 failed，不生成事件事实、不推进完成游标。可读历史头不等于可读历史合约状态；完成 30 日回补需要支持目标高度历史 `eth_call` 及 EIP-1898 blockHash 的来源。当前默认端点的完整历史回补尚未通过实网验收。
- 资金费 API 分页完整不等于持仓窗口成本完整；保留缺结算点/mark/实际持仓覆盖的 unknown。等待报告保留未完成请求及左/右删失，不把已完成样本百分位当全部等待。
- 需要拆分多个赎回请求时首版未核验逐笔舍入，conversion 保持 unknown。实际手续费、保证金、gas bootstrap、资金占用和未来基差未齐，净收益/APR 留空；尚未证明可稳定盈利或年化至少 3%。

## 审核版本快照

以下为最终关键 SHA-256。完整 34 文件集合是排序后的 `cmd/lst-data/*.go`、`internal/lst/*.go`、下列 ClickHouse 六文件、三个 LST 文档、`docs/market-data-storage.md` 和配置；以逐行 `sha256  相对路径\n` 连接后的集合摘要为 `203a423717c6e479407ceb430e9e7fb290c75d3507ec2dd73c9ad948381d4324`。race 测试后的唯一集合内变动，是实现文档新增上面的历史来源实测限制，代码字节未变；测试时集合摘要为 `0a41a84cde14efc3383b7cd83d75272f312d1445cda98b9467b30e930119093f`。共享文件 hash 表示此时整个文件字节，并不扩大审核到其中已有的其他业务。

| 文件 | SHA-256 |
| --- | --- |
| `cmd/lst-data/main.go` | `cf834f67077be55d8761e8d798264556099d99c401d98373d3c4c527ee354197` |
| `internal/lst/collector.go` | `f7a2afd60cb7fbc90356ad92b712c96b6316755c88cc81e3eaf9e2621dac3abd` |
| `internal/lst/runner.go` | `7b3e5cf4d4ad1b52133ecd70729fc1b3e74d6a29a2ed4b9bfd6170de16b0d6de` |
| `internal/lst/transport.go` | `319e9e2ac4ccf3c935e620babc90129d065df3a26ddc64aad8897f5da8467935` |
| `internal/lst/pending.go` | `19921a9cd7d62758725aa760b2e7f29d1b8cc49dc315659613374593dbf7f105` |
| `internal/lst/batch.go` | `f938a9f51c19a77fbdc9a52ece1cf4c0505ef11f2030389a52bc0ce24177fe90` |
| `internal/lst/report.go` | `492aa967ca1a3ccaecee17b7bf067a1ebc27f9dc4fe95db1350783bcdd30efc8` |
| `internal/lst/gas.go` | `4320a9b957c32250244e7f80f822a69a0ec0eb195a8ffb5facdd314ca2f1fa9a` |
| `internal/storage/clickhouse/lst.go` | `cc88ea4d2eb8c0f5aa6223f0211513d7333cc203d7db57da1adfac4e7ef852b3` |
| `internal/storage/clickhouse/lst_schema.sql` | `a2b1f3dbc9160876b7fd94557a1b6d2046a00a4aa8da061c9ea44620981b6df3` |
| `internal/storage/clickhouse/lst_integration_test.go` | `8dcebb6c64e26d9bcaad37b176ae74f6b9bea135eebdbd14aff89e42e19247ad` |
| `internal/storage/clickhouse/client.go` | `8bf2a03fe2e2128c86c8ac392b78d445ff6d7bfa63c2245138fa0cbb12565c82` |
| `internal/storage/clickhouse/dex_reader.go` | `9faf5a97dc1b920a28c3e879131dec6464ef8545e87a6776b8e3993fd0fa95db` |
| `internal/storage/clickhouse/perpetual_check.go` | `5fea0fd24309916747e8818640f7a9f9fc42e19222d9f5a8a781f1c20fb5c0ea` |
| `config/lst-lido-ethereum.json` | `cdb1ab2cd55bf49b022210f1f13548f21b91667ee43253504a4403caf1ebea61` |
| `docs/lst-redemption-data-mvp-design.md` | `9f2766a1d0c69c9b872df375d2b66d14322d895181dd6c3df1c50e3114e7b72f` |
| `docs/lst-redemption-data-schema.sql` | `43daebb17f1568013aed1e72ec658149047316571cb421049f65662de1f355d8` |
| `docs/lst-redemption-data-implementation.md` | `87229dd67aecf9bffe60114cea3b717fb4d66f86d72e0535106a2a54034f7d7d` |
| `docs/market-data-storage.md` | `c65ee94e2d065cc3c7cf9bffc96fac7f04995438dafa95b6a601a7628947fbcf` |

## 2026-10-03 默认 RPC 切换专项复审

用户指定 `eth.drpc.org` 后，LST CLI 默认值改为 `https://eth.drpc.org`，原 `LST_RPC_URL` 覆盖保持有效。此次只读复审结论：通过，没有新增 P1/P2。

独立核验将当前 `cmd/lst-data/main.go` 中 dRPC 默认 URL 还原成之前 PublicNode URL 后，整个文件 SHA-256 精确恢复到上一轮 `cf834f67077be55d8761e8d798264556099d99c401d98373d3c4c527ee354197`，确认实现改动仅这一默认值；其余原 34 文件集合内只有实现文档改变。请求仍通过原 Transport 的持久 gate、来源身份核验和失败语义；没有模型、schema、配额、重试、启动节奏、状态锁或交易范围变动。`report` 分支仍在构建 transport 之前返回，因此离线报告不因默认 URL 改变访问网络。

独立运行 `CLICKHOUSE_INTEGRATION=0 GOPROXY=off GOCACHE=/tmp/lst-review-go-cache go test ./cmd/lst-data -count=1` 编译通过，CLI 本身仍无单元测试。没有运行公共探针、采集程序、数据库或服务。由于代码只改一个默认字符串，没有重复此前全部 race/存储集成测试。

[实现说明](../docs/lst-redemption-data-implementation.md)与 [runtime LST 段](../docs/runtime-operations.md)已同步端点，保留旧 PublicNode 失败事实。[已归档来源探针](../research/2026-10-02-lst-implementation/public-archive-rpc-probe.json)显示 dRPC 原失败块及约 60 日前块的目标合约历史查询成功；这些是根实现者的实测，不能代替全部 CLI 路径。完整程序同一 512 块范围的有限复验及退出结果以[切换验收记录](../research/2026-10-03-lst-drpc/validation.md)为准，不据有限成功宣称 30/60 日已完整采集。切换端点也不会补出本程序尚未实现的历史 entry/followup AMM 报价或 CEX 盘口，因此历史净利润与年化结论仍不成立。

| 本次版本 | SHA-256 |
| --- | --- |
| `cmd/lst-data/main.go` | `7a1a868df219def7e3008693782523652b15c90691cdad379198848748818378` |
| `docs/lst-redemption-data-implementation.md` | `8bd15b872146fd92ef725eb25cd1f66bdaf55022a232907dd1dc17e4f9d70def` |
| `docs/runtime-operations.md` | `289db79ec00277f930903af23226ed8fbcaa8b8f021ffa4b058c493ef07e3ad9` |
| 原 34 文件集合的新摘要（成员规则同上，未加入 runtime 文档） | `b6d7fb2e55523593ecf6193c374b7db99c5417fcad5bdb7f8462e3aa9d078d40` |

runtime 文档的全文件 hash 仅用于定位 LST 段的此时版本，已有其他采集器/部署段修改不在本次审核范围。

## 2026-10-03 HTTP 400 RPC 错误识别与 dRPC 最终收尾复审

本轮结论：**代码修复复审通过，无新增未解决的 P1/P2；dRPC 的历史日志能力验收未通过。** 实时有限采集成功不替代历史回补、连续运行或盈利验收。

真实 `eth_getLogs` 请求为 `26104300..26104811`，含端点 512 块，原始请求与响应已归档。dRPC 返回 HTTP 400 和 RPC code 35，消息却称超过 10000 块；原 RPC.Call 在 HTTP 错误时提前返回，使查询层只能看到 `source_http_400`。本轮最小修复仅在 RPC 适配器识别已归档、完整且恰好对应 HTTP 400 的 JSON-RPC error；版本和请求 ID 必须匹配，code/message 必须存在且类型正确，不能同时带 result。`errors.Join` 保留 HTTP 错误及 RPCError。非 2xx result 不变成成功，429/403/418、body 限流、5xx、网络与归档失败保留原处理。

已独立复读 `rpc.go`、新增 `rpc_test.go`；比对上一阶段集合，确认实现只增加上述适配器处理与测试，Logs、Transport、runner、模型和 DDL 未改。code 35 原文不会命中 ErrLogRange，没有增加泛化 range 匹配、备用域名、逐块扫描或自动反复二分。严格解析未知错误仍保留原 HTTP 失败。

独立仅本地验证，无公共请求或数据库操作：

- 全套 `CLICKHOUSE_INTEGRATION=0 GOPROXY=off GOCACHE=/tmp/lst-review-go-cache go test -race ./internal/lst -count=1` 通过，3.671s，包含 15 种 HTTP/RPC 响应用例。
- 之后新增 `TestBackfillProviderPlanErrorDoesNotSplitOrAdvance` 已复读并用同环境定向 race 运行，通过，1.090s。真实 Backfill 调用路径在六次 mock RPC 后只保存一个 failed、canonical=false、零事件；Next 和 RangeSize 均不变，错误不等于 ErrLogRange，也没有继续请求。

根实现者的[最终切换验收](../research/2026-10-03-lst-drpc/validation.md)、watch/backfill 日志、HTTP 归档统计与数据库结果已只读核对：

- `watch --once` 退出 0，RPC 62 次、Binance 4 次；capture `fb2587fe-33a4-44b2-b627-d1f28d7dadc3`，区块 `26106248`，一条协议 ok、八条 entry 落库。10k USDT 路线 A 买入/转换/退出/对冲均 ok 且 fresh；250k 两路线链上完整但十档对冲容量不足，其他成员有预算缺失。批次仍 partial、canonical=true、finality=head；本次 once 不做后续最终性维护，不能当 finalized 历史行情或已获利。
- 修复后同 512 块有限回补退出 1，RPC 40 次、Binance 2 次；预检/历史 eth_call 成功，但日志 HTTP 400 / code 35 仍失败。落库原 HTTP 与 RPC 错误、failed、canonical=false、零事件，没有把拒绝当空范围成功。
- 全部有限窗口为 154 次 dRPC（145 个 200、9 个 400）和 8 次 Binance 200；无 429/403/418。最短实际发送间隔分别 714487 微秒、5496983 微秒；十二次 getLogs 间隔至少 10002069 微秒。这是根实现者的归档实测，独立审核者没有发起请求；短窗口不证明长期额度。
- 根实现者另运行只读 report，正常退出并导出，[summary](../research/2026-10-03-lst-drpc/report-summary.json) 的三个本轮 capture 全按 head/failed 排除，报价/情景计数为零，profit_status 仍 unknown，不输出 APR 或净利润。复核 report.go 确认这些被排除的 capture 不进入 LSTBatch/Validate，因此本项不代表这三个批次已由报告回读验证冻结成员或 digest。

低频对照仍不能确认拒绝的供应商内部原因。按 blockHash 返回空 logs 只表明该请求形式被接受，尚未证明实际非空事件完整性；不作为逐块历史扫描的依据。[实现说明](../docs/lst-redemption-data-implementation.md)和 runtime LST 段已明确保留 PublicNode 旧失败、dRPC 当前失败及“历史回补未通过”。回补历史合约状态可用，也不会自动补出当前程序未实现的历史 AMM entry/followup 与 CEX 盘口。

本阶段 35 文件集合采用上面的成员规则，新增 `internal/lst/rpc_test.go`；`sha256  相对路径\n` 排序连接后的 SHA-256 为 `a29ae1e1ee17d9519756bde2f8ee762ae28324082928903508166ccf0c61eaa4`。以下 hash 覆盖最终已复读的相关版本；此前各阶段 hash 保留用于审计历史。

| 本轮最终文件 | SHA-256 |
| --- | --- |
| `cmd/lst-data/main.go` | `7a1a868df219def7e3008693782523652b15c90691cdad379198848748818378` |
| `internal/lst/rpc.go` | `ae14f97ea9f85cf2eee78537e3ce8ffb9bf513adcbcc31e5a2031ffb9f301d66` |
| `internal/lst/rpc_test.go` | `8c56008f6073f1a154473f66064d8d22efc313f0c024451e02de7e08cc0d0468` |
| `internal/lst/logs.go`（未改） | `df41f5c36dbb7bf9043e2828e4bd5ec3fa63da27daa1f5f53861ca285f99d2b6` |
| `internal/lst/transport.go`（未改） | `319e9e2ac4ccf3c935e620babc90129d065df3a26ddc64aad8897f5da8467935` |
| `internal/lst/runner.go`（未改） | `7b3e5cf4d4ad1b52133ecd70729fc1b3e74d6a29a2ed4b9bfd6170de16b0d6de` |
| `docs/lst-redemption-data-implementation.md` | `ba6dce837f860ddc95f5651665afbbd51934d18947d2837b3adf7d447719df6c` |
| `docs/runtime-operations.md` | `c8981d012eac3fbdb2fe736d80285eaf09f5d819c783596529ec5a0bb5d25665` |
| `research/2026-10-03-lst-drpc/validation.md` | `6aada24266a3a041fbc0bb047a266cbc0202ac78c7af290b35e10cf39ade7597` |
## 2026-10-03 单金额轮转、收据日志覆盖与错误诊断修复复审

本轮独立代码复审通过，末次复读未发现未解决的 P1/P2。审核范围仅本次 LST 改动：市场调度、`not_scheduled` 语义、RPC/Transport 错误证据、live 日志游标暂停与 receipts 扫描；没有审核其他任务正在修改的 Ethereum、Across 或 Reserve 实现。审核者未访问外部网络、运行采集程序、修改实现代码或操作数据库/服务。

每分钟仅安排一个金额的 A/B，两路线轮转四档，保留八条 entry 身份，其余六条明确 `not_scheduled_this_round`。未安排成员以完整 blank 的 CanonicalHash 校验，不能携带数量、来源时钟、链锚点或 CEX 证据；ClickHouse 空数组与 Go nil 的原有一致编码保持不变。完整 mock 正常路径为 23 次 RPC；未放宽总 30 秒、CEX 15 秒、链/CEX 对齐或 canonical 条件。批次仍为 partial，只有实际完整且及时的单条候选可用于查询，跨分钟不能相加为同期容量。

receipts 模式每轮最多八个 finalized 高度，按逐块 header 的严格 256 字节 Bloom 预筛。阴性证明队列地址没有日志；阳性需取得同一 blockHash 的全部收据，与完整 header 交易清单逐项核对交易哈希/序号/高度/区块哈希、失败交易无日志、全块 logIndex 连续且无重复。对全部地址及 topics 重算聚合 Bloom，与 header 一致之后才筛 queue、解码并写事件；任一步失败清空本片事件、不推进游标。连续父哈希最终连接到再核验的末块 canonical 锚点。缺 Bloom、缺交易/收据、`null`、截断、错误身份和重组不能作为成功空覆盖。

| 本轮发现 | 最小修正与最终状态 |
| --- | --- |
| P1：`IsHexAddress` 接受无 `0x` 地址，Bloom 重算可通过，但字符串队列过滤会丢日志并声称空覆盖。 | 每条地址严格 `ParseHex(...,20)`，Bloom 和 queue 过滤都使用解析后的原始字节；缺前缀拒绝、正常大小写接受回归通过。 |
| P1：中间 header 未验证请求高度，阴性 Bloom 可错误跳过目标块；各块未形成连续父链。 | 每个 header 必须 `Number==n` 且相邻 ParentHash 等于前块 Hash；错误中间高度/父链及成功阴性覆盖回归通过。 |
| P1：`nextLogs=任务结束+一分钟` 配合分钟循环，实际可能隔两分钟扫描，八块上限不足以追赶新区块。 | 调度改为本轮 `start+interval`，每轮最多一次、无积压补发；没有提高请求上限。实际吞吐及追赶速度仍需运行实测。 |
| P2：追加 RPC 诊断后，429/封禁/5xx 的 code/message 可能驱动能力暂停或范围二分。 | 能力/范围分类仅接受 HTTP 200 的纯 RPCError 或 HTTP 400 的原 HTTP 错误；dRPC 特判同时要求 code 35。429/403/418/500 和 HTTP 200 body 限流保留原 gate 语义，回归通过。 |
| P2：中间/缓存末块身份错误的 Reason 可能引用前一块响应。 | 检查高度/父链前设置 `logs_header_identity` 和该 header Response，最后定向回归通过。 |
| 范围收口：receipts 历史 backfill 仍可能初始化为 512 块，超出首版运行范围。 | Collector.Backfill 入口拒绝 receipts 历史模式；CLI 仅允许人工显式 1—8 块缺口命令，Logs 自身也限制八块。没有增加新的历史框架。 |

默认 CLI 仍为 range；unit 显式选 receipts，同一 dRPC 来源和路由不变。可选 `LST_LOG_RPC_URL` 仅供人工配置，没有自动回退；另源须核验 chainId 与 primary 区块 hash。自动 watch 对已知能力拒绝按 endpoint+mode 持久暂停，重启不继续日志请求，保留原游标；人工 range 命令失败即结束，未实现新的历史能力暂停框架。HTTP 400/code 35 的原始 range 拒绝事实仍成立，receipts 成功不证明 range 可用，也不证明全历史可读。

失败原因保留 stage、cause、`http_attempted`、HTTP/RPC code 及打印 hex 证据 hash。`http_attempted=true` 表示已进入 HTTP.Do，不等于证明包已发送；本地 cooldown/配额/轮截止明确 attempted=false。RPC 解析只追加诊断，不清除原限流/禁用；实际网络失败及 partial read 保留已知时间和失败归档，消息不输出原 DNS/连接 URL。用户要求遇到真实连接/DNS/代理/网络超时停止外部测试并告知；本次审核只做离线测试，该要求不能被此处 mock 成功替代。

独立测试在 `/tmp/lst-repair-build` 运行：HEAD `422fac3` 隔离快照叠加本次 LST、CLI 与 schema；确认快照和当前 LST 代码字节一致，避免共享目录其他半成品影响编译。`CLICKHOUSE_INTEGRATION=0 GOPROXY=off GOCACHE=/tmp/lst-go-cache go test -race ./internal/lst ./cmd/lst-data -count=1` 通过，LST 16.540s，CLI 无独立测试；末次仅增加 header identity 诊断定位后，相关 receipts 完整/重组/中间高度与父链定向 race 通过，1.259s；`go vet ./internal/lst ./cmd/lst-data` 通过。没有据此宣称真实 ClickHouse 往返、部署二进制或生产持续运行已验收。

根实现者的[非空来源证据](../research/2026-10-03-lst-repair/drpc-summary.json)不是空 blockHash 响应替代：块 26106948 有 369 笔交易及收据，1773 条连续日志，其中两条 queue 日志。独立审核者离线逐项核对 header/全部 receipts 的块与交易身份、全块 logIndex、queue 完整子集与 blockHash getLogs 两条输出完全一致。原 [header](../research/2026-10-03-lst-repair/focused-drpc-header-00.json)、[receipts](../research/2026-10-03-lst-repair/focused-drpc-receipts-00.json)、[非空 getLogs](../research/2026-10-03-lst-repair/drpc-nonempty-blockHash.json) 保留证据。这是单一非空块的公共 RPC 一致性核验，未做 receipt trie/共识证明，亦不代表连续多日覆盖。

剩余限制：公开 RPC 额度与同出口其他进程仍未知；Bloom 阳性可能很多，完整 receipts 证据不能按两条事件行数估计空间。本块 receipts 源 JSON 2,038,029 bytes，对应 evidence gzip 247,696 bytes；header gzip 20,747 bytes，仅为实际单块样本。持续扫描速度、游标积压、每天 gzip 增量和查询耗时须按运行证据评估。本轮后续有限部署验证由根实现者另存记录，未在此处冒充独立执行。完整日 gas、等待/兑付分布、长期 finality 恢复和持仓窗口资金费仍须积累；未知手续费/未来基差/保证金/gas 留空，尚未证明净利或年化至少 3%。

本轮代码快照集合包含排序后的 `internal/lst/*.go`、`cmd/lst-data/*.go` 与 `internal/storage/clickhouse/lst_schema.sql`，共 28 文件，以 `sha256  相对路径\n` 串联后的 SHA-256 为 `4df980628f8ae1dcacfd65610ebfe6aac639e023e5575bdd3b032d3ad2743990`。不包含后续更新的运行说明/部署验收，未扩审共享源码。

| 本轮最终代码 | SHA-256 |
| --- | --- |
| `internal/lst/receipts.go` | `57136549d58c9714218e341c4fe6221c650649e3148352aad6d17ecbb76cd886` |
| `internal/lst/logs.go` | `e8a2c95b8382f140d5f8915e4f3fee398b028b127ffa5aefc06bf0d833080e52` |
| `internal/lst/runner.go` | `4a160a80643d84a1673b269cbf00375916fee8b7517f212db78e93555c6041ac` |
| `internal/lst/collector.go` | `a97800b423396dfbd05b31b8b6b239180872c6ad735b90e00d4e4611473dbf75` |
| `internal/lst/batch.go` | `ef9db861e1cfd6d282443fe51ae4bbdcd85174c5a5b1b58379ef5c5462cd14ac` |
| `internal/lst/rpc.go` | `c3f105b5d3b8a52a2a001eae7146a11ac37f3abe08edffb5fa68361720f7b57f` |
| `internal/lst/transport.go` | `63430f77e579a1950d698ed69b8feca9d324e35ebb612e9c0b82c942e2192b33` |
| `internal/lst/diagnostics.go` | `a55028d04175c1c5b9e080256bfb416741876396daa5cfc2acefc106722fa0fb` |
| `internal/lst/repair_test.go` | `50b18b19468a8956e36ab262201b5cc95d1ccab37ee74c7a527ef8c5f4e26b95` |
| `cmd/lst-data/main.go` | `ece77df8f1a6156d90e33dad214d3dc8980911cc1e0c53b0309150f04cbc8757` |
## 2026-10-03 lst-mvp-3 单路线降频与 32/min 预算复审

本轮代码复审通过，未发现未解决的事实正确性 P1/P2。范围仅当前 LST 调度和发送 gate；没有外部 RPC、数据库、服务或路由操作。生产吞吐、新 429 与新鲜度仍由根实现者另行实测，代码通过不等于来源容量验收通过。

生产 unit 显式配置 `LST_RPC_REQUESTS_PER_MINUTE=32`、`LST_MARKET_INTERVAL=2m`、`LST_ENTRY_ROUTES_PER_ROUND=1`。CLI/库未配置时保留原 120/min、1m、A/B 两路线默认值；配置只接受有限合法值。新的滚动 RPC 上限约束所有实际方法请求并继续读取原近期记录，重启不清除 cooldown/disabled；显式 backfill 使用 `min(配置上限,60)`，Binance 保留原独立 12 次/分钟及权重规则。当前同一 dRPC host 下可作为 LST 的 RPC 请求总上限；若以后人工设置另一域名 LogRPC，不能把两个各自的 host 上限称为跨 host 总上限。同出口其他进程仍不受此 gate 控制。

每个实际市场轮仅采一个金额/route，保留八条 entry，其余七条完整 blank、`not_scheduled`；单路线正常链上路径 A 为 19 RPC，B 为 20 RPC。UTC 的 `floor(time/marketInterval)` 为轮序号，预算采用 `round/2%4`、route 采用 `round%2`，八轮覆盖四金额乘 A/B 的八个组合；避免直接 `minute%4` 在两分钟节拍下只采两档。每个组合优先间隔约 16 分钟，不能合并成同期容量；UTC 12:00—12:04 仍优先 100k 种子。八个两分钟市场轮只意味着每组合一次，若要重复两次需十六轮，不能沿用上一版双路线分母。调度配置也在 Market/ValidateReady 校验，避免直接调用 Collector 时小于一秒的间隔造成除零；非法配置拒绝回归已补。

外层仍按分钟串行运行；市场分钟不扫日志，另一分钟 receipts 最多两片，每片最多八块、新 capture、独立 Validate/Commit 与游标持久化。首次失败结束本分钟扫描，成功前片不撤回，失败后片不生成事件、不推进它的范围；55 秒维护截止、原 HTTP 间隔、finalized/父链/Bloom/全收据验证保持不变。局部范围错误可以缩片，已知能力拒绝仍持久暂停，rate/ban 不改成范围错误。预算限制下的“最多两片”不是“保证每两分钟完成十六块”。

方案阶段否决了一个 P1 风险：用已取得的 totalPooledEther/totalShares 直接替代三个换算 view。我最初提出的简式只适用于已经证明 externalShares 为零的情况；查当前官方覆盖实现后已撤回其普遍适用性。[官方 v4.0.1 部署清单](https://raw.githubusercontent.com/lidofinance/core/v4.0.1/deployed-mainnet.json) 对应当前 pin 的 `0x028271...`；[Lido.sol](https://raw.githubusercontent.com/lidofinance/core/v4.0.1/contracts/0.4.24/Lido.sol) 使用内部 Ether 和内部 shares 作换算分子/分母，总量又包含一次外部 Ether 除法。[StETH.sol](https://raw.githubusercontent.com/lidofinance/core/v4.0.1/contracts/0.4.24/StETH.sol) 的两个换算还要求输入严格小于 UINT128_MAX。例：internalEther=10、internalShares=6、externalShares=4，则总量 Ether=16、shares=10；stETH=8 的官方 shares 为 4，总量简式为 5。本轮最终代码保留原三次真实链上 view，不使用总量简式或 1e18 参考率线性外推，没有改变整数舍入、schema 或冻结行编码。

独立验证仍在 HEAD `422fac3` 加本次 LST 的 `/tmp/lst-repair-build` 隔离快照，逐文件确认当前源码一致，关闭 ClickHouse 集成且 `GOPROXY=off`：

- 新调度、单/双路线兼容、八组合与种子、非法配置、80 次 mock 发送的 32/min 及中途重启回归已复读。独立完整 `go test -race ./internal/lst ./cmd/lst-data -count=1` 通过，17.427s；这次编译尚未含之后新加的两片预算测试。
- 新两片测试最初因 fake clock 向未来推进、Collector 使用真实时间而触发 `capture_time_order`；没有放宽生产时间不变量。最终采用真实时钟间隔与 mock HTTP，只本地等约 16 秒，独立定向 race 通过，16.611s。测试实际执行 `Logs→Validate→Commit→cursor.result` 两次，共 32 次 mock HTTP；第一片 100..107 完整提交、八条请求保留，第二片因本地滚动预算失败，Next=108、RangeSize=4、失败片零事件。它验证两段提交边界，不冒称执行完整 Watch 主循环。
- 最新 `go vet ./internal/lst ./cmd/lst-data` 通过。根实现者另报告含全部最终测试的整包 race 33.067s；这不是审核者重复执行的时长。没有本轮新真实数据库集成或生产数据验收结论。

剩余运行限制：原来源在一个实际滚动窗口 41 RPC 后返回 429/RPC15，只能说明那次请求被限流，不能推出官方额度为 40，也不能保证 32/min 永不触发 429。所有块 Bloom 阳性时，两片各八块所需请求数超过 32，后片会预算失败并缩片；即使没有新 429，也须实测完整 blocks/min 与 Next 积压是否缩小，以及后台支出是否挤占下一轮市场的 30 秒窗口。报价过期、链上转换失败、十档容量不足、未调度与本地预算失败分别统计，维持 unknown/stale，不用旧行情补齐。完整等待/兑付关联、完整日 gas、实际资金费窗口和净利润仍未验收。

本轮最终代码集合仍为排序后的 `internal/lst/*.go`、`cmd/lst-data/*.go` 与 `internal/storage/clickhouse/lst_schema.sql`，共 28 文件，集合 SHA-256：`648b0ea483aaefa4fe258941f0e2d1746ba6247a55addb12500aaeb6f796000f`。部署与后续说明文档不包含在这个代码快照中。

| 本轮最终版本 | SHA-256 |
| --- | --- |
| `internal/lst/collector.go` | `899aae4541063caa1d72d6c9e2a945617c4907640569129e9089d1ed6e796849` |
| `internal/lst/runner.go` | `bff6562b2ad0935519c973b8ee04ca6ae7bb47107424d7e3280d312ed7168b0a` |
| `internal/lst/transport.go` | `248f84f6eaebf56060c2891439b5255d382859799cdea7767561d9b0df8adbf2` |
| `internal/lst/manifest.go` | `d9365dab1088054e417929d87b232a561867f2e1acd5148210e0e1c2592e0950` |
| `internal/lst/repair_test.go` | `e6928bdd53f4250cae17b01e624315d53d3279cc64f236fe4e8b73dbf34b8fc8` |
| `cmd/lst-data/main.go` | `7d6b90d7306d1be3393d6db4aedd127b42c9382c2b6916ff1f0ac28fc3e6d9d4` |
| 仓库 `deploy/systemd/crypto-market-info-lst.service` | `8a3a2d4ca83323624e82c43fb2a401b946bcb878b479a28ffc8ae316fd411cbe` |

## 2026-10-03 lst-mvp-4 报价前等待滚动额度复审（待部署）

本轮独立代码复审通过，未发现未解决的 P1/P2。范围仅 `Transport.WaitRPCSlots`、Watch 的等待/开轮时钟及版本标识、对应离线回归；没有外部网络请求、数据库或服务操作，也没有修改实现代码。mvp4 尚未部署或实测，不能把本轮代码通过写成生产容量通过。

触发问题是 mvp3 后台日志占用近期 RPC 配额，下一市场轮在原 25 秒报价窗口内等待旧请求过期，链上换算出现 `local_gate_budget_exhausted`、报价 stale。新实现先等待单路线 20 个、双路线 23 个空余槽位，再创建 Market 的 StartedAt 及原 25/30 秒截止；它不发送 HTTP、不预扣、不裁剪 Recent、不修改持久冷却，也不跳过原 acquire。cap32/slots20 时只等至最近一分钟剩至多 12 项，实际发送仍受原 32/min 和请求间隔约束。这是为基本市场路径等待空间，不能保证附加 followup、慢响应或对方动态限额下每轮均成功。

等待持有同一 RPC host gate 的 mutex，当前 Collector 串行所有 RPC 任务，Binance 使用另一 host gate；没有新增互相等待的锁路径。循环及 Clock.Sleep 接受 ctx 取消，错误状态拒绝继续；disabled/cooldown 不被额度等待消耗或隐藏，返回原采集路径，由原 gate 保留并分类。它不预留并发名额，因此不能扩张为多 collector 共用额度保证，同出口其他程序仍不受它控制。

发现并修复 P2：第一版 Watch 的外层 `start` 在额度等待前取得，长等待后维护截止和分钟 sleep 可能已经过期，接着立即开始 idle 周期。最终在等待成功后 `start=Now()`，nextMarket、55 秒维护和分钟 sleep 从实际开轮起计，s.due 与 Market 的 UTC 档位也在等待后取当前时间；没有重放错过的市场轮。等待会延长实际间隔，UTC 轮转/12:00—12:04 种子窗口可能跳过或改变实际组合，八组合每 16 分钟的描述仅适用于无延误的连续轮次，必须以采到的 route/amount 时间统计覆盖。

独立离线验证在 `/tmp/lst-repair-build`（HEAD `422fac3` 叠加本次 LST/CLI/schema，逐文件确认一致），`CLICKHOUSE_INTEGRATION=0 GOPROXY=off GOCACHE=/tmp/lst-go-cache`：整包 `go test -race ./internal/lst ./cmd/lst-data -count=1` 通过，32.965s；该次编译尚未含随后刷新 start 的一行修正。最后修正后，容量等待、市场单/双路线成员、非法调度和 quiet 截止相关定向 race 再通过，13.360s；`go vet ./internal/lst ./cmd/lst-data` 通过。容量回归证明 32 次 mock HTTP 后等待到近期剩 12 项，HTTP 数量与原 Recent 长度 32 不变，已有长 cooldown 不被长等，超过配置容量的等待拒绝；没有冒称执行真实 Watch 或核验公网额度。

根实现者的[本地监测归档](../research/2026-10-03-lst-live-restart/paced/monitor-summary.json)记录 mvp3 从 07:38:09 到 07:50:25 UTC 的 3 个市场轮和 8 个日志轮，未完成八组合验收。[服务日志](../research/2026-10-03-lst-live-restart/paced/service-live.log)与[失败汇总](../research/2026-10-03-lst-live-restart/paced/database-final.jsonl)记录 07:50:25 的 Binance `cex_depth` HTTP timeout，失败 response hash 为 `1ea51da2c18cf530991363b1b73a5f8e69e2bdc479bd834fecc09a4c9ea6ef1b`；watchdog 于 07:50:25.843 UTC 停服务。遵守用户遇到实际网络故障即停的要求，本轮不部署 mvp4、不重启、不外测或改路由。源码通过不能解除这一运行限制。

剩余验收：等待耗时、每 route/amount 的 fresh/full 比例、实际八组合覆盖周期、完整 finalized blocks/min 与 Next 积压、证据空间增量均须另行实测；后台日志成功不代表后续市场永不被其他 gate 或慢持久化挤占，32/min 也不是供应商承诺。没有放宽未知/缺失/过期、最终性、整数换算和冻结批次条件，没有据此声称净利或年化至少 3%。

本轮最终 28 文件代码集合仍为排序后的 `internal/lst/*.go`、`cmd/lst-data/*.go` 与 `internal/storage/clickhouse/lst_schema.sql`，按 `sha256  相对路径\n` 串联，SHA-256：`7272f35dbee7698fc35185201aade056464a02548973e78a5b5b7f9d34f5cf4b`。运行文档、生产 unit 与后续验证记录不包含在此快照中。

| 本轮最终版本 | SHA-256 |
| --- | --- |
| `internal/lst/runner.go` | `971bea6e4ba01480c947bbadd90f7d4529c3e366fc27af5bc7401214baa500da` |
| `internal/lst/transport.go` | `c1956b8942ea7b8a1f3558cbffaf21b4278b1b3e853bca99750850b8122f3bb6` |
| `internal/lst/manifest.go` | `64e68cb5dd61c43d87d91aeb496353cde7c88f6f39ac08342996599a178ecfd3` |
| `internal/lst/repair_test.go` | `108d61d2c02799ad37c824df5c1fcdfce73966a14dd7d2ed84afd23bf5f13467` |

## 2026-10-03 lst-mvp-5 启动节拍、并行取数与日志计划复审

本轮独立代码复审通过，未发现未解决的 P1/P2。用户随后授权恢复持续采集并修复运行问题，因此上一节“待部署”是当时状态；本轮审核允许根实现者按新授权部署，但不把 mock 测试通过视为公网持续采集通过。范围仅 mvp4 之后的 LST/CLI/unit 增量；审核者未访问外网、运行真实 collector、操作数据库/服务或修改生产实现，仅新增独立回归测试和本审核记录。

`StartupRPCGap` 接受 0（原默认 2 秒）或 2—30 秒，CLI 对显式环境值作同样范围校验，仓库 unit 设置 10 秒。该间隔约束未完成初始化或启动首分钟的 RPC；正常稳态仍为 500ms、Binance 仍为 5 秒，backfill 原规则保留。原 Recent、NextAt、cooldown/disabled 不清除，所有发送继续原 acquire。它降低预检频率，不证明公开服务不会返回 429，也没有改变路由或自动换来源。

发现并修复的两项 P2：

- `MarkInitialized` 后，最后预检响应保存的 `NextAt=响应+10s` 仍可能落入第一个 Market 的 25 秒窗口。最终 WaitRPCSlots 同时等待原持久 NextAt 和所需近期额度，在创建 Market 及外层 start 前完成；不删除原 NextAt、Recent 或冷却。首轮额外等待回归已通过。
- 第二片要求八块完整最坏额度 21，不足即全跳过，会使一个八块片段后第二片长期无法安排，不能声称追赶稳定。最终使用生产私有函数 `receiptPieceBlocks`：slots<7 推迟，否则计划块数为 `min(planned,(slots-5)/2)`；实际 to 同时受 RangeSize、八块上限及已观测 finalized 限制。第零片计划从 `liveLogBlocks()` 初始化，避免首次游标未载入时为零。Validate→Commit→cursor.result→游标持久化顺序保持；缩小的是本片计划，不跳过未覆盖高度。

同一端点全 Bloom 阳性时，n 块成本包括 n 个 header、n 个 receipts、内层 finalized、首尾两次 queue identity、canonical，以及外层 finalized，共 `2n+5`；单块也因两次 identity 为七次。额度检查仅适用于当前同 URL 的两片路径，不预扣、不等待或修改 gate；不同人工配置 LogRPC 仍分别走原 host gate，不能宣称跨 host 合计上限。检查可减少预算耗尽时的半片浪费，但慢响应、旧额度、其他维护和动态限额仍可能令任何片段失败，失败不会生成事实或推进游标。

CEX goroutine 移到 head 成功之后、Protocol 之前，仍在 cap+12 秒开始取数，depth/mark 串行受同 host 五秒间隔约束，末尾 join 与 canonical 检查保持。这个移动避免 13 次协议读取把 CEX 定时任务再顺延，没有增加请求或复用旧报价。`quoteTimingReason` 对协议缺失、30 秒采集、60 秒 block age、未来区块两秒、CEX 来源结束时十五秒、availability lag [-2s,15s]、来源对 block ±30s 保留原严格阈值与包含边界；数值以整数毫秒输出。所有失败原因追加、不覆盖原来源错误；canonical 缺验证明确 stale，未安排七成员和 missed followup 保持原语义。没有增加 Float 或改变行/表身份。

独立离线验证在已与当前代码逐文件匹配的 `/tmp/lst-repair-build`，仍关闭真实 ClickHouse 集成、`GOPROXY=off`：

- 新增 [mvp5_review_test.go](../internal/lst/mvp5_review_test.go) 调用实际生产计划 helper，再运行真实 `Header→Logs→Validate→Commit→cursor.result`，HTTP 全部 mock、原 500ms gate 与真实时钟保持。每块不同 tx/requestId 且重算全块 Bloom；第一片 100..107 使用 21 次 RPC，剩余 11 槽第二片 108..110 使用 11 次，两份不同 capture 的 complete/canonical/finalized 批次保留共 11 条 request，Next=111、总 32，剩余零槽推迟不发送。定向 race 通过，16.606s。测试验证计划、事实提交和内存游标边界，不冒称执行完整 Watch 或真实数据库；游标持久化顺序由源码复读。首版 mock 错把 padded blockHash 当 quantity 解析产生 `rpc_quantity`；只修测试为严格 32 字节解析，没有修改生产 quantity parser。
- 启动间隔/只读可用额度、原时效边界与追加诊断、近期额度等待定向 race 通过，1.407s；首轮持久 NextAt 等待回归另行独立通过。当前 `go vet ./internal/lst ./cmd/lst-data` 通过。根实现者另报告包含最终新增回归的整包 race 49.502s、vet/build 通过；不冒充审核者重复运行整包的结果。

补正上一节监测分母：此前 `paced/monitor-summary.json` 的 market_cycles=3/log_cycles=8 是监测捕获行数，[数据库归档](../research/2026-10-03-lst-live-restart/paced/database-final.jsonl)实际有六份市场 capture、八份 complete 日志及一份 failed 日志。[journal 归档](../research/2026-10-03-lst-live-restart/paced/service-live.log)遗漏了若干市场/日志行，不能据 grep 总数验收八组合覆盖。后续应以监测窗口内 DB capture_id 去重及实际 route/amount/timing 状态为分母，区分已安排、容量不足、来源失败、stale、未安排；日志以实际 complete/canonical/finalized 连续高度为准。新增 finalized/backlog 打印使用本片已观测 end、实际 Next，成功仅推进实际 to；不把空事件数或“最多十六块”替代吞吐。

真实限制仍在：根实现者报告 mvp4 预检仅 16 个 RPC 后收到 HTTP 429，原 gate 冷却到 08:48:51 UTC，未清状态或改路由；本轮审核没有进行任何外部探针。mvp5 降预检频率和局部调度修复之后，冷却后连续初始化/采集、实际 fresh 比例、八组合覆盖、complete blocks/min 与 backlog 是否缩小仍须根实现者继续验证。价格容量或十档不足如实保留，不能以持续有 partial capture 宣称可盈利；尚未证明净利或年化至少 3%。

本轮最终 29 文件代码集合为排序后的 `internal/lst/*.go`、`cmd/lst-data/*.go` 和 `internal/storage/clickhouse/lst_schema.sql`，新增一个独立回归文件，按 `sha256  相对路径\n` 串联 SHA-256：`5f61102a9f9c4c7e5ae107a1fc5c67b57ba4a8ee5625fd56ff0c86b2496fbf2f`。下面 unit 单独记录；后续部署二进制、运行说明和持续验证不属于本代码快照。

| 本轮最终版本 | SHA-256 |
| --- | --- |
| `internal/lst/collector.go` | `ddc318e145a0ae7da3b8293f738ed513b1ab078b2c42fb2819fb0f9e9006adf8` |
| `internal/lst/runner.go` | `b9e979b961a25a1d3700ae900b1498bc5c3bf7e944ed87ca06bc6603a7888a4e` |
| `internal/lst/transport.go` | `0ba2eec3a7011783b4dcecb791d50203ad5ba3b342af7b6ec81651a447d28798` |
| `internal/lst/diagnostics.go` | `e2790c47b68a8593914842bfc035541459584e0b1f63289d70b9df4b2da095f1` |
| `internal/lst/manifest.go` | `dd09ea710d3c19e8bfdefccfddea68f31d1ba25494cdc0266796624eaa1e54d2` |
| `internal/lst/repair_test.go` | `afa86e0304a4ed86fdcc2a215d88778348d42d531c38011739648c7aae9c4cd1` |
| `internal/lst/mvp5_review_test.go` | `413c361245a7fe3124ebe5dd4f8095a6d44b03d68b86e7294028d37071c45dd8` |
| `cmd/lst-data/main.go` | `ec9ea2575e584dbb53a74652fb57e150bbff9217eefa9de586bc03d709281c26` |
| 仓库 `deploy/systemd/crypto-market-info-lst.service` | `0e65b3a111e3123e3e8edbd36851f29bb130ae0b1de24de3684caec63e8cb80c` |

## 2026-10-03 lst-mvp-6 最近 head 准入与失败观测复审

本轮增量独立代码复审通过，未发现未解决的 P1/P2。范围仅报价前 head admission、实际开轮时钟、失败锚点与未请求 CEX 的空值，以及新增独立回归；没有外部请求、真实 collector、数据库或服务操作，也未修改生产实现。公网持续采集仍须根实现者完成验收，不能把下列 mock 成功写成已解决全部运行问题。

[mvp5 首轮数据](../research/2026-10-03-lst-running/mvp5/database-first.jsonl)显示四腿均 ok，采集耗时 22.455 秒，却因 mark 对 block 相差 33.002 秒被判 stale。固定把 CEX 从第十二秒改到第九秒，若其余时钟不变仍为 30.002 秒，不能严格通过，更没有慢来源余量。本轮保留原 CEX 第十二秒取数及全部阈值，改为先取得较新的真实区块位置。

`marketHead` 有独立二十秒 ctx，最多三次只读 latest；仅成功解析但实际 age 不在 [-2s,10s] 的 head 才等至少三秒后再查。来源错误、限流、超时或解析失败立即返回，不做内层错误重试。成功选出的同一 Block 用于 Protocol、全部 entry/followup 和原 canonical 末核验，Market 不再重复取 latest；每次已获得的响应/失败证据均保留在 EvidenceRoot。准入后才建立 Capture.StartedAt 与原 25/30 秒窗口；区块 Time 和响应 Requested/Received/Available 原样保留，较早的 head 证据在归档中，不通过移动来源时钟伪造 fresh。较早准入耗时不计入市场报价窗口，因此运行评估还须计算整个循环的墙钟耗时。

Watch 在进入准入前等待单路线 22、双路线 25 个槽位，原基本路径包含一次 latest，最多三次尝试只是最多额外两次，不是另加三次；原 acquire/持久 Recent/NextAt/cooldown 仍约束实际发送。返回批次的实际 Capture.StartedAt 决定维护、nextMarket 和 sleep，UTC entry 档位与 seed 取真实时间；不补跑错过轮次。新的 age 准入约束不能保证所有块在二十秒内变新，也不能保证慢 CEX/链请求都满足原时效；未准入也保存八条身份及真实缺失原因，不静默减少分母。

| 本轮发现 | 最小修正与状态 |
| --- | --- |
| P1：保留锚点只检查 Number 和 Hash 长度；Header 解析 timestamp/parentHash 失败时可已填 Number/Hash、Time 却为零，导致 Seal/Validate 失败而无法保存本轮未知观测。 | 仅在非零 Number、goodHash(Hash,32) 和 timeValid(Time) 都成立时 setAnchor；半解析失败保留原响应，链锚点全 NULL。malformed timestamp 回归可 Validate/Commit、八身份不丢，通过。 |
| P2：准入失败后零值 CEXMarket 的 DepthErr=nil，ApplyHedge 写出未请求来源的 LastUpdateId=&0。 | 默认 depth/mark 为明确的 `cex_not_requested_head_admission_failed` 本地错误，只有实际 channel 结果替换；未请求 CEX 的序号、来源时钟与 hash 均为 NULL，http_attempted=false，原 head 错误保留。独立断言通过。 |

新增 [mvp6_review_test.go](../internal/lst/mvp6_review_test.go) 使用真实 wall clock、原三秒等待和完全 mock 的 HTTP，无生产时间/解析/哈希规则放宽：

- 旧、旧、新三次只产生三个 latest，间隔至少三秒，选中第三个真实位置，三份不同响应证据及原时钟保留。
- 连续三次旧或未来 head 各产生一次可存储 partial 批次，八条 quote 身份仍在（七条 not_scheduled、一条计划成员 stale），一条 unknown protocol，四腿 unknown、无旧数量/价格或未请求 CEX 证据；已解析的最后锚点保留、canonical=false。三个 response envelope 从 EvidenceRoot 回读验 hash，再核对全部原始 body，之后 Commit 成功。
- 首个 429（RPC15）、网络 timeout 或 malformed timestamp 仅一个请求，无第二次 source retry；来源错误及失败证据保留，不完整位置不造锚点，未知批次仍可 Commit。网络用 mock context.DeadlineExceeded，没有真实外网探针。

独立定向 `go test -race ./internal/lst -run '^TestHeadAdmission' -count=1` 最终通过，13.153s，当前 `go vet ./internal/lst ./cmd/lst-data` 通过；使用与当前源码一致的 `/tmp/lst-repair-build`、关闭真实 ClickHouse 集成、GOPROXY=off。新增测试首次把原始 body digest 当作归档 envelope hash 直接比较；仅修测试为先 Archive.Get 再核对 envelope.Response，没有改生产归档或哈希规则。根实现者此前整包 race 48.797s 不含本轮最终新增测试/锚点修正，不引用它证明最终整包已通过；末次整包结果由根实现者单独记录。

真实状态由本地归档核对：[mvp5 HTTP 汇总](../research/2026-10-03-lst-running/mvp5/http-summary.json)记录 288 次 HTTP，274 次 RPC，滚动一分钟 RPC 最大 28，287 次 HTTP200、一次 Binance 五秒 timeout，未见新 429；[数据库归档](../research/2026-10-03-lst-running/mvp5/database-final.jsonl)六个计划成员中一条 usable、四条旧头导致 stale、一条 timeout。八片完整日志连续覆盖 26105688..26105740 共 53 块，先前本地 reservation-expired 失败范围后来完整采到，失败批次没有覆盖事实；不能由这些局部样本推导已追上实时链。[暂停记录](../research/2026-10-03-lst-running/mvp5/service-paused.txt)为 inactive/dead、MainPID=0。后续四次 Binance 成功是根实现者的恢复证据；重新恢复须遵守当前用户授权与既有网络故障处理约束，审核者没有代做服务操作。

剩余验收：mvp6 同路由恢复后的准入成功/失败率、整个周期耗时、全部八组合实际覆盖、单条 fresh/full 比例、连续 complete blocks/min 与 backlog、证据空间和最终性恢复。保留所有缺失/失败/过期观测，不把有 partial 批次当作收益证明；净利润或年化至少 3% 尚未验收。

本轮最终 30 文件代码集合仍为排序后的 `internal/lst/*.go`、`cmd/lst-data/*.go` 与 `internal/storage/clickhouse/lst_schema.sql`，新增独立 mvp6 回归，按 `sha256  相对路径\n` 串联 SHA-256：`dad878722067b5fba19b5e5a768c1ccb200631c2c01099842c7b7efa706ec857`。运行说明、部署二进制和后续真实验收不属于该快照。

| 本轮最终增量 | SHA-256 |
| --- | --- |
| `internal/lst/collector.go` | `b06d45501a0e7f5eaa7f07724200bb23c28c9903e612b2c2a8631e9e6fe79da4` |
| `internal/lst/runner.go` | `9c81df0b7650f56829dc1c54ec58c70c9653641736acce68c9b630883f6e0b3a` |
| `internal/lst/manifest.go` | `2cd21718b46ebeecbcb3c5d03e0eab5032282c4dc5163c30e07b436c087049e1` |
| `internal/lst/mvp6_review_test.go` | `4999e4fd8b21263feea91c577666426d3b6c9a78c4dac325bef21de8703733a0` |

## 2026-10-03 lst-mvp-7 Multicall3、共享 finalized 证据与请求预算复审

本轮生产代码增量独立复审通过，未发现未关闭的 P1/P2。范围是固定协议视图聚合、独立运行时代码 pin、维护轮的 finalized 响应复用、二十次滚动分钟预算和失败诊断。审核者只读取本地实现/归档、浏览官方技术来源、保存公开部署证据及新增独立 mock 测试；没有公共 RPC、数据库/服务操作、路由修改或生产源码编辑。代码审核通过与公网持续采集验收分开。

此前 mvp6 首轮三个旧 head 保存未知批次，第二轮 250k A 链上腿齐全且 timing fresh、CEX 十档容量不足；根实现者报告随后实际 RPC429，滚动峰值 28 仍未超过当时的本程序 32 次配额。本程序请求上限并非供应商额度保证。[dRPC 官方限流说明](https://drpc.org/docs/howitworks/ratelimiting)按 IP 的 CU、地域需求和网络设置描述动态限额，页内 eth_call 换算的单位也不一致，不能据此承诺 20 次/分钟不会再限流。

`internal/dex/ethereum` 当前没有 Multicall3 地址、ABI 或可信运行时代码 pin 可复用；复用的是原 Archive，ABI 使用 LST 现有 go-ethereum 定型路径。[官方 Multicall3 源码](https://raw.githubusercontent.com/mds1/multicall3/main/src/Multicall3.sol)的 aggregate3 为 CALL 聚合，子调用看到 Multicall3 作为 msg.sender，因此只聚合已审阅的固定、无账户依赖协议读取；不聚合报价的依赖路径、资金转移或写调用。当前 13 项原视图完整保留，另加聚合器自读 block number/timestamp 共十五项，以同一 EIP-1898 requireCanonical blockHash 调用一次；不以 `blockAndAggregate` 的当前块 `blockhash(block.number)` 返回值替代真实区块哈希。输入角色、selector、参数和位置由独立固定列表检查，返回数、外层/每项 canonical ABI、恰 32 字节、地址高位、Bool 0/1、身份及执行高度/时间均严格校验。任何失败保留同一真实来源响应并把协议置 unknown，没有十三次直接调用回退、常量伪装当前状态、浮点或一单位换算线性外推。

独立从[官方 README](https://raw.githubusercontent.com/mds1/multicall3/main/README.md)中完整已签部署交易取得运行时 pin，公开证据在 [multicall 目录](../research/2026-10-03-lst-running/multicall/runtime-pin.json)。[verify_runtime_pin.go](../research/2026-10-03-lst-running/multicall/verify_runtime_pin.go)完全离线：严格 canonical RLP/整数字节、零值 nonce0 创建、官方 gas 参数、legacy 签名恢复以及创建地址，然后匹配完整 32 字节初始化程序、CODECOPY/RETURN 的 offset32/length3808 和无剩余构造数据。签名交易 3926 字节、creation3840 字节，deployer `0x05f32B3cC3888453ff71B01135B34FF8e41263F2`，生成主网部署地址 `0xcA11bde05977b3631167028862bE2a173976CA11`。运行时 Keccak-256 为 `0xd5c15df687b16f2ff992fc8d767b4216323184a2bbc6ee2f9c398c318e770891`，原始字节 SHA-256 为 `0x2756d7c52baee85cacb504f6ee1df7aad6809ac8d94a4a111d76991f90d36d6e`，与生产常量匹配。归档明确为官方页面 web export，不冒称 byte-for-byte 原始 README；commit API/history 页面无法通过 web tool 获取，所以以签名交易和来源 export hash 冻结输入、source_commit=NULL。该结果是官方部署字节 pin，审核者没有把它称为已经取得真实 eth_getCode。VerifyIdentity 的额外代码请求与原时钟/哈希进入 IdentityResponses；不新增原 Manifest role，原 manifest 字节/hash 和日志游标身份保留，CollectorVersion 变为 7。

本轮发现并关闭的两项 P2：聚合成功后的本地视图映射 Response{} 覆盖 lastResponse，会让未知实现/协议不变量错误虚报未发送、丢真实 MC 响应身份；最终只在 multicall 的空本地映射时保留原 aggregate Response。同时该修复最初也忽略 sequential 的空失败响应，可能借用上一成功 view 标成 http_attempted=true；最终 sequential 始终更新 lastResponse，未发送预算失败明确 http_attempted=false/http0，无上一 response hash。两种模式均有独立负向回归。

维护轮仅复用刚真实取得的 finalized Block，其原 Response/timestamps/payload hash进入日志 EvidenceRoot，不改 Capture.SourceTime、不拿旧市场 head代替当前报价。公开 Logs仍自行取 finalized，私有 logsWithFinalized 校验传入完整位置与证明。每片仍首尾 queue implementation、逐块 header链/receipt完备性及末 canonical；Validate→Commit→cursor.result→持久游标顺序不变。所有 receipts 片都按余额缩小并留两槽，包含共享外层 finalized 的全 Bloom 阳性最坏成本为 `2n+4`，单块六次；最多两片，也允许市场轮后扫描。Multicall 的 WaitRPCSlots 为单路线10/双路线13，顺序模式保留22/25；无预扣或清除 Recent/NextAt。FinalizePending 无待复核时零 RPC；同 quiet finalized复用后仍逐个核对实际 market hash，multicall 每轮最多一条复核，顺序模式四条不变。

独立新增 [mvp7_review_test.go](../internal/lst/mvp7_review_test.go)，在 `/home/ubuntu/crypto-market-info` 使用 GOPROXY=off、关闭 ClickHouse 集成，HTTP 全 mock：

- 固定十五项输入逐 target/selector/1e18 参数核验；全部九个金额/ID/换算值分别超过 2^53，两个 Bool 包括已知 false，逐字段核对原整数。只一次 eth_call，协议 proof仅真实 header+aggregate两份；未知实现、不变量、子调用失败、执行时间不符、429均保留来源证据且不报 ok。根实现者基础测试另覆盖短数组、错误Bool/地址 padding、执行块不符、ABI尾随数据。
- 没有 pending 市场、只有 finalized/orphaned/logs/uncommitted时零 RPC；复用当次 finalized 后仍核对各可终结 market 的区块哈希，匹配者 finalized、不匹配者 orphaned、超过 finalized者不提前修改。原公开无复用调用继续产生一次实际 finalized读取。
- 真实 wall clock与原500ms gate下，全 Bloom阳性七块100..106连同共享外层 finalized恰十八次 RPC，仅一次 finalized来源；Validate/Commit后 Next107，剩两槽计划跳过不发送。故意直接越过计划的第二片仍被原20次配额挡住，失败批次零事件、无canonical覆盖，Next保持107，首片七条请求事实不损坏。EvidenceRoot包含共享 finalized原哈希。

最终独立定向 race通过10.826s、独立 vet通过。新增回归首次误用 Archive.Get 签名，另一次在 mock HTTP callback内重复获取生产持有的 host锁导致测试自身死锁；只修测试为实际 cap1 触发未发送错误，旧测试进程已终止，没有修改生产锁/解析规则。根实现者另报告含全部最终回归的整包 race70.966s及vet/build通过。根实现者报告10:04:39 UTC部署二进制SHA `637ed277dcfeddab82bf633531379c1543dd878a045e40c36c1ee9736b223df6`，原 cooldown10:03:50自然结束、状态保留；真实代码身份、MC字段和持续采集验收刚开始，本节不改写为公网已通过。

预算剩余限制：正常单路线 A/B通常七/八次，三次准入 head时九/十次，跟踪退出另计。cap20且每片留两槽时，全 Bloom阳性 idle维护通常最多七块、普通市场维护最多三块，约十块/两分钟仅接近链增长；额外 head、慢响应、gas和最终性工作可使吞吐更低。Bloom阴性或跨窗口额度释放可能改善实测，但不能从上限推出稳定追赶。必须以 DB连续 complete/canonical/finalized高度、真实 Next/backlog斜率和全过程墙钟时间验收，不能以零429、有partial或fresh报价替代追赶，更不代表净利润或年化3%。

本轮另只读审核 `validate-multicall.py`：2464字节返回布局、十五项offset/32字节word、十三协议项与DB精确整数以及执行位置检查正确；已建议补核对 actual request 的 aggregate3 selector/十五项固定输入，以及 startup code原请求 method/target/blockRef而非只相信汇总列表。此为本地验收证明范围，未把尚未运行/修订的脚本称为独立公网验收。

本轮代码快照为排序后的33个 `internal/lst/*.go`、`cmd/lst-data/*.go` 与 `internal/storage/clickhouse/lst_schema.sql`，按 `sha256  相对路径\n` 串联 SHA-256：`d4ab2498b77b01d1e7b202625cd325ed2112a7a32b49e67ea16db17e8b3e5f9c`。其后运行材料不属于该快照。

| 本轮最终版本 | SHA-256 |
| --- | --- |
| `internal/lst/multicall.go` | `5341f5f428a0614cf4b2245aa3819a8cbfb7292730e706c883859a640c6e1668` |
| `internal/lst/protocol.go` | `425b20c354977c7e1f7590d59cacf78ad0ec4de9d2c8d36fa3e36e16128525e4` |
| `internal/lst/logs.go` | `c6a06324a1e95b9c8366e9c72e0cc98679f18d968c30f90f8d8f13cad0de2773` |
| `internal/lst/runner.go` | `da4ad2fd00ed99e431c151336dc075b4fd0343cc2b435a95b7176431f58c05d1` |
| `internal/lst/mvp7_review_test.go` | `8acf59acc07069bb60a4773b42376c537fdbc9794bc3a6e13c437daa7c411743` |
| `cmd/lst-data/main.go` | `c78f932e5632d27e9a74603ef0ee6b84114b8e8d829458ba71285c05c4d9aef4` |
| 原 `config/lst-lido-ethereum.json` | `cdb1ab2cd55bf49b022210f1f13548f21b91667ee43253504a4403caf1ebea61` |
| 仓库 unit | `59ec6753447a37de05e9f3ca272777a4b3b8a4ab1bc2ccc9c33bcd6887a1b36d` |
| `multicall/runtime-pin.json` | `bb50c6d71d16158b2a95273b243c1dcfaac3fcb5fc756b54fc0f85a885d4d1bd` |
| `multicall/verify_runtime_pin.go` | `0fad249a9ffc7ab23085095e3cf8c4389d6556a789a2f109ce689b0963c584db` |

## 2026-10-03 lst-mvp-7 请求间隔、空事件证明与有界日志前缀复审

本次增量独立本地复审通过，未发现未关闭的 P1/P2。范围是每种 RPC 阶段的响应后最小间隔、持久额度/冷却重启、可见的初始化诊断、receipts 空事件证明与逐事件块实现核验，以及 watch 的预算前主动截断。审核者只写独立测试、审核记录和获准的离线验收 helper；没有发送公共请求、运行真实 collector/DB 查询、操作服务、清理 gate 或更改路由。上节的 33 文件、整包 race70.966s 和首版部署二进制是之前快照，不能用于证明本轮末次代码已整包测试或部署。

先补正上节运行时间线：根实现者核对，mvp6 在 10:03:50 原 cooldown 到期后仅发送五次 RPC，第五次同历史块 26105755/hash `e53fdd…` 的 queue implementation eth_call 于 10:03:54 再次真实 429；前四次 finalized/历史 header 成功。该参数/块哈希与 09:33:49 首次失败一致，尚不能推断是 IP 封禁、历史状态成本或供应商异常。持久 LimitHits=11、cooldown 到 10:33:54.64 UTC；10:04:39 启动的首版 mvp7 尚未发送 HTTP、仍等原状态，本轮后续二进制与它不同。当前代码不绕过既有冷却，也不把未发送的初始化称为成功。

新增 `RPCGap`/`LST_RPC_GAP` 默认500ms、只接受500ms..10s；生产 unit2s。所有 RPC 启动、稳态和显式 backfill 分支均取原阶段最小值与配置 gap 的较大值，Binance 独立5s不变。发现初版仅稳态遵守 gap 的 P2，已修为三阶段都遵守。HTTP 响应后仍持久 NextAt，重启保留原 Recent/NextAt/cooldown，第一发送仍受本进程冷启动间隔约束。CLI 首个 finalized Header 有独立有限 ctx（生产20s），超过窗口的冷却仅本地返回 source_cooldown 并每60s打印初始化诊断，不发送探针；成功后 VerifyIdentity 仍逐项受原 gate。启动日志只打印 RPC hostname 和模式/预算/间隔，没有 URL 凭据。没有扩大报价25/30s窗口或放宽任一来源时间阈值。

完整空事件覆盖无需知道该块 queue ABI：真实 header Bloom 对 queue 地址阴性，或 Bloom 阳性但全部 transactions 对应的完整 receipts 经 tx/hash/index/global-log/Bloom 校验后原始 queue 地址日志数组为零，都足以证明无 queue 日志。此时不查询历史 queue implementation，也不把“无日志”宣称为实现身份已知。只要原始 queue 地址日志非空，每个实际含日志的块恰一次以同 blockHash/requireCanonical 的 implementation 核验，发生在 known-topic 过滤/解码之前；匿名/未知 topic 也不能跳过。未知实现或 Upgrade 过渡仍整片失败、全部事实清空、游标不推进。range 模式保留原首尾 identity；官方 Manifest/hash/schema/历史 cursor 未换身份。

含共享外层 finalized 与末 canonical 时，n块全 Bloom 阴性成本 `n+2`；全 Bloom 阳性且完整 receipts 中无 queue 日志成本 `2n+2`；每块都有 raw queue 日志最坏 `3n+2`。不能把 Bloom 阳性等同于真实 queue 事件，也不能按预期稀疏率规划固定范围。公开 Logs/显式 backfill 保留请求整段全成败；watch 同 RPC 来源的 receipts 私有 liveBounded 每步先检查剩余额度/期限、预留 canonical 和维护槽，最多候选八块，必要时仅提交已完全验证的连续前缀。仅请求发送前的本地预算不足允许主动停止；实际 RPC/receipt/ABI/解析/canonical 失败仍整片失败，不能用已完成部分遮盖失败。成功 anchor/末 canonical 指向实际 verifiedLast，Validate→Commit→cursor.result→持久游标顺序保持，Next=实际 ToBlock+1；失败 covered_to 打印0。idle 可以在 quiet期限内等滚动额度释放后重新规划，最多两片，无补跑来源失败。

本轮独立发现并修复：P1，去除 live planned-tail 预读后，全部候选都验证成功的分支未更新 last，可能把多块 facts 留在首块 Capture 中；已无条件使用 verifiedLast/setAnchor，完整三块回归通过。P2，liveBounded 与不同 LogRPC URL 组合会把 planned-tail 来源 header 与首块比较；已归一为仅 receipts 且相同 RPC URL 启用，独立来源保持完整范围合同。不同来源未获预算优化收益，但不伪造覆盖。

新增 [mvp7_gap_review_test.go](../internal/lst/mvp7_gap_review_test.go) 独立 mock/fake-clock 验三阶段 gap、默认与非法配置、慢响应后的响应后间隔、重启冷启动与原预约列表不变，以及第三次真实 mock429后持久30min退避。重启调用 WaitRPCSlots 不隐藏冷却，20s Do 只返回本地 source_cooldown，发送数、请求时钟/响应证据和冻结状态都不变。测试 callback 不重复获取生产持有的 host锁。

新增 [mvp7_receipts_review_test.go](../internal/lst/mvp7_receipts_review_test.go) 共十一场景，使用真实 wall-clock/500ms gate、HTTP全 mock，调用实际 Header/Logs/Validate/Commit/cursor：阴性零 receipts/eth_call；通过其他地址 topics 构造真实 Bloom 假阳性并完整重算 receipts/header Bloom，零 queue 日志时零 identity；仅100/102两个日志块核验，101阴性跳过；未知实现、匿名/未知topic未知实现都整片零事实/Next100；live完整100..102三块十一RPC/三条请求/Next103；cap20主动覆盖100..104（十八实际RPC含未验证105 header），准确To104/Next105；首块前预算不足不能生成complete；实际 receipts500或第三块未知实现均整片失败，不保存前缀。原 [mvp7_review_test.go](../internal/lst/mvp7_review_test.go) 配额回归同步为五块十七RPC/Next105，再故意越界触发原gate失败，已提交五条事实不损坏。

最终独立 `go test -race ./internal/lst -run '^TestMVP7' -count=1` 通过19.451s，当前 `go vet ./internal/lst ./cmd/lst-data` 通过；工作目录 `/home/ubuntu/crypto-market-info`、GOPROXY=off、ClickHouse集成关闭。真实 HTTP 全未调用。本轮末次根实现者整包 race/vet/build 和持续采集验收在本节写入时仍待其单独回报，不借用之前快照。瞬时额度、两秒间隔、空状态读优化不保证供应商不会再次429；应实际验收代码pin、MC十五输入/十三字段/执行位置、fresh比例、各组合覆盖、连续完整日志与 backlog斜率及空间/耗时。暂无净利润或至少3%年化验收。

获准补强 [validate-multicall.py](../research/2026-10-03-lst-running/validate-multicall.py)：独立固定十五项 target/selector/allowFailure/1e18由 [离线 calldata helper](../research/2026-10-03-lst-running/multicall/calldata/generate.go) 生成2980字节 aggregate3 请求；验收脚本对归档 actual calldata逐字节比较，核对其真实 method/target/同DB blockHash/requireCanonical/零value。startup code证明再次从实际请求检查 eth_getCode/MC地址/合法32字节canonical blockRef，并与官方3808字节运行时及runtime-pin原始SHA全等比较。离线 helper执行和Python AST解析通过；审核者未运行脚本的本地DB查询，也未宣称已经取得成功公网MC/code证明。

本轮35文件快照仍为排序后的 `internal/lst/*.go`、`cmd/lst-data/*.go` 与 `internal/storage/clickhouse/lst_schema.sql`，按 `sha256  相对路径\n` 串联 SHA-256：`33ae313aa647578c3853edeac2890721f57921bcb3937c8450563e0440ded699`。运行验收、之后文档/部署不属于该代码快照。

| 本次复审快照 | SHA-256 |
| --- | --- |
| `internal/lst/transport.go` | `83722aaa582fd521099b8a8e8148a23a65c7ce2696f9b6d157de74c41f02ffec` |
| `internal/lst/logs.go` | `ed5af1a77d8215c470fcfb292b76455aef7c08f2fbaa34df71785777485eba0a` |
| `internal/lst/runner.go` | `766ec46b16a49520a2b9b1af5c6ba3e63111655fe63680a486f28a7a93dd9fdd` |
| `internal/lst/mvp7_review_test.go` | `8bd6619dd6c9af0aff84950d11f8ae47227e5338b13b22ab70ece1eae6caf1a0` |
| `internal/lst/mvp7_gap_review_test.go` | `dc734d13f141c3d515c9040e4955eec400b2954bbfd97b1622106f353dda405a` |
| `internal/lst/mvp7_receipts_review_test.go` | `e820597978964cd7b13043485d91779f465eb7c3a2b12b99b0ab8a5fc3812aaf` |
| `cmd/lst-data/main.go` | `845c599b9ef00bdc0ee2e765911e1afda25181731c8b7935fbf4f12478c2dddd` |
| 仓库 unit | `9817a914404af79453fda6242348c94541a77400ed15a4c922e3633e090bac1c` |
| `validate-multicall.py` | `2ea344626238c07ccb0a5804e8f95daba3635b69f49b4581f50aa1aa311d31c9` |
| `multicall/expected-protocol-calldata.hex` | `ae4139a770421454fa577988286cd7ca842a15249e18e8d5e50e88204651deb7` |
| `multicall/calldata/generate.go` | `a3db99c9db144e08fc52687a90a597542adc557c9ea31ba9cce889c841cfc493` |


## 2026-10-03 11:11 UTC 公开 RPC 切源研究复审

独立Agent只读复审通过：原35个构建输入与mvp7最终b8aa...b5b7二进制完全一致；新增可选EnvironmentFile覆写正确、var目录忽略、实际凭据文件未创建。研究探针非法CaptureMode已改合法backfill（不Commit）、任意JSON使用UseNumber、聚合宽度及收据表示比较明确仅初步验证，不冒充生产能力。Merkle复测200→3.001677秒后429，13HTTP=9x200+3x429+1x525；525为上游响应、不定位本机路由。报告和运行文档明确当前服务暂停、没有候选通过。最后两处文字已修正为先0600再填凭据、精确归档请求时刻11:04:41.647134。没有生产代码改动，无额外RPC或重复测试；取得个人来源后仍需实际重新验收。

## 2026-10-03 lst-mvp-8 实时报价与旧日志补缺分离复审

本轮最小增量独立本地复审通过，未发现未关闭的 P1/P2。范围为显式暂停 watch 日志扫描、保留其他维护、严格 CLI 开关、当前身份锚点与 BlockPI 生产配置。审核者仅读取本地实现/证据、添加独立 mock 测试与本记录；没有外部请求、真实 collector/DB 查询、服务操作、路由或持久 gate 修改。该结论是代码复审，实际启动及持续采集验收仍由根实现者完成。

`Collector.PauseLiveLogs` 默认 false；`LST_PAUSE_LIVE_LOGS` 使用严格 Bool 解析，非法值拒绝，true 只允许 watch，显式 backfill/probe 不会被静默跳过。Watch 仅在整个日志任务入口新增 guard，因此 true 不读取/改写 live-logs.gob、不初始化新日志起点、不发送该任务的 headers/receipts/state 请求，也不生成 logs capture 或虚假 complete 覆盖。资金费、已有市场最终性复核、基于已有完整覆盖的 gas 抽样仍独立运行；故此模式不承诺完全没有历史 header/receipt 读取。原协议视图、整数/NULL、来源证明、鲜度阈值、最终性、manifest 与游标身份保持。CollectorVersion 变为 8。

独立发现并关闭 P2：暂停时 CLI 仍预检单独配置的 LogRPC chainId，冷却或停用的旧日志来源可能阻止当前行情初始化。当前已加 `!pauseLiveLogs` guard；恢复扫描后仍执行该来源检查。暂停时启动身份头改为真实 latest，未暂停仍 finalized；完整 VerifyIdentity 保留 chain、proxy 实现、全部 code pin、decimals、池及官方 Multicall runtime，每个状态请求仍使用同一真实 blockHash/requireCanonical，最后再核对 canonical。latest 仅用于当前身份检查，不被标为 finalized，也不证明历史 ABI/覆盖；它在预检期间重组、被裁剪或任一 pin 不匹配仍会失败。启动明确打印 identity_anchor，随后行情自己的 head/最终性维护不变。

独立新增 [mvp8_pause_logs_review_test.go](../internal/lst/mvp8_pause_logs_review_test.go)：真实 Watch/Validate/Commit/cursor 流程、HTTP 全 mock、真实 wall clock 与原500ms gate，并同时运行暂停和默认两种配置。预先冻结有效旧游标原字节；当前 latest 的真实 mock RPC 错误仍提交一条 partial 市场及完整八个未知身份。暂停分支恰三次 RPC（latest、finalized、旧市场 canonical header），零日志扫描/新覆盖，游标逐字节不变；空 funding API 响应仍落 source-window 完整批次，旧市场 revision2/finalized，gas 仍检查既有覆盖。默认分支实际扫 Bloom 阴性区块、提交完整日志并推进原游标，验证默认行为保留。测试未伪造可盈利报价或 gas 样本。

独立 pause/default race通过9.082s，CLI 非法值/非watch拒绝定向 race通过1.031s，当前 LST/CLI vet通过。根实现者另报告整包 LST+CLI race81.226s；末次 latest 身份分支完成后，CLI race1.062s、pause/default9.069s、vet/build通过。最后分支只读复审通过，未重复既有测试。隔离构建 [build.json](../research/2026-10-03-lst-realtime-split/build.json) 二进制 SHA 为 `f082e9c562db1599c3dc9a6c9ae2a0c54bd18c401ab9e375759d24cf806eb4f3`；该材料写入时状态为 candidate_tested_not_started，不冒称已经持续成功。

生产候选 unit 选择用户授权的 BlockPI 公开 RPC，20次滚动分钟、响应后2s、启动2s、每2m单条路线、multicall，显式暂停旧日志。原 state/gate、manifest 和 Next26105760 保留，可选私有 EnvironmentFile 仍保留且未新建凭据。公开节点先前的当前状态成功不代表完整启动、报价或历史能力通过，也不保证足够持续额度。原日志缺口与赎回等待/兑现、gas 样本限制仍应在报告保留 unknown；没有净利润、每天200美元或至少3%年化验收。后续须实际验收完整 startup、连续 fresh/full 报价、八组合覆盖、资金费/最终性与失败保存；不把旧日志暂停当作历史完整。

本轮37文件快照为排序后的 `internal/lst/*.go`、`cmd/lst-data/*.go` 与 `internal/storage/clickhouse/lst_schema.sql`，按 `sha256  相对路径\n` 串联 SHA-256：`a9b64b86b966c0820e581c9b6253043ce57b80281bbcda9b17287527db1d2715`。运行验收与随后文档更新不属于该代码快照。

| 本次复审快照 | SHA-256 |
| --- | --- |
| `internal/lst/collector.go` | `681f0d254af45089362931518113aef42077c4b5389156b72247d39f585e841c` |
| `internal/lst/runner.go` | `0e74e1773aece73cb98f317ec6d77e756f5a37b3c68766f808dc2340cfff4627` |
| `internal/lst/mvp8_pause_logs_review_test.go` | `98145d1742c448a54681c33564949aeaaa0658179664c3ae92fc6e0199ac6619` |
| `cmd/lst-data/main.go` | `072fbe6935a702a241b7171a86a42f0f463f16f6f6ab7b32ce52f1d1832e6411` |
| `cmd/lst-data/main_test.go` | `362fdd2976b2d36a44c2ccea346c304b6d51a2466f84b4d60bc3c1a3bb2c9ef5` |
| 仓库 unit | `bf5705586c01baec87a413d13cd2f3ae3094a79bf5cd0631c5502d540847ee2d` |
| `docs/lst-redemption-data-implementation.md` | `aab9214ec2f41ac4101c2cec0b3532dd7a162c10f6182cbfb7a51e9c5680e4a6` |
| `docs/runtime-operations.md` | `04fbe89595eb4a7e0080c5c80d4893306b59589816945f20343e7de1ccfaf0fe` |

### mvp8 运行验收后的只读复核

根实现者保存了 [实际运行验收](../research/2026-10-03-lst-realtime-split/report.md)，独立审核 Agent 随后仅读取本地材料复核通过，未改实现、请求外部源或操作服务。8个partial市场批次、64个entry成员中，56个未调度、6个四腿ok/fresh/canonical、2个失败；6条可用中仅1条finalized，其余5条head。真实Binance超时仅计一次，后续本地cooldown不重复计网络失败。准确停止时间为14:08:46 UTC，报告明确不是14:04:33失败当秒停止；旧游标和历史缺口保留，当前文档没有宣称正常持续运行或盈利验收通过。

根实现者另对109份RPC响应的实际Response字段逐一解码，核对jsonrpc=2.0、请求/响应id、result存在及无error；证据见final-evidence-check.json。安装binary、37项构建输入、仓库和安装unit均仍与上述审核摘要相同，文档更新不改变已审核实现。

## 2026-10-04 lst-mvp-9 新日志段、历史日志来源及旧缺口复审

本轮最终增量独立本地复审通过，未发现未关闭的 P1/P2。范围为显式固定新日志起点与独立游标、历史日志/实现读取切到 LogRPC、单进程有预算的旧缺口补采，以及实时 getLogs 五分钟配额。审核者仅阅读本地代码/公开证据，写 [mvp9_review_test.go](../internal/lst/mvp9_review_test.go) 与本记录；没有外部请求、数据库/服务操作、路由、原 gate 或生产源码修改。本地代码测试、有限来源能力实测与连续覆盖验收分别记录。

`watch --from-block N` 固定新的日志段，其他模式不把该参数解释成新 live 起点；watch 的 to-block 和 pause=true+from-block 拒绝，负数/非整数由 flag 解析拒绝。新段文件为 `live-logs-from-N.gob`，同原 StateDir/锁/Transport/pending/Manifest。初始化读取 N 的真实主源 header，先冻结 Start=Next=N 和实际区块时间，再仅沿 committed/complete/canonical/finalized 的真实连续区间恢复；存在游标时校验 Manifest、Start 与 Next。重启不重新挑 head，丢文件也不会从最早旧批次跳起或越过未覆盖高度。新段 range 每分钟最多八块，普通原游标 range 的五分钟/512块边界不变，公开 Logs/backfill 仍完整整段全成败。报告按实际区间合并相邻/重叠覆盖，不能把两段的最早/最晚高度当作连续；跨缺口等待仍 coverage_unknown，缺请求的领取仍 left-censored。没有 schema/manifest 身份迁移。

主来源继续 BlockPI 当前行情与 finalized/事件区块/canonical headers，独立 `LST_LOG_RPC_URL` 使用 MEV Blocker 的日志和每个历史身份调用。queueIdentityAt 现调用 c.logRPC().View，以同 blockHash/requireCanonical 核验固定实现；不同日志来源的末端 header 必须与主源高度/hash一致，来源错误没有自动 fallback。watch 启动当前身份锚点统一为真实 latest，完整 VerifyIdentity/code pins/末 canonical 核对仍保留；历史事件的真实块身份单独核验，没有把当前 pin 当作旧状态或把 latest 标 finalized。Source/Response 原文和事件 payload hash保留。

旧 `live-logs.gob` 不作为新段写入目标；只有明确的旧补缺 helper 能依据真实完整落库推进它。helper 仅显式新段/range/未暂停、非市场轮、新段已追到本轮真实 finalized、空闲至少35秒且主源/日志源尚有最小余量时运行，每五分钟最多一片32块，止于新起点前一块。旧文件缺失跳过，没有第二个 worker/state目录。失败批次正常保存、事件清空，不推进；预算失败可缩片；DB/FS失败保留 pending 或报错。独立发现的 P2 已关闭：初版将采集和 Commit 共用 quiet ctx，会让已封存批次在采集截止后无谓写库失败/重启。最终 helper 明确传入 fetch ctx 和 Watch 父 Commit ctx，保持父取消，没有用 Background/WithoutCancel 隐藏取消。

根实现者在部署前发现并修复 P1可用性边界：原 getLogs live 额外只有四次/滚动五分钟，新段每分钟加旧片每五分钟至少需要六次，原配置可能不断本地预算失败、缩片而追不上。新增 `LogRequestsPerFiveMinutes` 默认0保留 live4/backfill30；CLI `LST_LOG_REQUESTS_PER_5_MINUTE` 显式只接受1..30，unit=8。仍使用现有持久 Recent 统计、不清旧预约/cooldown，至少10秒/getLogs间隔、全部RPC每host20次/分钟与响应后2秒保持。BlockPI和MEV是分别限额，不能称所有RPC合计20次或整个出口全局限速；其他模块同供应商流量也不由本状态目录保证。

独立 mock 回归覆盖以下实际路径：

- 新段100的已提交100..107恢复到108；失败/未提交/非canonical/head批次和109..120不会跨108缺块。重启零HTTP，丢文件只读取固定100再恢复；真实108补齐后才到121。旧Start50/Next60及原字节保留；坏Start/Next/manifest和错误返回高度拒绝、不重置。
- 实际双来源 Logs/Validate/Commit：历史 eth_call 仅日志源，主 finalized/范围/事件/canonical五次 header仍核验；100bit金额精确保留，typed event proof 的真实Host为日志源。日志源末hash冲突、历史state错误、未知实现、事件hash冲突、主canonical冲突均failed、零事实、Next不推进，原游标不损坏。实际Watch只提交100..107并写新Next108。
- 旧32块60..91成功落库才旧Next92；来源失败保存failed且Next60。DB失败原cursor字节不变、pending保留；同批无重抓写入后由实际complete事实恢复Next92。采集ctx取消后父ctx仍成功提交failed批次，新游标不改；规划时间/两host余量不足不发送、不改预约。
- 默认live4的四个实际mock预约在重启显式8后原样保留，再发四次累计8；第九次有限ctx只返回本地budget错误，无HTTP/新预约。恰最早预约满五分钟才可再发，滚动窗口和原10秒间隔保持；非法-1/31拒绝，未配置的backfill仍允许九次/五分钟内。

在共享目录、GOPROXY=off、ClickHouse集成关闭、HTTP全mock下，前三组和旧helper的独立 race通过10.378s，末次配额增量独立 race通过1.179s；LST/CLI独立vet通过。共享目录CLI参数测试包通过1.884s，但整条命令因另一个任务未完成的 options_live_spec.go 依赖vet错误退出，不冒称该命令整体通过，未编辑其他模块。根实现者隔离最终生产源码（含logcap8）的整包race LST98.583s、CLI1.359s及vet通过；末次独立quota测试文件稳定后另同步定向验证。此前不含logcap8的100.125s属于旧快照，不能替代此次最终生产源码测试。

有限实网证明以根实现者归档为准，审核者只读：[BlockPI见证](../research/2026-10-04-lst-logs-restore/blockpi-probe.json)能读取近期非空range，但同块历史实现RPC-32000，保持failed零typed事件；[MEV见证](../research/2026-10-04-lst-logs-restore/mev-probe.json)单块26112806含typed request1、实际交易收据一致。随后[双来源preflight](../research/2026-10-04-lst-logs-restore/restore-preflight.json)已Validate/Commit旧26105760..26105767（claim1）及单块26112806（request1）；这只是两个局部真实区间，不证明两者之间旧缺口已补齐或等待分布可靠。

根实现者报告16:52:36 UTC（北京时间2026-10-04 00:52:36）启动mvp9，PID2196152、NRestarts0。部署前真实 finalized 更新固定新Start26113044，unit冻结该数值；先前候选26112980的新游标尚未创建，未用“重启刷新起点”跳过已经开始采集的新段。Continuous complete/canonical/finalized blocks/min、backlog斜率、新旧游标推进与来源/时间/整数回读、分host请求峰值和真实错误仍待生产观察，不能以active或有限非空证明正常持续。旧gap不完整、利润及年化至少3%仍未验收。

本轮末次38文件快照为排序后的 `internal/lst/*.go`、`cmd/lst-data/*.go` 与 `internal/storage/clickhouse/lst_schema.sql`，按 `sha256  相对路径\n` 串联 SHA-256：`7477c332304d1eae6fc3651fe8d4ff8c28e3833ad9de0f48eb61a38f99014f48`。此前 `aceee11f349aeb03588e52452f426d9edf775096e46e7b0718d03414fdf7880f` 未含logcap与末次quota回归，不混称当前输入。运行验收和后续说明不属于此代码快照。

| 本次最终复审快照 | SHA-256 |
| --- | --- |
| `internal/lst/collector.go` | `07a89a1143c07059eb45230258573543c5ff7870a78a3800bd100fe62877acba` |
| `internal/lst/logs.go` | `49a3ee1e52c3912636667c4e496c04f8bbcffed19d7527229673e98f527844b6` |
| `internal/lst/runner.go` | `96d761ac5368fe5ca91e5dcd4a655b644a7bac72509b1c70aecf05815878ad60` |
| `internal/lst/transport.go` | `6d698e48e376900a933951a43f45c2142110ed1e4d21650799e3c90a5437cc4a` |
| `internal/lst/mvp9_review_test.go` | `cb9b74c17e824c0ea38ba79c12e7089d9c4c10ba3ad8ac8e1b65b62eb33d2dc5` |
| `cmd/lst-data/main.go` | `6eeb72bedc3b8c0b62cf11820e088c77bf4b2d393ee058b71ce90ba64aa3f192` |
| `cmd/lst-data/main_test.go` | `b9fcf5bf0e57390988b5e2de2efe6998a50d44161fa959a81cb75bd3a792fcde` |
| 仓库 unit（固定Start26113044） | `5a881cd1181f24eecb12ddf20e73917cd03590908f6ebd7d0001503b72e82da8` |
| 原manifest | `cdb1ab2cd55bf49b022210f1f13548f21b91667ee43253504a4403caf1ebea61` |

### mvp9 固定运行窗口的最终只读复核

独立审核者仅读取 [恢复记录](../research/2026-10-04-lst-logs-restore/report.md) 和冻结的本地JSON/文本，复核通过，没有外部请求、数据库查询、服务操作或源码改动。窗口严格为 `2026-10-03T16:40:02.453473527Z` 至 `17:07:20Z`，包含两次preflight及16:52:36启动后的服务；随后状态检查不扩大数据分母。最后的新增quota回归在隔离snapshot定向race1.176s通过。38输入/生产实现摘要仍为上一节值，运行binary SHA `8fb7c70c1e9323e2e1e1add8511a606ce7c7b1c8cbad1fc434c39a7f947c7a10`、unit固定Start26113044。

`database-final.jsonl` 独立加总一致：五个partial/canonical/head市场、四十个entry，三十五未调度与五个实际调度；五条均四腿ok且fresh，未调度未计有效，没有把head晋级finalized。这只是五个组合，不是八组合轮转完成或同时容量。一个funding complete。十二个logs均complete/canonical/finalized：两preflight（旧八块含claim1、见证单块含request1）、九个新片严格相邻且无重叠的26113044..26113108共六十五块、一个后台旧片26105768..26105775。九个新片没有queue事件，不能据空片声称新请求/领取已出现；非空能力由实际preflight证明。累计事实request2/claim2包含此前已有数据，本窗口只新增request1/claim1/finalization0，不把0 finalization解释成已无等待。

新游标Start26113044/Next26113109/Range8，对应九片实际覆盖，追到窗口内最后真实观测finalized26113108；没有拿行情head26113192冒充可终结日志。旧游标Start26105583/Next26105776/Range9，仅恢复真实preflight八块再后台Commit八块，没有跳到新段。其Next距新Start七千二百六十八个高度，单块26112806另有独立覆盖；报告明确该距离不是所有高度都未采集，也没有跨洞合并。旧整体缺口、赎回等待/兑付关联、日gas和净利润仍未验收。

`typed-data-validation.json` 十八批次与六十三不同hash回读校验；Multicall材料五个协议观测的十三项原字段、执行块/时间、固定十五项输入与官方3808字节runtime逐项相等，Bool/地址不误称十三个整数。审核者仅对现有校验材料交叉核对，没有重新执行DB验收脚本。修正恢复报告唯一小措辞“13整数”为“十三项协议字段（整数、地址和布尔值）”，其余统计不改。

`http-evidence.json` 独立重算一致：228实际HTTP，214RPC全部HTTP200、rpc_envelope_valid/result为真且无RPC error；14Binance为13HTTP200及一次初始化exchangeInfo真实5.003847秒transport_http_timeout，随后同PID重新获取成功。没有把该失败算成成功或声称全程无网络故障，也没有把它重复算作行情失败。BlockPI164RPC、滚动60秒峰20、最小响应至下次请求2.001188秒；MEV50RPC、峰8、间隔2.001186秒。合计峰21是跨host统计、合计相邻请求可只有0.003596秒，不等于单host限速违约；每host20和响应后2秒的口径明确。十二次getLogs均在MEV，实测滚动五分钟峰6≤配置8。摘要463个新对象SHA、成员63hash是不同验证集合，不相加冒称不同来源数量；未观察到429不作以后配额保证。

根实现者保存的17:16:15 UTC状态仍active/running/enabled、唯一PID2196152、NRestarts0，与启动状态一致；当前文档明确本轮已经恢复运行，并保留预检超时、旧缺口和head限制。此为有限窗口的采集恢复与真实局部覆盖通过，不承诺长期无故障、全部历史补齐或利润/年化3%。
