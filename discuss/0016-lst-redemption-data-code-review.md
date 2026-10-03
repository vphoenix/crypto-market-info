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
