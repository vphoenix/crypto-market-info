# Across 稳定币中继数据采集

2026-10-02 已实现独立命令 `cmd/across-data`，并在现有 ClickHouse 实例创建 `crypto_market_info_across` 的专项表（现为八张）。范围是 Base ↔ Arbitrum One 原生 USDC；同时保留这两个 SpokePool 的其他路线事实，用来识别退款混合来源与研究覆盖缺口。程序采集公开数据，报告输出费用空间，不产生净盈利承诺。

## 使用

在仓库根目录运行：

```bash
go build -o var/across/bin/across-data ./cmd/across-data
var/across/bin/across-data init-schema
var/across/bin/across-data preflight
var/across/bin/across-data watch
var/across/bin/across-data report --from 2026-10-02T00:00:00Z --to 2026-10-03T00:00:00Z --out var/across/reports/2026-10-02
```

`backfill` 与 `watch` 使用同一研究库时按顺序运行。命令以 ClickHouse 地址和库名生成本机排他锁，锁目录 `/tmp/crypto-market-info-across-locks`。`report` 使用只读数据库连接，可以在采集时运行。CLI 不自行安装系统服务；2026-10-02已按用户指令安装独立实时/历史两个 user unit，实际状态和管理命令见[运行说明](runtime-operations.md#across-独立研究采集器2026-10-02)。常驻运行时不要直接重复上述主库 backfill/watch 命令。

可用 `watch --once` 验收单轮；`backfill --chain 8453 --from-block N --to-block M --max-ranges 1` 限定窗口。显式高度必须选择单链，结束高度不能超过该链 finalized 头；每段至多 512 块。`--database`、`--clickhouse`、`--evidence`、`--manifest`、`--out` 可调整；`--no-prices` 关闭可选估值。CLI 拒绝将 Across 表建入现有生产盘口库、全量验收库或 Reserve 库。

命令返回成功表示所要求的日志范围已完成处理；未知 ABI 仍为 partial，收据也可能有独立缺口，必须读 coverage/report。收据队列由已提交日志与已存收据重建；重新运行回溯或持续 watch 会继续补采。报告窗口按存款的区块时间选择订单，可使用窗口后的已存成交/probe证据；不是过去某时刻的可见信息回放。

## 数据入口与配置

| 用途 | 默认入口 | 覆盖环境变量 |
|---|---|---|
| Base RPC | `https://mainnet.base.org` | `ACROSS_BASE_RPC_URL` |
| Arbitrum RPC | `https://arb1.arbitrum.io/rpc` | `ACROSS_ARBITRUM_RPC_URL` |
| ETHUSDT、USDCUSDT 公开 BBO | `https://api.binance.com/api/v3/ticker/bookTicker` | manifest 中 `binance_url` |
| ClickHouse | `127.0.0.1:9000`，库 `crypto_market_info_across` | `--clickhouse`、`--database`；凭据 `ACROSS_CLICKHOUSE_USER` / `ACROSS_CLICKHOUSE_PASSWORD` |

来源只记录主机名，不写带凭据的RPC URL；响应正文仅在内存中解析。币安按用户要求用 `.com`，每 60 秒尝试刷新两个币对，原始十进制价格、数量与时间随 probe 保存。失败不刷新旧报价时间；超过 60 秒不附加到新 probe。

[manifest](../config/across-research.json) 固定链、token、SpokePool、部署实现与代码 hash。加载 manifest 会核对本地 verified 源文件 SHA-256、实际 bytecode 和嵌入 ABI；部署时须带上其引用的 `research/2026-10-02-across-implementation/verified-*.json`。Base explorer 的源验证标志是部分验证，实际部署 runtime 与 RPC 返回 bytecode 已逐字节对应；Arbitrum 同样进行了实际 bytecode 对应。启动及固定区块读取仍核验链身份、代理实现和 USDC 元数据。证据说明见[协议验证](../research/2026-10-02-across-implementation/protocol-sources.json)。

Base 实测每批 JSON-RPC 最多 10 成员；Arbitrum 每批最多 20。每个worker/链最多 2 个 HTTP 请求在途（多worker合计不受此2请求上限约束，host gate限制发起额度）。区块头按批读取，范围超限或失败会记录状态/hash并尝试更小范围；数据库或证据文件写入失败直接停止，不能被当成范围过大继续网络重抓。

历史 unit 显式传 `--rpc-min-interval 500ms`：每个 Reader 拆成单成员串行请求，响应后至少再等500毫秒，取消时停止排队，不生成未发起请求的证据。Header预算包含每个请求及等待，单请求仍有5秒超时。默认0保留实时批量行为。这是单进程节流，不保证两进程合计不触及来源限额；历史限流退出后由systemd退避60秒再续跑。

## 保存与恢复语义

八表字段见 [DDL](across-stablecoin-data-schema.sql)，嵌入程序的同结构 DDL 位于 `internal/storage/clickhouse/across_schema.sql`。原子金额与 ID 用 UInt256，价格用 Decimal(38,18)，时间统一 UTC 微秒；二进制 hash/address 保持原始字节，成员摘要不通过 JSON 字符串转换。

RPC/API响应在内存中严格解析，定类型事实直接写入数据库，成功提交后释放内存；正文不写磁盘，也不转存为JSON数据库备份。先保存紧凑capture成员/锚点元数据，再批写事实，最后提交capture；读取校验数据库成员摘要和紧凑元数据，不逐份读响应正文。写入失败不提交成功capture；重抓使用新观测身份，既有重试保持冻结内容。新增`across_receipt_transfers`保存原生USDC Transfer全集；空数组表示已观测空集合，缺行表示缺采。旧六组成员摘要兼容，新Transfer组编号7。

重组撤销触及旧分支的所有 capture 类型和尝试；源链孤块的 probe 不能进入候选。finalized hash 冲突停止进程。日志恢复使用区间并集，二分重抓的多段可以共同填补缺口。重新加载的未过期订单标为 restart，不能伪造此前持续在线。

用户明确放弃的旧解码缺口，通过原capture的新revision在reason追加`repair_abandoned=user_requested_historical_state_unavailable`，自动partial补采跳过这些capture，重启后仍生效。status仍为partial，原失败原因、成员、链锚点和最终性保留，覆盖查询仍计为缺口；其他新区块和未标记缺采按原逻辑继续。

重启后的积压超过单次`max_log_blocks`时，常驻watch先采当前链头，随后按正常顺序采新块；不让旧历史状态查询拖住当前数据。被跳过的区间仍是明确缺口，由同一个进程已有maintenance worker每轮补一个最多64块的原始扫描区间，两链轮流；只处理已保存capture之间的孔洞，不采首次启动前的数据，也不另启history服务。补采写为catchup，新capture保持自己的可见时间和head最终性，后续Reconcile取得证明才晋级。未知实现或不可用历史状态仍为partial；原始覆盖和完整解码继续分别统计。一次性`watch --once`保持原来的顺序扫描语义。

实时订单包含实际计划和执行的 +2/+5/+10 秒 probe。每轮 probe RPC 预算 5 秒，超时、迟到、跳过、取消都保存。目标延后超过 1 秒不算该档准时样本；失败或孤块查询里的 Filled 状态不能终结订单。超过本地截止而无法再查询时，仅记录 `local_deadline_elapsed_state_unverified` 取消，不声称已经成交。新鲜门槛默认 5 秒，未来时钟容忍 2 秒。

## 报告与当前限制

输出 `coverage.csv`、`orders.csv`、`refunds.csv`、`summary.json`。原始条款、实际更新后 fast fill 和真实 live 开放观测分别计算逐单非负费用空间；不把多个 probe 或多条更新重复算成订单。CSV 的成交 gas 是整笔共享交易费用，不能逐订单相加；summary 按链/区块 hash/交易 hash 去重。

退款只按收据中的唯一 USDC Transfer 核验地址/批次实际到账；逐单归属保持 unknown。Base 收据缺适用 operator fee 信息时总费用为 NULL；Arbitrum 不重复加 L1 gas。库存 3/6/12/24 小时仅是假设情景，真实周转、实际容量和净收益均未验证。

初次验收使用两链各512个 finalized 区块的小窗口，以及独立 `crypto_market_info_across_livecheck` 库的90秒实时测试。2026-10-02 13:57 UTC开始主库常驻采集，随后启动独立 `crypto_market_info_across_history` 库的固定30日回补。回补按 Base 后 Arbitrum 顺序执行，不代表已经完成。证据分别位于 `var/across/evidence` 与 `var/across/history-evidence`；查询 history 时必须同时指定相应 `--database` 和 `--evidence`。跨库可能有重复事件，不能直接累加两个报告的订单或费用空间。

90秒测试公共 RPC 循环约6–14秒，本次启动追赶阶段约12–36秒，`poll_millis=1000` 是目标间隔，不能当作已达到每秒采集。默认 Arbitrum 节点部分历史状态不可用，相关原始日志保存为 partial；日志扫描游标推进不等于全部事件已经解码。当前实现保存真实延迟和缺测；这组实时样本不足以判断抢单能力，也不能据此否决 Across 的全部机会。

审核与验证见 [代码审核](../discuss/0013-across-stablecoin-code-review.md) 和 [验收记录](../research/2026-10-02-across-implementation/validation.md)。

## 2026-10-03 修复：历史、最终性、费用、排程与报告

用户后续明确只需要首次启动后的数据：历史服务已停止并禁用，独立history库、专用原文归档和导出的历史数据已删除。已核对全部history区块早于实时采集起点，无需迁入实时库。以下历史实现说明保留作修复记录；当前只运行实时watch，从已保存位置续采，补齐首次启动之后范围内的缺块、partial及缺收据，不自动扩展到启动前30日。当前状态见[运行说明](runtime-operations.md)。

2026-10-04已按用户意思进一步取消正文归档和每批文件删除机制。`migrate-transfers`仅本地读取旧收据并新增定类型Transfer事实/capture，不改旧事实和旧成员摘要；暂停同库writer后运行，结束恢复watch。`prune-responses`只读核验后删除已转存及无引用旧响应，不连接RPC/API，不备份原文。尚未完整替代的旧日志和未迁移退款暂留。退款查询只读数据库，不将未知金额当成零；报告不再以源正文存在作为事实有效条件。范围及实际结果见[本轮验收](../research/2026-10-04-across-no-raw/validation.md)。

固定历史任务改用单进程 `history --range 8453:50783591:52079591 --range 42161:500994686:511000892`，使用历史库及证据目录。每链每轮最多64块，独立退避；Base未完成或被限流不会阻止Arbitrum开始。起止高度不随重启滚动。原始扫描成功但解码partial的区间进入单独补采；补采生成新capture，不修改旧事实、成员摘要或可见时间。永久未知实现/历史状态不可用不会当成完整事实，不无限重试同一未知区间。两链日志处理后还有独立收据补采；unit完成不等于成本和所有事件都完整。

所有修正版live/history请求按RPC主机共用 `var/across/rpc-quota/` 中的本机文件gate。默认400ms/成员、每批最多3个成员是本机保守预算，不是供应商配额保证；HTTP429与JSON-RPC -32016共享冷却，保留协议错误码，-32601才是方法不存在。可配置 `--rpc-quota-dir`、`--rpc-quota-interval`、`--rpc-burst`，两个进程须使用相同设置。保留旧的 `--rpc-min-interval` 单Reader冷却选项，但正式history不再依赖其独立500ms限速。gate约束合作进程，无法计算别的应用或同出口用户消耗的额度。不要删除状态文件来清除来源冷却，也不会修改路由或代理。

新RPC归档保存真正进入HTTP发送的 `StartedAt` 和响应完成 `At`。传输失败亦归档脱敏的失败类别，不冒充成功响应；本地等待超时与实际发送失败分开。供应商响应完成量不得称为发起QPS。

最终性维护每轮最多核验24个capture，小组成功即持久提交revision；后面的RPC失败不会丢掉已证实的晋级。先核对既有finalized checkpoint，首尾hash都一致且结束高度不超过成功返回的finalized头才晋级。发现孤块仍撤销所有与其高度相交的capture种类/尝试；finalized hash冲突停止研究采集。revision.reason的 `finality_proof=0x...` 引用单独归档的核验请求证据，原evidence_hash不变。网络预算不用于冻结写入，避免一次RPC迟延让本已获得的事实丢失。

probe有独立定时唤醒循环和独立HTTP槽位/Reader证据缓冲；日志与最终性长工作不再决定任务何时醒来。probe优先额度会暂缓普通维护，但单个任务仍有真实RPC超时、源端延迟和竞争限制。取消已成功证实Filled/过截止的单仍记录cancelled_terminal；每轮最多一个网络任务，只延后余下到期任务，不因共享5秒预算用尽而伪造跳过。超过planned+1秒完成仍标late，独立排程不能保证公开RPC下所有2/5/10秒样本准时。

Base普通交易读取收据所在区块hash的GasPriceOracle.getOperatorFee(gasUsed)，保存来源hash，只有成功证明的费额（含零）才补齐total。收据scalar/constant按Isthmus与Jovian的整数公式交叉核验；不按当前日期推断历史费制，缺证明仍unknown。旧费用unknown在启动时安排新capture补采，成功但仍unknown本运行不反复伪造新证明。费用依据、支持交易类型、实测样例见[费用修复记录](../research/2026-10-03-across-repair/fee-research.md)。

report和恢复采用每512个capture固定六次事实SELECT，逐capture校验全部成员，包括预期空表的额外行。没有新增永久索引。summary.performance分开记录metadata、事实SQL/校验、gzip归档验证耗时及查询数。summary.log_coverage_all_saved_captures与coverage.csv分别给出raw扫描与完整解码区间；旧partial后来补齐可覆盖decoded区间，但旧partial行保留。报告目前纳入canonical/committed且成员/归档合格的head与finalized记录，summary.eligible_capture_finality_policy及counts明确此口径；head并未因此变成finalized。

报告窗口按存款block_time选订单，匹配可使用之后已保存的成交/probe；coverage统计当前manifest的全部已保存capture。费用空间不扣未知成本，net_profit_usdt仍null；它不能给出稳定盈利或无机会结论。两库相同事件不可直接相加。`version`和服务启动日志给出构建标识；部署应核对磁盘、两服务/proc/exe的SHA-256一致，具体修改及实际验收见[修复记录](../research/2026-10-03-across-repair/validation.md)。

归档校验使用固定8个worker，每批1000个capture，RPC payload按hash共享一次校验；完整性要求不变。报告写入phase-progress.jsonl记录SQL、归档和总进度，取消或超时的运行不能凭已有CSV或旧summary视为完成。probe每轮最多执行一个网络任务；fresh live baseline与准时followup优先，已经迟到的样本不得抢占新单。restart与open_check使用普通额度，新live到达时可取消其网络工作，失败证据仍用父context提交，reason记probe_preempted_for_live；本地预算耗尽记probe_budget_exhausted/RPC层rpc_operation_budget_exhausted。已知近期live deadline前不启动普通长任务。普通任务预算12秒，优先任务5秒；超时/late仍是未知样本。优先额度只在第一次实际准入时开始固定窗口，最多4秒，窗口内RPC及排队均不续约；过期后为后台保留一个成员间隔的可用额度机会，供应商cooldown不能消耗此让行窗口。各批大小使用相同的下一次准入时间，按成员数预留后续间隔，避免小请求持续借走大批次额度。链RPC确认DNS、TLS或不可达错误后CLI以2退出，两unit禁止自动重试，等待用户处理网络。
