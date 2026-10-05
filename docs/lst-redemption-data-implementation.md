# LST 公开数据采集实现

2026-10-05 11:22北京时间完成重启恢复检查与修复，当前 `lst-mvp-11` active/running、enabled、NRestarts0。人工补1852块，并修复保存游标领先数据库完整覆盖的问题：从保留Start重建已提交连续前缀，真实补回8块断口，随后重启已从最新进度续采。11:19与11:21两轮报价四腿完整及时，48个本次boot后批次回读校验与原文清理通过；新段Next26123313与DB连续覆盖一致。今天08:00实际资金费已补回，停机报价缺失保持缺失；旧2296块历史缺口继续低速补采。原端点、路由、unit和持久限速未改，详见[修复与实际运行验收](../research/2026-10-05-lst-reboot-check/report.md)。以下是此前时点记录。

2026-10-04 22:27北京时间健康复核：14:35启动的同一mvp10进程仍在持续采集、未重启；最近15分钟7轮调度报价四腿完整及时，25个近期落库批次成员摘要和结构验证通过、已提交原文已清理。新段22:24:52追上当时finalized，旧缺口仍在补采。偶发失败如实排除，一次后台本机数据库读取超时未阻断后续落库与最终性复核。详见[晚间数据检查](../research/2026-10-04-lst-evening-check/report.md)；以下为此前启动记录。

2026-10-04 14:35:16北京时间按用户指令再次启动已安装的 `lst-mvp-10`，14:39复核active/running、enabled、NRestarts0，并确认新market、funding、logs实际落库。首轮25万USDT链上三腿ok、对冲十档容量不足，正确保留不可用；日志从原Next续采16块，历史缺口仍保留。4个新批次成员摘要/结构校验和提交后原文清理通过。沿用原端点、路由、unit与持久限速，不因零星超时手动停止健康采集。当前恢复证据见[本轮启动核验](../research/2026-10-04-lst-restart/report.md)；下段为此前mvp9恢复记录。

2026-10-03 16:52:36 UTC（北京时间2026-10-04 00:52:36）按用户指令恢复赎回事件，部署 `lst-mvp-9`，唯一MainPID2196152、active/running、enabled、NRestarts0。主RPC BlockPI继续报价，日志及对应历史queue实现使用 `https://rpc.mevblocker.io`；固定新段Start26113044与旧缺口分别推进，共用原锁、pending与持久gate。一次Binance预检5秒超时后自动恢复，固定窗口5轮四腿ok/fresh/canonical、9片新日志连续覆盖65块，原缺口后台补采已真实推进Next26105776；旧整体缺口仍不完整。详见[本轮恢复及落库核验](../research/2026-10-04-lst-logs-restore/report.md)。偶发失败留痕并继续，持续异常排查；没有修改路由、真实交易或私钥操作。

设计见[方案](lst-redemption-data-mvp-design.md)，七表字段见[DDL](lst-redemption-data-schema.sql)，审核见[0016](../discuss/0016-lst-redemption-data-code-review.md)。程序是一个 Go CLI、一个持久状态目录、一个证据目录和现有 ClickHouse，没有消息队列或新常驻基础设施。

## 运行

从仓库根目录执行：

下面的初始化和前台命令用于安装或调试；当前已由 systemd 管理，先按后文维护步骤停止服务，再运行需要采集锁的命令。日常查看状态或生成报告无需停止服务。

```bash
go build -o var/lst/lst-data ./cmd/lst-data
var/lst/lst-data init-schema
LST_LOG_MODE=receipts var/lst/lst-data watch --once --duration 5m
LST_LOG_MODE=receipts var/lst/lst-data watch
```

`init-schema` 已对研究库执行，可以重复运行。`watch --once` 包含缓速身份预检，只生成一个市场批次；不会启动历史回补，也不执行该轮后台维护。`watch` 是前台持续运行，收到 SIGINT/SIGTERM 退出。2026-10-03 02:36:08（北京时间）按用户要求启动用户级 systemd 常驻服务 `crypto-market-info-lst.service`，并启用开机启动；[unit](../deploy/systemd/crypto-market-info-lst.service) 只运行 `watch`，沿用上述默认状态和证据目录，失败后等待 60 秒重启。

部署目标是持续实时采集，补采只用于断线缺口，不运行 30/60 日历史回补。2026-10-03 06:16:46 UTC 已部署修复版；预检在 06:19:28 UTC 出现 `code_curve: transport_http_timeout`，按用户“有网络问题即停止、不改路由”的要求停止服务。这是此前停止记录；07:03:54 UTC 按用户指令恢复，成功推进日志后遇到真实 429；07:38:20 UTC 已部署 `lst-mvp-3` 并再次启动，保留原限流状态，随后在 07:50:24 UTC 出现真实 Binance 十档 HTTP timeout，07:50:25 UTC 已按用户要求停止，该时点 inactive/dead。后续08:47:06 UTC恢复mvp5，当前状态以本文开头和本轮持续运行记录为准。最新实测见[启动与限速修复记录](../research/2026-10-03-lst-live-restart/report.md)。证据见[修复记录](../research/2026-10-03-lst-repair/report.md)。查看状态和生成只读报告：

```bash
systemctl --user status crypto-market-info-lst.service
journalctl --user-unit=crypto-market-info-lst.service -n 30 --no-pager
var/lst/lst-data report --out var/lst-reports/latest
```

`LST_PAUSE_LIVE_LOGS=true` 可显式暂停日志扫描（mvp8生产曾使用，mvp9恢复后为false）：不读取/修改原游标、不写新logs覆盖批次，启动明确记录paused/cursor retained。此开关仅用于watch，默认false；显式backfill/probe设置true会拒绝，不能静默忽略补采命令。资金费、已有市场的finalized/canonical复核及基于已有完整覆盖的gas抽样继续运行，不宣称完全禁止历史header/receipt读取。mvp9已实测MEV Blocker近期非空事件、交易回执及原游标处的历史状态；生产将日志与历史queue实现查询交给该来源，主源仍核验finalized、区块和canonical。

未暂停时的 `LST_LOG_MODE=receipts` 行为：每个市场轮次结束后的空闲窗口，从持久 `live-logs.gob` 的原游标扫描至多 8 个 finalized 区块，目标间隔一分钟。逐块校验高度、父哈希和 logsBloom；队列地址 Bloom 阴性时不请求收据，阳性时按区块 hash 读取全部交易收据，校验交易列表、交易/日志连续索引、区块身份及重建 Bloom 后过滤队列事件。只有实际覆盖的整片完成且末端仍 canonical 才推进游标；任何真实来源、解析或身份失败整片保持 failed，事件不提交。未含任何原始队列日志的块不需要解释事件ABI，因此不查询历史合约状态；含任意队列地址原始日志（包括未知topic/anonymous）的每块在解码前以同块hash核验queue实现，未知实现整片失败。空覆盖只证明该地址无日志，不声明历史implementation已核验。Bloom 假阳性可能很多，收据流量按实测评估，不能预设稀疏。此路径没有独立验证 receipt trie root，证明范围是 RPC 返回集与头部/交易列表的一致性。

mvp9生产显式 `watch --from-block 26113044` 固定新事件段，使用独立 `live-logs-from-26113044.gob`；每分钟至多8个finalized区块，失败整片不覆盖、不推进，预算不足缩片。生产 `LST_LOG_REQUESTS_PER_5_MINUTE=8` 显式给新段5次/5分钟加旧补缺至多1次留余量；同host既有Recent记录、冷却和20RPC/min总限额继续保留，不增加并发。CLI未配置时仍保留实时4次/5分钟、显式backfill30次/5分钟，配置只接受1..30。重启或丢失新文件从同一固定Start结合数据库连续已提交覆盖恢复，不能滚动重设当前头而跳过断线缺口。原 `live-logs.gob` 不跳到新段；新段追上finalized且非市场轮空闲预算至少35秒、两来源有预留额度时，每五分钟至多32块低速补原缺口（从原片大小逐步增加，失败缩小），只补到新段起点前。采集截止与父写入截止分开；两进度共用原锁、限速、pending及七表。失败不形成覆盖，成功空区间只证明该返回区间没有队列日志；最大已成功高度不证明两段之间完整。取消 `--from-block` 会回到旧游标；不得把它当作恢复新段的常规操作。

CLI 未设置模式时仍为 `range`：每五分钟、至多 512 块。已知 dRPC code 35 的不一致免费计划拒绝会持久暂停该日志来源/模式，重启后不继续无效请求；真正的范围过大错误才缩片。来源 URL 或模式改变可重新尝试，但保留原游标和 HTTP 冷却。

需要手动补指定缺口时，先停止常驻服务，成对设置 `backfill --from-block`、`--to-block`；receipts 模式仅接受 1–8 块，拒绝 days 回补；range 模式显式范围至多 512 块。当前部署不使用 `backfill --days`。`watch`、`backfill`、`probe` 共享状态目录和锁，不同时运行，不删除或更换目录绕过冷却。行情错过的轮次保持缺失。

恢复已建立的实时段时，从保留 `Start` 读取数据库的连续完整日志覆盖，不能仅凭保存的 `Next` 跳过实际断口。匹配 manifest、complete、committed、canonical、finalized 的显式 backfill 也可衔接该前缀，不再重复抓已补的范围；backfill 不能为尚未建立的旧流选择起点。失败、区间反转或实际未补的间隔不推进，数据库查询失败不改持久游标。手动覆盖复用及旧段隔离的完整 race 回归见[本轮核查记录](../research/2026-10-05-reboot-recovery/report.md)。

`report` 只读本地库，不访问交易所或 RPC，也不补数据；可用 `--from`、`--to` 指定 UTC RFC3339 区间。输出 coverage、quotes、withdrawals、funding、scenarios 五份 CSV 和 summary.json。历史统计默认只取 finalized、canonical、committed 批次；刚取得的 head 行情暂不进入历史统计。

## 来源和配置

| 用途 | 默认 HTTPS 来源 |
| --- | --- |
| Ethereum 实时区块与合约只读调用（生产unit） | https://ethereum.public.blockpi.network/v1/rpc/public |
| 赎回日志与对应历史queue实现状态（生产unit） | https://rpc.mevblocker.io |
| ETHUSDT 元数据、10 档 REST 盘口、mark 与资金费结算 | https://fapi.binance.com |

这三处是中心化 HTTPS 接口，不是本机通过 P2P 同步区块链。生产unit现显式选择BlockPI，路由域名 `ethereum.public.blockpi.network`；CLI未设置环境变量时仍保留dRPC兼容默认值，没有自动回退。2026-10-03（北京时间）按用户选择将 LST 默认 RPC 改为免注册、无需 API key 的 `https://eth.drpc.org`，路由域名为 `eth.drpc.org`。端点配置使用 `LST_RPC_URL`、`LST_BINANCE_URL`；可选 `LST_LOG_RPC_URL` 单独指定日志与历史queue实现读取来源并校验 Ethereum chainId，生产mvp9设置为 `https://rpc.mevblocker.io`（域名 `rpc.mevblocker.io`）；`LST_LOG_MODE` 只接受 `range` 或 `receipts`。程序不自动切换来源。没有设置 `LST_RPC_URL` 时直接使用 dRPC，无需额外 export。不要把带凭据的端点写进 manifest、命令行或仓库。数据库默认 `127.0.0.1:9000`，可用 `--clickhouse`、`--database`，认证环境变量为 `LST_CLICKHOUSE_USER`、`LST_CLICKHOUSE_PASSWORD`。

`config/lst-lido-ethereum.json` 固定公开地址、实现代码 hash、两条 Uniswap 池和四档购买预算。2026-10-02 的低速 probe 已核验这份配置；每次采集启动仍校验链、代理实现、代码、decimals、池 token/fee。未知身份不自动接受。协议升级时先人工核对官方部署，再显式 `probe --out <candidate.json>` 生成候选并审核，不通过 watch 自动改白名单。

**端点实测与历史回补限制：** 旧按高度 512 块 `eth_getLogs` 的 HTTP 400/RPC 35 拒绝仍成立，不能解释为超时或宣称缩片已解决。2026-10-03 在同一 dRPC、同一生产锁/持久 gate 下有限核验区块 `26106948`：全部 369 笔收据含 1773 条连续日志，其中队列 2 条，与标准 `blockHash` 查询的非空日志全集一致；三次请求无 429。PublicNode 的范围探针返回 403、要求 archive token，未启用且其 gate 保持停用。上述是已完成的有限接口核验，不是生产连续覆盖或大范围回补验收。此前预检 timeout 的停止记录见[修复与验证结果](../research/2026-10-03-lst-repair/report.md)。随后同机有限 HTTPS/RPC 复查成功，用户要求再次启动；最新运行状态见[启动记录](../research/2026-10-03-lst-live-restart/report.md)，未更换端点或路由。

个人RPC配置只保存在本机 `var/lst/rpc.env`，不提交或打印该文件。unit以可选 `EnvironmentFile` 读取，其中 `LST_RPC_URL` 会覆盖unit的主RPC值；文件不存在时生产继续BlockPI，CLI独立运行仍用自身默认值。先保持服务暂停，以0600权限创建该文件（尚无文件时，在 `umask 077` 下创建空文件并核验权限），再使用编辑器写入供应商给出的完整个人archive端点。文件格式是 `LST_RPC_URL=完整HTTPS地址`，不要加 `export`。配置完成后由维护者检查（只显示hostname）、验证权限/chain/contract/code/历史非空日志和收据，再启动持续采集；不要直接清除旧Disabled或冷却。若个人端点沿用已被停用的host，须保留旧状态审计、明确处理新的授权身份，不能通过别名绕过旧gate。

默认状态目录 `var/lst/state`，原文证据 `var/lst/evidence`。采集先按 SHA-256 归档 gzip 原文，数据库保存 manifest、来源时间、payload hash 与每批证据根。凭据不进入来源标识；状态绑定数据库目标。写库失败留下不可变 pending 批次，重启先重写同一批，不重新抓源冒充原时间。

2026-10-04（北京时间）按用户要求进行一次性清理：仅删除已提交且成员计数、摘要及类型校验通过的旧批次所引用原文；保留近期及未证明已提交的数据、待写批次、当前身份核验依赖、非空事件回归样本和错误类别样本，共享依赖也保留。删除前核验原文 SHA-256，删除时检查文件身份未被实时写入替换；不生成原文备份。数据库及 payload hash 不删除，现有 `report` 仍只读数据库并核验成员，但已清原文无法重新回读核验。服务仍会归档新请求，本次未增加自动清理任务。清理程序、释放空间和实际服务/查询核验见[清理记录](../research/2026-10-04-lst-cleanup/report.md)。

**mvp10默认留存规则：** `watch` 和 `backfill` 将请求/响应暂存，冻结每批对应文件清单到既有 `pending-batch.gob`；`WriteLST` 确认完整写入后即删除本批原文及证据根文件，不另做备份。数据库失败时原文与原批次一起保留，重启先以同一ID/时间/成员摘要重写，再清理。数据库字段、整数精度、来源时间、payload hash、链上锚点、完整性与finality规则不变；旧pending没有新清单字段也可读取。CLI `probe` 的人工核验材料仍保留，不受此规则影响。

协议/报价解析不完整、失败日志及初始化错误，在 `var/lst/evidence/diagnostics/` 按market、logs、funding、identity、maintenance各保留最新一个gzip样本；每个样本原始内容总量至多1 MiB，附明确Truncated/Missing及hash，不作为完整原文/覆盖证明。样本原子替换后才释放临时原文，清理重试不会用缺失文件覆盖同一份样本。正常未调度成员和十档容量不足不触发原文留存。最终性/规划请求的成功临时文件归入下个提交批次；后台失败及未核验出收据的gas补充查询单独保留有界样本，不被下个成功批次无声清掉。存在待写批次时诊断清理也不删除其原文。启动重试会先保存有界样本并释放该次文件，不无限积累。崩溃发生在冻结清单之前时可能留下未提交原文，保留待诊断，不启动全目录扫描。`report` 明确 `raw_response_policy=typed_database_with_bounded_diagnostics`，只验证数据库成员与摘要，不声称回读已删原文。本次改动及实际服务验收见[验证记录](../research/2026-10-04-lst-response-retention/report.md)。

## 发出节奏

所有外部请求都经同一个 gate：无 JSON-RPC batch、无重定向、每个来源串行。初始化 RPC 每次响应后至少 `LST_RPC_STARTUP_GAP`（未设置默认2秒，生产unit为2秒，允许2..30秒）；初始化完成且启动满 60 秒后，实时 RPC 每次响应后间隔 `LST_RPC_GAP`（默认500毫秒，生产2秒，允许500毫秒..10秒），Quoter 另至少 1 秒；显式历史 RPC 至少 1 秒，日志至少 10 秒。Binance 所有请求间隔至少 5 秒，滚动一分钟至多 12 次和 60 权重，资金费另有低额度。

429 至少冷却 5 分钟并尊重更长 Retry-After，403/418 停用该源；状态落盘，重启不清空。网络失败退避；发送时隙因本地磁盘变慢而失效时放弃发送，不把积压请求一起发出。启动预检失败也等待，不进入高速重试循环。

生产 unit 使用 `LST_RPC_STARTUP_GAP=2s`、`LST_RPC_REQUESTS_PER_MINUTE=20`、`LST_MARKET_INTERVAL=2m`、`LST_ENTRY_ROUTES_PER_ROUND=1`、`LST_PROTOCOL_MODE=multicall`、`LST_RPC_GAP=2s`。同host的所有 RPC 任务共享持久滚动一分钟20次上限；主源与独立日志源各20次，不是两源合计20次，其他进程或其他state目录也不共享此gate；这个本地上限不等于供应商保证的额度。每两分钟只调度一个金额的一条路线，八种金额/路线组合在准时运行时约16分钟轮转一次，配额等待、错过轮次与种子窗口会改变实际覆盖；每日 UTC 12:00 至12:04优先100k，路线继续轮转。每轮仍保存八条entry，未调度七条为 `timing_status=not_scheduled`、`reason=not_scheduled_this_round`，各腿unknown，来源时间/hash/数值为空。分开统计，不用旧报价补齐，也不把跨轮报价拼成同刻容量。CLI未设置上述参数仍兼容120次/分钟、1m、两路线、sequential；interval只接受1m/2m，routes只接受1/2，protocol mode只接受sequential/multicall。

`lst-mvp-7` 起用主网 Multicall3 `0xcA11bde05977b3631167028862bE2a173976CA11` 将13个协议视图合为一次 `eth_call`，保留实际shares与unwrap视图，不用简单总量比例替代。额外同批读取执行区块高度和时间并与选定头核对。全部调用固定、与调用者无关，不发送交易。所有结果严格ABI重编码、逐项成功、返回数/字长/整数/address/bool校验；任一失败协议保持unknown，保留真实外层错误证据，不退回13次串行调用。启动校验官方部署运行时代码Keccak256 `0xd5c15df687b16f2ff992fc8d767b4216323184a2bbc6ee2f9c398c318e770891`，实际code响应加入身份原文证据；不改原manifest及日志游标身份。官方输入与离线pin核验见[公开合约证据](../research/2026-10-03-lst-running/multicall/runtime-pin.json)。单路线普通A/B完整链上路径由19/20降为7/8次RPC，仍按同一 `blockHash`、`requireCanonical=true` 读取，并保留末尾canonical检查。

市场窗口前先等待10个单路线或13个双路线名额，覆盖最多两次额外head查询；独立链头选择阶段最多20秒、3次latest，仅成功但时间较旧/未来时等待至少3秒再查，来源失败立即结束。选中age≤10秒且future≤2秒的真实头后创建Capture；30/25秒市场窗口、发送前5秒预约及15/30秒新鲜度阈值不变。准入响应原时间及全部hash保留，Capture.StartedAt是选择完成后的实际取数开始。选择失败仍写八身份unknown/stale，七条未调度保持空值；无CEX请求时seq/time/hash全NULL，格式错误的头不造anchor。Watch维护和下轮从真实Capture.StartedAt计算，不赶错过轮次。

receipts日志在每轮空闲窗口串行扫描，2m配置每分钟最多两片、候选每片至多8块。规划、日志及随后最终性复用该轮刚返回的真实finalized头，原hash与时间归档。固定整片的最坏成本包括outer/canonical：全Bloom阴性为 `n+2`，全Bloom阳性但无原始queue日志为 `2n+2`，全块含queue日志为 `3n+2`；单凭Bloom阳性不能证明有queue日志，不能据假设稀疏放大请求预算。

`watch` 同来源的live receipts每步发送前检查真实剩余滚动额度及窗口、保留canonical与一个维护槽；额度不足时仅在已完全验证的前缀边界主动结束、核验真实末端canonical，再把实际ToBlock/hash/时间及全部原文证据提交。Reason明确planned_to与covered_to，Next只取实际覆盖末块加1。没有完成首块则失败；任何已发送请求、解析、收据、ABI或canonical失败都保持整片failed/零事件/Next不动，不能把失败转换为成功前缀。独立日志来源、公开Logs和显式backfill仍遵守原指定完整范围全成败契约。空闲分钟会在自身deadline内等待滚动额度释放，错过分钟不重放，不把等待记HTTP或覆盖。

multicall配置每轮最多复核1个已达到finalized的市场，无待复核时不发最终性RPC。Bloom阳性、source限流或慢响应会减少实际吞吐，不能承诺固定进度或仅用最大高度证明追上；以连续游标和真实backlog斜率验收。资金费与有限gas抽样按各自周期维护。最终部署与实测见[本轮记录](../research/2026-10-03-lst-running/report.md)。

同出口上的其他进程不受这个 CLI 的 gate 控制。Binance 响应中的共享权重也用于保守暂停，但公共 RPC 没有可保证的免费额度；这套规则降低突发请求，不能承诺供应商不会封禁。

## 事实语义和当前边界

- 时间为 UTC 微秒；链上金额为 UInt256；盘口价格/数量为 integer tick/lot；资金费为 Decimal。价格与金额路径不用二进制浮点。
- 所有链上结果带区块高度/hash/时间，head 后续核验到 finalized；孤块保留事实但排除统计。完成事件保存官方原始 **[from,to] 两端包含**，单个请求的完成事件合法。
- 两条链上路线是定额报价，不是执行保证；十档永续盘口只是带源时间的 REST 快照，不冒充连续 L2。需要深于十档才能对冲时保持 unknown。
- 首版对需要拆成多个赎回请求的金额保存 request_parts，但换算状态保持 unknown，原因是尚未逐笔核验拆分的整数舍入。普通单请求完整执行合约整数换算。
- gas 只对已完整覆盖的 UTC 日，每天 request/claim 各选择两笔固定交易；最近 30 个完整日最多 120 笔，一轮至多补十笔。补证新增批次，不覆盖原事件；整笔交易费用去重，不当作完整策略的总 gas。
- 后续报价固定原 ETH 赎回量和原短仓量；超过两分钟的目标标 missed。固定延迟情景不能证明真实赎回等待或兑付数量。
- 报告提供名义毛折价、队列匹配/覆盖、已领取兑付比和条件毛差额。费用、账户保证金、完整资金费覆盖和真实执行成本未齐时不输出净利润或 APR，不将 unknown 当作无机会或零成本。

实际建表、实网样本及验证结果记录在[验收记录](../research/2026-10-02-lst-implementation/validation.md)。

## 健康检查与异常解释

进程 active 只说明服务存活。先检查 journal，再检查研究库最近的 market 时间是否前进、协议状态以及各档报价的有效成员比例：

```bash
systemctl --user show crypto-market-info-lst.service \
  -p ActiveState -p SubState -p UnitFileState -p NRestarts
journalctl --user-unit=crypto-market-info-lst.service -n 30 --no-pager
/home/ubuntu/.local/share/crypto-market-info-clickhouse-bin/clickhouse client \
  --multiquery < docs/lst-data-health.sql
```

[健康 SQL](lst-data-health.sql) 只读本地库，显示最近 15 分钟的 capture、协议状态、各档 entry 的调度/未调度、完整及时与十档容量不足计数，以及增量日志成功范围。最大成功高度不能单独证明连续覆盖，需核对按高度排列的成功范围和持久游标。购买预算字段为 USDT 原子单位（1 USDT = 1,000,000 raw）。生产行情目标每两分钟一轮，低频任务有各自周期，不要求每分钟都有资金费或日志批次。健康统计包含 head；历史报告会排除 pending finality、非 canonical 或未提交批次，因此两者行数不应直接对齐。SQL 不核验成员摘要，不能代替报告对已接受批次的回读校验。

| 记录或现象 | 含义与处理 |
| --- | --- |
| `partial` / 某成员 `unknown` | 批次中存在不完整成员；逐条检查各腿状态，不把八条成员都算作机会，也不丢掉同批已取得的有效观测。 |
| `not_scheduled_this_round` | 生产计划本轮只抓一个金额的一条路线，其余七条是无观测占位；不是采集失败或有效报价。 |
| `round_budget_exhausted` / `local_gate_budget_exhausted` | 本轮预算或本地限速时隙不足，未发送的请求与网络超时分开；后续按原节奏继续，不补发旧报价。 |
| `source_cooldown until=...` | 持久冷却跨越本轮，立即保存本地缺失，不等待到整轮耗尽、不清空冷却。 |
| `transport_dns_error` / `transport_connect_error` / `transport_proxy_error` / `transport_http_timeout` | HTTP 传输失败；`HTTPStatus=0`表示未取得HTTP响应，单次超时不能定位本机路由故障。按用户最新指令，偶发失败留痕、排除有效数据，使用现有退避继续下一轮；连续多轮来源失败或持续无有效报价时排查并告知用户，不修改路由。attempt保留时间、请求hash及失败分类。 |
| `stage=timing cause=... value_ms=...` | 记录违反原新鲜度门槛的具体原因，可同时出现多个；不能用采集时间覆盖来源时间来消除。 |
| `transport_send_reservation_expired` | 本地持久化发送预约过期，程序放弃发送以避免突发；持续出现时检查磁盘延迟及进程负载，保留原限速状态。 |
| `cex_ten_level_capacity_insufficient` | 这笔量不能在采集到的十档对冲深度内完成；不能把链上腿的报价视为完整容量。 |
| `log_source_range_capability_rejected` | 已知不一致免费计划范围拒绝或 receipts 方法不支持；日志任务持久暂停，failed 不推进游标，显式补缺退出。需核验来源能力后恢复，重启不清除。 |
| `stage=... cause=... http_attempted=... http=... rpc=... request=... response=...` | 分层错误定位，包括 shares、CEX depth/mark 和链上报价；关联归档请求/响应，保留限流语义，不再用通用 unavailable 隐藏原因。 |
| `source_rate_limited` / `source_disabled` | 进入持久冷却或来源停用；检查归档响应，先解决来源问题，不通过删除 gate、换状态目录或快重启绕过。 |
| head 有数据但 report 无该行情 | 尚未通过最终性条件，查看 coverage 的排除原因；不可手工提升 finality。coverage 的 `orphaned` 标签表示非 canonical，也可能是失败批次，不单凭标签认定真实链重组。 |
| `unknown_costs_and_funding_coverage` | 尚不能给出净收益或 APR；缺失成本、日志覆盖和完整持仓期资金费仍为 unknown。 |

## 维护、配置与恢复

升级二进制或执行手动补缺/probe 时，先停止常驻实例，避免两份进程争用同库/状态锁：

```bash
systemctl --user stop crypto-market-info-lst.service
GOCACHE=/tmp/lst-go-cache go build -o var/lst/lst-data ./cmd/lst-data
systemctl --user start crypto-market-info-lst.service
systemctl --user status crypto-market-info-lst.service
```

仅修改 Go 代码时无需重新安装 unit。修改公开启动参数或端点时，维护[仓库 unit](../deploy/systemd/crypto-market-info-lst.service)，验证后复制到 `/home/ubuntu/.config/systemd/user/crypto-market-info-lst.service`，再 `systemctl --user daemon-reload` 与 `restart`。service 固定了 `Environment=LST_RPC_URL`、`Environment=LST_LOG_RPC_URL`、`Environment=LST_LOG_MODE=range`、`Environment=LST_PAUSE_LIVE_LOGS=false`、固定 `watch --from-block 26113044` 及上述节奏配置；在终端 export 只影响前台 CLI，不会覆盖正在运行的 systemd 配置。带凭据的端点和认证环境文件不得写入仓库或研究证据。

恢复所需材料包括研究库、整个 `var/lst/state`、现存 `var/lst/evidence` 和固定 manifest；如制作恢复副本，需停止服务后取得一致副本，恢复时核对数据库目标与 manifest 身份。已按上述策略清理的历史原文无法从数据库 hash 还原。七份旧程序备份已按用户要求删除，当前在用程序保留。`pending-batch.gob` 保存待重写的原批次，`database-target` 绑定数据库，`live-logs.gob` 保存旧缺口进度，`live-logs-from-26113044.gob` 保存固定新段进度；两者分别保存实际覆盖、片大小及被暂停的来源/模式身份和原因，`http-*.json` 保存来源额度/冷却/停用；不要手工改写或删除这些文件。重启先重写 pending，再恢复种子和游标，不把新行情贴到旧采集时间。

断线补缺从原日志覆盖点逐片恢复，实际资金费按固定窗口重查；该次恢复因 Binance 真实 timeout 已停止；实际推进速度、报价完整率和剩余覆盖缺口见[启动验收](../research/2026-10-03-lst-live-restart/report.md)。本程序不能重建断线时的 AMM 报价或 CEX 十档快照，缺口保持缺失。仅 range 模式保留旧 `backfill --days`，当前部署不运行。

## 记录时点与待验收数据

- [恢复运行与节奏修复](../research/2026-10-03-lst-live-restart/report.md)：实际 429、降低发送密度、报价与日志落库验收。
- [本轮修复](../research/2026-10-03-lst-repair/report.md)：非空收据实测、代码修复、测试、部署和网络超时停止记录。
- [初版验收](../research/2026-10-02-lst-implementation/validation.md)：建表、测试、PublicNode 短跑及旧失败记录。
- [dRPC 切换验收](../research/2026-10-03-lst-drpc/validation.md)：历史状态与日志接口能力分别核验。
- [常驻启动记录](../research/2026-10-03-lst-drpc/service-startup.md)：2026-10-03 启动、首轮缺失与后续恢复。

这些都是对应时点的证据，当前状态以 systemd 和只读查询为准。已完成单块非空日志与全部收据一致性核验；尚待验收的是生产连续赎回覆盖、调度报价的实际成功率、等待/兑付关联、完整 UTC 日 gas 抽样，以及包含原始证据空间的日占用和长期查询耗时；缺失项不能用理论数字补成验收通过。用户已取消大范围历史回补，这些数据从现有实时采集与允许的断线补缺中逐步积累；来源限制未解决的部分继续保持未验收。


mvp8显式暂停日志时，启动身份锚点使用真实latest头（启动日志identity_anchor=latest），仍完整核验chain、proxy实现、官方code pins、decimals、池与固定Multicall runtime；EIP-1898 requireCanonical不变。mvp9所有watch预检都用真实latest，实际日志仍须finalized与对应历史ABI证据。当前身份不能证明旧日志ABI或完成历史覆盖，因此旧游标/缺口保留；每条行情的head/finalized状态仍由真实锚点及随后finality复核区分。暂停的独立LogRPC初始化链检查同时跳过，避免停用/冷却的旧来源挡住当前主源；恢复日志后仍重新校验来源。
