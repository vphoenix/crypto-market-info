# LST 公开数据采集实现

2026-10-02，入口 `cmd/lst-data`，独立库 `crypto_market_info_lst`。采集 Ethereum Lido stETH/wstETH 的定额买入、官方赎回队列、ETH 退出报价，以及 Binance ETHUSDT 永续对冲价格和实际结算资金费。只读取公开数据，不使用账户或私钥。

设计见[方案](lst-redemption-data-mvp-design.md)，七表字段见[DDL](lst-redemption-data-schema.sql)，审核见[0016](../discuss/0016-lst-redemption-data-code-review.md)。程序是一个 Go CLI、一个持久状态目录、一个证据目录和现有 ClickHouse，没有消息队列或新常驻基础设施。

## 运行

从仓库根目录执行：

下面的初始化和前台命令用于安装或调试；当前已由 systemd 管理，先按后文维护步骤停止服务，再运行需要采集锁的命令。日常查看状态或生成报告无需停止服务。

```bash
go build -o var/lst/lst-data ./cmd/lst-data
var/lst/lst-data init-schema
var/lst/lst-data watch --once --duration 5m
var/lst/lst-data watch
```

`init-schema` 已对研究库执行，可以重复运行。`watch --once` 包含缓速身份预检，只生成一个市场批次；不会启动历史回补，也不执行该轮后台维护。`watch` 是前台持续运行，收到 SIGINT/SIGTERM 退出。2026-10-03 02:36:08（北京时间）按用户要求启动用户级 systemd 常驻服务 `crypto-market-info-lst.service`，并启用开机启动；[unit](../deploy/systemd/crypto-market-info-lst.service) 只运行 `watch`，沿用上述默认状态和证据目录，失败后等待 60 秒重启。

当前按用户要求持续实时采集，补采只用于断线缺口，不运行 30/60 日历史回补。查看常驻状态和生成只读报告：

```bash
systemctl --user status crypto-market-info-lst.service
journalctl --user -u crypto-market-info-lst.service -n 30 --no-pager
var/lst/lst-data report --out var/lst-reports/latest
```

`watch` 每五分钟尝试一次增量日志范围，从持久 `live-logs.gob` 恢复缺口游标，每片最多 512 块，完成才推进游标；行情错过的轮次不能补造。需要手动指定日志缺口时，先停止常驻服务，再成对使用 `backfill --from-block`、`--to-block`（至多 512 块），完成后恢复服务。CLI 保留显式 `backfill --days` 的旧功能，但本部署不使用它。`watch`、`backfill`、`probe` 共享同一状态目录和锁，不能同时运行。不要删除或更换目录来绕过冷却；日志补缺仍受下述 dRPC 接口限制。

`report` 只读本地库，不访问交易所或 RPC，也不补数据；可用 `--from`、`--to` 指定 UTC RFC3339 区间。输出 coverage、quotes、withdrawals、funding、scenarios 五份 CSV 和 summary.json。历史统计默认只取 finalized、canonical、committed 批次；刚取得的 head 行情暂不进入历史统计。

## 来源和配置

| 用途 | 默认 HTTPS 来源 |
| --- | --- |
| Ethereum 区块、合约只读调用、日志与有限收据 | https://eth.drpc.org |
| ETHUSDT 元数据、10 档 REST 盘口、mark 与资金费结算 | https://fapi.binance.com |

这两处是中心化 HTTPS 接口，不是本机通过 P2P 同步区块链。2026-10-03（北京时间）按用户选择将 LST 默认 RPC 改为免注册、无需 API key 的 `https://eth.drpc.org`，路由域名为 `eth.drpc.org`。替换端点仍使用 `LST_RPC_URL`、`LST_BINANCE_URL`；没有设置 `LST_RPC_URL` 时直接使用 dRPC，无需额外 export。不要把带凭据的端点写进 manifest、命令行或仓库。数据库默认 `127.0.0.1:9000`，可用 `--clickhouse`、`--database`，认证环境变量为 `LST_CLICKHOUSE_USER`、`LST_CLICKHOUSE_PASSWORD`。

`config/lst-lido-ethereum.json` 固定公开地址、实现代码 hash、两条 Uniswap 池和四档购买预算。2026-10-02 的低速 probe 已核验这份配置；每次采集启动仍校验链、代理实现、代码、decimals、池 token/fee。未知身份不自动接受。协议升级时先人工核对官方部署，再显式 `probe --out <candidate.json>` 生成候选并审核，不通过 watch 自动改白名单。

**端点实测与历史回补限制：** dRPC 已通过一次完整 `watch --once`，落库一条协议状态、八条报价观测，其中一条完整且及时；其他成员保留缺失原因。原失败区块及约 60 日前高度 `25672300` 的历史合约状态，按高度和 EIP-1898 blockHash 均通过，见[状态探针](../research/2026-10-02-lst-implementation/public-archive-rpc-probe.json)。但完整程序在 `26104300..26104811` 的 512 块日志查询被 dRPC 以 HTTP 400 / code 35 拒绝，因此**历史回补未通过**。程序保留 HTTP 与 RPC 错误，保存 failed、零事件；这条不一致的“超过 10000 块”提示不触发连续二分。按 blockHash 的单块空日志响应不能代替完整历史日志验收。原 PublicNode 历史状态失败记录继续保留。实测与限速证据见[切换记录](../research/2026-10-03-lst-drpc/validation.md)。

默认状态目录 `var/lst/state`，原文证据 `var/lst/evidence`。SHA-256 寻址的 gzip 原文、manifest 和每批证据根与数据库一同保留。凭据不进入来源标识；状态绑定数据库目标。写库失败留下不可变 pending 批次，重启先重写同一批，不重新抓源冒充原时间。

## 发出节奏

所有外部请求都经同一个 gate：无 JSON-RPC batch、无重定向、每个来源串行。初始化 RPC 每次响应后至少 2 秒；初始化完成且启动满 60 秒后，实时 RPC 间隔至少 500 毫秒，Quoter 另至少 1 秒；显式历史 RPC 至少 1 秒，日志至少 10 秒。Binance 所有请求间隔至少 5 秒，滚动一分钟至多 12 次和 60 权重，资金费另有低额度。

429 至少冷却 5 分钟并尊重更长 Retry-After，403/418 停用该源；状态落盘，重启不清空。网络失败退避；发送时隙因本地磁盘变慢而失效时放弃发送，不把积压请求一起发出。启动预检失败也等待，不进入高速重试循环。

市场循环每 60 秒一次、总预算 30 秒，预留结束时的区块核验。优先到期的同量后续询价；四个购买金额档按分钟轮转首位，每档约四分钟一次优先机会。每日 UTC 12:00 至 12:04 为种子窗口，优先 100k 两条路线。不同分钟的报价不能拼成同一时刻的容量。来不及取得的成员保持 unknown；仍保存八条 entry，不能用上一轮报价补齐。后台按各自周期执行增量日志、实际资金费确认、有限 gas 抽样与最终性核验，错过的行情轮次不集中重放。

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
journalctl --user -u crypto-market-info-lst.service -n 30 --no-pager
/home/ubuntu/.local/share/crypto-market-info-clickhouse-bin/clickhouse client \
  --multiquery < docs/lst-data-health.sql
```

[健康 SQL](lst-data-health.sql) 只读本地库，显示最近 15 分钟的 capture、协议状态、各档 entry 完整及时观测，以及增量日志成功覆盖。购买预算字段为 USDT 原子单位（1 USDT = 1,000,000 raw）。行情目标每分钟一轮，低频任务有各自周期，不要求每分钟都有资金费或日志批次。健康统计包含 head；历史报告会排除 pending finality、非 canonical 或未提交批次，因此两者行数不应直接对齐。SQL 不核验成员摘要，不能代替报告对已接受批次的回读校验。

| 记录或现象 | 含义与处理 |
| --- | --- |
| `partial` / 某成员 `unknown` | 批次中存在不完整成员；逐条检查各腿状态，不把八条成员都算作机会，也不丢掉同批已取得的有效观测。 |
| `round_budget_exhausted` | 本轮来不及完成；记录缺失，后续轮次按原节奏继续，不集中补发旧报价。 |
| `transport_send_reservation_expired` | 本地持久化发送预约过期，程序放弃发送以避免突发；持续出现时检查磁盘延迟及进程负载，保留原限速状态。 |
| `cex_ten_level_capacity_insufficient` | 这笔量不能在采集到的十档对冲深度内完成；不能把链上腿的报价视为完整容量。 |
| `source_http_400` / `rpc_error_35` | 本次 dRPC 日志请求被拒绝；failed 范围不推进覆盖，live 五分钟后再尝试，显式补缺退出。重启或拆小不能被当作已解决端点能力问题。 |
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

仅修改 Go 代码时无需重新安装 unit。修改公开启动参数或端点时，维护[仓库 unit](../deploy/systemd/crypto-market-info-lst.service)，验证后复制到 `/home/ubuntu/.config/systemd/user/crypto-market-info-lst.service`，再 `systemctl --user daemon-reload` 与 `restart`。service 固定了 `Environment=LST_RPC_URL`；在终端 export 只影响前台 CLI，不会覆盖正在运行的 systemd 配置。带凭据的端点和认证环境文件不得写入仓库或研究证据。

备份与恢复需一起保留研究库、整个 `var/lst/state`、`var/lst/evidence` 和固定 manifest；停止服务后取得一致副本，恢复时核对数据库目标与 manifest 身份。`pending-batch.gob` 保存待重写的原批次，`database-target` 绑定数据库，`live-logs.gob` 保存日志覆盖进度，`http-*.json` 保存来源额度/冷却/停用；不要手工改写或删除这些文件。重启先重写 pending，再恢复种子和游标，不把新行情贴到旧采集时间。

断线补缺能力分别看待：官方日志可以从已确认覆盖点逐片重查，实际资金费可以按固定窗口重查，但当前 dRPC 日志接口拒绝仍会阻止前者；本程序不能重建断线时的 AMM 报价或 CEX 十档快照，这些时间段保持缺失。CLI 的 `backfill --days` 留作旧功能，当前部署不运行它。

## 记录时点与待验收数据

- [初版验收](../research/2026-10-02-lst-implementation/validation.md)：建表、测试、PublicNode 短跑及旧失败记录。
- [dRPC 切换验收](../research/2026-10-03-lst-drpc/validation.md)：历史状态与日志接口能力分别核验。
- [常驻启动记录](../research/2026-10-03-lst-drpc/service-startup.md)：2026-10-03 启动、首轮缺失与后续恢复。

这些都是对应时点的证据，当前状态以 systemd 和只读查询为准。尚待实测的是非空赎回日志的完整覆盖、等待/兑付关联、完整 UTC 日 gas 抽样，以及包含原始证据空间的日占用和长期查询耗时；缺失项不能用理论数字补成验收通过。用户已取消大范围历史回补，这些数据从现有实时采集与允许的断线补缺中逐步积累；来源限制未解决的部分继续保持未验收。
