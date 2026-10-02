# Across 稳定币中继数据采集

2026-10-02 已实现独立命令 `cmd/across-data`，并在现有 ClickHouse 实例创建 `crypto_market_info_across` 的七张表。范围是 Base ↔ Arbitrum One 原生 USDC；同时保留这两个 SpokePool 的其他路线事实，用来识别退款混合来源与研究覆盖缺口。程序采集公开数据，报告输出费用空间，不产生净盈利承诺。

## 使用

在仓库根目录运行：

```bash
go build -o var/across/bin/across-data ./cmd/across-data
var/across/bin/across-data init-schema
var/across/bin/across-data preflight
var/across/bin/across-data backfill --days 30
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

不将带凭据的 RPC URL 写入原始证据；只保留主机名、公开请求和响应。币安按用户要求用 `.com`，每 60 秒尝试刷新两个币对，原始十进制价格、数量与时间随 probe 保存。失败不刷新旧报价时间；超过 60 秒不附加到新 probe。

[manifest](../config/across-research.json) 固定链、token、SpokePool、部署实现与代码 hash。加载 manifest 会核对本地 verified 源文件 SHA-256、实际 bytecode 和嵌入 ABI；部署时须带上其引用的 `research/2026-10-02-across-implementation/verified-*.json`。Base explorer 的源验证标志是部分验证，实际部署 runtime 与 RPC 返回 bytecode 已逐字节对应；Arbitrum 同样进行了实际 bytecode 对应。启动及固定区块读取仍核验链身份、代理实现和 USDC 元数据。证据说明见[协议验证](../research/2026-10-02-across-implementation/protocol-sources.json)。

Base 实测每批 JSON-RPC 最多 10 成员；Arbitrum 每批最多 20。每链最多 2 个 HTTP 请求在途。区块头按批读取，范围超限或失败会保留证据并尝试更小范围；数据库或证据文件写入失败直接停止，不能被当成范围过大继续网络重抓。

历史 unit 显式传 `--rpc-min-interval 500ms`：每个 Reader 拆成单成员串行请求，响应后至少再等500毫秒，取消时停止排队，不生成未发起请求的证据。Header预算包含每个请求及等待，单请求仍有5秒超时。默认0保留实时批量行为。这是单进程节流，不保证两进程合计不触及来源限额；历史限流退出后由systemd退避60秒再续跑。

## 保存与恢复语义

七表字段见 [DDL](across-stablecoin-data-schema.sql)，嵌入程序的同结构 DDL 位于 `internal/storage/clickhouse/across_schema.sql`。原子金额与 ID 用 UInt256，价格用 Decimal(38,18)，时间统一 UTC 微秒；二进制 hash/address 保持原始字节，成员摘要不通过 JSON 字符串转换。

原始 RPC、价格响应和 capture 成员清单先写 `var/across/evidence/<前两位>/<SHA256>.json.gz`，再批量写事实，最后提交 capture。重复写使用冻结的 ID、内容、时间和摘要；读取先取 capture 最新 revision，再校验 canonical、committed、表成员计数/摘要与原始证据。证据目录和研究数据库需要一起保留。

重组撤销触及旧分支的所有 capture 类型和尝试；源链孤块的 probe 不能进入候选。finalized hash 冲突停止进程。日志恢复使用区间并集，二分重抓的多段可以共同填补缺口。重新加载的未过期订单标为 restart，不能伪造此前持续在线。

实时订单包含实际计划和执行的 +2/+5/+10 秒 probe。每轮 probe RPC 预算 5 秒，超时、迟到、跳过、取消都保存。目标延后超过 1 秒不算该档准时样本；失败或孤块查询里的 Filled 状态不能终结订单。超过本地截止而无法再查询时，仅记录 `local_deadline_elapsed_state_unverified` 取消，不声称已经成交。新鲜门槛默认 5 秒，未来时钟容忍 2 秒。

## 报告与当前限制

输出 `coverage.csv`、`orders.csv`、`refunds.csv`、`summary.json`。原始条款、实际更新后 fast fill 和真实 live 开放观测分别计算逐单非负费用空间；不把多个 probe 或多条更新重复算成订单。CSV 的成交 gas 是整笔共享交易费用，不能逐订单相加；summary 按链/区块 hash/交易 hash 去重。

退款只按收据中的唯一 USDC Transfer 核验地址/批次实际到账；逐单归属保持 unknown。Base 收据缺适用 operator fee 信息时总费用为 NULL；Arbitrum 不重复加 L1 gas。库存 3/6/12/24 小时仅是假设情景，真实周转、实际容量和净收益均未验证。

初次验收使用两链各512个 finalized 区块的小窗口，以及独立 `crypto_market_info_across_livecheck` 库的90秒实时测试。2026-10-02 13:57 UTC开始主库常驻采集，随后启动独立 `crypto_market_info_across_history` 库的固定30日回补。回补按 Base 后 Arbitrum 顺序执行，不代表已经完成。证据分别位于 `var/across/evidence` 与 `var/across/history-evidence`；查询 history 时必须同时指定相应 `--database` 和 `--evidence`。跨库可能有重复事件，不能直接累加两个报告的订单或费用空间。

90秒测试公共 RPC 循环约6–14秒，本次启动追赶阶段约12–36秒，`poll_millis=1000` 是目标间隔，不能当作已达到每秒采集。默认 Arbitrum 节点部分历史状态不可用，相关原始日志保存为 partial；日志扫描游标推进不等于全部事件已经解码。当前实现保存真实延迟和缺测；这组实时样本不足以判断抢单能力，也不能据此否决 Across 的全部机会。

审核与验证见 [代码审核](../discuss/0013-across-stablecoin-code-review.md) 和 [验收记录](../research/2026-10-02-across-implementation/validation.md)。
