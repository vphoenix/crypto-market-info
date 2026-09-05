# Crypto Market Info

本项目用于持续采集可公开验证的市场、利率和链上规则数据，并将其标准化后持久化，供后续套利机会检查、历史回放、流动性分析和人工风险审查使用。

当前已经实现：

- Binance、OKX 现货及永续合约 L2 盘口，以及 Bybit USDT 线性永续 L2 盘口；
- Binance、OKX 和 Bybit 永续资金费率；
- JustLend TRX 收益、TRON 原生质押，以及 SOL 第一、第二阶段收益（LST、原生质押、Kamino 和 Save）；
- AVAX 第一阶段：OKX 公开出借 APR、Aave V3/V4 WAVAX 基础存款历史 APY；
- AVAX 第二阶段：BENQI sAVAX、Ankr ankrAVAX 兑换率，以及 BENQI AVAX 基础借贷 APR、同块现金和退出规则（已部署；实际运行状态见运行说明）。

以后还可能增加其他 CEX、DEX、收益协议、链状态、桥和二层流通状态、借贷费率、指数价格、手续费或 gas 等公开数据。当前六张表不是最终边界；不同语义的数据应建立自己的定类型模型和表，不能全部塞入盘口或收益表。

项目当前不负责下单、资金划转或密钥管理；它只生产可核验的数据。

整体组件关系、启动顺序、失败边界和未来扩展原则见[系统总体架构](docs/architecture.md)。套利机会定义和已迁入的 `ARB-xxxx` 策略资料见[套利机会与策略资料](docs/arbitrage/README.md)。这些资料用于说明数据需求，不会自动启用交易执行。

## 当前机器的运行方式

当前长期采集使用**宿主机原生 ClickHouse + 编译后的 collector + 用户级 systemd 开机服务**，不是 Docker Compose。`ubuntu` 用户已启用 linger，因此机器启动后无需登录就会拉起数据库和采集器。实际路径、启用的数据源、检查命令和重启注意事项见[当前部署与运行说明](docs/runtime-operations.md)。`docker compose ps` 为空不代表数据库未运行；在这台机器上不要直接执行下面的开发环境启动命令。

## 可选的本地开发环境

以下是另一套独立开发环境，需要 Go、Docker 和 Docker Compose。它使用 `compose.yaml` 固定的 ClickHouse 镜像，并不描述上面的长期运行服务。

仅在没有现有数据库占用 `8123`、`9000` 端口、且确实需要独立开发数据库时启动：

```bash
docker compose up -d clickhouse
```

检查服务和版本：

```bash
docker compose ps
docker compose exec clickhouse clickhouse-client --query "SELECT version()"
```

这套 Compose 环境的数据库名为 `crypto_market_info`，HTTP 和 native 端口分别为 `127.0.0.1:8123`、`127.0.0.1:9000`。无密码访问只用于本机开发，端口不得绑定到公网地址。其数据保存在 Docker volume 中，与当前宿主机数据库的数据目录不同；`docker compose down` 不删除 volume，但 `down -v` 会删除开发数据。

## 开发时运行采集程序

以下 `go run` 示例用于开发调试，不是当前机器的守护启动方式；不要在已有采集器运行时再向同一个数据库启动一份。程序只使用公开 REST/WebSocket 接口，不需要交易所 API key。默认采集 Binance 与 OKX 的 BTC/USDT 现货和 USDT 线性永续；Bybit 和收益采集默认关闭，需要明确启用：

```bash
go run ./cmd/collector
```

只在独立开发库启用一个 Bybit USDT 线性永续流：

```bash
CLICKHOUSE_DATABASE=bybit_market_dev \
BINANCE_SPOT_SYMBOLS=- BINANCE_PERP_SYMBOLS=- \
OKX_SPOT_SYMBOLS=- OKX_PERP_SYMBOLS=- \
BYBIT_PERP_SYMBOLS=BTCUSDT \
go run ./cmd/collector
```

Bybit 进入生产前还必须按专项设计跨过一个资金费结算边界，并对真实 `orderbook.1000` 长时记录相邻 `u` 差值；当前逐一连续校验是 fail-closed 的待实盘验证假设。

常用环境变量：

| 变量 | 默认值 | 说明 |
|---|---|---|
| `CLICKHOUSE_ADDRS` | `127.0.0.1:9000` | 逗号分隔的 native 地址 |
| `CLICKHOUSE_DATABASE` | `crypto_market_info` | 数据库名 |
| `CLICKHOUSE_USERNAME` | `default` | 用户名 |
| `CLICKHOUSE_PASSWORD` | 空 | 密码；不要写入仓库 |
| `BINANCE_SPOT_SYMBOLS` | `BTCUSDT` | 逗号分隔；设为 `-` 可禁用 |
| `BINANCE_PERP_SYMBOLS` | `BTCUSDT` | Binance USDT-M 永续 |
| `OKX_SPOT_SYMBOLS` | `BTC-USDT` | OKX 现货 |
| `OKX_PERP_SYMBOLS` | `BTC-USDT-SWAP` | OKX USDT 线性永续 |
| `BYBIT_PERP_SYMBOLS` | `-` | Bybit USDT 线性永续；逗号分隔，设为 `-` 可禁用 |
| `FUNDING_ENABLED` | `true` | 是否采集永续资金费率 |
| `MINUTE_QUEUE_CAPACITY` | `512` | 等待写入 ClickHouse 的分钟盘口批次数；队列满时丢弃新完成的分钟并记录错误 |
| `JUSTLEND_YIELD_ENABLED` | `false` | 是否每小时采集四条 JustLend TRX 收益路线 |
| `JUSTLEND_BASE_URL` | `https://openapi.just.network` | JustLend 公开 API 地址 |
| `TRON_STAKING_YIELD_ENABLED` | `false` | 是否每 6 小时采集 TRON 前 127 名 SR 收益 |
| `TRON_HTTP_URL` | `https://api.trongrid.io` | TRON 公开 HTTP 节点地址 |
| `SOL_YIELD_ENABLED` | `false` | 是否每 6 小时采集固定 SOL 收益路线（含 LST、原生质押、Kamino 与 Save）及配置的验证者收益 |
| `AVAX_YIELD_ENABLED` | `false` | 是否每小时采集 AVAX 第一、第二阶段共六条路线；OKX 复用 `OKX_REST_URL` |
| `AVALANCHE_RPC_URL` | `https://api.avax.network/ext/bc/C/rpc` | 第二阶段共用的 Avalanche C-chain 主网 RPC；必须支持 finalized 和按 block hash 读取 |
| `SOLANA_RPC_URL` | `https://api.mainnet.solana.com` | Solana mainnet finalized RPC 地址 |
| `SOL_VALIDATOR_VOTE_ACCOUNTS` | `-` | 逗号分隔的原生质押 vote account 白名单；`-` 表示不采集验证者 |
| `JITO_SOL_BASE_URL` | `https://kobe.mainnet.jito.network` | JitoSOL 官方公开 API 地址 |
| `MARINADE_APY_BASE_URL` | `https://apy.marinade.finance` | mSOL 与 Marinade Native 官方 APY API 地址 |
| `MARINADE_VALIDATORS_BASE_URL` | `https://validators-api.marinade.finance` | 原生验证者历史 API 地址 |
| `KAMINO_BASE_URL` | `https://api.kamino.finance` | Kamino Main SOL 历史指标 API 地址 |
| `SAVE_BASE_URL` | `https://api.solend.fi` | Save Main SOL 当前与历史利率 API 地址 |

同时启用当前两类 TRX 收益：

```bash
JUSTLEND_YIELD_ENABLED=true \
TRON_STAKING_YIELD_ENABLED=true \
go run ./cmd/collector
```

启用 SOL 第一、第二阶段全部固定路线；如需原生验证者历史，再填写 vote account 白名单：

```bash
SOL_YIELD_ENABLED=true \
SOL_VALIDATOR_VOTE_ACCOUNTS=CcaHc2L43ZWjwCHART3oZoJvHLAe9hzT2DJNUpBzoTN1 \
go run ./cmd/collector
```

仅在独立开发库运行 AVAX 第一、第二阶段（不启用盘口）：

```bash
CLICKHOUSE_DATABASE=avax_yield_dev \
BINANCE_SPOT_SYMBOLS=- BINANCE_PERP_SYMBOLS=- \
OKX_SPOT_SYMBOLS=- OKX_PERP_SYMBOLS=- \
BYBIT_PERP_SYMBOLS=- \
AVAX_YIELD_ENABLED=true \
go run ./cmd/collector
```

六条路线各自每小时采集，启动即首采，失败按 10 分钟重试。第一阶段重抓近期历史窗口；第二阶段读取 finalized 同块快照，从启动日起积累历史。两种 LST 的利率暂为 NULL，收益以后按兑换率历史计算；BENQI 借贷直接保存基础 APR。固定 `LAST_WEEK` 来源平均 APY 不与当前快照混写，永续资金费率不计入收益。

仍使用两张收益表，不需要 API key。新版启动时会为旧观测表幂等增加 `pool_cash`、`redemption_window_seconds` 两列，旧数据保留 NULL；请勿用上述开发命令向生产库启动第二份采集器。

端点也可通过 `BINANCE_SPOT_REST_URL`、`BINANCE_FUTURES_REST_URL`、`BINANCE_SPOT_WS_URL`、`BINANCE_FUTURES_WS_URL`、`BINANCE_FUTURES_MARKET_WS_URL`、`OKX_REST_URL`、`OKX_WS_URL`、`BYBIT_REST_URL` 和 `BYBIT_WS_URL` 覆盖，便于代理、测试环境和区域域名切换。Binance Futures 深度默认使用 `/public/ws`，mark price 资金费率默认使用 `/market/ws`，不能混用。

盘口启动和重连受进程内共享门控保护：Binance 1000 档 REST 快照相邻请求至少间隔 1 秒，OKX 盘口与资金费率 WebSocket 相邻建连至少间隔 500 毫秒，所有 Bybit 盘口与资金费率连接相邻拨号至少间隔 1 秒。REST 返回 `429` 或 `418` 时，同一交易所客户端会遵守 `Retry-After` 并暂停全部后续请求；Bybit 仅在 HTTP `403` 响应体明确包含 `access too frequent` 时等待至少 10 分钟，地区或权限封锁类 403 直接报错；响应体 `retCode=10006` 也会按响应头或保守退避更新同一个请求门控。限流期间不进行快速重试，metadata 启动会在当前进程内等待后继续，而不是依赖 systemd 30 秒重启形成请求风暴。

打印程序实际执行的完整 ClickHouse DDL：

```bash
go run ./cmd/collector -print-ddl
```

回放某个 UTC 秒的前 50 档盘口：

```bash
go run ./cmd/collector \
  -replay-instrument 1 \
  -replay-time 2026-08-19T01:02:03Z
```

单元测试与 ClickHouse 集成/一天压缩测量：

```bash
go test ./...
CLICKHOUSE_INTEGRATION=1 go test ./internal/storage/clickhouse -v
```

采集进程不使用 Redis、消息队列或套利计算组件。网络断开、序列断档、解析失败或增量队列溢出时，对应盘口立即失效，重新同步成功前不会被秒级采样器保存。

永续资金费率的估算值来自 Binance、OKX，以及启用时的 Bybit 各一条公共 WebSocket 连接；实际结算值在目标结算后第 2、5、15、60 分钟由各交易所独立的串行 REST worker 确认，相邻请求至少间隔 1 秒。WebSocket 推送过期时该整点留空，不回退到逐 instrument REST 轮询。

JustLend、TRON、SOL 和 AVAX 收益使用独立 Runner 与 ClickHouse writer。收益单轮失败只形成明确缺口并按规则重试，不会用旧利率冒充当前数据，也不会把永续资金费率加入收益率。

## 数据存储模型

每个交易流按秒维护前 50 档买盘和前 50 档卖盘，但不把每秒的完整盘口重复写入数据库：

1. 每分钟第 0 秒写入一次完整的 50 档盘口快照；
2. 第 1 至 59 秒仅写入相对上一个有效采样状态发生变化的价格和数量；
3. 查询任意秒时，以该分钟快照为起点回放最多 59 秒差量。

所有价格和数量以整数 tick / lot 保存；时间统一为 UTC。新登记的永续合约还会把交易所发布的产品版本字段纳入 instrument 定义。部署包含该迁移的版本时，既有 Binance、OKX 永续会取得新 `instrument_id`，旧事实仍保留在旧 ID 下；启动补查会为语义兼容的旧版本继续确认最近 24 小时实际资金费率。详见 [市场数据与存储设计](docs/market-data-storage.md)。

### 为什么盘口采用两张数据表

- `order_book_minute` 保存每分钟第 0 秒的完整盘口，是恢复该分钟任意秒盘口的确定起点。
- `order_book_second_delta` 只保存分钟内价格和数量的变化，避免把 60 份完整的 50 档盘口重复写入磁盘。

两张表配合后，查询任意秒只需读取一个分钟完整盘口并回放至多 59 秒差量；既保留精确盘口，又避免按秒重复存储完整盘口。

收益数据使用 `yield_route` 保存稳定产品身份，使用 `yield_observation` 保存每次完整利率快照。收益量较低，不采用盘口的分钟快照和秒级差量编码。

## 文档

- [当前部署与运行说明](docs/runtime-operations.md)：实际运行服务、路径、配置、上游接口、只读检查和维护注意事项。
- [系统总体架构](docs/architecture.md)：盘口、资金费率和收益三类采集分支、启动和失败边界、健康判断及未来数据扩展原则。
- [市场数据与存储设计](docs/market-data-storage.md)：当前六张核心表的完整数据字典。
- [行情采集程序设计](docs/implementation-design.md)：旧代码复用、采集流程、ClickHouse 写入和实现顺序。
- [Bybit USDT 线性永续采集设计](docs/bybit-usdt-perpetual-market-data.md)：产品筛选、1000 档序列、稀疏 ticker、限流和版本迁移的精确定义。
- [套利机会与策略资料](docs/arbitrage/README.md)：`ARB-0001` 至 `ARB-0022` 机会库，以及 ARB-0002、ARB-0016、ARB-0022 的详细文档。
- [ARB-0016 收益数据采集设计](docs/arbitrage/strategies/arb-0016-yield-data.md)：通用收益两表、字段含义和理论筛选规则。
- [ARB-0016 TRX 收益采集实现设计](docs/arbitrage/strategies/arb-0016-trx-yield-implementation.md)：JustLend 与 TRON 原生质押的采集和写入细节。
- [ARB-0016 SOL 收益采集第一阶段实现设计](docs/arbitrage/strategies/arb-0016-sol-yield-phase-1.md)：bSOL、JitoSOL、mSOL、验证者白名单和 Marinade Native 的采集细节。
- [ARB-0016 SOL 收益采集第二阶段实现设计](docs/arbitrage/strategies/arb-0016-sol-yield-phase-2.md)：laineSOL、JupSOL、hSOL、Kamino Main SOL 和 Save Main SOL 的采集细节。
- [ARB-0016 AVAX 收益采集第一阶段实现设计](docs/arbitrage/strategies/arb-0016-avax-yield-phase-1.md)：OKX、Aave V3/V4 的固定历史窗口、单位换算、分页和两表写入。
- [ARB-0016 AVAX 收益采集第二阶段实现设计](docs/arbitrage/strategies/arb-0016-avax-yield-phase-2.md)：BENQI、Ankr 的同块快照、兑换率、基础 APR、现金与旧表兼容迁移。
- [开发协作说明](AGENTS.md)：项目目标、数据不变量、实现和验证要求。
