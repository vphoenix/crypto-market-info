# Crypto Market Info

本项目用于持续采集可公开验证的市场、利率和链上规则数据，并将其标准化后持久化，供后续套利机会检查、历史回放、流动性分析和人工风险审查使用。

当前已经实现：

- Binance、OKX 现货及永续合约 L2 盘口；
- Binance、OKX 永续资金费率；
- JustLend TRX 收益和 TRON 原生质押收益。

以后还可能增加其他 CEX、DEX、收益协议、链状态、桥和二层流通状态、借贷费率、指数价格、手续费或 gas 等公开数据。当前六张表不是最终边界；不同语义的数据应建立自己的定类型模型和表，不能全部塞入盘口或收益表。

项目当前不负责下单、资金划转或密钥管理；它只生产可核验的数据。

整体组件关系、启动顺序、失败边界和未来扩展原则见[系统总体架构](docs/architecture.md)。套利机会定义和已迁入的 `ARB-xxxx` 策略资料见[套利机会与策略资料](docs/arbitrage/README.md)。这些资料用于说明数据需求，不会自动启用交易执行。

## 本地开发环境

需要 Go、Docker 和 Docker Compose。ClickHouse 使用项目固定的官方 LTS 镜像，不需要在主机安装独立服务。

启动 ClickHouse：

```bash
docker compose up -d
```

检查服务和版本：

```bash
docker compose ps
docker compose exec clickhouse clickhouse-client --query "SELECT version()"
```

本地数据库名为 `crypto_market_info`，HTTP 和 native 端口分别为 `127.0.0.1:8123`、`127.0.0.1:9000`。无密码访问只用于本机开发，端口不得绑定到公网地址。数据保存在 Docker volume 中，执行 `docker compose down` 不会删除数据。

## 运行采集程序

程序只使用公开 REST/WebSocket 接口，不需要交易所 API key。默认采集 Binance 与 OKX 的 BTC/USDT 现货和 USDT 线性永续；收益采集默认关闭，需要明确启用：

```bash
go run ./cmd/collector
```

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
| `FUNDING_ENABLED` | `true` | 是否采集永续资金费率 |
| `MINUTE_QUEUE_CAPACITY` | `512` | 等待写入 ClickHouse 的分钟盘口批次数；队列满时丢弃新完成的分钟并记录错误 |
| `JUSTLEND_YIELD_ENABLED` | `false` | 是否每小时采集四条 JustLend TRX 收益路线 |
| `JUSTLEND_BASE_URL` | `https://openapi.just.network` | JustLend 公开 API 地址 |
| `TRON_STAKING_YIELD_ENABLED` | `false` | 是否每 6 小时采集 TRON 前 127 名 SR 收益 |
| `TRON_HTTP_URL` | `https://api.trongrid.io` | TRON 公开 HTTP 节点地址 |

同时启用当前两类 TRX 收益：

```bash
JUSTLEND_YIELD_ENABLED=true \
TRON_STAKING_YIELD_ENABLED=true \
go run ./cmd/collector
```

端点也可通过 `BINANCE_SPOT_REST_URL`、`BINANCE_FUTURES_REST_URL`、`BINANCE_SPOT_WS_URL`、`BINANCE_FUTURES_WS_URL`、`BINANCE_FUTURES_MARKET_WS_URL`、`OKX_REST_URL` 和 `OKX_WS_URL` 覆盖，便于代理、测试环境和区域域名切换。Binance Futures 深度默认使用 `/public/ws`，mark price 资金费率默认使用 `/market/ws`，不能混用。

盘口启动和重连受进程内共享门控保护：Binance 1000 档 REST 快照相邻请求至少间隔 1 秒，OKX 盘口与资金费率 WebSocket 相邻建连至少间隔 500 毫秒。REST 返回 `429` 或 `418` 时，同一交易所客户端会遵守 `Retry-After` 并暂停全部后续请求，不进行快速重试。

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

永续资金费率的估算值来自 Binance/OKX 各一条公共 WebSocket 连接；实际结算值在目标结算后第 2、5、15、60 分钟由各交易所独立的串行 REST worker 确认，相邻请求至少间隔 1 秒。WebSocket 推送过期时该整点留空，不回退到逐 instrument REST 轮询。

JustLend 和 TRON 收益使用独立 Runner 与 ClickHouse writer。收益单轮失败只形成明确缺口并按规则重试，不会用旧利率冒充当前数据，也不会把永续资金费率加入收益率。

## 数据存储模型

每个交易流按秒维护前 50 档买盘和前 50 档卖盘，但不把每秒的完整盘口重复写入数据库：

1. 每分钟第 0 秒写入一次完整的 50 档盘口快照；
2. 第 1 至 59 秒仅写入相对上一个有效采样状态发生变化的价格和数量；
3. 查询任意秒时，以该分钟快照为起点回放最多 59 秒差量。

所有价格和数量以整数 tick / lot 保存；时间统一为 UTC。详见 [市场数据与存储设计](docs/market-data-storage.md)。

### 为什么盘口采用两张数据表

- `order_book_minute` 保存每分钟第 0 秒的完整盘口，是恢复该分钟任意秒盘口的确定起点。
- `order_book_second_delta` 只保存分钟内价格和数量的变化，避免把 60 份完整的 50 档盘口重复写入磁盘。

两张表配合后，查询任意秒只需读取一个分钟完整盘口并回放至多 59 秒差量；既保留精确盘口，又避免按秒重复存储完整盘口。

收益数据使用 `yield_route` 保存稳定产品身份，使用 `yield_observation` 保存每次完整利率快照。收益量较低，不采用盘口的分钟快照和秒级差量编码。

## 文档

- [系统总体架构](docs/architecture.md)：盘口、资金费率和收益三类采集分支、启动和失败边界、健康判断及未来数据扩展原则。
- [市场数据与存储设计](docs/market-data-storage.md)：当前六张核心表的完整数据字典。
- [行情采集程序设计](docs/implementation-design.md)：旧代码复用、采集流程、ClickHouse 写入和实现顺序。
- [套利机会与策略资料](docs/arbitrage/README.md)：`ARB-0001` 至 `ARB-0022` 机会库，以及 ARB-0002、ARB-0016、ARB-0022 的详细文档。
- [ARB-0016 收益数据采集设计](docs/arbitrage/strategies/arb-0016-yield-data.md)：通用收益两表、字段含义和理论筛选规则。
- [ARB-0016 TRX 收益采集实现设计](docs/arbitrage/strategies/arb-0016-trx-yield-implementation.md)：JustLend 与 TRON 原生质押的采集和写入细节。
- [开发协作说明](AGENTS.md)：项目目标、数据不变量、实现和验证要求。
