# Crypto Market Info

本项目用于持续采集多个交易场所的公开行情数据，并将其标准化后持久化，供后续套利机会检查、历史回放和流动性分析使用。

覆盖范围包括：

- CEX：现货、永续合约和交割合约的 L2 盘口；
- 订单簿型 DEX：链上或链下撮合订单簿；
- AMM 型 DEX：池子状态和按统一规则生成的可交易深度曲线。

项目当前不负责下单、资金划转或密钥管理；它只生产可核验的市场数据。

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

## 运行第一版采集程序

程序只使用公开 REST/WebSocket 接口，不需要交易所 API key。默认采集 Binance 与 OKX 的 BTC/USDT 现货和 USDT 线性永续：

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

端点也可通过 `BINANCE_SPOT_REST_URL`、`BINANCE_FUTURES_REST_URL`、`BINANCE_SPOT_WS_URL`、`BINANCE_FUTURES_WS_URL`、`OKX_REST_URL` 和 `OKX_WS_URL` 覆盖，便于代理、测试环境和区域域名切换。

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
CLICKHOUSE_INTEGRATION=1 go test ./internal/storage/clickhouse -run TestClickHouseDDLWriteReplayAndDayMeasurement -v
```

采集进程不使用 Redis、消息队列或套利计算组件。网络断开、序列断档、解析失败或增量队列溢出时，对应盘口立即失效，重新同步成功前不会被秒级采样器保存。

永续资金费率的估算值来自 Binance/OKX 各一条公共 WebSocket 连接；实际结算值在目标结算后第 2、5、15、60 分钟由各交易所独立的串行 REST worker 确认，相邻请求至少间隔 1 秒。WebSocket 推送过期时该整点留空，不回退到逐 instrument REST 轮询。

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

## 文档

- [市场数据与存储设计](docs/market-data-storage.md)：四张核心表的完整数据字典。
- [行情采集程序设计](docs/implementation-design.md)：旧代码复用、采集流程、ClickHouse 写入和实现顺序。
- [开发协作说明](AGENTS.md)：项目目标、数据不变量、实现和验证要求。
