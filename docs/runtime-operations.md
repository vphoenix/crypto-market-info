# 当前部署与运行说明

最近核实：2026-08-27（Asia/Shanghai）。本文记录 `/home/ubuntu/crypto-market-info` 所在机器的实际部署，不是另一套部署方案。路径、版本和启用配置变更后同步更新本文；运行状态仍以现场检查为准，不保存固定 PID。

## 1. 实际使用的本机服务

| 服务 | 实际运行方式 | 连接与用途 |
|---|---|---|
| ClickHouse | 宿主机原生二进制，`--daemon` 启动并带 ClickHouse watchdog；核实版本 `26.8.1.1825` | HTTP `127.0.0.1:8123`；native `127.0.0.1:9000`；数据库 `crypto_market_info` |
| collector | 编译后的单个二进制，由外层 Bash `while true` 循环守护 | 连接上述 native 端口，采集盘口、资金费率及 TRX/SOL/AVAX 收益；没有独立 HTTP 服务端口 |

这两项是本项目的运行依赖，不使用 Redis、PostgreSQL、消息队列或其他项目的服务。当前 collector 二进制由当前工作区构建，包含 SOL 第二阶段及 AVAX 第一阶段；工作区改动尚未提交，因此构建元数据标记为 `dirty`。

仓库的 [compose.yaml](../compose.yaml) 只是可选的独立开发环境，固定镜像为 `clickhouse:26.3.17.56-jammy`，与当前原生服务不是同一个实例。核实时 `docker compose ps -a` 为空，但原生 ClickHouse 正常响应。两套环境默认占用相同端口，不能直接同时启动，也不能混用数据目录。

目前记录的是原生 daemon 和 Shell 循环，没有已核实的 systemd unit 或开机自启安排；不能据此保证机器重启后会自动恢复。

## 2. 路径、启动方式与配置

| 项目 | 当前路径 |
|---|---|
| 仓库 | `/home/ubuntu/crypto-market-info` |
| ClickHouse 二进制（也提供 client 子命令） | `/home/ubuntu/.local/share/crypto-market-info-clickhouse-bin/clickhouse` |
| ClickHouse 数据目录 | `/home/ubuntu/.local/share/crypto-market-info-clickhouse-data/` |
| ClickHouse 普通日志 | `/home/ubuntu/.local/share/crypto-market-info-clickhouse-logs/server.log` |
| ClickHouse 错误日志 | `/home/ubuntu/.local/share/crypto-market-info-clickhouse-logs/error.log` |
| ClickHouse PID 文件 | `/tmp/crypto-market-info-clickhouse.pid` |
| collector 二进制 | `/home/ubuntu/.local/share/crypto-market-info-collector/collector` |
| collector 日志 | `/home/ubuntu/.local/share/crypto-market-info-collector/logs/collector.log` |

下面两段记录当前启动参数，**不是状态检查命令**；仅在确认对应旧实例已退出、需要恢复服务时使用。不要为排查问题再启动第二个数据库或第二份采集器。

ClickHouse 的实际启动参数（将进程中的相对二进制路径展开为绝对路径）：

```bash
/home/ubuntu/.local/share/crypto-market-info-clickhouse-bin/clickhouse server --daemon \
  --log-file=/home/ubuntu/.local/share/crypto-market-info-clickhouse-logs/server.log \
  --errorlog-file=/home/ubuntu/.local/share/crypto-market-info-clickhouse-logs/error.log \
  --pid-file=/tmp/crypto-market-info-clickhouse.pid \
  -- \
  --path=/home/ubuntu/.local/share/crypto-market-info-clickhouse-data/ \
  --listen_host=127.0.0.1 \
  --http_port=8123 \
  --tcp_port=9000
```

采集器当前守护循环的等价命令如下。实际运行时由 `setsid -f` 启动，使该循环脱离启动终端；若用于恢复，同样需要让外层 Shell 持续存活，不能把一次前台执行当成开机自启服务：

```bash
bash -c 'while true; do
  env BINANCE_SPOT_SYMBOLS=BTCUSDT \
      BINANCE_PERP_SYMBOLS=BTCUSDT \
      OKX_SPOT_SYMBOLS=BTC-USDT \
      OKX_PERP_SYMBOLS=BTC-USDT-SWAP \
      BINANCE_SPOT_REST_URL=https://data-api.binance.vision \
      FUNDING_ENABLED=true \
      JUSTLEND_YIELD_ENABLED=true \
      TRON_STAKING_YIELD_ENABLED=true \
      SOL_YIELD_ENABLED=true \
      SOL_VALIDATOR_VOTE_ACCOUNTS=- \
      AVAX_YIELD_ENABLED=true \
      /home/ubuntu/.local/share/crypto-market-info-collector/collector \
      >> /home/ubuntu/.local/share/crypto-market-info-collector/logs/collector.log 2>&1
  sleep 30
done'
```

collector 退出后，外层循环等待 30 秒再启动。仅退出 collector 子进程不会永久停止采集；改变当前终端的环境变量也不会改变已运行的守护循环。

数据库连接使用默认的 `127.0.0.1:9000`、`crypto_market_info` 和本地 `default` 用户。当前本机查询不需要密码；不得把无认证端口开放到公网，也不要把任何凭据或 `.env` 写入仓库。完整可配置项见 [README](../README.md#开发时运行采集程序)与 [internal/config/config.go](../internal/config/config.go)。

## 3. 已启用的采集与外部接口

| 分支 | 当前启用范围 | 采集频率 |
|---|---|---|
| Binance、OKX 盘口 | 各自的 BTC/USDT 现货及 USDT 线性永续，共四个交易流 | 每秒采样，结束的分钟批量写入 |
| 永续资金费率 | 上述两个永续合约 | 公共 WebSocket 估算；REST 确认实际结算值 |
| JustLend | 四条固定 TRX 路线 | 每小时 |
| TRON 原生质押 | 前 127 名 SR | 每 6 小时 |
| SOL 固定收益 | 下表九条路线，各自独立 Runner | 每 6 小时 |
| SOL 单验证者原生质押 | 未启用：`SOL_VALIDATOR_VOTE_ACCOUNTS=-`；这不影响 Marinade Native | 配置白名单后才采集 |
| AVAX 第一阶段 | OKX AVAX 公开出借 APR、Aave V3/V4 WAVAX 基础存款历史 APY | 每小时 |

收益 Runner 启动即首采，失败后等待 10 分钟重试；数据库写入失败会重试原批次，不会用旧利率伪造新时间的观测。历史接口会重复抓取短历史窗口，逻辑去重由现有收益表完成。

SOL 的九条固定路线及对应日志 `source`：

| 产品 | 数据库 `provider / product_code` | 日志 `source` |
|---|---|---|
| bSOL | `BlazeStake / bsol` | `solana-stakepool-bsol` |
| JitoSOL | `Jito / jitosol` | `jitosol` |
| mSOL | `Marinade / msol` | `marinade-msol` |
| Marinade Native | `Marinade / marinade-native` | `marinade-native` |
| laineSOL | `Laine / lainesol` | `solana-stakepool-lainesol` |
| JupSOL | `Jupiter / jupsol` | `solana-stakepool-jupsol` |
| hSOL | `Helius / hsol` | `solana-stakepool-hsol` |
| Kamino Main SOL | `Kamino / main-sol` | `kamino-main-sol` |
| Save Main SOL | `Save / main-sol` | `save-main-sol` |

当前使用的公共接口 base URL 如下；具体路径和校验规则见各采集设计及代码。除 Binance spot REST 显式覆盖外，以下使用配置默认值：

| 来源 | 当前接口 |
|---|---|
| Binance REST | 现货 `https://data-api.binance.vision`；永续 `https://fapi.binance.com` |
| Binance WebSocket | 现货 `wss://stream.binance.com:443/ws`；永续盘口 `wss://fstream.binance.com/public/ws`；资金费率 `wss://fstream.binance.com/market/ws` |
| OKX | REST `https://www.okx.com`；WebSocket `wss://ws.okx.com:8443/ws/v5/public` |
| JustLend / TRON | `https://openapi.just.network` / `https://api.trongrid.io` |
| Solana RPC | `https://api.mainnet.solana.com`；用于链上池状态、身份和区块锚点校验 |
| Jito | `https://kobe.mainnet.jito.network` |
| Marinade | `https://apy.marinade.finance`；验证者接口 `https://validators-api.marinade.finance` 目前因白名单为空未调用 |
| Kamino / Save | `https://api.kamino.finance` / `https://api.solend.fi` |

这些接口只用于读取公开数据，不涉及钱包、签名、账户操作或下单；收益不混入永续资金费率。

## 4. 只读检查

在**宿主机终端**执行。若使用隔离沙箱，沙箱的 PID 列表和 `127.0.0.1` 可能不是宿主机的视图；看不到进程或连不上时，应申请宿主机只读检查权限，不能直接判定服务停止，更不能据此启动另一份。

先检查端口、HTTP 和采集进程：

```bash
ss -ltnp 'sport = :8123 or sport = :9000'
curl --connect-timeout 3 --max-time 10 --fail --silent --show-error \
  'http://127.0.0.1:8123/ping'
pgrep -af '^/home/ubuntu/\.local/share/crypto-market-info-collector/collector$'
pgrep -af '^bash -c while true; do.*crypto-market-info-collector/collector'
```

正常 HTTP 返回 `Ok.`，应有一个 collector 子进程和一个外层守护循环；在 30 秒重启间隔内可能暂时没有子进程，需要结合日志判断。

查询实际数据库版本和表：

```bash
/home/ubuntu/.local/share/crypto-market-info-clickhouse-bin/clickhouse client \
  --host 127.0.0.1 --port 9000 --database crypto_market_info --multiquery \
  --query 'SELECT version(), currentDatabase(); SHOW TABLES;'
```

进入交互查询：

```bash
/home/ubuntu/.local/share/crypto-market-info-clickhouse-bin/clickhouse client \
  --host 127.0.0.1 --port 9000 --database crypto_market_info
```

连接后执行[架构文档的健康检查 SQL](architecture.md#6-运行状态判断)，逐项核实盘口、资金费率、TRX 和 SOL 的最新写入。进程存在或 `/ping` 成功都不能单独证明各来源在正常采集。

查看近期日志与二进制构建来源：

```bash
tail -n 80 /home/ubuntu/.local/share/crypto-market-info-collector/logs/collector.log
tail -n 80 /home/ubuntu/.local/share/crypto-market-info-clickhouse-logs/error.log
go version -m /home/ubuntu/.local/share/crypto-market-info-collector/collector
```

数据表时间字段显式使用 UTC。当前主机日志带 `+08:00`，ClickHouse 默认服务器时区也是 `Asia/Shanghai`；不要把日志的本地显示时间直接当成表内 UTC 值，手写时间字面量时要明确时区。

## 5. 维护注意事项与已知缺口

- 更新代码不等于更新运行程序。先测试并构建新二进制，确认构建版本；再将它安装到 collector 同目录的临时文件并原子替换目标，避免直接覆盖正在执行的文件。最后确认准确子进程 PID，发送 `SIGTERM`，由现有循环在退出并等待 30 秒后重新启动；不要另起一份守护循环。
- 修改采集环境变量需要更新外层守护循环。若要永久停止采集，应先停止已核实的外层循环，再让其 collector 子进程正常退出；不要仅凭宽泛名称批量杀进程。
- 单个来源失败先查日志和最近成功批次，不随意重启 ClickHouse。原生数据库的数据目录与 Docker volume 完全独立；不要清理数据目录、执行 `docker compose down -v` 或停掉其他项目的服务来排障。
- 2026-08-26 核实时，SOL 第一、第二阶段九条固定路线均已有成功写入。日志同时显示 Binance 实际资金费率 REST 曾返回 HTTP `451`（来源的地区限制）；这表示相应数据可能有缺口，不能将 SOL 成功写入概括为全部数据源无故障。保持缺口并按来源错误排查，不绕过地区限制。
- 本文不引入新的守护框架。长期停机恢复、开机自启和日志轮转若需要另行配置，应记录实际采用的方法，不把尚未配置的能力写成已有保障。
