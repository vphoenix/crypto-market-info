# 当前部署与运行说明

最近核实：2026-09-01（Asia/Shanghai）。本文记录 `/home/ubuntu/crypto-market-info` 所在机器的实际部署，不是另一套部署方案。路径、版本和启用配置变更后同步更新本文；运行状态仍以现场检查为准，不保存固定 PID。

## 1. 实际使用的本机服务

| 服务 | 实际运行方式 | 连接与用途 |
|---|---|---|
| ClickHouse | 用户级 systemd unit `crypto-market-info-clickhouse.service`；宿主机原生二进制以前台模式运行并带 ClickHouse watchdog；核实版本 `26.8.1.1825` | HTTP `127.0.0.1:8123`；native `127.0.0.1:9000`；数据库 `crypto_market_info` |
| collector | 用户级 systemd unit `crypto-market-info-collector.service`；直接运行编译后的单个二进制 | 依赖 ClickHouse 健康后启动，采集盘口、资金费率及 TRX/SOL/AVAX 收益；没有独立 HTTP 服务端口 |

这两项是本项目的运行依赖，不使用 Redis、PostgreSQL、消息队列或其他项目的服务。当前正在运行的 collector 二进制包含 SOL 第二阶段、AVAX 第一/第二阶段及 Bybit USDT 线性永续；构建元数据为 `66c68ec+dirty`，表示它由尚未提交的本工作区版本构建。

AVAX 第二阶段已于 2026-08-27 部署。collector unit 保持 `AVAX_YIELD_ENABLED=true`，新增 BENQI sAVAX、Ankr ankrAVAX、BENQI AVAX 借贷三条 Runner；启动时已对生产 `yield_observation` 幂等补齐两列，并成功写入三条首批同区块观测。

Bybit USDT 线性永续已于 2026-09-05 部署，collector unit 设置 `BYBIT_PERP_SYMBOLS=BTCUSDT`。启动时已幂等增加 `instrument.venue_contract_version`：迁移前 Binance、OKX 永续 ID 2、4 保留，新版本分别登记为 ID 5、6，Bybit `BTCUSDT` 登记为 ID 7。首个完整生产分钟的五个当前行情流均有 60 个有效秒；Bybit 公共 ticker 实测产生了指向下一结算时刻的完整资金费率估算。

仓库的 [compose.yaml](../compose.yaml) 只是可选的独立开发环境，固定镜像为 `clickhouse:26.3.17.56-jammy`，与当前原生服务不是同一个实例。核实时 `docker compose ps -a` 为空，但原生 ClickHouse 正常响应。两套环境默认占用相同端口，不能直接同时启动，也不能混用数据目录。

两个 unit 已启用到 `ubuntu` 用户的 `default.target`，且 `loginctl show-user ubuntu -p Linger` 为 `Linger=yes`。因此用户服务管理器会在机器启动时运行，无需交互登录。ClickHouse 失败后等待 5 秒重启；collector 失败或正常退出后等待 30 秒重启。

## 2. 路径、启动方式与配置

| 项目 | 当前路径 |
|---|---|
| 仓库 | `/home/ubuntu/crypto-market-info` |
| ClickHouse 二进制（也提供 client 子命令） | `/home/ubuntu/.local/share/crypto-market-info-clickhouse-bin/clickhouse` |
| ClickHouse 数据目录 | `/home/ubuntu/.local/share/crypto-market-info-clickhouse-data/` |
| ClickHouse 普通日志 | `/home/ubuntu/.local/share/crypto-market-info-clickhouse-logs/server.log` |
| ClickHouse 错误日志 | `/home/ubuntu/.local/share/crypto-market-info-clickhouse-logs/error.log` |
| ClickHouse PID 文件 | `/run/user/1000/crypto-market-info-clickhouse/clickhouse.pid`（由 systemd 运行目录创建，重启后重建） |
| collector 二进制 | `/home/ubuntu/.local/share/crypto-market-info-collector/collector` |
| collector 日志 | `/home/ubuntu/.local/share/crypto-market-info-collector/logs/collector.log` |
| unit 仓库源文件 | `/home/ubuntu/crypto-market-info/deploy/systemd/` |
| 实际安装的 unit | `/home/ubuntu/.config/systemd/user/crypto-market-info-{clickhouse,collector}.service` |

unit 的仓库源文件是启动参数和采集环境变量的维护入口，安装副本由它们生成。不要绕过 systemd 手工启动第二个数据库或第二份采集器。

安装或更新 unit 后执行：

```bash
install -d -m 0755 /home/ubuntu/.config/systemd/user
install -m 0644 deploy/systemd/crypto-market-info-clickhouse.service \
  /home/ubuntu/.config/systemd/user/crypto-market-info-clickhouse.service
install -m 0644 deploy/systemd/crypto-market-info-collector.service \
  /home/ubuntu/.config/systemd/user/crypto-market-info-collector.service
systemd-analyze --user verify deploy/systemd/*.service
systemctl --user daemon-reload
systemctl --user enable --now \
  crypto-market-info-clickhouse.service \
  crypto-market-info-collector.service
```

ClickHouse unit 以前台模式运行数据库并在启动阶段轮询 `/ping`；collector unit 通过 `Wants`/`After` 启动并等待它，在执行二进制前再次检查 `/ping`。两个服务分别重试，避免数据库一次启动失败让 collector 永久留在 stopped。collector 的标准输出和错误仍追加到原日志文件，systemd 启停事件可通过用户 journal 查看。

数据库连接使用默认的 `127.0.0.1:9000`、`crypto_market_info` 和本地 `default` 用户。当前本机查询不需要密码；不得把无认证端口开放到公网，也不要把任何凭据或 `.env` 写入仓库。完整可配置项见 [README](../README.md#开发时运行采集程序)与 [internal/config/config.go](../internal/config/config.go)。

## 3. 已启用的采集与外部接口

| 分支 | 当前启用范围 | 采集频率 |
|---|---|---|
| Binance、OKX 盘口 | 各自的 BTC/USDT 现货及 USDT 线性永续，共四个交易流 | 每秒采样，结束的分钟批量写入 |
| 永续资金费率 | 上述两个永续合约 | 公共 WebSocket 估算；REST 确认实际结算值 |
| Bybit USDT 线性永续 | `BTCUSDT`，已部署启用 | 每秒盘口采样；公共 ticker 估算并由 REST 确认实际资金费率 |
| JustLend | 四条固定 TRX 路线 | 每小时 |
| TRON 原生质押 | 前 127 名 SR | 每 6 小时 |
| SOL 固定收益 | 下表九条路线，各自独立 Runner | 每 6 小时 |
| SOL 单验证者原生质押 | 未启用：`SOL_VALIDATOR_VOTE_ACCOUNTS=-`；这不影响 Marinade Native | 配置白名单后才采集 |
| AVAX 第一阶段 | OKX AVAX 公开出借 APR、Aave V3/V4 WAVAX 基础存款历史 APY | 每小时 |
| AVAX 第二阶段 | BENQI sAVAX、Ankr ankrAVAX 兑换率；BENQI AVAX 基础借贷 APR | 每小时 |

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

当前使用的公共接口 base URL 如下；具体路径和校验规则见各采集设计及代码。Bybit 默认域名可能按访问地区返回 403，只有响应体明确包含 `access too frequent` 的 403 才按限频冷却，其他 403 会立即失败并要求运维配置合规可用的 `BYBIT_REST_URL`。除 Binance spot REST 显式覆盖外，其余已启用来源使用配置默认值：

| 来源 | 当前接口 |
|---|---|
| Binance REST | 现货 `https://data-api.binance.vision`；永续 `https://fapi.binance.com` |
| Binance WebSocket | 现货 `wss://stream.binance.com:443/ws`；永续盘口 `wss://fstream.binance.com/public/ws`；资金费率 `wss://fstream.binance.com/market/ws` |
| OKX | REST `https://www.okx.com`；WebSocket `wss://ws.okx.com:8443/ws/v5/public` |
| Bybit | REST `https://api.bybit.com`；线性合约 WebSocket `wss://stream.bybit.com/v5/public/linear` |
| JustLend / TRON | `https://openapi.just.network` / `https://api.trongrid.io` |
| Solana RPC | `https://api.mainnet.solana.com`；用于链上池状态、身份和区块锚点校验 |
| Jito | `https://kobe.mainnet.jito.network` |
| Marinade | `https://apy.marinade.finance`；验证者接口 `https://validators-api.marinade.finance` 目前因白名单为空未调用 |
| Kamino / Save | `https://api.kamino.finance` / `https://api.solend.fi` |

这些接口只用于读取公开数据，不涉及钱包、签名、账户操作或下单；收益不混入永续资金费率。

## 4. 只读检查

在**宿主机终端**执行。若使用隔离沙箱，沙箱的 PID 列表和 `127.0.0.1` 可能不是宿主机的视图；看不到进程或连不上时，应申请宿主机只读检查权限，不能直接判定服务停止，更不能据此启动另一份。

先检查开机启用状态、运行状态、端口、HTTP 和采集进程：

```bash
loginctl show-user ubuntu -p Linger
systemctl --user is-enabled \
  crypto-market-info-clickhouse.service \
  crypto-market-info-collector.service
systemctl --user status \
  crypto-market-info-clickhouse.service \
  crypto-market-info-collector.service
ss -ltnp 'sport = :8123 or sport = :9000'
curl --connect-timeout 3 --max-time 10 --fail --silent --show-error \
  'http://127.0.0.1:8123/ping'
pgrep -af '^/home/ubuntu/\.local/share/crypto-market-info-collector/collector$'
```

正常情况下 linger 为 `yes`，两个 unit 都是 `enabled` 和 `active (running)`，HTTP 返回 `Ok.`，并且只有一个 collector 进程。collector 的 30 秒重启间隔内可能暂时没有进程，需要结合 unit 状态和日志判断。

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
journalctl --user -u crypto-market-info-clickhouse.service \
  -u crypto-market-info-collector.service -n 80 --no-pager
go version -m /home/ubuntu/.local/share/crypto-market-info-collector/collector
```

数据表时间字段显式使用 UTC。当前主机日志带 `+08:00`，ClickHouse 默认服务器时区也是 `Asia/Shanghai`；不要把日志的本地显示时间直接当成表内 UTC 值，手写时间字面量时要明确时区。

## 5. 维护注意事项与已知缺口

- 更新代码不等于更新运行程序。先测试并构建新二进制，确认构建版本；再将它安装到 collector 同目录的临时文件并原子替换目标，避免直接覆盖正在执行的文件；最后执行 `systemctl --user restart crypto-market-info-collector.service` 并检查状态和最新数据。
- 首次部署包含 Bybit 支持的版本时，`InitSchema` 会幂等增加 `instrument.venue_contract_version`。既有 Binance、OKX 永续会登记有版本的新 `instrument_id`，旧事实不会迁移或删除；部署验收必须确认健康查询只看每个交易所代码的最新 ID，并确认最近 24 小时迁移前旧 ID 的待确认资金费率仍可由启动补查完成。启用 Bybit 前还应按专项设计做真实公共端点的 metadata、1000 档序列与 funding history 边界验证。
- 修改采集环境变量时先更新仓库中的 collector unit，再复制到用户 unit 目录、执行 `systemctl --user daemon-reload` 和重启 collector。不要只修改当前终端环境，也不要直接编辑安装副本而遗漏仓库源文件。
- 临时启停使用 `systemctl --user stop|start`；取消开机启动使用 `systemctl --user disable --now`。不要用宽泛名称批量杀进程。`Linger=yes` 是无需登录即可开机启动的必要条件，不要随意关闭。
- 单个来源失败先查日志和最近成功批次，不随意重启 ClickHouse。原生数据库的数据目录与 Docker volume 完全独立；不要清理数据目录、执行 `docker compose down -v` 或停掉其他项目的服务来排障。
- 2026-08-26 核实时，SOL 第一、第二阶段九条固定路线均已有成功写入。日志同时显示 Binance 实际资金费率 REST 曾返回 HTTP `451`（来源的地区限制）；这表示相应数据可能有缺口，不能将 SOL 成功写入概括为全部数据源无故障。保持缺口并按来源错误排查，不绕过地区限制。
- collector 日志目前继续追加到单个文件，尚未配置独立日志轮转；systemd journal 只记录 unit 启停和未重定向的控制进程输出。
