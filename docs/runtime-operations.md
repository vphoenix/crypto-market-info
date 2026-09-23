# 当前部署与运行说明

最近核实：2026-09-23（Asia/Shanghai）。本文记录 `/home/ubuntu/crypto-market-info` 所在机器的实际部署，不是另一套部署方案。路径、版本和启用配置变更后同步更新本文；运行状态仍以现场检查为准，不保存固定 PID。

## 1. 实际使用的本机服务

| 服务 | 实际运行方式 | 连接与用途 |
|---|---|---|
| ClickHouse | 用户级 systemd unit `crypto-market-info-clickhouse.service`；宿主机原生二进制以前台模式运行并带 ClickHouse watchdog；核实版本 `26.8.1.1825` | HTTP `127.0.0.1:8123`；native `127.0.0.1:9000`；数据库 `crypto_market_info` |
| collector | 用户级 systemd unit `crypto-market-info-collector.service`；直接运行编译后的单个二进制 | 依赖 ClickHouse 健康后启动，采集盘口、资金费率、TRX/SOL/AVAX 收益及 Ethereum DEX 链上数据；没有独立 HTTP 服务端口 |
| 全量永续验收 collector | 用户级 systemd unit `crypto-market-info-perp-soak.service`；独立二进制与数据库 | `crypto_market_info_perp_soak`；仅三家共有 USDT 永续盘口和资金费率，不重复采集现货与收益 |
| 桌面状态指示器 | 图形会话用户级 systemd unit `crypto-market-info-status-indicator.service`；Python/Gtk AppIndicator | 每 30 秒只读检查生产 unit、五路盘口及最新收益写入；不访问交易所、不写数据库 |

本项目不使用 Redis、PostgreSQL、消息队列或其他项目的服务。生产 collector 保持 BTC 显式列表及现有收益配置，尚未切换自动全量；新版本先在独立验收服务运行。具体构建版本用下文 `go version -m` 查询，不以当前仓库 HEAD 推断正在运行的二进制版本。

AVAX 第二阶段已于 2026-08-27 部署。collector unit 保持 `AVAX_YIELD_ENABLED=true`，新增 BENQI sAVAX、Ankr ankrAVAX、BENQI AVAX 借贷三条 Runner；启动时已对生产 `yield_observation` 幂等补齐两列，并成功写入三条首批同区块观测。

2026-09-21 已在现有collector启用 Deribit 期权采集，unit 设置 `OPTIONS_ENABLED=true`、`OPTIONS_SYMBOLS=auto`，没有新增期权专用服务。当前run `5ec3ced8-4d3a-44e9-bf1f-62386d0c505b` 自动固定选择24个期权、4个同到期期货及四个指数。启动过渡分钟如实保留无效锚点；下一完整分钟 `2026-09-20T19:28:00Z` 的28个盘口和四个指数均为60/60有效秒，批次hash为 `ace2270262f3a7bdec6524cd70bcaea13abf9b7d3a5064c102e43531c1a2a779`。详情见[期权实时采集说明](arbitrage/strategies/arb-0009-options-live.md)。

2026-09-22 已在同一个生产 collector 启用 Ethereum DEX 采集，unit 设置 `DEX_ENABLED=true`、`DEX_ETH_RPC_URL=https://ethereum-rpc.publicnode.com`，证据写入持久目录。部署后二进制 SHA-256 为 `cf49b0795aad53a8b5ae00f623d66f867836aac07154761c8c13097f61aaf7a4`。验收区间 `26027452` 至 `26027477` 共26个连续高度无缺口，其中25个区块达到58/58完整报价；`26027460` 因一次RPC/Sky状态读取不完整保留为 `partial/unknown`，后续区块自动恢复。首份24高度机会报告没有毛正窗口。DEX分支不包含钱包、签名或交易发送。

2026-09-23 排查发现 DEX 公共 RPC 的响应读取超时集中出现在最终性校验阶段，旧版在校验失败后额外等待5秒，并在下一轮立即重复昂贵的校验。现版将 RPC 响应读取超时、截断与超过16 MiB分别记录；最终性校验把 `finalized`、`safe` 和原 checkpoint 合为一个 RPC 批次，每轮最多校验20个高度、最多占用6秒，失败后30秒再重试。实时新区块采集与补采、最终性、日志和回执维护现已独立运行，维护任务不会在调度上阻塞实时轮询；共享 RPC 仍可能使实时请求变慢。实时请求失败后按正常2秒轮询间隔重试。Binance 实际资金费率历史中部分 `fundingTime` 比计划整点晚数毫秒，现版查询并匹配整点后1秒内唯一的来源记录，保留其原始毫秒时间，启动补查窗口扩大到72小时；重试耗尽而来源尚无记录时会明确告警。2026-09-23 12:39 CST重启时，OKX启动元数据请求超时曾导致整个collector首次启动失败，此类启动缺口只能补回区块元数据，不能补回当时的实时报价。现已将DEX与CEX等其他来源拆成独立启动和重试分支，并分别使用数据库连接；CEX元数据超时不会停止已运行的DEX分支。12:52 CST重启验收时，DEX先于CEX元数据初始化完成写入新区块。当前生产二进制 SHA-256 为 `bbf1bf5666d9ce3e154e8cd21e127bdc7299d584ae5d796fb6851fb7e256eca7`。更新未增加外部域名。

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
| DEX 原始证据 | `/home/ubuntu/.local/share/crypto-market-info-dex/evidence/` |
| 全量验收二进制 | `/home/ubuntu/.local/share/crypto-market-info-perp-soak/collector` |
| 全量验收日志 | `journalctl --user -u crypto-market-info-perp-soak.service` |
| 桌面指示器程序 | `/home/ubuntu/crypto-market-info/tools/desktop-status/crypto_market_status.py` |
| unit 仓库源文件 | `/home/ubuntu/crypto-market-info/deploy/systemd/` |
| 实际安装的 unit | `/home/ubuntu/.config/systemd/user/crypto-market-info-*.service` |

unit 的仓库源文件是启动参数和采集环境变量的维护入口，安装副本由它们生成。每个采集数据库只允许一个 collector 写入；不得绕过 systemd 对同一库手工启动第二份采集器。全量验收服务共享现有 ClickHouse 实例，但必须使用上表独立库，不另起数据库进程。

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
| Binance、OKX 盘口 | 各自的 BTC/USDT 现货及 BTC/USDT 线性永续，共四个交易流 | 每秒采样，结束的分钟批量写入 |
| 永续资金费率 | 上述两个永续合约 | 公共 WebSocket 估算；REST 确认实际结算值 |
| Bybit USDT 线性永续 | `BTCUSDT`，已部署启用 | 每秒盘口采样；公共 ticker 估算并由 REST 确认实际资金费率 |
| Deribit 期权 | BTC/ETH币本位和USDC线性四族；自动固定24个C/P期权、4个同到期期货及四个指数 | 每秒前10档与指数；元数据每30分钟复核，重连时提前复核 |
| Ethereum DEX | 主网4个策略池、2个成本参考池及Sky相关状态；每块固定58条状态报价 | 每2秒检查新区块；按区块hash采集并跟踪最终性 |
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
| Ethereum RPC | `https://ethereum-rpc.publicnode.com`；仅使用公开只读 JSON-RPC 方法 |

这些接口只用于读取公开数据，不涉及钱包、签名、账户操作或下单；收益不混入永续资金费率。

## 4. 只读检查

在**宿主机终端**执行。若使用隔离沙箱，沙箱的 PID 列表和 `127.0.0.1` 可能不是宿主机的视图；看不到进程或连不上时，应申请宿主机只读检查权限，不能直接判定服务停止，更不能据此启动另一份。

GNOME 顶栏右侧的桌面指示器提供同一套轻量检查：绿色表示生产 collector
运行、五路盘口均为最新完整分钟且收益写入不超过两小时；黄色表示盘口仍在写入但
存在无效秒、延迟或收益检查降级；红色表示 collector、ClickHouse 或盘口更新中断。
点击圆点可查看五路详情、立即刷新或打开生产日志。它随图形会话启动，退出桌面后
停止，不影响开机后由 linger 维持的采集服务。实现与安装说明见
[桌面指示器 README](../tools/desktop-status/README.md)。

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

正常情况下 linger 为 `yes`，上面两个生产 unit 都是 `enabled` 和 `active (running)`，HTTP 返回 `Ok.`，并且该 `pgrep` 路径下只有一个生产 collector 进程。独立验收服务使用另一二进制和数据库，允许另一个进程；不要把两者误判为同库重复写入。collector 的 30 秒重启间隔内可能暂时没有进程，需要结合 unit 状态和日志判断。

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

自动 universe 版本连接后执行[架构文档的健康检查 SQL](architecture.md#6-运行状态判断)，使用当前进程启动日志的 `run_id` 核实当前成员；不能用全部已登记 instrument 猜测正在采集的范围。当前生产盘口仍是 BTC 显式模式，并同时启用期权、收益和DEX专项表；盘口用下列只读查询检查，其他来源按各自专项表核验。进程存在或 `/ping` 成功都不能单独证明各来源在正常采集。

```sql
SELECT i.exchange, i.market_type, i.exchange_symbol,
       b.latest_minute, b.valid_seconds
FROM
(
    SELECT exchange, market_type, exchange_symbol, max(instrument_id) AS instrument_id
    FROM instrument FINAL
    WHERE (exchange = 'Binance' AND exchange_symbol = 'BTCUSDT')
       OR (exchange = 'OKX' AND exchange_symbol IN ('BTC-USDT', 'BTC-USDT-SWAP'))
       OR (exchange = 'Bybit' AND exchange_symbol = 'BTCUSDT')
    GROUP BY exchange, market_type, exchange_symbol
) AS i
LEFT JOIN
(
    SELECT instrument_id, max(minute_time) AS latest_minute,
           bitCount(argMax(valid_bitmap, minute_time)) AS valid_seconds
    FROM order_book_minute FINAL
    GROUP BY instrument_id
) AS b USING (instrument_id)
ORDER BY i.exchange, i.market_type;
```

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
- DEX上线69个区块时，gzip证据为2,101个文件、逻辑大小39,251,056字节、磁盘分配约43 MiB。按该短样本线性外推约4.1 GB和21.9万个文件/日，仅用于容量预警，不是24小时实测。上线时根分区尚余约695 GiB、约6,600万inode；在建立实测保留/归档策略前持续监控两项余量，不能让证据写满磁盘。

### 10 档版本的上线与回退

2026-09-21：每侧 10 档版本已先后部署到独立全量验收服务和生产 collector。旧 50 档历史保持原样，新分钟以 `stored_depth=10` 写入；运行进程仍应以实际安装版本和数据库字段为准，不能只看仓库常量判断线上是否生效。更新前二进制分别保存在 `/home/ubuntu/.local/share/crypto-market-info-perp-soak/collector.rollback-20260920-legacy50` 和 `/home/ubuntu/.local/share/crypto-market-info-collector/collector.rollback-20260921-legacy50`。

此次同时修复了全量验收中暴露的采样超时故障：取前 N 档不再对内存中全部保留价位完整排序；单个采样周期超过截止时间时，已采样来源保持有效，其余来源在 `valid_bitmap` 中明确标为无效，并由后续秒继续采样，不再退出整个 collector。部署后全量服务连续观察超过 10 分钟无重启、无采样超时，采样 p99 约 46 ms；生产首个完整新分钟的五路 BTC 行情均为 60 个有效秒、`stored_depth=10`，且第 11 至 50 档均为零。

升级时先完成测试与构建，再按上面的原子替换步骤逐个更新对应 collector。生产的 BTC 显式列表、独立全量验收的自动列表保持各自配置；不能因档位升级顺带把全量 universe 推广生产。新 collector 的 `InitSchema` 只为盘口添加 `stored_depth UInt8 DEFAULT 50`，保留原 50 档物理列和旧历史。新分钟显式写 `10`，第 11 至 50 档写零；分钟切换时建立独立起点，不改写旧分钟或已有差量。

升级后的只读检查（分别连接生产库和验收库）：

```sql
SELECT stored_depth, count(), min(minute_time), max(minute_time)
FROM order_book_minute FINAL
WHERE minute_time >= now() - INTERVAL 10 MINUTE
GROUP BY stored_depth;

SELECT instrument_id, minute_time, stored_depth, bitCount(valid_bitmap) AS valid_seconds
FROM order_book_minute FINAL
WHERE minute_time >= now() - INTERVAL 3 MINUTE
ORDER BY instrument_id, minute_time DESC
LIMIT 1 BY instrument_id;
```

新进程完成初始化后的分钟应为 `stored_depth=10`，旧分钟仍为 `50`。用新查询程序分别回放切换前后的有效秒，核对 `StoredDepth` 与返回档数；稀疏盘口可少于上限。回退旧 collector 时保留新列及默认值 `50`，旧 writer 省略该列仍会正确标记其 50 档数据；查询继续使用支持 `stored_depth` 的新版程序。不要为回退删除新列，否则已写的 10 档分钟会丢失深度语义。

改变深度只降低后续增长。压缩对照方法和样本结果见 [10 档容量对照](book-depth-10.md)，不应直接把旧日占用乘以 `10/50`，也不自动转换或删除旧历史。

## 6. 三家共有永续全量验收

验收采用 [crypto-market-info-perp-soak.service](../deploy/systemd/crypto-market-info-perp-soak.service)，生产服务不变。三家 `*_PERP_SYMBOLS=auto`，现货和收益均禁用，资金费率启用；任意两家共有的规范化 USDT 永续产品在所有符合条件的场内分别采集。字典及其单位依据见 [perpetual-asset-aliases.md](../config/perpetual-asset-aliases.md)。

2026-09-10 的完整目录预检选出 635 组规范化产品、1,529 条场内交易流：Binance 477、Bybit 630、OKX 422，合计 60 条盘口/资金费率 WebSocket。这是该次目录的结果，不是固定白名单，启动时重新计算。独立验收显式设置 `BYBIT_PERP_MAX_INSTRUMENTS=700`、`PERP_MAX_TOTAL_INSTRUMENTS=2000` 和 `MARKET_DATA_MAX_SAMPLE_SOURCES=2100`；程序默认保护上限仍是单家 500、总数 1000、source 1100，不能把验收覆盖直接当作生产容量结论。unit 设置 `MemoryMax=6G` 和 `LimitNOFILE=65536`，异常退出 30 秒后重启。

修复实盘发现的 Binance 单请求字节预算、错误帧解析和 funding 静默 topic 隔离后，全量采集于 2026-09-10 00:24:29 +08:00 重新启动；提交的 run 为 `aa8dceab-bf35-4d99-bc50-c7cf0a40a967`，`started_at=2026-09-09 16:24:36.609 UTC`。该 UUID 仅记录此次核验，重启后检查必须从新日志取得当前 run。此次使用的 mapping revision 为 `1f287a7cfe9fce6090da2a625f2e770afdf88eb8702c8bce2ecca4c7208f888a`。启动空间基线（2026-09-09 16:24:29 UTC，包含先前失败 run metadata）：活跃 part 的 `data_compressed_bytes=83,642`，`bytes_on_disk=109,746`。

小规模独立库 `crypto_market_info_perp_validation` 已完成 BTC、ETH、PEPE 三家共 9 条盘口检查，连续完整分钟均有 60 个有效秒，各家活跃/低活跃代表合约各 100 次只读随机秒回放通过。小规模临时 unit 设置 600 秒运行期限，结束后不继续采集，已写数据保留；资金费率按小时落库，未跨小时不能宣称资金费率落库验收完成。

2026-09-10 的全量验收还发现价格编码前置问题，不仅是运行时长不足。当日 00:40:36 +08:00 核实：Bybit 630/630、OKX 422/422、Binance 457/477 盘口 ready；共 1,509 条已有分钟数据，20 条 Binance 流仍在重同步。独立受限诊断已确认其中 SOLUSDT 的原因：`exchangeInfo.tickSize=0.01`，但三次 1000 档快照都含 `105.002`，约在卖方第 173–175 档，不能按当前步长精确编码。其余 19 条尚未逐一验证原因。

[Binance 官方调整公告](https://www.binance.com/en/support/announcement/detail/8bfe2e9b86734cd08f3cbcdb82b79f51) 明确 SOLUSDT 从 `0.001` 改为 `0.01` 后，既有订单仍按原步长匹配。因此当前把新订单价格步长同时用于全部盘口整数编码的语义不足以覆盖旧挂单，不能假定等待即可解决。程序仍严格拒绝无法整除的价格、保持相应盘口无效；已有效的其余交易流继续写入，不丢档、不取整。要支持此情况需另行确认盘口编码单位与下单步长分离的模型变更，本次未实施该变更，不能认定全量采集完成或推广生产。

当前部署版本的全项目 `go test -race ./...`、`go vet ./...`、10/50 档混合历史 ClickHouse 集成检查均通过。2026-09-21 部署后的新全量 run 为 `4add8a8d-bda7-4989-a0ee-5e2cde0da8c4`；三家活跃与低活跃代表各 100 次随机秒回放共 600/600 次通过，无回放失败。连续观察超过 10 分钟时服务无重启、无采样超时、无待写积压，采样 p99 约 46 ms；这些仍是短时运行观测，不是 24 小时容量结论。

全量的 24 小时持续验收尚未完成。新 run 启动初期 Binance 合约仍会受 REST 限频而分批完成初始化，部分安静的 Bybit ticker 尚未形成完整资金费率估算；程序会将其保持为未就绪或缺失，不会伪造有效盘口或费率。启动失败或修改代码后的新 run 必须重新计算连续运行时间，不得用旧 run 拼接得到“连续 24 小时”。生产仍只采集显式配置的五路 BTC 行情；验收程序不会自动把全量 universe 推广生产。

只读查看服务与最近健康报告：

```bash
systemctl --user show crypto-market-info-perp-soak.service \
  -p ActiveState -p MainPID -p NRestarts -p ActiveEnterTimestamp \
  -p MemoryCurrent -p MemoryPeak -p CPUUsageNSec
journalctl --user -u crypto-market-info-perp-soak.service -n 100 --no-pager -o cat
```

`perpetual_universe_started` 包含当前 `run_id`、mapping/selection revision 和预算。每分钟的 `perpetual_runtime_health` 分别列出 book、funding 和 sampler：检查 ready/expected、重连/断档/队列溢出、采样 p99、写入 p99 与待写积压。Binance 初始化 REST 快照至少需要所选 Binance instrument 数量乘以 1 秒，等待期间相应源保持无效，不能把这一阶段算成完整覆盖。

sampler 的 `SampleOverruns` 表示进程启动后跨过秒级截止时间的次数，`MissedSamples` 表示这些秒中被明确标无效的 instrument 样本数；两者增长需要调查 CPU、GC 和盘口快照耗时，但单次增长不会再终止 collector。`LastOverrunAt`/`LastOverrunID` 用于定位最近一次边界。分钟第 0 秒未及时取得锚点的 instrument 不写该分钟，不能用第 1 秒状态伪造第 0 秒快照。

运行数据库与回放只读验收（替换为当前进程日志的 UUID）：

```bash
go run ./cmd/perp-check \
  -database crypto_market_info_perp_soak \
  -run-id '<current-run-id>' -min-duration 24h
```

快速阶段检查可改为 `-min-duration 0`。JSON 的 `pending` 表示时长、写入或代表样本尚不足，`failed` 表示执行或不变量错误，`passed_checks` 只表示该命令的数据库与回放检查通过；退出码依次为 2、1、0。命令不建库、不写表、不访问交易所，但会执行查询，不应高频轮询。`system.parts` 空间是该独立库所有活跃物理 part 的实际压缩占用（包含之前失败 run 和 metadata），不是严格的单次 run 新增量；24 小时空间增长应记录两次同口径读数相减，不能把启动前几分钟线性外推当作实测日占用。

## Ethereum DEX 实验分支（2026-09-22）

已完成代码与 `crypto_market_info` 内五张 `dex_*` 表的创建，并于2026-09-22 01:44:23 +08:00在现有生产collector启用。unit只增加 `DEX_ENABLED=true`、`DEX_ETH_RPC_URL=https://ethereum-rpc.publicnode.com`、`DEX_EVIDENCE_DIR=/home/ubuntu/.local/share/crypto-market-info-dex/evidence`，保持同库单writer。应用配置默认值仍为关闭，只有部署unit显式开启。证据目录必须持久可写并与数据库共同备份。不要把有限 `dex-check --sample-block` 研究命令当作持续采集。

建表命令、查询、成本情景及验收记录见 [DEX 实现说明](dex-arbitrage-implementation.md)。`dex-check`数据库连接启用服务端readonly；可选成本RPC补报只保存报告文件。DEX RPC失败单独退避，不取消CEX、收益或期权分支；既有finalized区块hash出现冲突时仅暂停DEX并记录错误，排查来源后通过现有collector生命周期恢复。

部署验收时连续高度 `26027452..26027477` 已落库，25/26区块为完整58/58报价，最近完整区块的58条状态全为 `ok`；完整区块单次采集耗时约3.9至7.3秒。继续观察到69个连续区块时服务仍为零重启，早期区块已由 `head` 追加修订为 `safe`，证明最终性重检链路在运行；当时Ethereum的 `finalized` 锚点尚未推进到本次启动后的高度。五路生产CEX盘口同期写到最新完整分钟且各有60个有效秒。首份只读报告位于 `var/dex-reports/initial-live/`，使用 `--include-head` 仅为启动验收；日常机会结论继续使用默认finalized口径。
