# 当前部署与运行说明

最近核实：2026-10-07（Asia/Shanghai；含期权辅助存储精简）。本文记录 `/home/ubuntu/crypto-market-info` 所在机器的实际部署，不是另一套部署方案。路径、版本和启用配置变更后同步更新本文；运行状态仍以现场检查为准，不保存固定 PID。

2026-10-07 02:14:10（Asia/Shanghai）已部署主collector紧凑期权辅助存储版本，现行SHA-256为 `ccec72f25771ad48fb275e2b80675f9d3a3f59f1073b8512a18953add0bd51fa`。五张辅助表迁移18,504,278条逻辑记录，全部原字段逐批精确核验；相同数据物理占用10.75 GB→4.49 GB，净减少6.26 GB。历史2,862合约/171,720合约秒迁移前后实际回放完全相同；新02:17分钟2,866合约/90分片/171,960有效合约秒实际回放及原摘要核验通过后，已删除五个旧展开副本。02:19分钟仍完整提交，五路CEX各60有效秒，服务active/running、NRestarts0，原文目录未重建。主采集器切换暂停约56秒，启动过渡按真实缺失/无效记录；不填造停机盘口。精确微秒、序号和档数仍可恢复，未删除唯一事实或降低深度、频率、覆盖；fsync设置已继承。测量不是每日增长，实际范围、测试、查询兼容及边界见[本轮清理记录](../research/2026-10-07-options-redundancy-prune/report.md)。

2026-10-06 21:16（Asia/Shanghai）已部署主 collector 无响应原文归档版本，SHA-256 `19f48c87e8db988230ae563b8e8745039e719fe29f23a486d834ad466f0f0d6a`。主 Ethereum DEX 使用 `Archive.HashOnly`，期权公开目录及生命周期在内存核验 payload hash 后直接写定类型表；旧 `DEX_EVIDENCE_DIR`、`OPTIONS_EVIDENCE_DIR` 仅兼容接受，运行程序不再读写两目录。来源摘要、UTC时间、价格数量编码、链锚点、质量表与批次提交语义不变。清理仅涉及确认不再使用的运行响应原文，独立研究样本、配置、续采状态和锁保留；Across compact capture/finality 仍被历史报告读取，不可整目录删除。LST 按已审清单清理前，先停服持原锁并复用 `FlushPending` 正常排空待写批次，禁止手工删除 pending。部署验证、精确删除统计和历史报告比较见[本轮清理记录](../research/2026-10-06-redundant-data-cleanup/report.md)。不要用旧版本回滚恢复原文依赖。

本轮实际删除 2,569,434 份冗余文件及 258 个空目录，测得释放分配空间 42.0224 GB；Across 另删除的 342,004 字节仅统计压缩文件长度，未混入分配空间合计。最终验收又复现期权旧分组恢复缺陷：计划异步入库期间新增预热成员，使下一轮超过单 run 32 个上限并反复发布失败。22:53:39 部署修复版，该版主 collector SHA-256 为 `be1b072b19b1c414206959f82d73b2543e28f71d13b7bd7d0edf44ff88680fcc`：每组最多 32 个，溢出成员继续分配，旧预热列表不抢回其他组已接管的成员。源码、回归及独立审核证明此错误不是原文读取依赖；清理期间磁盘负载可能加重数据库超时，不能宣称全程采集无中断。各次恢复与最终数据核验以本轮记录为准。

22:58 最终核验：22:57 期权2,862成员、90个run、171,720个成员秒实际回放全部有效；五路CEX同分钟各60有效秒。主collector、Across、LST、Reserve、keeper和ClickHouse均运行，两处主原文目录不存在且未重新生成。另记录Across旧的内部取消可能被当作成功退出、从而未触发自动重启的问题；本次已恢复采集，但该独立退出缺陷尚未修复，见清理记录中的诊断。

2026-10-05 11:23（Asia/Shanghai）重启后再次核验：七个项目unit均active/running，五路盘口最新分钟60/60；期权11:20、11:21两个完整分钟均3,104成员、每成员60秒有效。已给五个生产库59张MergeTree表显式启用插入、目录及合并fsync，51张本次有新数据的表各选一个有界part校验，全部通过；不涉及其他项目的表设置。Across已部署`across-reboot-head-repair-20261005`（SHA256 `34753e49bfba8965975d34e0acbd78ae5239880788240f869cbc8dc767f97015`），长积压时当前链头与旧缺口补采由已有worker分别推进，两链当前事实已恢复完整解码；旧放弃标记仍为993个partial。原始扫描与完整事实必须分开核验，最新高度不能证明历史连续；Arbitrum停机原始缺口仍在补，旧历史状态不可用仍partial。启动时的空文件隔离块和既有坏历史不视为已恢复；块号包含关系不能证明行完整。具体修改、真实验证与限制见[重启检查记录](../research/2026-10-05-reboot-check/report.md)。

## 1. 实际使用的本机服务

| 服务 | 实际运行方式 | 连接与用途 |
|---|---|---|
| ClickHouse | 用户级 systemd unit `crypto-market-info-clickhouse.service`；宿主机原生二进制以前台模式运行并带 ClickHouse watchdog；核实版本 `26.8.1.1825` | HTTP `127.0.0.1:8123`；native `127.0.0.1:9000`；数据库 `crypto_market_info` |
| collector | 用户级 systemd unit `crypto-market-info-collector.service`；直接运行编译后的单个二进制 | 依赖 ClickHouse 健康后启动，采集盘口、资金费率、TRX/SOL/AVAX 收益及 Ethereum DEX 链上数据；没有独立 HTTP 服务端口 |
| 全量永续验收 collector | 用户级 systemd unit `crypto-market-info-perp-soak.service`；独立二进制与数据库 | `crypto_market_info_perp_soak_20260928`；仅三家共有 USDT 永续盘口和资金费率，不重复采集现货与收益 |
| 桌面状态指示器 | 图形会话用户级 systemd unit `crypto-market-info-status-indicator.service`；Python/Gtk AppIndicator | 每 30 秒只读检查生产 unit、五路盘口及最新收益写入；不访问交易所、不写数据库 |

本项目不使用 Redis、PostgreSQL、消息队列或其他项目的服务。生产CEX永续保持BTC显式列表及现有收益配置；Deribit期权auto已部署R5持续全量发现。永续全量验收服务当前停用。具体构建版本用下文 `go version -m` 查询，不以当前仓库 HEAD 推断正在运行的二进制版本。

AVAX 第二阶段已于 2026-08-27 部署。collector unit 保持 `AVAX_YIELD_ENABLED=true`，新增 BENQI sAVAX、Ankr ankrAVAX、BENQI AVAX 借贷三条 Runner；启动时已对生产 `yield_observation` 幂等补齐两列，并成功写入三条首批同区块观测。

2026-09-21 已在现有collector启用 Deribit 期权采集，unit 设置 `OPTIONS_ENABLED=true`、`OPTIONS_SYMBOLS=auto`，没有新增期权专用服务。当时run `5ec3ced8-4d3a-44e9-bf1f-62386d0c505b` 自动固定选择24个期权、4个同到期期货及四个指数。启动过渡分钟如实保留无效锚点；下一完整分钟 `2026-09-20T19:28:00Z` 的28个盘口和四个指数均为60/60有效秒，批次hash为 `ace2270262f3a7bdec6524cd70bcaea13abf9b7d3a5064c102e43531c1a2a779`。详情见[期权实时采集说明](arbitrage/strategies/arb-0009-options-live.md)。

2026-10-04 15:43（Asia/Shanghai）已完成Deribit R5部署：四族全部未到期期权及交割期货随新挂牌加入、到期退出。部署前独立库全量真实DB验收通过，并由同一Agent复审批量写入修复。常驻collector SHA-256为 `3a39581b13a8abe0dcf03383d1d4ae8337a29bf350bb11aec44f5787f98a0ee4`，生产已创建五张R5表；15:46、15:47两个分钟全部3108成员均60/60有效，15:46全部秒实际回放通过。原文持久目录及备份见[部署记录](arbitrage/strategies/arb-0009-options-lifecycle-deployment.md)。检查另发现9月期权质量表旧分片UNKNOWN_CODEC，未执行数据修复；上述成功仅针对新分钟及现场范围。上段28合约为R4历史。

2026-10-05 02:40（Asia/Shanghai）已按用户批准并要求继续的方案，完成九月旧历史坏块清理：生产期权质量表的`202609_1_5549_21`及停用旧验收库`crypto_market_info_perp_soak.order_book_second_delta`的7个坏parts，共8块。均有原始列全部或局部全零及实际读失败证据，未找到完整可用恢复副本，已逐块`DETACH PART`后`DROP DETACHED PART`，删除约1.97 GB。324个文件的哈希、UTC范围及当前最终状态保存于`var/recovery/history-codec-cleanup-2026-10-05.json`。剩余九月期权质量8块、旧永续差量2块均通过逐块校验和全列实际读取，健康块与删除前内容哈希相同。02:34分钟全部3,094合约及五路CEX全秒实际回放通过，02:36—02:38全量分钟均60/60；02:32有30个合约各1秒采样延迟，已按无效秒记录且随后恢复。服务未重启。删除不是历史恢复，旧验收库仍有明确缺口；具体清单、原始异常及验证边界见[清理完成记录](../research/2026-10-05-history-codec-cleanup/report.md)，[初次隔离记录](../research/2026-10-05-history-codec-isolation/report.md)保留当时中间状态。

2026-09-22 已在同一个生产 collector 启用 Ethereum DEX 采集，unit 设置 `DEX_ENABLED=true`、`DEX_ETH_RPC_URL=https://ethereum-rpc.publicnode.com`，证据写入持久目录。部署后二进制 SHA-256 为 `cf49b0795aad53a8b5ae00f623d66f867836aac07154761c8c13097f61aaf7a4`。验收区间 `26027452` 至 `26027477` 共26个连续高度无缺口，其中25个区块达到58/58完整报价；`26027460` 因一次RPC/Sky状态读取不完整保留为 `partial/unknown`，后续区块自动恢复。首份24高度机会报告没有毛正窗口。DEX分支不包含钱包、签名或交易发送。

2026-09-23 排查发现 DEX 公共 RPC 的响应读取超时集中出现在最终性校验阶段，旧版在校验失败后额外等待5秒，并在下一轮立即重复昂贵的校验。现版将 RPC 响应读取超时、截断与超过16 MiB分别记录；最终性校验把 `finalized`、`safe` 和原 checkpoint 合为一个 RPC 批次，每轮最多校验20个高度、最多占用6秒，失败后30秒再重试。实时新区块采集与补采、最终性、日志和回执维护现已独立运行，维护任务不会在调度上阻塞实时轮询；共享 RPC 仍可能使实时请求变慢。实时请求失败后按正常2秒轮询间隔重试。Binance 实际资金费率历史中部分 `fundingTime` 比计划整点晚数毫秒，现版查询并匹配整点后1秒内唯一的来源记录，保留其原始毫秒时间，启动补查窗口扩大到72小时；重试耗尽而来源尚无记录时会明确告警。2026-09-23 12:39 CST重启时，OKX启动元数据请求超时曾导致整个collector首次启动失败，此类启动缺口只能补回区块元数据，不能补回当时的实时报价。现已将DEX与CEX等其他来源拆成独立启动和重试分支，并分别使用数据库连接；CEX元数据超时不会停止已运行的DEX分支。12:52 CST重启验收时，DEX先于CEX元数据初始化完成写入新区块。2026-09-28 部署时二进制 SHA-256 为 `dd55a3a80fbba9bec3b61a18401f41b1ba436322d4de8012f4889fdca6859a21`。更新未增加外部域名。

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
| 期权旧原文目录（现行 collector 不再读写） | `/home/ubuntu/.local/share/crypto-market-info-options/evidence/` |
| 主 DEX 旧原文目录（现行 collector 不再读写） | `/home/ubuntu/.local/share/crypto-market-info-dex/evidence/` |
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
| Deribit 期权 | BTC/ETH币本位和USDC线性四族；全部未到期期权及交割期货，四个指数 | 每秒前10档与指数；生命周期推送，目录每30分钟及重连后复核 |
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

2026-09-28 旧验收库 `crypto_market_info_perp_soak` 的秒级差量表有 22 个无法完整读取的数据分片（约 21.94 GiB）。逐一隔离并重启 ClickHouse 后，该表可读取的剩余差量为 33,432,284 行。另在本项目生产库和旧验收库中检查了 802 个由 ClickHouse 自动标为 `broken-on-start` 的历史分片：593 个存在空目录、缺失元数据、空数据文件或已证实无法解压的全零压缩头；39 个完整解压时报错；170 个有损坏元数据，其中 18 个逐个在隔离恢复表装载失败。824 个确认不能完整恢复的分片已按用户要求删除，总计约 22.45 GiB；清单和验证结果保存在 `var/recovery/*-2026-09-28.json`。普通 `detached` 分片没有被判定为损坏，仍保留。旧库保留作不完整历史检查；由于缺失秒级差量，不能将其盘口分钟当成完整可回放数据，也不能把新 run 继续写入旧库。

验收服务配置已切换到 `crypto_market_info_perp_soak_20260928` 新库，2026-09-28 13:19:54 UTC 开始的新 run 已写入部分有效分钟，但交易所 WebSocket 重连量很高，并与生产 Binance、Bybit 盘口断流同时出现。为优先保障生产行情，独立验收服务已停止并禁用开机自启；重启验收后必须从新 run 重新计算连续运行时间，并先确认生产五路盘口仍有完整 60/60 分钟。

同日现场直连 Bybit 公共 `orderbook.1000.BTCUSDT` 时，快照在订阅后约 8 秒到达，订阅确认约 13 秒才到；原采集器的 10 秒等待会在收到有效盘口后仍报 `subscribe acknowledgements timed out` 并断开。生产 Bybit 盘口订阅等待已调为 30 秒，继续按 request ID 严格验证确认，并使用现有有界缓冲在确认前保留消息。该修复于 21:42 +08:00 部署，旧二进制保存在 `/home/ubuntu/.local/share/crypto-market-info-collector/collector.rollback-20260928-pre-bybit-ack`。

Binance 永续当时持续报 `Binance snapshot bridge timeout`，生产仍将该路标为无效。公网 1000 档快照实测耗时约 16 秒，原快照请求等待为 20 秒、快照后的差量桥接等待仅 5 秒；现分别调到 35 秒和 30 秒，序列断档仍强制重新取快照，不能因等待变长而沿用旧盘口。2026-09-28 21:54 +08:00 项目状态指示器检查生产五路 BTC 盘口均为最新完整分钟 60/60，生产 collector 零次重启；随后 Binance、Bybit WebSocket 仍出现 TCP 读超时，21:57 +08:00 状态为黄色且有无效秒。网络波动期间继续保留无效标记，不能把这次短时绿色状态写成持续稳定验收。

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
  -database crypto_market_info_perp_soak_20260928 \
  -run-id '<current-run-id>' -min-duration 24h
```

快速阶段检查可改为 `-min-duration 0`。JSON 的 `pending` 表示时长、写入或代表样本尚不足，`failed` 表示执行或不变量错误，`passed_checks` 只表示该命令的数据库与回放检查通过；退出码依次为 2、1、0。命令不建库、不写表、不访问交易所，但会执行查询，不应高频轮询。`system.parts` 空间是该独立库所有活跃物理 part 的实际压缩占用（包含之前失败 run 和 metadata），不是严格的单次 run 新增量；24 小时空间增长应记录两次同口径读数相减，不能把启动前几分钟线性外推当作实测日占用。

## Ethereum DEX 实验分支（2026-09-22）

已完成代码与 `crypto_market_info` 内五张 `dex_*` 表的创建，并于2026-09-22 01:44:23 +08:00在现有生产collector启用。unit只增加 `DEX_ENABLED=true`、`DEX_ETH_RPC_URL=https://ethereum-rpc.publicnode.com`、`DEX_EVIDENCE_DIR=/home/ubuntu/.local/share/crypto-market-info-dex/evidence`，保持同库单writer。应用配置默认值仍为关闭，只有部署unit显式开启。证据目录必须持久可写并与数据库共同备份。不要把有限 `dex-check --sample-block` 研究命令当作持续采集。

建表命令、查询、成本情景及验收记录见 [DEX 实现说明](dex-arbitrage-implementation.md)。`dex-check`数据库连接启用服务端readonly；可选成本RPC补报只保存报告文件。DEX RPC失败单独退避，不取消CEX、收益或期权分支；既有finalized区块hash出现冲突时仅暂停DEX并记录错误，排查来源后通过现有collector生命周期恢复。

部署验收时连续高度 `26027452..26027477` 已落库，25/26区块为完整58/58报价，最近完整区块的58条状态全为 `ok`；完整区块单次采集耗时约3.9至7.3秒。继续观察到69个连续区块时服务仍为零重启，早期区块已由 `head` 追加修订为 `safe`，证明最终性重检链路在运行；当时Ethereum的 `finalized` 锚点尚未推进到本次启动后的高度。五路生产CEX盘口同期写到最新完整分钟且各有60个有效秒。首份只读报告位于 `var/dex-reports/initial-live/`，使用 `--include-head` 仅为启动验收；日常机会结论继续使用默认finalized口径。

## Reserve r5 研究采集（2026-10-02）

本节早期操作记录保留日期；现行七表、直接入库和响应清理政策见文末「Reserve 2026-10-04 存储修正」。旧原文已迁移并删除，不再按早期五表/证据目录方式恢复。

新增独立 user unit `crypto-market-info-reserve.service`，二进制 `/home/ubuntu/crypto-market-info/var/reserve/reserve-data`，工作目录仓库根，隔离库 `crypto_market_info_reserve`，证据 `/home/ubuntu/crypto-market-info/var/reserve/evidence`。默认 HTTPS POST 到 ethereum-rpc.publicnode.com，路由沿用用户配置。

```bash
systemctl --user status crypto-market-info-reserve.service
journalctl --user -u crypto-market-info-reserve.service -n 30 --no-pager
/home/ubuntu/crypto-market-info/var/reserve/reserve-data report
```

report 默认只读finalized；启动验收可显式 `--finalized-only=false`。后台watch与backfill共享文件锁，回补前停止这一独立unit，完成后重启。当前首个白名单为DFX完整六成分。免费RPC不保证30天历史，拒绝范围保留为missing；完整数据模型、命令与限制见 [Reserve实现](reserve-data-implementation.md)。

Watch主循环对明确的RPC传输/读取超时、截断及HTTP429/5xx保留游标后重新poll/复核，区块头复核每请求最多五个成员。非法响应、证据/DB失败、finalized hash冲突仍退出；启动预检失败仍会由systemd重启。2026-10-02 11:29 UTC部署这一运行修正，短时验收保持active、继续写入，没有自动重启；此前版本曾因公共节点读取超时重启，不能把这次短时观察当成长期无缺口证明。实网验收文件在 `research/2026-10-02-reserve-implementation`。

2026-10-03补齐的[Reserve操作说明](reserve-data-implementation.md)含当前manifest的只读SQL、报告计数口径、RPC环境覆盖、固定高度回补和备份恢复要求。前台export不会修改已运行的systemd环境；回补前停止这一unit，固定from/to及chunk，结束后恢复watch。日志完整而收据未齐时可能已经推进日志游标，Watch没有独立收据重试队列，应通过显式范围回补核验。备份须同时保留五表和原始证据；当前没有自动清理或已验收的自动恢复流程。


## Across 独立研究采集器（2026-10-03 修复）

当前运行实时 `crypto-market-info-across.service`，磁盘binary为 `var/across/bin/across-data`，工作目录仓库根，使用实时库 `crypto_market_info_across` 和证据 `var/across/evidence`。只有一个writer进程，report只读，不要手工启动同库第二个writer。user linger已开启。固定历史unit模板仍保留，但已disabled，其独立库和归档已按用户要求删除。

2026-10-03用户明确收窄采集范围：首次启动前的数据不再回补。已停止并禁用 `crypto-market-info-across-history.service`（inactive/dead、MainPID=0、disabled）。核对所有history事实均早于实时首次已保存区块（Base 52077937、Arbitrum 510991304），没有可补实时缺口的数据；已删除独立库 `crypto_market_info_across_history`、`var/across/history-evidence`（75,702个归档文件）和导出的历史报告数据，保留修复说明及少量验证统计。实时库、实时证据、RPC冷却状态均保留。实时unit继续active/running、enabled：首次无保存游标时从当时链头开始，以后从已保存位置续采；只修补其已有采集范围内的缺块、partial和缺收据，并继续最终性核验。重启实时服务不重置首次采集起点。

此前历史unit使用一条 `history --range 8453:50783591:52079591 --range 42161:500994686:511000892`，固定窗口约对应2026-09-02至2026-10-02，每链一轮64块、独立退避与补解码；现在已停用，不再执行该窗口。实时普通失败15秒重启；已确认网络故障以2退出，unit配置RestartPreventExitStatus=2，保持停止待用户处理。

两进程共同使用 `var/across/rpc-quota` 的host gate，默认400ms/成员、每批最多3成员，所有批次共同等待上一批额度后准入，限流共享冷却。仅fresh live baseline及准时followup获准后有固定最多4秒优先窗口，不续租，届满按实际额度可用时间让行一个成员间隔；restart/open_check没有优先额度，且可被新live到达取消其网络工作。此预算不证明源端额度，也不约束不使用此gate的应用。不要删quota文件清冷却；不会自动改路由/代理。默认RPC仍为 `https://mainnet.base.org` 和 `https://arb1.arbitrum.io/rpc`，行情为用户指定 `https://api.binance.com`。对明确限流与网络错误分别排查；确认连通性故障先停止修复工作并告知用户。

```bash
systemctl --user status crypto-market-info-across.service crypto-market-info-across-history.service
journalctl --user -u crypto-market-info-across.service -n 30 --no-pager
journalctl --user -u crypto-market-info-across-history.service -n 30 --no-pager
var/across/bin/across-data version
var/across/bin/across-data report --out var/across/reports/live-latest
```

升级前停止Across实时unit并确认历史unit保持停止，保存旧binary和unit；构建候选版本，测试后原子替换磁盘binary、复制unit、daemon-reload，只启动实时unit，历史unit保持disabled，除非用户重新要求历史回补。部署后以`systemctl show ... -p MainPID`取得实时PID，对磁盘和`/proc/PID/exe`做sha256sum，验证没有deleted旧映像；启动日志含构建版本。只管理Across服务，不停止其他项目collector。此前双服务修复的构建、进度、报告与剩余缺口见[修复验收](../research/2026-10-03-across-repair/validation.md)，其运行快照先于本次范围收窄。

健康检查使用[Across只读SQL](across-data-health.sql)：每链最近capture时间、fixed窗口raw/decoded union、finalized待核验量、最终性晋级新增revision、费用完整性去重和probe准时/取消要分开。原始coverage推进不能替代完整事实；pending积压要测趋势。成功返回finalized头不等于整尾已核验；只认实际存储的已证明revision。report performance明确SQL和归档耗时，finality策略明确包括head，不把报告存在当作盈利成立。

2026-10-04按用户要求改为内存解析后直接落库，不归档RPC/API响应。实时RPC及Binance适配器只计算payload hash；不再依赖response-retention.json或每批删文件。新增`across_receipt_transfers`保存完整原生USDC Transfer数组，退款查询读数据库。旧数据先用`var/across/bin/across-data migrate-transfers`本地迁移（需暂停同库writer，命令不联网），随后恢复实时服务；`prune-responses`可并发只读核验并清理旧response，包括未被引用的旧请求。未完整替代的旧日志、未迁移退款及两分钟内的请求暂留，compact capture/finality元数据和quota保持。history继续disabled；不重新补首次启动前的历史。修改及实际验收见[本轮记录](../research/2026-10-04-across-no-raw/validation.md)。

2026-10-05用户明确放弃已核验的Arbitrum旧历史状态解码缺口。30段共35,257块所涉及的993条partial日志capture追加新revision与reason标记`repair_abandoned=user_requested_historical_state_unavailable`，自动partial补采跳过，重启后仍生效；其他缺采、新块、收据和最终性继续按原逻辑处理。状态仍partial，原失败原因及事实/成员/锚点/最终性保留，覆盖统计仍显示缺口。02:15:25北京时间部署跳过逻辑，磁盘与进程SHA256为`042ea8b0e6b13937575d1d81d295a95c6df7b520802074858d94883d45311ed2`，两链随后实际落库，history保持disabled。修改和验证见[记录](../research/2026-10-05-across-abandon/validation.md)。

备份和恢复须同时保留每库七表、相应完整evidence目录、manifest及其引用的verified实现文件。恢复前停止对应unit并验证成员/归档摘要，不删除源错误证据或重写旧capture。启动时旧partial/费用unknown安排新capture尝试补齐；暂缺历史状态/未知实现仍保持unknown，换RPC应由用户配置并重新验证，不能静默回退到latest。当前无需私钥或真实账户凭据。

## LST 独立研究采集（2026-10-02 UTC）

**当前恢复验收（2026-10-05 11:22 北京时间）：** 已部署mvp11，active/running、enabled、NRestarts0。机器10:42重启后LST自动启动，但日志积压且游标曾领先实际DB覆盖8块；本轮人工补1852块，修改恢复为从保留Start重建完整连续前缀，自动补回断口并验证重启后跳过已补大段。Next26123313与DB连续覆盖一致，11:19、11:21两轮四腿ok/fresh/canonical；48个本次boot后批次校验和原文清理通过。旧2296块缺口仍低速补，停机报价不能重建，资金费实际结算已补回。程序SHA256 `77b5545b6ac6d74f5e3b9730b5d5a247ba5b28c75dfb29fdb4f6c4abb2536a59`，原unit/路由/端点/限速不改，不建旧程序备份。见[修复记录](../research/2026-10-05-lst-reboot-check/report.md)。

**最近健康检查（2026-10-04 22:27 北京时间）：** 同一mvp10进程持续运行、NRestarts0、enabled；最近15分钟7轮调度报价四腿完整及时、canonical，25个近期批次回读校验与原文清理通过。新段22:24:52追上当时finalized，旧缺口尚未补齐。此前容量不足及零星失败保留原状态，孤立后台数据库超时后落库和最终性继续，没有停止服务或改路由。具体窗口、失败分母与覆盖核验见[晚间检查](../research/2026-10-04-lst-evening-check/report.md)。下段为启动时点记录。

**当前状态（2026-10-04 14:39 北京时间）：** 按用户指令于14:35:16启动现有mvp10，active/running、enabled、NRestarts0；已确认新的market、funding、logs实际落库，4个新批次成员摘要和结构验证通过，成功落库后临时原文已清理。首轮25万USDT链上三腿ok、十档对冲容量不足，正确保持不可用；日志从原游标连续推进16块，未宣称全历史完整。程序SHA256 `24c812ec0a0f5cd7f3d1f7a38f4327f68f32661b224f65f26855dbbe541c0b6b`，原unit、端点、路由、额度和持久状态未改。13:31:19是此前助手手动停止记录，三轮初始化各一次超时不证明持续网络中断；程序本来会退避重试。此次短时恢复及原文清理核验见[启动记录](../research/2026-10-04-lst-restart/report.md)，此前代码与清理测试见[修改记录](../research/2026-10-04-lst-response-retention/report.md)。

**mvp9恢复验收（2026-10-03 17:13 UTC，历史记录）：** 16:52:36 UTC（北京时间2026-10-04 00:52:36）按用户“恢复”部署mvp9，唯一MainPID2196152、active/running、enabled、NRestarts0。BlockPI继续当前报价，MEV Blocker负责赎回日志与历史queue实现状态；固定新段Start26113044每分钟至多8块，原live-logs.gob只依据真实落库补旧缺口，每5分钟至多32块且让实时任务优先。每host20RPC/min、响应后2秒、日志8次/5分钟；不清旧预约或cooldown、不改路由。一笔真实request与claim已通过有限双来源读取落库；随后固定窗口5轮四腿完整及时、9片新日志连续覆盖65块，后台旧缺口已真实推进Next26105776；新Next26113109追上当时finalized26113108。一次Binance预检超时自动恢复，旧整体缺口仍不完整。原始证据、输入/binary/unit SHA及后续验收见[本轮记录](../research/2026-10-04-lst-logs-restore/report.md)。

已创建 `crypto_market_info_lst` 七张专项表及 instrument，程序 `var/lst/lst-data`，工作目录仓库根。状态与限速冷却在 `var/lst/state`，原始公开证据在 `var/lst/evidence`。2026-10-03 02:36:08（北京时间）按用户要求安装并启动用户级 `crypto-market-info-lst.service`，启用开机启动；用户管理器已有 `Linger=yes`。unit 源文件为 [crypto-market-info-lst.service](../deploy/systemd/crypto-market-info-lst.service)，安装在 `/home/ubuntu/.config/systemd/user/`，只运行 `watch`，不启动 30/60 日历史回补。补采仅用于断线后的增量日志缺口，旧状态目录和持久限速沿用。失败后等待 60 秒重启，日志进入 journal。

2026-10-03 18:09:36 UTC（北京时间2026-10-04 02:09:36）完成用户授权的一次性空间清理：删除36,823份已落库旧证据原文及7份旧程序备份，实际释放约292 MiB；未保留原文副本。保留数据库、当前程序、配置/游标/冷却/待写状态、近期或未证明已提交的数据和必要核验/错误样本。清理期间服务未停止，MainPID2196152、NRestarts0、程序SHA不变；清理后历史report仍通过成员摘要校验，且新market批次继续提交。已删除原文不能再按hash回读；本次没有启用自动清理，详情见[清理记录](../research/2026-10-04-lst-cleanup/report.md)。

初次启动核验（旧版本，非本轮验收）已连续写入三轮市场批次（24 条报价观测，其中四条完整且及时）和六条资金费观测。首轮本地发送预约过期被标 unknown，后两轮协议状态恢复 ok；服务未重启。实时日志补缺仍被 dRPC 拒绝，保留 failed、不推进覆盖。有限核验详见[常驻启动记录](../research/2026-10-03-lst-drpc/service-startup.md)。

2026-10-03（北京时间）按用户选择将此 LST CLI 的默认 RPC 改为 `https://eth.drpc.org`，无需注册或 API key。`LST_RPC_URL` 仍可覆盖；共享状态目录、排他锁、启动节奏和回补限速沿用，验证见[切换记录](../research/2026-10-03-lst-drpc/validation.md)。

旧版 dRPC 的一次实时 `watch --once` 已落库协议状态和报价，但该免费端点按高度查询日志返回 HTTP 400 / code 35，512 块历史回补未通过。程序保存错误与 failed 范围，不把拒绝当作成功空范围，不据这条不一致的范围提示反复拆分重试；未运行完整 30/60 日回补。

2026-10-03 06:16:46 UTC 已部署 `lst-mvp-2`，同一 `eth.drpc.org`、原状态目录和冷却，unit 显式 `LST_LOG_MODE=receipts`；非空区块 26106948 的全部收据与 blockHash 日志已有限核验。每轮轮转一个金额的 A/B 两条路线，六条未调度观测保持空值。启动预检在 06:19:28 UTC 记录 `code_curve: transport_http_timeout`；按用户有网络故障即停止、不改路由的要求停止服务，确认 `inactive/dead`、`MainPID=0`、`NRestarts=0`。本次没有新增市场/日志批次，持久 Next 仍为 26105616；连续生产验收未通过。修复、测试、只读数据库核验和归档证据见[本轮记录](../research/2026-10-03-lst-repair/report.md)。这是此前停止状态，后续同机复查见[网络复核](../research/2026-10-03-lst-network-recheck/report.md)。

用户要求恢复后，07:03:54 UTC 启动，首次日志覆盖成功推进至 26105623；随后过去 60 秒共 41 次 RPC 的轮次出现真实 429，来源进入原持久冷却。07:38:20 UTC 部署 `lst-mvp-3` 并启动，该次启动 active/running、enabled、无自动重启；二进制 SHA-256 `375fb00eee7975cda359d51bfc025b2fe35bf3dfc7e5ac94e94a03e46d1cbf82`。unit 显式 32 RPC/滚动分钟、2m 市场周期、一条路线/轮，另一分钟串行尝试最多两段八块 receipts；额度与冷却跨重启保留。限速是本程序保守配置，不等于供应商保证额度，也不约束同出口其他进程。服务继续使用 eth.drpc.org，未修改路由。07:50:19.743042..07:50:24.747729 UTC 的 Binance 十档请求发生真实 `transport_http_timeout`，本地观察在07:50:25.843994 UTC按用户约束停止服务。当前 inactive/dead、MainPID=0、NRestarts=0，保留enabled；未继续外部探测。已提交连续日志26105624..26105687，共64块，含一条请求与一条领取；失败26105688..26105695未覆盖，Next=26105688。另有轮前额度等待源码 `lst-mvp-4` 经本地测试和审核，尚未替换已安装二进制，恢复前须先部署。最新落库、剩余报价问题、完整证据见[启动修复记录](../research/2026-10-03-lst-live-restart/report.md)。

2026-10-03 10:35:52 UTC（北京时间18:35:52）已安装并启动最终 `lst-mvp-7`，MainPID2111383、active/running、enabled、NRestarts=0，二进制SHA256 `b8aa795efe98378f4379ae4a10727c7c8c859753f044e990804e3a1b01d7b5b7`。沿原eth.drpc.org/fapi.binance.com与原state/cursor，不修改路由。此前mvp6最后429的持久冷却10:33:54自然到期；本版Protocol13视图合为一条已pin运行时代码的Multicall3只读请求、核验执行高度/时间；unit20RPC/滚动分钟、响应后2s、启动gap10s、市场2m单路线。初始化20s head窗口对冷却作明确诊断，60s再试。receipts只在原始queue日志非空的真实块核验历史实现；空日志覆盖不声明历史implementation。live发送前额度不足可收束在完全核验的真实前缀，再canonical/Commit/按实际To推进；任何真实source/解析/身份失败整片零事实/Next不动。已通过最终全包race78.257s、vet及独立审核；实际预检/持续采集验收见[本轮记录](../research/2026-10-03-lst-running/report.md)，不能仅凭active认定正常。


以上是历史停止记录。用户要求再次恢复后，08:18:11 UTC启动mvp4，预检16RPC后收到HTTP429，原gate冷却至08:48:51 UTC。08:47:06 UTC已安装并启动mvp5，二进制SHA256 `af121d3bb626f934d38896beaa4b8471efbc647eeea65a37482fa721ea8cf654`，MainPID2081638，active/running、enabled、NRestarts=0，冷却未清除。unit增加 `LST_RPC_STARTUP_GAP=10s`，报价轮前等待持久NextAt和额度，CEX时序及陈旧原因诊断已修复，第二段receipts按剩余额度缩片。最终race49.502秒、vet及独立审核通过；真实预检和持续落库验收进行中，不能仅凭active认定数据正常，详见[本轮运行修复](../research/2026-10-03-lst-running/report.md)。

```bash
systemctl --user status crypto-market-info-lst.service
journalctl --user-unit=crypto-market-info-lst.service -n 30 --no-pager
var/lst/lst-data report --out var/lst-reports/latest
```

不要手工启动第二份 `watch`；同库采集由这个 unit 管理。手动补缺或 probe 前先 `systemctl --user stop crypto-market-info-lst.service`，完成后用 `start` 恢复。`--once` 只有一份市场观测，不触发后台维护或历史回补。来源 URL、严格限速、恢复、只读报告与边界见 [LST 实现说明](lst-redemption-data-implementation.md)，初版数据验证见[记录](../research/2026-10-02-lst-implementation/validation.md)。

数据健康检查使用[只读 SQL](lst-data-health.sql)：最近 15 分钟 market 应持续产生观测，协议/报价可用性按成员状态检查，`partial` 不代表所有成员无效。增量日志覆盖只认 complete/canonical/committed；失败范围与资金费重叠采样不作为新增覆盖或重复现金流。head 观测不会立即进入历史报告。升级、配置覆盖和状态备份见[维护说明](lst-redemption-data-implementation.md#维护配置与恢复)，不要删除状态目录来绕过冷却。




## JustLend keeper 独立研究采集（2026-10-03）

当前版本（2026-10-04 02:09:06北京时间启动）已去除请求/响应/summary文件归档：正常采集内存严格解析后直接落库，后台补查及export/report只读DB。七表包括新增jl_keeper_index_page，3244旧成功页进度已本地迁移，旧六表行数/内容指纹不变；state预算/游标保留。实际binary SHA256为480b8d916a21bf2bf013c55a2b954444fbf2d85ed2cac1865dba0c58295a7b47，原unit/路由/限速不改，自启动保留。旧evidence及其硬链接备份已清理，正常目录不存在时采集/后台/30天导出均验证通过。详细范围及持续运行记录见[本轮验证](../research/2026-10-04-keeper-no-raw/validation.md)和[独立审核](../discuss/0019-justlend-keeper-no-raw-review.md)。

按用户要求已启用常驻用户服务 `crypto-market-info-justlend-keeper.service`，2026-10-03 03:02:31 Asia/Shanghai 首次启动；本次边界与报告修复后13:43:36恢复，修复验证版本14:13:14启动，14:33:36曾因两次transport_error停止；该停止判定过于敏感，用户明确要求恢复后，14:55:43（北京时间）已启动并恢复自启动，当前active/running、UnitFileState=enabled。独立库 `crypto_market_info_justlend_keeper` 初版五表已建（当前七表），不接入主盘口 collector。该次二进制 SHA-256 为 `c264480c1eb67456fe242ce1e2368c26f0dd21f9269e3e660bc473eaa9d0b182`（最终报告优化版已安装并实际运行）；同目录其他模块同时维修，使用已提交依赖加keeper补丁的独立快照构建，只安装keeper程序。先前四条无法跨进程核验的报价提交保留原撤回状态，本次不改旧事实／摘要。实际服务、游标和验证证据见[修复记录](../research/2026-10-03-keeper-repair/validation.md)。

二进制在仓库 `var/justlend-keeper/bin/justlend-keeper-data`，配置 `config/justlend-keeper-tron.json`；状态在 `var/justlend-keeper/state/`；旧原始归档已按2026-10-04策略清理，正常运行不依赖evidence目录。仓库 unit 已复制到用户服务目录并 enable，已有 Linger=yes 使退出登录后继续运行。每次七天时限结束或失败后等60秒恢复，进度与预算保留。

```bash
systemctl --user status crypto-market-info-justlend-keeper.service
journalctl --user -u crypto-market-info-justlend-keeper.service -n 30 --no-pager
var/justlend-keeper/bin/justlend-keeper-data report --days 30
```

PublicNode `tron-rpc.publicnode.com` 承担只读节点数据，事件分页仍来自 `api.trongrid.io`，TRXUSDT来自 `api.binance.com`。第一请求等五秒，前五分钟全局至少五秒/次，此后至少一秒/次；TronGrid及后台请求仍至少五秒/次，单请求在途，每日总上限40,000次。初始化 Rent/Return 最多60页和固定50样本；unit显式启用 `--history-days 30`，生命周期事件追上后按一天一个窗口后台回补，冻结区间不随重启滚动。429保存来源冷却，401/403保存停用；不通过重启清掉状态，不自动改路由。

本轮两笔非停服时段的约10秒transport_error不足以证明持续网络故障；按用户后续明确指令已恢复采集及自启动。偶发传输失败保存证据并按现有机制重试，不因两次零星失败停止服务或取消自启动；持续采集不可用时再报告。未更换端点或修改路由。不要再手动启动一份采集器。恢复来源前先停止此unit，完成后重新start；report只读可以同时执行。历史backfill与watch共用状态时禁止带另一模式的待办切换，单纯停止unit不能排空watch队列，当前没有自动排空命令。模式切换限制、配置、备份及[数据健康SQL](justlend-keeper-data-implementation.md#数据健康检查与排查)见[实现说明](justlend-keeper-data-implementation.md)，独立[代码审核](../discuss/0017-justlend-keeper-data-code-review.md)与[真实验收](../research/2026-10-03-keeper-implementation/validation.md)。

04:07首批实际验收已有9条事件、9份收据、5条只读模拟、45条报价/费用观测，正式独立报告成员及原始证据核验通过。模拟均为TVM revert，source请求成功不被当作合约执行成功。启动五分钟51次已回读请求均HTTP200，最小间隔5.54603秒；其余固定样本继续经统一gate恢复核验。

### Reserve 2026-10-03 修复运行

实时 user unit 保留 PublicNode 来源，drop-in `~/.config/systemd/user/crypto-market-info-reserve.service.d/repair.conf` 指定 `watch --simulate-every=5m --from=26109689 --reconcile-manifest=0x98cf5cc757bbbe69d9cc8219210cad0490ae70e6a19a4531f6f6aa74afc3a4a9`，`RestartPreventExitStatus=4`。明确网络故障停止后先看归档诊断并联系用户，不改路由；普通来源限流由程序冷却，冷却元数据在 `var/rpc-state`，重启/换库不得删除它。

本轮独立临时历史 unit 使用 MEV Blocker、隔离历史库/证据。任务名与完整验收结果记录在 [修复记录](../research/2026-10-03-reserve-repair/report.md)，不要依赖临时 unit 在成功退出后仍可重启。固定窗口续跑命令见 [实现说明](reserve-data-implementation.md)。历史与实时不共享 writer.lock，但必须协调同 hostname 的来源额度。

旧 binary 已备份 `var/reserve/reserve-data.v1-20261003`；只替换/restart Reserve，其他会话正在改的 Across/LST/keeper 服务不属于本次部署。回滚时保留新六表、证据和配额目录，停止服务后原子替换二进制；回滚旧版会重新引入密集复核/限流问题，不能把旧参数当现行参数。联合模拟表随六表一起备份；它的 canonical/finality 必须通过 quote/capture 关联读取。

2026-10-03 16:57:13（北京时间，08:57:13 UTC）完成源采集/核验拆分升级并启动；active/running、enabled、NRestarts=0。新二进制SHA256为0749ecd085f5b187150bc88c424cd7a7896e0adc8a79b74a79e50342cb60009c。专用库新增jl_keeper_indexed_event及Capture兼容列，共六表；源页提交独立推进，后台父子核验不阻塞抓取。export及兼容report只输出数据CSV与metadata，不运行获利分析。原state/五表/证据/旧binary已备份，预算与来源状态保留、路由和unit参数不改。实际持续游标、非空索引、父子批次、导出及请求间隔见[本轮验收](../research/2026-10-03-keeper-collection-split/validation.md)。

### Reserve 2026-10-04 存储修正

现行 `reserve-r5-mvp-3`，DFX manifest `0xfb3914d4f2dbc4a357a45ca7aa7876cc78e82feca991a8a77d46746acc8ba332`。只在内存接收、解析和校验RPC；事实写七张定类型表，正文不归档。回执的完整调用参数（calldata）和全部日志在 `reserve_receipt_data`，查询/补采从数据库读取；源哈希、链锚点、旧成员摘要和首次可见时间保留。静态版本配置/ABI/模拟产物在 `/home/ubuntu/crypto-market-info/var/reserve/rules`，约232KiB；四个旧 evidence 目录均只剩 writer.lock。`var/rpc-state` 持久配额/冷却保留。

2026-10-04 02:17:07 CST仅停止 Reserve 服务，四库981旧回执迁移后于02:23:48 CST启动新版。二进制实际SHA256 `4a770096440460a0580a0d475df42084bb4d85ffa6423d152dcf420816a0bfa8`，已核对 `/proc/<pid>/exe` 与安装文件。drop-in 现为 `watch --simulate-every=5m --from=26113531 --reconcile-manifest=0x04af64a255a41d2e2182eb44cc8148338ee62e7fbc5e37a43eac86a73ef93abc`；保留 `RestartPreventExitStatus=4`，路由/RPC来源不改。停机区间日志已自动补齐；旧 v2 尾部正常复核最终性。

本轮删除209,324份运行归档gzip，文件正文压缩后合计7,144,459,564字节，已保留独立静态规则；不备份原文。历史研究目录的既有排障记录未批量删除。删档后从只读数据库逐一验证982回执（981迁移+1实网新抓），33,331条完整回执日志；30日原窗口报告一致。验收检查时 active/running、enabled，本次启动NRestarts=0，11/11完整状态及报价、61/61完整日志范围、128连续高度、零响应文件。该状态为短时现场核验，不代表长期稳定率。

迁移命令需要同目录 writer.lock，不能在运行中的watch旁绕过锁。日常核查使用 readonly `report`；每个旧 manifest 也可用 `--manifest-hash` 检查。备份七表、静态rules、公开配置、构建和unit参数；不要回滚到依赖原文的v1/v2。明确网络故障仍停下通知用户；429仍按来源冷却。修改、实网验证及测试限制见[验收记录](../research/2026-10-04-reserve-storage/report.md)。

交付前02:47:12 CST再次只读核对：Reserve同一进程保持active/running、NRestarts=0，17/17轮完整状态和报价、93/93完整日志范围，167连续高度，四目录零响应文件；见[最后核验](../research/2026-10-04-reserve-storage/last-check.json)。

### Reserve 2026-10-04 服务器重启核查

服务器12:33:55 CST重启，ClickHouse12:34:26、Reserve及其他采集服务12:35:04自动恢复；active/running、enabled、NRestarts=0，Reserve运行binary SHA保持不变。12:47抽查重启后9/9完整状态/报价采样，每批13报价；最新12:46:35落库。旧982回执逐笔校验及215,066高度/1,946日志/957回执的原30日历史报告通过，四目录仍零响应文件。

日志不完全正常：PublicNode `eth_getLogs`反复403/-32602，明确 `rpc_archive_auth_required`；积压在重启前已存在，不是整体网络不通。本次恢复后已推进465高度至26116373，已完成并集内部连续，但相对最新报价仍落后314块、约63分钟；现有watch继续回补。不要把服务running或报价完整当成日志已追平。本轮只读检查，未重启服务、额外回补、修改来源或路由；完整证据见[重启核验](../research/2026-10-04-reserve-reboot/report.md)。

### Reserve 2026-10-05 请求优化

01:39:04 CST部署请求优化，PID223487，安装和运行SHA256同为`8a5604db1a0bd055a2822fdc3b93ab8b2e0aaf3db7dd9dc296ae363260a39978`。保留原manifest、RPC、unit及报价/模拟采样规则，响应正文仍不保留。普通head轮询只读latest，safe/finalized按原分钟复核及链冲突需要读取；失败范围不因授权/限流而缩片，成功后恢复512批量，授权拒绝每5分钟复测且不跳游标。仅Reserve主动重启一次，路由未改。

前两片实际完整补入1024块、2日志/1新回执，第三片512块仍遇来源archive授权限制，保留missing等待冷却；不能仅凭批量恢复就宣称当前已追平。原报价节奏持续落库、全20笔回执数据库摘要检查通过。后续运行状态及严格连续覆盖验收见[本轮记录](../research/2026-10-05-reserve-cadence/report.md)。

01:46:16现场：同一PID、NRestarts0；部署后6/6完整snapshot（78报价）、4联合模拟。01:45:10在约312秒后重试原512块范围仍为archive授权拒绝，没有缩片或跳游标。成功区间连续到26119712，相对最新完整报价26120573尚差861块、2小时52分48秒；来源限制尚未解除，不能把已补1024块说成全部补齐。

### 期权2026-10-04持续运行修复

23:59:51 CST原子替换主collector为SHA-256 `41059f71a06f0064dbb05db7ecbea9380aec350fd2871c3da9e6c8bb42265c0c`，PID201072。部署仅包含期权修复，沿用已安装unit，其他独立采集服务未重启。生命周期合批、丢失屏障主动确认、创建/open及epoch排序、目录状态证据刷新和退订ACK均通过新一轮独立复审。孤立真实公开候选3,090成员全部60秒回放通过；生产跨35分钟持续验证结果见[修复记录](arbitrage/strategies/arb-0009-options-sustained-repair.md)，不能把运行状态当作数据健康。

2026-10-05 00:39:51完成40分钟真实生产核验：37个新计划生效分钟均提交全部预期成员/分片，最新3,094成员、97分片、pending=0；真实新C/P自动开始采集。00:30状态引用实际更新，00:38全量185,640秒回放通过。窗口有效成员秒99.64%，8次连接退役和两次采样迟滞的24,151无效成员秒如实保存并随后恢复；无状态证据、retry容量或writer错误。最终CEX5流各60秒，DEX继续推进，其他独立服务PID不变；实网瞬断根因及下一次真实到期的未观察边界保留在修复记录。

### 2026-10-05 10:42服务器重启后核验

10:50–11:08 CST检查六个数据库/采集服务均active/running、enabled并持续落库，自动重启计数0。Reserve运行SHA仍为`8a5604db1a0bd055a2822fdc3b93ab8b2e0aaf3db7dd9dc296ae363260a39978`；11:07严格只读报告确认26113531..26123372共9842高度日志/所需收据连续无缺口，停机后自动补入1986高度，无需手工补采。启动后17轮状态/报价完整、每轮13报价，24收据完整明细摘要通过，响应正文仍零留存。

本次启动隔离457个全部文件0字节的空parts。452个有现存合并块覆盖，24个对应健康块逐块校验全部通过；另外5个probe尾块以全量capture承诺数量核对，Across/keeper已提交成员缺失均0。空块继续隔离，没有强挂或删除。停机前3094、重启后3104个期权及五路CEX完整分钟全部实际60秒回放通过；10:50有一秒采样迟滞如实标无效，11:01–11:04全量分钟已恢复60/60。未发现需人工重建的本次已提交数据缺失；不撤销已有九月坏块/历史缺口记录。

主DEX旧区块仍在自动补采，Across Arbitrum仍有既有历史状态来源限制；不能据六个服务running宣称全部历史完整。停机期间的实时盘口及报价不可事后还原。本轮没有主动重启、改来源/路由、另起writer；同期LST维护改变PID，11:06已恢复新市场批次。详细时间、查询、空块清单和验证边界见[本次核验](../research/2026-10-05-reserve-reboot/report.md)。

11:22 CST 增补检查与修复：keeper 虽无已提交成员数量缺失，扫描覆盖仍发现停机前的三个 30 秒断口。已正常停止原服务、取得原锁后用原 Collector 定点补采，三片无事件且分页穷尽，来源哈希、成员摘要及窗口连续性核验通过；11:21:16 已恢复 enabled 的常驻服务，实时游标和额度保留。同期 LST 人工补入1852块，新版恢复逻辑又自动补回8块持久游标领先数据库的断口，实际续采与成员验证通过。七个服务均 active/running、enabled；五路 CEX 最新分钟各60/60，期权97分片/3104成员，停机前后完整分钟均已实际回放。未改路由/来源或取消自启动。

本次 CEX 04:29–10:44 CST 共376个分钟缺失，无法精确事后补回；不要填入重启后的盘口冒充历史。共享 ClickHouse 的另一个项目表 `crypto_grid_trading.quality_events_raw` 仍有旧分片 `UNKNOWN_CODEC`，本轮只定位并记录，没有删改该项目数据。Across 的部分 Arbitrum 历史状态及旧 LST 缺口仍按原限制保留。操作、实际查询、补采程序和完整 race 回归见[重启核查与修复](../research/2026-10-05-reboot-recovery/report.md)。

### 2026-10-07：5档与 OKX 配对版本待部署

用户选择“期权10档、现货/永续/交割期货5档，补齐 OKX 可借贷 USDT 现货/永续配对”。代码与仓库中的 systemd 模板已加入 `MARKET_BOOK_DEPTH=5`、`OKX_PAIRED_ENABLED=true`、`OKX_PAIRED_MAX_PAIRS=256`、`OKX_PAIRED_REFRESH=30m`。本轮为编程和审核，**尚未覆盖安装二进制、安装 unit 或重启生产采集器**；线上实际深度仍以数据库 `stored_depth` 与运行二进制为准。新版本细节与验证见 [实现说明](okx-paired-five-level.md)。

部署时先备份实际二进制和 unit，停止原采集器后安装已验证版本，保持现有其他来源环境配置。新现货/期货从新分钟写5档；期权继续10档。核验新配对目录、相应两腿有效秒、资金费率及来源政策时间。回退时设 `MARKET_BOOK_DEPTH=10`、`OKX_PAIRED_ENABLED=false`，用支持5/10/50回放的新查询程序读取混合历史，不删除或重写已有历史。
