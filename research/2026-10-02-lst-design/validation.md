# LST 采集设计验证记录

2026-10-02。验证范围是设计、DDL及公开来源结构；采集器尚未实现，也未应用正式数据库迁移。

## 数据库语法

使用本机官方 ClickHouse local 26.8.1.1825，在独立 `/tmp/lst-design-ddl-validation-r2-20261002` 目录执行修订版完整 DDL，退出码0，实际建立七表：

```text
lst_capture
lst_funding_settlement
lst_protocol_state
lst_quote_observation
lst_withdrawal_claim
lst_withdrawal_finalization
lst_withdrawal_request
```

未连接生产 ClickHouse。失败来源时间及未知区块锚点的 Nullable 修订之后重新验证；DDL不是只由文字检查或模拟解析通过。

可复验：将 [DDL](../../docs/lst-redemption-data-schema.sql) 复制至一个临时 SQL 文件，末尾添加 `SELECT name FROM system.tables WHERE database=currentDatabase() ORDER BY name FORMAT TSV;`，再用全新临时路径运行：

```text
/home/ubuntu/.local/share/crypto-market-info-clickhouse-bin/clickhouse local \
  --path /tmp/lst-design-ddl-fresh \
  --multiquery --queries-file /tmp/lst-design-ddl.sql
```

该验证证明引擎接受字段和表结构，不替代实现后的整数/NULL读回、冻结批次与成员摘要、重组、来源失败和队列区间测试。

## 官方公开来源

[source-manifest.json](source-manifest.json) 保存八份原始响应的来源、UTC请求/收到时间和SHA-256；已独立逐份复核哈希。

- Lido v4.0.1 的 WithdrawalQueue、WithdrawalQueueBase、WithdrawalQueueERC721 源码已归档。核对到三类事件字段、finalized `(from,to]`，以及wstETH先unwrap成stETH再取shares的实现。源码版本是研究依据，不能替代当前代理实现核验。
- Uniswap官方QuoterV2已固定commit `0682387198a24c7cd63566a2c58398533860a5d1`。初次尝试v1.4.4标签文件返回404，manifest保留失败地址和原因；修正后的源码哈希为 `7c0a974a98e224b1ec63158bad6c13c73a659dec669dead1fd99a0b0d85418a6`。
- Binance公开ETHUSDT depth返回100档、E、T、lastUpdateId。mark接口返回markPrice/indexPrice/time/nextFundingTime，资金费最近5笔全部含对应markPrice。
- 本轮exchangeInfo显示ETHUSDT为PERPETUAL、TRADING、USDT保证金，交易tickSize=0.01、qty step=0.001、MIN_NOTIONAL=20。本轮indexPrice=`2758.46093023`，确实不能按0.01交易tick舍入存储。

该检查没有回填30日历史、运行市场循环、取得Curve/Uniswap实额报价或确认当前链上池/代理身份。正式程序启动仍需设计中的身份及RPC能力gate；上述样本只证明公开字段可取得及精度要求。

复验脚本：[verify_public_sources.py](verify_public_sources.py)。脚本是设计验证辅助，串行读取公开源码和市场响应，不是常驻采集程序。原始价格仅属采集时点，不用来证明当前盈利。

## 独立审核

独立Agent审核正式正文、七表字段、Lido规则及Binance官方结构。初审和复审见 [0014](../../discuss/0014-lst-redemption-data-design-review.md)；设计按审核补齐同量后续报价、mark精度、边界/删失、原生ETH路径、费用情景、新鲜度及未知时间/锚点NULL语义。审核结论以该文件的最终状态为准。

## 启动及限速专项修订

2026-10-02，按用户对启动突发请求的担忧复查。原稿只有并发／batch／单轮片数上限，缺少实际发出速率；显式历史回补、header/receipt和重启补查仍可能密集请求。设计第8节已增加启动缓速、所有任务共用发出gate、禁止多成员RPC batch、二分/重试计入额度、持久冷却、跨mode排他锁、逾期任务不补跑及共用出口检查。采集器仍未实现，不能将这些设计规则写成已经验证的运行行为；七表DDL及业务精度不变。

Binance首版改为depth `limit=10`，因为分析只使用前10档。此前归档样本确实是100档，本次未重抓或改写原始证据／source-manifest；历史记录继续保留真实的100档来源。

一次性`verify_public_sources.py`同步使用10档、每host请求至少间隔5秒，遇HTTP403/418/429记录失败后终止整个脚本。离线用临时目录、假时钟、mock URL响应和已归档结构验证，实际运行结果：

```text
PASS: eight-source run, per-host >=5s, ten-level depth
PASS: HTTP 403 archived failure then stopped, no next request/retry
PASS: HTTP 418 archived failure then stopped, no next request/retry
PASS: HTTP 429 archived failure then stopped, no next request/retry
PASS: existing evidence unchanged; all network calls mocked
```

测试仅验证辅助脚本的解析、节拍和拒绝后停发，没有向RPC/CEX公共数据端点发请求，没有启动研究采集或改动在运行的服务。正式collector还必须按第8节用本地模拟服务覆盖实际发出时刻、滚动额度、持久冷却／重启、市场截止时间和日志积压，再做低速真实验收。
