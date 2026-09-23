# ARB-0009：期权实时采集与查询

日期：2026-09-21。已按[R4设计](arb-0009-options-collection-design.md)实现公共实时采集，并复用原审核Agent完成[代码复审](../../../discuss/0008-options-live-code-review.md)。代码接入现有collector，程序默认关闭；本机常驻collector现已设置 `OPTIONS_ENABLED=true`、`OPTIONS_SYMBOLS=auto` 并开始保存历史。部署前的公开抓取测试使用自动清理的临时库，测试数据没有混入长期历史。

## 实际采集范围

`OPTIONS_SYMBOLS=auto` 在每次启动时选定清单，运行中不移动行权价中心：

- BTC、ETH币本位，以及BTC_USDC、ETH_USDC线性四族；
- 每族选一个剩余2–45天、同时有期权与交割期货的近期到期日；
- 以该期货的新鲜双边报价中价选择平值及两侧相邻实际行权价，C/P成对；
- 共24个期权、4个同到期期货，以及 `btc_usd`、`btc_usdc`、`eth_usd`、`eth_usdc` 四个指数。

也可用 `OPTIONS_SYMBOLS` 指定逗号分隔的原始symbol。清单必须包含每个行权价的C/P和对应同到期期货，最多32个合约；不支持combo。auto范围不完整时报错并重试，显式清单校验失败也不删腿凑数。合约到期后保留关闭/到期原因，需要重启任务以选择新到期日；当前没有自动换期调度。

盘口保存每秒买卖各最多10档，分钟第0秒为完整快照，随后按价格保存最终差量。静默合约保留其真实最后源时间；连接健康不能刷新行情时间。金额和指数使用精确Decimal，盘口使用整数tick/lot。合约单位、规则版本、来源时间、来源hash、连接状态和缺失原因一并保留。

元数据按所选option/future scope启动获取、每30分钟复核，盘口重连后立即复核；失败/遗漏一分钟后重查，期间受影响合约标未知。共同请求身份、解析数量、响应hash与观测窗口写入定类型表。市场状态依据是最近目录观测，不代表独立生命周期频道的实时确认。

## 配置与运行

先只查看启动会选哪些合约；此命令仅调用公共REST，不连接数据库或WebSocket：

```bash
OPTIONS_SYMBOLS=auto go run ./cmd/collector -print-options-plan
```

在现有collector的配置中增加以下两项，使用包含本次代码的构建启动：

```text
OPTIONS_ENABLED=true
OPTIONS_SYMBOLS=auto
```

沿用现有ClickHouse配置；启用任务时自动幂等建表，不需要独立期权服务或另建长期数据库。同一数据库只运行一个collector，避免并发分配instrument ID。现有宿主机服务的配置与构建路径见[运行说明](../../runtime-operations.md)。

默认公共地址为 `DERIBIT_REST_URL=https://www.deribit.com`、`DERIBIT_WS_URL=wss://www.deribit.com/ws/api/v2`，不需要API key。支持仅启用期权：将原盘口symbol配置全部设为 `-`，关闭不需要的资金费率/收益开关即可。

正常启动日志含 `options run started` 和成员数量；每个完整分钟写完后记录 `options minute committed`。从启动后的下一完整UTC分钟采样，因此第一条完整分钟提交需要等待约一至两分钟。连接问题自动退避重连；源错误不会取消其他交易所任务。

只读查看最新run的最新完整分钟，输出全部成员的有效秒数、缺失原因及指数：

```bash
go run ./cmd/options-check -database crypto_market_info -profile live
```

从上个命令取得实际run/instrument，再回放指定UTC秒；输出包含规格、该秒交易规则、10档盘口、原始行情时间与指数：

```bash
go run ./cmd/options-check -database crypto_market_info -profile live \
  -run '<实际run UUID>' -instrument '<实际instrument ID>' \
  -at '2026-09-21T00:00:12Z'
```

`-at`应替换为已经采集的时间；未采集或未提交的分钟返回找不到，不补造数据。实时输出profile为 `deribit_live_v1`；默认 `-profile offline` 继续用于原fixture/synthetic批次。检查命令不建表、不启动采集。

## 实现与存储

适配器位于 `internal/exchange/deribit`；选合约、单一有序入口、秒冻结和任务恢复位于 `internal/optionslive`。原规格、规则、分钟快照/差量/质量代码继续复用。仅增加run、元数据复核、指数分钟和实时提交四表，字段及NULL/缺失语义见[存储字典10.3](../../market-data-storage.md#103-实时新增四表)。

每个盘口必须完成订阅确认、新snapshot和连续前驱校验；重连需重新核验元数据。冻结完成晚于T+250ms的秒不标有效，旧连接消息不能覆盖新连接。分钟数据全部写完才发布提交，重试使用原内容和身份；缺预期行或元数据证据返回incomplete。指数断线只影响指数，不使健康盘口失效。

入口上限256条/16MiB、本地每侧最多20,000个价位；分钟写队列最多两个批次、最老积压45秒。这些是当前小清单的内存保护。超限明确结束本run再重试，不静默少抓档位；尚未完成的分钟形成缺口。正常停止限时写完已排队的完整分钟，末尾不完整分钟不提交。

## 已完成验证

全仓 `go test -race ./...`、`go vet ./...` 通过。模拟公共协议覆盖完整/部分ACK、心跳响应、严格数值解析和限频冷却；引擎覆盖断序重建、旧连接隔离、采样超时、跨分钟启动、晚到规则、闭市、缺首锚点以及元数据/指数失败隔离。真实ClickHouse验证提交前不可见、提交应答丢失后的同内容重试、冲突拒绝、缺指数/元数据检测，以及原50档和新10档混合历史回放。

实际公共REST/WS到本机临时ClickHouse的两次有界测试均通过。最后一次观测为 `2026-09-20T19:04:00Z`（北京时间9月21日03:04），28个盘口均有60个有效秒，四个指数均有数据；run为 `c5019724-96fa-4db2-8529-82a96d84dd1b`，分钟批次hash为 `a589d17a5a00966332294c9e94af58190606a01ee8b12d9e7981b667f6889586`。

该次实际成员为9月23日到期；BTC两族行权价80500/81000/81500，ETH两族2600/2620/2640，各有C/P和同到期期货。观察到活跃期货与静默期权的不同源时间，例如BTC_USDC-23SEP26-80500-C最后源时间仍为19:03:08.304Z，没有伪造成本分钟时间。这是一次短窗口连通性与数据正确性验证，不代表全天覆盖。

合并临时表后，一分钟完整28流加4指数的压缩数据统计如下，不含静态规格/规则、run与元数据复核行：

| 表 | 压缩字节 | 行数 |
| --- | ---: | ---: |
| 分钟盘口 | 4,850 | 28 |
| 秒级差量 | 13,653 | 422 |
| 分钟质量 | 23,929 | 28 |
| 指数分钟 | 3,615 | 4 |
| 实时提交 | 2,682 | 1 |
| 合计 | 48,729 | 483 |

按这一分钟直乘1440约70.17 MB/天，**仅为这一组合的粗略估算**；实际活跃度、元数据增长和长时间合并压缩都会改变结果，且不是完整磁盘占用。对同一已提交分钟执行100次完整28成员读取、证据校验与逐秒回放，P50=123.56ms、P95=144.78ms、P99=160.14ms；这不是大规模历史随机查询性能。既有活跃/静默单流整日合成测量见[离线记录](arb-0009-options-phase-1.md#整日合成容量与查询测量)。

复现有界公共抓取及短窗口测量：

```bash
GOCACHE=/tmp/crypto-options-go-cache \
CLICKHOUSE_INTEGRATION=1 OPTIONS_PUBLIC_INTEGRATION=1 \
go test ./internal/storage/clickhouse \
  -run '^TestOptionsLivePublicCollection$' -count=1 -timeout 7m -v
```

测试连接本机ClickHouse并自动创建/清理临时库，公共采集最多四分钟；不写现有历史库、不安装服务。长期数据积累由启用后的现有collector负责。IV/Greeks、全链BBO、combo和净套利判断均不在本次采集范围内。

## 当前启用记录

2026-09-21 03:26:21（Asia/Shanghai）更新现有collector二进制并重启，未改变原现货、永续、资金费率和收益配置。期权run `5ec3ced8-4d3a-44e9-bf1f-62386d0c505b` 于03:26:55启动，选择28个盘口和四个指数。

`2026-09-20T19:27:00Z` 是启动过渡分钟：四条较早完成元数据复核的流为60/60，其余流因第0秒尚未取得可用元数据锚点而整分钟不可回放；四个指数有58秒价格。该分钟按质量状态保留，没有补造。下一分钟 `2026-09-20T19:28:00Z` 已稳定：28个盘口全部60/60有效，四个指数也全部60/60有价格，批次hash为 `ace2270262f3a7bdec6524cd70bcaea13abf9b7d3a5064c102e43531c1a2a779`。

更新前二进制保存在 `/home/ubuntu/.local/share/crypto-market-info-collector/collector.rollback-20260921-options-predeploy`；当前安装二进制SHA-256为 `a1788010ee71e01733a92d6687461f08562bc9a162570efe496811318cd7bfd0`。
