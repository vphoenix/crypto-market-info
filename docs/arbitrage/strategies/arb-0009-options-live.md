# ARB-0009：期权实时采集与查询

更新：2026-10-04。代码已按[R5设计](arb-0009-options-lifecycle-design.md)修复固定清单到期后不抓新合约的问题，并由同一独立Agent复审。R5已于2026-10-04 15:43（Asia/Shanghai）部署到常驻collector；生产全量3108成员连续两个完整分钟均60/60有效，全部秒回放通过。实际版本、证据和旧历史坏分片限制见[部署记录](arb-0009-options-lifecycle-deployment.md)，代码验证见[实施记录](arb-0009-options-lifecycle-implementation.md)。2026-09-21部署的R4历史记录保留在本文末尾。

## 当前代码的采集范围

`OPTIONS_SYMBOLS=auto` 持续采集BTC、ETH币本位和BTC_USDC、ETH_USDC线性四族的全部未到期期权及交割期货。所有到期日和行权价均可准入，单个C或P独立采集，不要求C/P、同到期期货齐全，也不使用平值或2–45天分析筛选。

生命周期WS先完成creation/state/platform订阅确认，再从六个完整目录建立基线，每30分钟及重连后核对。新合约的身份、规则和状态证据落盘后立即预订阅，按不可变UTC分钟计划保存历史；到期时立即失效并取消订阅，Supervisor继续发现新合约。永续、其他资产和combo有明确排除记录。四个指数由一个共享连接采集。

显式 `OPTIONS_SYMBOLS` 继续使用最多32个成员的固定C/P及同到期期货清单。自动范围中的单合约失败只影响该symbol，完整目录失败影响对应scope。规则金额使用Decimal，盘口价量使用整数tick/lot。每秒保存买卖各最多10档，第0秒快照，其后按价格保存最终差量；静默书的最后源时间不因心跳而刷新。

目录、状态和public/status原文先按hash归档，再写定类型证据。负向状态先失效，正向恢复等待证据并重新取得源snapshot。旧响应、旧epoch和乱序状态不能放开新的失效屏障。目录失败一分钟后重查，单合约失败按5/15/60秒重试。

## 配置与查询

查看当前支持范围，仅调用公共REST，不连接数据库或WebSocket：

```bash
OPTIONS_SYMBOLS=auto go run ./cmd/collector -print-options-plan
```

常驻服务已启用的自动模式配置：

```text
OPTIONS_ENABLED=true
OPTIONS_SYMBOLS=auto
OPTIONS_EVIDENCE_DIR=/home/ubuntu/.local/share/crypto-market-info-options/evidence
```

沿用现有ClickHouse配置，启动幂等初始化新增五表。期权使用独立连接池；同一采集库只允许一个writer，本机按数据库身份持有进程锁直到旧分钟排空。默认4096个书、20条物理WS（含生命周期和指数）、每条256个书频道、逻辑run最多32个成员、共享64MiB入口及200万个完整L2价位。全部配置见[README](../../../README.md)；本次整机公开源和全量真实DB短窗口验收已通过，结果见部署记录。

默认公共地址 `DERIBIT_REST_URL=https://www.deribit.com`、`DERIBIT_WS_URL=wss://www.deribit.com/ws/api/v2`，无需API key。自动模式日志包含 `options catalog plan published` 及books/pending数量。合约先预热，计划默认在当前UTC分钟后第二个边界生效，给落盘和订阅留出1–2分钟；首条完整分钟还要等待该分钟结束及writer提交。来源、分片和指数独立恢复。

只读检查上一个完整UTC分钟的全部计划，返回预期、有效、缺失合约和未提交分片：

```bash
go run ./cmd/options-check -database crypto_market_info -profile live
```

按计划回放指定合约的已采集UTC秒：

```bash
go run ./cmd/options-check -database crypto_market_info -profile live \
  -instrument '<实际instrument ID>' -at '<实际UTC时间>'
```

加 `-run '<实际run UUID>'` 可读指定R4/R5历史；没有R5表的库保留R4最新run读取。缺失不补造。默认 `-profile offline` 继续用于原fixture/synthetic批次。查询命令不建表、不启动采集。

## 实现与存储

Deribit适配器位于 `internal/exchange/deribit`；Supervisor、连接共享、逻辑分片和分钟采样位于 `internal/optionslive/catalog_*`。新 `catalog_v2` 使用独立计划、60槽证据和提交摘要域；旧run结构、hash与10/50档事实回放保持兼容。新增五表见[存储字典10.4](../../market-data-storage.md#104-期权自动发现模型r5)。

每书必须取得订阅确认、snapshot并校验连续性，过期或超时质量不可回放。T边界新run立即接管采样；旧writer仅后台排空T之前的分钟。分钟写入验证owner、完整成员、源状态和规则证据，commit最后发布。

分片入口256条/16MiB，分钟队列最多两个批次，最老积压45秒；全任务共享字节和价位预算。生命周期控制证据与慢REST目录使用独立有界队列；status请求单飞，订阅每条连接仅有一个待ACK批次，订阅限速等待不阻塞心跳。完整分钟验证及提交限三路并发，控制数据另有额度和锁，超限不静默少抓。

## 2026-09-21 R4验证与部署历史

以下为当时28合约固定清单的验证，不能代表R5全量容量。R5最新验证见[实施记录](arb-0009-options-lifecycle-implementation.md)。


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
