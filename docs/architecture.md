# 系统总体架构

本文是项目的全局入口，说明当前各类采集任务如何组合运行，以及以后增加其他公开数据时应放在哪一层。盘口编码、表字段和具体收益公式仍以各专项文档为准。

## 1. 目标与边界

项目持续采集可公开验证的市场、利率和链上规则数据，为套利研究、历史回放和人工风险审查提供输入。当前已经实现：

- Binance、OKX 现货及永续 L2 盘口，以及 Bybit USDT 线性永续 L2 盘口；
- Binance、OKX 和 Bybit 永续资金费率；
- Deribit 期权、同到期期货及指数的公开实时观测；
- Ethereum AMM／Sky 固定路线状态、金额报价、规则、日志和收据；
- Reserve 拍卖／篮子申赎和 Across 稳定币中继的独立研究采集；
- JustLend TRX 收益产品；
- JustLend 能源租单清理 keeper 的公开事件、收据、只读模拟和资源／兑换成本（独立研究库与常驻命令）；
- TRON 原生质押收益；
- SOL 的 bSOL、JitoSOL、mSOL、laineSOL、JupSOL、hSOL、配置白名单验证者和 Marinade Native 收益；
- Kamino Main SOL、Save Main SOL 的基础存款收益；
- AVAX 的 OKX 公开出借 APR、Aave V3/V4 WAVAX 基础存款历史 APY，以及 BENQI sAVAX、Ankr ankrAVAX、BENQI AVAX 基础借贷的链上观测（默认关闭，实际启用范围以运行说明为准）；
- 独立的 Ethereum Lido stETH/wstETH 定额报价、赎回事件与 Binance ETHUSDT 对冲/实际资金费观测，运行在 `crypto-market-info-lst.service`。

项目以后还可能增加其他 CEX、DEX、收益协议、链状态、桥和二层流通状态、借贷费率、指数或标记价格、手续费及 gas 等公开数据。当前六张核心表及三张永续集合元数据表不是最终边界；新数据必须有与语义一致的定类型模型。

除非用户明确扩大范围，项目不负责交易执行、私钥、签名、资金划转、自动下单或收益与做空头寸的自动组合。

## 2. 当前运行结构

每个采集数据库只由对应的采集服务管理写入；扩容验收和专项研究使用独立库与 systemd unit。宿主机服务的路径及实际启用配置见[当前部署与运行说明](runtime-operations.md)；下面先描述主 `cmd/collector` 的内部结构，不表示使用 Docker 部署：

```text
cmd/collector：配置、启动顺序、生命周期
│
├─ 完整永续 catalog → 版本别名字典 → 至少两家共有集合 → 容量检查
│  └─ 场内 instrument 登记 → mapping → runtime 校验 → member/run 提交
│
├─ Binance / OKX / Bybit 盘口（永续多 topic 分片）
│  └─ 适配器 → 标准化 tick/lot → 本地 L2 → 每秒采样
│     → CompletedMinute 完成屏障 → 每 100 个 instrument 分块 writer
│
├─ Binance / OKX / Bybit 资金费率
│  └─ WebSocket 估算 + REST 实际确认
│     → scheduler / 串行 worker → funding writer
│
├─ JustLend 收益
│  └─ client → collector → 每小时 Runner → yield writer
│
├─ TRON 原生质押
│  └─ client → collector → 每 6 小时 Runner → yield writer
│
├─ SOL 收益（每条路线独立、每 6 小时 Runner → yield writer）
│  ├─ 通用 Stake Pool：bSOL、laineSOL、JupSOL、hSOL
│  ├─ 专用 API 与身份校验：JitoSOL、mSOL
│  ├─ 原生质押 API：Marinade Native、白名单验证者（可选）
│  └─ 借贷 API 与身份校验：Kamino Main SOL、Save Main SOL
│
├─ Deribit 期权／期货／指数
│  └─ 规格与规则校验 → 有序市场入口 → 每秒采样 → 分钟批次提交
│
├─ Ethereum DEX
│  └─ RPC → 同 hash 状态与报价 → 完整性校验 → DEX writer
│     └─ 增量日志、收据与最终性维护
│
└─ AVAX 收益（每个来源独立、每小时 Runner → yield writer）
   ├─ OKX AVAX：公共出借历史，串行分页
   ├─ Aave V3/V4 WAVAX：分别校验市场身份与固定 LAST_WEEK 曲线
   └─ BENQI sAVAX / Ankr ankrAVAX / BENQI AVAX 借贷
      └─ 共用 C-chain RPC 与 gate；每条路线独立固定 finalized block hash
```

主 collector 的分支复用 ClickHouse、公开 HTTP／RPC 和定点数工具；连接、启动失败和采集重试按具体分支隔离。实际恢复边界见[运行说明](runtime-operations.md)，不能由复用工具推断所有任务共用一个重试周期。

Reserve 与 Across 是并列的独立进程：

```text
cmd/reserve-data → 固定 r5／池白名单与同 hash 调用
  → 完整篮子、拍卖权限、金额报价、日志及收据
  → 原始证据 + 三张 reserve_* 表 + dex_log／dex_tx_receipt
  → crypto_market_info_reserve → 默认 finalized 的只读 JSON 报告

cmd/across-data → Base／Arbitrum RPC + 公开币安价格
  → 订单、更新、成交、退款、实际 live probe 与收据
  → 原始证据 + 七张 across_* 表
  → 实时库 crypto_market_info_across／历史库 crypto_market_info_across_history
  → 各自只读 CSV／JSON 报告
```

各库按单 writer 管理；Reserve 写命令沿用同一 evidence 目录锁，Across 实时和历史任务分库运行。它们独立于主 collector，但共享宿主机数据库、磁盘及部分上游额度。公开 HTTPS／RPC 接口提供链数据，当前没有本机 P2P 全节点。回补恢复链上事实，不能补造错过的实时 Quoter 报价或抢单可见时间。完整语义见 [Reserve](reserve-data-implementation.md) 和 [Across](across-stablecoin-data-implementation.md)。

LST 是并列的独立链路，不在主 collector 内启动：

```text
crypto-market-info-lst.service → cmd/lst-data watch
  → 共享持久限速 gate（dRPC / Binance，单来源串行）
  → 身份校验、协议状态、定额报价、增量事件、实际资金费 collector
  → 分钟市场循环与串行低频维护
  → 冻结成员及证据摘要 → pending 批次 → OpenLSTWriter
  → crypto_market_info_lst：instrument 与七张 lst_* 表

cmd/lst-data report → 只读本地库 → 最终性筛选及已接受批次校验 → CSV / JSON
```

此 CLI 的 `init-schema` 才执行专项 DDL，`watch` 打开已有库。市场、资金费、日志和最终性维护分别保存状态；单个来源任务失败不会伪造其他任务的数据，写库失败则留下原 pending 待恢复。所有联网/写入模式共享 `var/lst/state` 的排他锁和持久额度，`report` 不占采集锁、不联网。其 gate 不约束同出口的其他进程。

当前部署只运行实时 `watch`，从持久日志游标尝试补断线缺口；错过的历史报价保留缺失。dRPC 的按高度日志查询仍失败，因此事件、队列等待和基于完整日志日的 gas 样本尚不能靠运行时间自动补齐。详细频率、恢复和运维见 [LST 运行说明](lst-redemption-data-implementation.md)。

JustLend keeper 是与上述主 collector 并列的低频采集链路，由独立用户服务运行，写入 `crypto_market_info_justlend_keeper`，不复用收益表或盘口表：

```text
cmd/justlend-keeper-data
  → PublicNode / TronGrid / Binance 只读适配器 + 全部请求共用发送 gate
  → 严格解析、固化事件／收据核验、latest 模拟范围标注
  → 单进程调度 + 持久化游标／限速／固定样本
  → SHA-256 原始证据 + 冻结批次 + 五表 writer
  → report：只读数据库、核验成员及证据、导出 CSV/JSON
```

其初始化、停止、来源阻断与写入失败独立于主 collector。keeper 的源码、命令和限制见[实现说明](justlend-keeper-data-implementation.md)，部署路径见[运行说明](runtime-operations.md#justlend-keeper-独立研究采集2026-10-03)。

不使用 Redis、Kafka 或跨进程实时状态。盘口的当前 L2 只存在于内存；ClickHouse 保存已经结束的分钟、资金费率及低频收益快照。

## 3. 分层职责

- 数据源适配器只处理 URL、请求、响应结构、WebSocket 序列和来源错误，不做套利判断。
- 标准化与 collector 把来源数据转换成有明确身份、UTC 时间和定点数值的内部模型，并执行完整性校验。
- 盘口 sampler 负责固定秒边界和新数据前 10 档编码；分钟 `stored_depth` 区分新 10 档和旧 50 档，查询按对应深度回放；低频 Runner 负责采集间隔、单批重试和不重叠执行。
- ClickHouse writer 只负责 ID 登记、批量写入和确定性重试，不重新解释来源业务含义。
- 查询端使用 `FINAL` 或等价的 `argMax` 消除 `ReplacingMergeTree` 的逻辑重复；盘口查询还负责回放分钟差量。

当前主要代码位置：

```text
cmd/collector/                 入口与进程生命周期
cmd/reserve-data/              独立 Reserve 采集与报告入口
cmd/across-data/               独立 Across 实时／历史采集与报告入口
cmd/lst-data/                  独立 LST 采集与只读报告入口
internal/config/               环境变量配置
internal/universe/             通用共有集合、规范化身份和精确版本别名
internal/exchange/             公共 HTTP、限频及交易所适配器
internal/orderbook/            本地 L2 状态
internal/sampler/              秒级采样和分钟缓冲
internal/funding/              资金费率调度与确认
internal/yield/                收益模型、Runner 和来源采集器
internal/optionslive/          Deribit 实时任务与分钟提交
internal/dex/                  Ethereum 固定路线、RPC 与链上事实
internal/reserve/              Reserve 完整篮子、拍卖与报价
internal/across/               Across 订单、成交、退款与 probe
internal/lst/                  LST 类型、来源适配、持久 gate、collector、runner 和报告
internal/storage/clickhouse/   核心事实与集合元数据、登记、写入和查询
internal/replay/               盘口恢复
```

## 4. 启动与退出顺序

主 `cmd/collector` 正常启动依次执行：

1. 加载三态 symbol 配置及版本别名字典；完整读取所有已启用永续目录；
2. 统一选择至少两家共有的规范化交易对，计算分片、FD、缓存、采样和写入预算；任何不完整或超限都在数据库写入前失败；
3. 连接 ClickHouse、初始化表，读取现货目录，统一预校验并批量登记场内 instrument；
4. 为已存及新选中的 USDT 永续写本次 mapping revision；无网络副作用地构造并校验 BookManager、sampler 和 funding runtime，装配收益 Runner；
5. 写全部 universe member，最后写 run 行作为可见提交标记；
6. 并发运行各组件，直到退出信号或不可内部恢复的错误；
7. 取消公共 context，等待组件退出并关闭 ClickHouse。

`-print-perp-universe` 只执行目录、映射、选择和容量检查并输出 JSON，不访问 ClickHouse；`-print-perp-catalogs` 提供维护别名字典所需的精确源身份。运行中不热增删交易对，重启才重新发现。交易所 registry 和公共选择接口见[专项设计](perpetual-common-universe.md)。

环境变量无法解析等加载错误、ClickHouse 初始化失败或不可恢复的交易所 metadata 错误会阻止整个进程启动。Bybit metadata 遇到 HTTP `429`、响应体 `retCode=10006`，或响应体明确包含 `access too frequent` 的 HTTP `403` 时，会在当前进程内按共享 REST gate 等待后继续分页，避免外部 30 秒重启形成请求风暴；地区或权限封锁类 403 则立即失败。当前收益 URL 只作为字符串加载，不在启动阶段探测或验证；URL 格式错误、无法连接、响应解析失败或写入失败都会表现为对应收益 Runner 的单轮失败并按间隔重试，不会停止盘口。盘口短暂断线由对应 runtime 失效并重建。任何组件意外返回不能在内部恢复的错误时，`app.Run` 会取消其余组件，因此长期运行仍应由操作系统或简单进程守护器负责重启。

## 5. 数据库关系

当前六张核心表分为三组：

```text
instrument 1 ── N order_book_minute 1 ── N order_book_second_delta
instrument 1 ── N funding_rate_hourly
yield_route 1 ── N yield_observation
```

另有三个小型元数据表：`instrument_canonical_mapping` 保存按 revision 可回溯的语义映射；`perpetual_universe_run` 和 `perpetual_universe_member` 保存每次启动的精确采集集合。当前映射从当前进程 run 的 `mapping_revision` 取得，不能用最大的映射时间戳判断；A → B → A 字典回滚会明确回到 A。

盘口、资金费率和收益是独立事实，不在写入时合并。尤其不能把永续资金费率加入 `yield_observation.rate`；需要研究对冲后收益时，由查询或分析层按时间和资产身份组合。

期权、DEX 和研究采集器另有专项表，完整关系见[数据字典](market-data-storage.md)。有 capture 提交表的分支，先解析最新修订，再筛 canonical／committed／所需最终性，并校验成员计数和摘要。共享收据引用、重试身份、去重规则各有专项不变量；事实行本身不足以证明该批可用于分析。

LST 的七张专项表在独立研究库中，通过 `capture_id` 关联公共完整性元数据、协议状态、报价、三类赎回事件及实际资金费。金额为 UInt256，CEX 价格/数量为整数 tick/lot，资金费为 Decimal；同刻报价、后续同量报价、赎回兑付和费用只在报告/分析层组合。字段与不变量见[数据字典的 LST 段](market-data-storage.md#lst-折价官方赎回与对冲2026-10-02-已实现)。

## 6. 运行状态判断

“进程存在”不等于“数据正常”。主 collector 的最低检查应包括：

- ClickHouse 可连接且九张表存在；
- 每个启用 instrument 的最新 `minute_time` 持续前进；
- `valid_bitmap` 能显示有效秒，断线分钟允许少于 60，但恢复后的完整分钟应回到 60；
- 资金费率最新时间符合该合约结算周期，估算值和实际值没有混淆；
- JustLend 最近一次完整批次包含四条固定路线；
- TRON 原生质押最近一次成功批次在同一观测时间恰好包含 127 条；
- 启用 SOL 时，九条固定路线各自有最近成功采集；白名单验证者仅在配置非空时检查，不能要求它们与九条固定路线同批完成；
- 启用 AVAX 时，第一阶段三条历史路线分别每小时成功采集，最新来源时间不得旧于 6 小时；没有历史区块锚点的官方 API 行保持区块字段为空。第二阶段三条同块路线的区块时间不得旧于采集时间 10 分钟，且必须含 finalized 区块高度/哈希；
- 实时收益观测都有 payload hash；直接链上行有区块锚点，API 历史行不能伪造锚点；
- 日志中没有持续重复的启动失败、写入失败或无法恢复的序列断档。

短暂缺口必须如实保留，不能用上一批数据填成当前有效值。

先按[运行说明中的只读检查](runtime-operations.md#4-只读检查)连接相应采集库，再执行下面的 SQL，检查九张表、各盘口的最近一分钟、资金费率及收益的最近批次。生产库尚未升级到自动 universe 版本时仍为六张核心表，使用运行说明中 BTC 显式模式的检查，不能把尚不存在的新 metadata 表判断为采集失败。不要仅凭 `docker compose ps` 判断数据库状态。

`instrument` 保留历史版本。永续 expected instruments 必须使用当前进程 `perpetual_universe_started` 日志中的 `run_id` 关联 member；不能使用全部 instrument 的最新 ID 或所有 mapping 行推测当前采集集合。下面 SQL 的 `<run-id>` 必须替换为该值。现货仍按当前显式 spot 配置检查。每 60 秒 `perpetual_runtime_health` 同时报告每家 ready/invalid、重连、队列峰值和采样/写入 p99。

```sql
SELECT count() AS core_tables_found
FROM system.tables
WHERE database = currentDatabase()
  AND name IN ('instrument', 'order_book_minute', 'order_book_second_delta',
    'funding_rate_hourly', 'yield_route', 'yield_observation',
    'instrument_canonical_mapping', 'perpetual_universe_run', 'perpetual_universe_member');

SELECT i.instrument_id, i.exchange, i.exchange_symbol, i.venue_contract_version,
       m.canonical_market_key, b.latest_minute, b.valid_seconds
FROM perpetual_universe_member AS m FINAL
INNER JOIN perpetual_universe_run AS r FINAL USING (run_id)
INNER JOIN instrument AS i FINAL USING (instrument_id)
LEFT JOIN
(
    SELECT instrument_id, max(minute_time) AS latest_minute,
           bitCount(argMax(valid_bitmap, minute_time)) AS valid_seconds
    FROM order_book_minute FINAL
    GROUP BY instrument_id
) AS b USING (instrument_id)
WHERE m.run_id = toUUID('<run-id>')
ORDER BY i.exchange, i.exchange_symbol;

SELECT i.exchange, i.exchange_symbol, f.latest_hour, f.funding_time, f.is_actual
FROM perpetual_universe_member AS m FINAL
INNER JOIN perpetual_universe_run AS r FINAL USING (run_id)
INNER JOIN instrument AS i FINAL USING (instrument_id)
LEFT JOIN
(
    SELECT instrument_id, max(hour_time) AS latest_hour,
           argMax(funding_time, hour_time) AS funding_time,
           argMax(is_actual, hour_time) AS is_actual
    FROM funding_rate_hourly FINAL
    GROUP BY instrument_id
) AS f USING (instrument_id)
WHERE m.run_id = toUUID('<run-id>')
ORDER BY i.exchange, i.exchange_symbol;

SELECT
    r.provider,
    o.collected_at AS batch_collected_at,
    uniqExact(r.product_code) AS route_count,
    count() AS observation_rows,
    uniqExact(o.observation_time) AS distinct_observation_times,
    countIf(isNull(o.source_payload_hash)) AS missing_payload_hashes
FROM
(
    SELECT yield_route_id, observation_time, collected_at, source_payload_hash
    FROM yield_observation FINAL
) AS o
INNER JOIN
(
    SELECT yield_route_id, provider, product_code
    FROM yield_route FINAL
    WHERE provider IN ('JustLend', 'TRON')
) AS r USING (yield_route_id)
GROUP BY r.provider, o.collected_at
ORDER BY r.provider, o.collected_at DESC
LIMIT 1 BY r.provider;
```

正常情况下 `core_tables_found` 为 `9`；持续运行并恢复稳定后的完整盘口分钟 `valid_seconds` 应为 `60`。收益批次的 `route_count` 应分别为 JustLend `4`、TRON `127`，缺失 payload hash 数为 `0`；按每轮统一的 `collected_at` 分组，TRON 的 `distinct_observation_times` 仍应为 `1`。查询只判断数据是否持续形成，不替代对收益规则、协议安全或退出能力的人工审查。

SOL 按路线查看最近成功批次，而不是要求一个统一批次或固定历史条数：

```sql
SELECT
    r.provider,
    r.product_code,
    o.collected_at AS batch_collected_at,
    max(o.observation_time) AS latest_observation_time,
    count() AS observation_rows,
    countIf(isNull(o.source_payload_hash)) AS missing_payload_hashes,
    countIf(isNotNull(o.block_height) AND isNotNull(o.block_hash)
        AND isNotNull(o.finality)) AS anchored_rows
FROM yield_observation AS o FINAL
INNER JOIN
(
    SELECT yield_route_id, provider, product_code
    FROM yield_route FINAL
    WHERE network = 'solana-mainnet'
) AS r USING (yield_route_id)
GROUP BY r.provider, r.product_code, o.collected_at
ORDER BY r.provider, r.product_code, o.collected_at DESC
LIMIT 1 BY r.provider, r.product_code;
```

与运行说明中的九条固定路线逐一核对；从未成功写入的路线不会出现在查询结果中，缺行也算异常。JustLend 正常每小时采一次，TRON 和 SOL 每 6 小时采一次；超过对应间隔并持续重试失败时检查日志。判断历史 API 是否仍在被采集要看 `batch_collected_at`，不能只看可能按天或 epoch 更新的 `latest_observation_time`；后者的新鲜度由各 collector 按自己的来源规则校验。

AVAX 默认不启用；启用后按[第一阶段验收查询](arbitrage/strategies/arb-0016-avax-yield-phase-1.md#9-测试与完成标准)核对 OKX、Aave V3、Aave V4 三行。它们各自重抓近期窗口、不插值补缺、不回填当前费用或状态；成功写入时间超过 2 小时应检查日志。来源失败只影响自己的 Runner，写入失败仍重试原批次。

第二阶段三条路线已在当前生产启用，按[第二阶段原始历史查询](arbitrage/strategies/arb-0016-avax-yield-phase-2.md#7-历史能保存到什么程度)分别核对；成功写入时间超过 2 小时同样检查日志。两种 LST 的 `rate=NULL` 是预期，不能当作采集失败；三个 RPC Runner 的解析失败互不影响，但共享节点故障可能同时导致三条缺口。独立永续验收服务没有启用收益，不对验收库要求这些行。

`missing_payload_hashes` 应为 `0`。bSOL、laineSOL、JupSOL、hSOL 每轮各一条且有 `finalized` 锚点；Save 当前点有 `finalized_anchor`，历史点无锚点；JitoSOL、mSOL、Marinade Native、验证者和 Kamino 的 API 历史点无锚点是预期行为。历史窗口每轮重取，查询使用 `FINAL`，观测条数不应作为固定常量；日志中的 `routes` 字段实际计数为批次观测条数，不是去重后的产品数。

LST 独立服务应另查其 unit、最近 market 时间、协议与报价成员状态，以及 complete/canonical/committed 的增量日志覆盖。[健康 SQL](lst-data-health.sql) 只读研究库；head 与 partial 可以用于运行状态检查，但不意味着已满足历史报告的最终性条件或已获利。来源拒绝时事件数为零也不意味着链上没有事件，详见[LST 异常解释](lst-redemption-data-implementation.md#健康检查与异常解释)。

## 7. 增加其他数据时

新增数据先判断其语义，而不是先决定复用哪张表：

1. 与现有模型完全一致，例如新的单资产收益来源，可以新增适配器并复用 `yield_route`、`yield_observation`；
2. 身份、时间、完整性和查询方式一致，但来源协议不同，可以复用标准模型和 writer；
3. 数据含义明显不同，例如 AMM 池状态、桥储备、二层退出状态、借贷利用率或链上 gas，应建立自己的定类型 model、校验规则和表；
4. 链上数据必须保存可复现的区块位置和最终性；实时自动采集的外部 API 数据必须保存来源时间或明确使用采集时间，并填写可核验的 payload hash。物理字段仅为兼容历史或人工导入而允许为空；
5. 新类型先完成一个真实来源，不为尚未实现的第二个来源预建插件系统、消息队列或通用 JSON 大表。

所有时间统一为 UTC，价格和数量继续使用整数 tick/lot，利率、金额和比例使用十进制定点数。新增表或标准化语义时同步更新数据字典和对应专项文档。

## 8. 文档分工

- [当前部署与运行说明](runtime-operations.md)：宿主机服务、路径、启用配置、连接和维护方式。
- [Reserve 实现与查询](reserve-data-implementation.md)：五表、同块完整篮子、采样、回补恢复和报告口径。
- [Across 实现与查询](across-stablecoin-data-implementation.md)：独立实时／历史库、probe 可见性、退款核验和费用空间。
- [行情采集程序设计](implementation-design.md)：盘口和资金费率的具体实现。
- [Bybit USDT 线性永续采集设计](bybit-usdt-perpetual-market-data.md)：Bybit 产品身份、盘口序列、资金费率和限流细节。
- [市场数据存储数据字典](market-data-storage.md)：核心表、集合元数据及专项研究表的字段和不变量。
- [LST 采集运行说明](lst-redemption-data-implementation.md)：独立服务、七表、来源限制、断线恢复与只读健康检查。
- [ARB-0016 收益数据采集设计](arbitrage/strategies/arb-0016-yield-data.md)：通用收益模型和理论筛选。
- [ARB-0016 TRX 收益采集实现设计](arbitrage/strategies/arb-0016-trx-yield-implementation.md)：JustLend 与 TRON 采集细节。
- [ARB-0016 SOL 收益采集第一阶段实现设计](arbitrage/strategies/arb-0016-sol-yield-phase-1.md)：SOL 第一阶段五类 Runner、来源校验和历史写入细节。
- [ARB-0016 SOL 收益采集第二阶段实现设计](arbitrage/strategies/arb-0016-sol-yield-phase-2.md)：新增三条 LST 和两条借贷路线，沿用相同 Runner 与收益两表。
- [ARB-0016 AVAX 收益采集第一阶段实现设计](arbitrage/strategies/arb-0016-avax-yield-phase-1.md)：三个独立历史来源、严格利率单位、分页完整性及重试写入。
- [ARB-0016 AVAX 收益采集第二阶段实现设计](arbitrage/strategies/arb-0016-avax-yield-phase-2.md)：三个链上来源、同块锚点、整数换算和两列兼容迁移。
- [套利机会与策略资料](arbitrage/README.md)：数据为何采集，不参与采集进程运行。
- [JustLend keeper 实现与运行说明](justlend-keeper-data-implementation.md)：独立五表采集器、发送预算、状态恢复、健康查询及报告口径；[目标设计](justlend-keeper-data-mvp-design.md)另列已实现与待验收部分。
