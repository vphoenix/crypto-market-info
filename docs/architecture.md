# 系统总体架构

本文是项目的全局入口，说明当前各类采集任务如何组合运行，以及以后增加其他公开数据时应放在哪一层。盘口编码、表字段和具体收益公式仍以各专项文档为准。

## 1. 目标与边界

项目持续采集可公开验证的市场、利率和链上规则数据，为套利研究、历史回放和人工风险审查提供输入。当前已经实现：

- Binance、OKX 现货及永续 L2 盘口；
- Binance、OKX 永续资金费率；
- JustLend TRX 收益产品；
- TRON 原生质押收益；
- SOL 的 bSOL、JitoSOL、mSOL、laineSOL、JupSOL、hSOL、配置白名单验证者和 Marinade Native 收益；
- Kamino Main SOL、Save Main SOL 的基础存款收益；
- AVAX 的 OKX 公开出借 APR、Aave V3/V4 WAVAX 基础存款历史 APY，以及 BENQI sAVAX、Ankr ankrAVAX、BENQI AVAX 基础借贷的链上观测（默认关闭，实际启用范围以运行说明为准）。

项目以后还可能增加其他 CEX、DEX、收益协议、链状态、桥和二层流通状态、借贷费率、指数或标记价格、手续费及 gas 等公开数据。当前六张表不是最终边界，但新数据不能为了省表而被硬塞进语义不相符的旧表。

除非用户明确扩大范围，项目不负责交易执行、私钥、签名、资金划转、自动下单或收益与做空头寸的自动组合。

## 2. 当前运行结构

当前只使用一个 `collector` 进程和一个 ClickHouse 数据库。宿主机原生服务的路径、Shell 守护方式和实际启用配置见[当前部署与运行说明](runtime-operations.md)；下面描述程序内部结构，不表示使用 Docker 部署：

```text
cmd/collector：配置、启动顺序、生命周期
│
├─ Binance / OKX 盘口
│  └─ 适配器 → 标准化 tick/lot → 本地 L2 → 每秒采样
│     → 分钟缓冲 → order_book writer
│
├─ Binance / OKX 资金费率
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
└─ AVAX 收益（每个来源独立、每小时 Runner → yield writer）
   ├─ OKX AVAX：公共出借历史，串行分页
   ├─ Aave V3/V4 WAVAX：分别校验市场身份与固定 LAST_WEEK 曲线
   └─ BENQI sAVAX / Ankr ankrAVAX / BENQI AVAX 借贷
      └─ 共用 C-chain RPC 与 gate；每条路线独立固定 finalized block hash
```

所有分支共用 ClickHouse Client、schema 初始化、HTTP 重试工具和结构化日志。

不使用 Redis、Kafka 或跨进程实时状态。盘口的当前 L2 只存在于内存；ClickHouse 保存已经结束的分钟、资金费率及低频收益快照。

## 3. 分层职责

- 数据源适配器只处理 URL、请求、响应结构、WebSocket 序列和来源错误，不做套利判断。
- 标准化与 collector 把来源数据转换成有明确身份、UTC 时间和定点数值的内部模型，并执行完整性校验。
- 盘口 sampler 负责固定秒边界和前 50 档编码；低频 Runner 负责采集间隔、单批重试和不重叠执行。
- ClickHouse writer 只负责 ID 登记、批量写入和确定性重试，不重新解释来源业务含义。
- 查询端使用 `FINAL` 或等价的 `argMax` 消除 `ReplacingMergeTree` 的逻辑重复；盘口查询还负责回放分钟差量。

当前主要代码位置：

```text
cmd/collector/                 入口与进程生命周期
internal/config/               环境变量配置
internal/exchange/             公共 HTTP、限频及交易所适配器
internal/orderbook/            本地 L2 状态
internal/sampler/              秒级采样和分钟缓冲
internal/funding/              资金费率调度与确认
internal/yield/                收益模型、Runner 和来源采集器
internal/storage/clickhouse/   六张表、登记、写入和查询
internal/replay/               盘口恢复
```

## 4. 启动与退出顺序

正常启动依次执行：

1. 加载并校验配置；
2. 连接 ClickHouse，创建数据库并初始化全部表；
3. 拉取启用交易所的 metadata，登记或复用 `instrument_id`；
4. 收益采集启用时加载 `yield_route` registry；
5. 装配盘口、sampler、资金费率 worker 和收益 Runner；
6. 并发运行各组件，直到收到退出信号或某个组件发生不能在内部恢复的错误；
7. 取消公共 context，等待组件退出并关闭 ClickHouse 连接。

环境变量无法解析等加载错误、ClickHouse 初始化失败或交易所 metadata 启动失败会阻止整个进程启动。当前收益 URL 只作为字符串加载，不在启动阶段探测或验证；URL 格式错误、无法连接、响应解析失败或写入失败都会表现为对应收益 Runner 的单轮失败并按间隔重试，不会停止盘口。盘口短暂断线由对应 runtime 失效并重建。任何组件意外返回不能在内部恢复的错误时，`app.Run` 会取消其余组件，因此长期运行仍应由操作系统或简单进程守护器负责重启。

## 5. 数据库关系

当前六张核心表分为三组：

```text
instrument 1 ── N order_book_minute 1 ── N order_book_second_delta
instrument 1 ── N funding_rate_hourly
yield_route 1 ── N yield_observation
```

盘口、资金费率和收益是独立事实，不在写入时合并。尤其不能把永续资金费率加入 `yield_observation.rate`；需要研究对冲后收益时，由查询或分析层按时间和资产身份组合。

## 6. 运行状态判断

“进程存在”不等于“数据正常”。最低检查应包括：

- ClickHouse 可连接且六张表存在；
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

先按[运行说明中的只读检查](runtime-operations.md#4-只读检查)连接 `crypto_market_info`，再执行下面的 SQL，检查六张表、各盘口的最近一分钟、资金费率及收益的最近批次。不要仅凭 `docker compose ps` 判断数据库状态。

盘口查询会列出 `instrument` 表中历史登记过的全部交易对；该表不保存启用状态，因此判断是否过期时，只检查当前 `BINANCE_SPOT_SYMBOLS`、`BINANCE_PERP_SYMBOLS`、`OKX_SPOT_SYMBOLS` 和 `OKX_PERP_SYMBOLS` 配置中启用的交易对，已经停采的历史行只供识别旧数据。

```sql
SELECT count() AS core_tables_found
FROM system.tables
WHERE database = currentDatabase()
  AND name IN (
    'instrument',
    'order_book_minute',
    'order_book_second_delta',
    'funding_rate_hourly',
    'yield_route',
    'yield_observation'
  );

SELECT
    i.instrument_id,
    i.exchange,
    i.market_type,
    i.exchange_symbol,
    b.latest_minute,
    b.valid_seconds
FROM
(
    SELECT instrument_id, exchange, market_type, exchange_symbol
    FROM instrument FINAL
) AS i
LEFT JOIN
(
    SELECT
        instrument_id,
        max(minute_time) AS latest_minute,
        bitCount(argMax(valid_bitmap, minute_time)) AS valid_seconds
    FROM order_book_minute FINAL
    GROUP BY instrument_id
) AS b USING (instrument_id)
ORDER BY i.instrument_id;

SELECT
    i.exchange,
    i.exchange_symbol,
    f.latest_hour,
    f.funding_time,
    f.is_actual
FROM
(
    SELECT instrument_id, exchange, exchange_symbol
    FROM instrument FINAL
) AS i
INNER JOIN
(
    SELECT
        instrument_id,
        max(hour_time) AS latest_hour,
        argMax(funding_time, hour_time) AS funding_time,
        argMax(is_actual, hour_time) AS is_actual
    FROM funding_rate_hourly FINAL
    GROUP BY instrument_id
) AS f USING (instrument_id)
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

正常情况下 `core_tables_found` 为 `6`；持续运行并恢复稳定后的完整盘口分钟 `valid_seconds` 应为 `60`；最近收益批次的 `route_count` 应分别为 JustLend `4`、TRON `127`，两个实时 collector 的 `missing_payload_hashes` 都应为 `0`。收益批次按同一轮统一的 `collected_at` 分组，而不是按 `observation_time` 分组，因为 JustLend V2 可以使用来源时间、其余路线使用采集时间；TRON 完整批次的 `distinct_observation_times` 仍应为 `1`。查询只判断数据是否持续形成，不替代对收益规则、协议安全或二层退出能力的人工审查。

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

新版还装配第二阶段三条路线，按[第二阶段原始历史查询](arbitrage/strategies/arb-0016-avax-yield-phase-2.md#7-历史能保存到什么程度)分别核对；成功写入时间超过 2 小时同样检查日志。两种 LST 的 `rate=NULL` 是预期，不能当作采集失败；三个 RPC Runner 的解析失败互不影响，但共享节点故障可能同时导致三条缺口。当前生产仍是第一阶段，不应在部署前把缺少第二阶段记录误判成故障。

`missing_payload_hashes` 应为 `0`。bSOL、laineSOL、JupSOL、hSOL 每轮各一条且有 `finalized` 锚点；Save 当前点有 `finalized_anchor`，历史点无锚点；JitoSOL、mSOL、Marinade Native、验证者和 Kamino 的 API 历史点无锚点是预期行为。历史窗口每轮重取，查询使用 `FINAL`，观测条数不应作为固定常量；日志中的 `routes` 字段实际计数为批次观测条数，不是去重后的产品数。

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
- [行情采集程序设计](implementation-design.md)：盘口和资金费率的具体实现。
- [市场数据存储数据字典](market-data-storage.md)：当前六张表及编码不变量。
- [ARB-0016 收益数据采集设计](arbitrage/strategies/arb-0016-yield-data.md)：通用收益模型和理论筛选。
- [ARB-0016 TRX 收益采集实现设计](arbitrage/strategies/arb-0016-trx-yield-implementation.md)：JustLend 与 TRON 采集细节。
- [ARB-0016 SOL 收益采集第一阶段实现设计](arbitrage/strategies/arb-0016-sol-yield-phase-1.md)：SOL 第一阶段五类 Runner、来源校验和历史写入细节。
- [ARB-0016 SOL 收益采集第二阶段实现设计](arbitrage/strategies/arb-0016-sol-yield-phase-2.md)：新增三条 LST 和两条借贷路线，沿用相同 Runner 与收益两表。
- [ARB-0016 AVAX 收益采集第一阶段实现设计](arbitrage/strategies/arb-0016-avax-yield-phase-1.md)：三个独立历史来源、严格利率单位、分页完整性及重试写入。
- [ARB-0016 AVAX 收益采集第二阶段实现设计](arbitrage/strategies/arb-0016-avax-yield-phase-2.md)：三个链上来源、同块锚点、整数换算和两列兼容迁移。
- [套利机会与策略资料](arbitrage/README.md)：数据为何采集，不参与采集进程运行。
