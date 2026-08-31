# ARB-0016 AVAX 收益采集第一阶段实现设计

状态：实现及双方代码评审通过；实际部署状态见[运行说明](../../runtime-operations.md)，`AVAX_YIELD_ENABLED` 默认 `false`。设计接口核验日期：2026-08-26；实现验收日期：2026-08-27（北京时间）。

这里的“第一阶段”指上一轮安排的 **OKX、Aave V3、Aave V4**，不包括 AVAX 原生委托、BENQI 或 Ankr。

## 1. 做什么，以及不做什么

本期实现三条固定收益路线的公开利率采集和 ClickHouse 写入：

| 路线 | 采集内容 | 来源 |
|---|---|---|
| OKX AVAX 活期 | 公开出借 APR，不是借款 APR、个人到账利息或净收益 | OKX Savings 公共历史接口 |
| Aave V3 Avalanche WAVAX | 基础存款历史 APY | Aave V3 官方 GraphQL |
| Aave V4 Avalanche Main WAVAX | 不含奖励的基础存款历史 APY | Aave V4 官方 GraphQL |

**最小方案：每小时重新取得同一近期历史窗口，持续积累来源历史曲线。**第一次运行就补入这个窗口；重启或短暂停机后仍走同一流程。不另外建立“实时快照”和“历史回补”两套任务。

本期不采当前额度、流动性或产品状态快照；历史接口没有给出的费用、额度和退出状态留空或记为未知。Aave 的 `avgRate` 是来源统计的平均年化指标，不是该时刻的即时利率，也不是已经实现的持有收益。

不包含：永续资金费率、做空深度、交易执行、账户/API key、个人收益测算、额外激励、sAVAX 组合、其他市场自动发现、全生命周期历史回补、归档节点、原始 JSON 热表或风险审查表。

## 2. 程序结构：复用现有链路

```text
3 个定类型 collector，各自一个现有 yield.Runner
  → Batch.NormalizeAndValidateForLiveCollection
  → 现有 ClickHouse.WriteYieldBatch
  → yield_route + yield_observation
```

- 仍只有一个 `collector` 进程、一个 ClickHouse；不加服务、队列、插件框架或持久化游标。
- 每条路线只有一个 Runner 写入；启动立即采集，正常间隔 `1h`，失败间隔 `10m`，同一路线不重叠执行。
- OKX、Aave V3、Aave V4 互相独立。某个来源出错，只让自己的本轮失败，不影响另外两条或盘口采集。
- HTTP 继续使用 `exchange.Get` / `exchange.PostJSON` 和现有重试工具；客户端超时 `20s`。每个 collector 使用自己的重试配置和限频 gate。
- GraphQL 使用固定查询字符串和 Go 响应结构，不引入 SDK、代码生成器或通用 GraphQL 框架。

不修改表结构、现有观测唯一键或 Runner 行为。模型与写入约束以[通用收益设计](arb-0016-yield-data.md)和[数据字典](../../market-data-storage.md)为准。

## 3. 三条路线的固定身份

链上地址用于比较、生成资产键和落库时统一为小写；GraphQL 请求中的校验和大小写可以保留。不能只根据 `WAVAX` 符号识别资产。

固定地址：

```text
chain_id = 43114
WAVAX   = 0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7
V3 pool = 0x794a61358d6845594f94dc1db02a252b5b4814ad
V3 aToken = 0x6d80113e533a2c0fe82eabd35f1875dcea89ea97
V4 Main spoke = 0x435272ceff93a1e657e8abfdf0a13e95900a3a56
V4 onChainId = "0"
```

下表中的缩写只用于说明；实现时展开为完整字符串：

```text
W = eip155:43114:erc20:<WAVAX>
A = eip155:43114:erc20:<V3 aToken>
S = eip155:43114:protocol-position:aave-v4:<V4 Main spoke>:0
C = cex:okx:AVAX
```

| 字段 | OKX | Aave V3 | Aave V4 |
|---|---|---|---|
| `provider_type` | `cex` | `protocol` | `protocol` |
| `provider` | `OKX` | `Aave` | `Aave` |
| `product_code` | `simple-earn-flexible-avax` | `avalanche-v3-wavax-supply` | `avalanche-v4-main-wavax-supply` |
| `product_name` | OKX AVAX 活期公开历史 APR | Aave V3 WAVAX 历史基础 APY | Aave V4 Main WAVAX 历史基础 APY |
| `yield_type` | `cex_earn` | `lending` | `lending` |
| `deposit_asset_key` / `redeem_asset_key` | `C` | `W` | `W` |
| `position_asset_key` | `cex:okx:earn-flexible:AVAX` | `A` | `S` |
| `network` | `NULL` | `avalanche-c-mainnet` | `avalanche-c-mainnet` |
| `contract_address` | `NULL` | V3 pool | V4 Main spoke |
| `price_exposure_asset` | `AVAX` | `AVAX` | `AVAX` |
| `income_source` | `borrow_interest` | `borrow_interest` | `borrow_interest` |
| `collection_enabled` | `true` | `true` | `true` |

`source_url` 分别为 OKX 历史接口（固定 `ccy=AVAX`，不带翻页游标）、V3 GraphQL、V4 GraphQL，下节给出完整地址。

WAVAX 是 Avalanche C-chain 原生 AVAX 的包装资产，本期不跨桥、不去其他 L1/L2。OKX 资产键只表示交易所账内权益，不假装对应某个链上合约。V4 的 `S` 表示内部 supply share，不是杜撰的 ERC20，更不能默认每份等于一个 WAVAX。

## 4. 来源请求与字段映射

### 4.1 OKX AVAX

```text
GET https://www.okx.com/api/v5/finance/savings/lending-rate-history
    ?ccy=AVAX&limit=100[&after=<上一页最小 ts>]
```

无需认证。[官方接口](https://www.okx.com/docs-v5/en/#financial-product-savings-get-public-borrow-history-public)规定 `after` 向更早的数据翻页，`ts` 为 Unix 毫秒。

本轮实测记录示例：

```json
{"ccy":"AVAX","lendingRate":"0.0206","rate":"0.127","ts":"1787756400000","amt":""}
```

映射：

- `observation_time = ts`，转为 UTC，保留毫秒。
- `rate = Decimal(lendingRate)`；示例 `0.0206` 表示 2.06%，不能再除以 100。
- `rate_kind=apr`、`rate_origin=reported`、`rate_mode=variable`。
- **禁止取 `rate` 字段作为收益。**它是借款 APR；`lending-rate-summary` 中的 `avgRate/preRate/estRate` 也不是本期收益来源。
- `amt` 已弃用，不能用作 TVL、额度或合资格余额。

顶层 `code` 必须是 `"0"`，必须有数组 `data`；保留的行必须 `ccy=AVAX`、`ts` 为合法正整数、`lendingRate` 为非负十进制字符串。缺失、空字符串、`null` 均不是 0。

`lendingRate` 于 [2026-02-27 新增](https://www.okx.com/docs-v5/log_en/)，不能用更老记录里的借款 `rate` 补它。

OKX [2026-08-25 公告](https://www.okx.com/help/okx-earn-simple-earn-flexible-updates)说明活期收益已按合资格申购池分摊，并计划自 8 月 27 日取消最低出借 APR 设置。因此不沿用“只有逐笔撮合成功才付息”的旧假设；本期只保存公开出借 APR，不计算个人余额、池分摊或到账净收益。公告中的当前服务费不能直接填进过去的历史行，也不能未经确认再从 API 利率扣一遍。

### 4.2 Aave V3

`POST https://api.v3.aave.com/graphql`，一个请求同时取得身份和历史：

```graphql
query {
  market(request: {
    address: "0x794a61358D6845594F94dc1DB02A252b5b4814aD", chainId: 43114
  }) {
    address
    chain { chainId }
    reserves {
      underlyingToken { address decimals }
      aToken { address decimals }
    }
  }
  supplyAPYHistory(request: {
    chainId: 43114,
    underlyingToken: "0xB31f66AA3C1e785363F0875A1B74E27b85FD66c7",
    market: "0x794a61358D6845594F94dc1DB02A252b5b4814aD",
    window: LAST_WEEK
  }) {
    date
    avgRate { raw decimals }
  }
}
```

必须校验：market 地址和 chain ID 正确；在 `reserves` 中恰有一个目标 WAVAX，其 underlying/aToken 地址与第 3 节一致，两个 token 的 decimals 都为 18。其他资产不采集。

`observation_time=date`。`avgRate.decimals` 本轮实测为 27，必须明确校验；`raw` 是非负整数字符串。转换规则：

```text
rate = Decimal(avgRate.raw).Shift(-27).Truncate(18)
rate_kind = apy
```

示例 `raw=6668427376540457481976400` 对应入库 `0.006668427376540457`。不读取只保留两位百分数的 `formatted`，也不误用借款历史。基础存款利息与其他 incentives 分开，本期不请求或合入奖励。

来源：[V3 接入说明](https://aave.com/docs/aave-v3/getting-started/graphql)、[市场及历史数据](https://aave.com/docs/aave-v3/markets/data)。

### 4.3 Aave V4

`POST https://api.v4.aave.com/graphql`。只采 Main spoke 的 WAVAX，不把其他 spoke 或 sAVAX 算进来。

以下 reserve ID 是官方接口返回的固定不透明标识，不自行按名称猜测或拼接：

```text
NDMxMTQ6OjB4NDM1MjcyQ2VmRjkzYTFFNjU3RThBQmZkZjBBMTNlOTU5MDBBM2E1Njo6MA==
```

```graphql
query {
  reserve(request: { query: { reserveInput: {
    chainId: 43114,
    spoke: "0x435272CefF93a1E657E8ABfdf0A13e95900A3a56", onChainId: "0"
  } } }) {
    id
    onChainId
    chain { chainId }
    spoke { address }
    asset { underlying { address info { decimals } } }
  }
  supplyApyHistory(request: {
    reserve: "NDMxMTQ6OjB4NDM1MjcyQ2VmRjkzYTFFNjU3RThBQmZkZjBBMTNlOTU5MDBBM2E1Njo6MA==",
    window: LAST_WEEK, includeRewards: false
  }) {
    date
    avgRate { normalized }
  }
}
```

必须同时校验返回的 `id`、`chainId`、spoke、`onChainId`、WAVAX 地址和 decimals=18。ID 或资产不匹配时拒绝整批，不自动切换市场。

`observation_time=date`。V4 的 `normalized` 是百分数，不是 V3 的比例：

```text
rate = Decimal(avgRate.normalized).Shift(-2).Truncate(18)
rate_kind = apy
```

示例 `normalized=1.36082551852043339681246800` 入库为 `0.013608255185204333`。

**`includeRewards: false` 必须显式发送。**[V4 官方说明](https://aave.com/docs/aave-v4/liquidity/reserves)指出历史接口默认包含 Merkl 奖励，不得依赖默认值。

## 5. 历史窗口与完整批次

每轮固定一次 `T=UTC 当前时间（毫秒精度）`，全批 `collected_at=T`。这是本轮取数时间，不替代来源 `ts/date`。

### OKX 分页

1. 目标范围为 `[T-7天, T]`。第一页不带 `after`，固定 `ccy=AVAX&limit=100`。
2. 每页校验响应，再以该页最小 `ts` 作为下一页 `after`。从第二页起，每条记录都必须 `ts <= 本页请求的 after`；只允许 `ts=after` 且币种、未截断的 `lendingRate` 十进制值与上一页边界一致的重叠，其余记录必须更早。此处利率比较只用于允许的分页边界重叠，非边界的窗口外旧点不要求利率。时间边界检查在窗口过滤前执行，不能只看最小时间就认为翻页成功。请求串行，复用 `RequestGate(1s)`，包括重试在内不密集请求。
3. 取得早于窗口下界的记录，或取得合法空页时结束。空页只表示来源已无更早数据，不承诺一定覆盖七天。
4. 每行先校验合法时间及 `ccy=AVAX`，再确定窗口；窗口外的旧记录不要求存在 `lendingRate`，也不写入。窗口内缺少有效 `lendingRate` 则整批失败。
5. 窗口内重复 `ts` 按币种及**未截断**的 `lendingRate` 十进制值比较，相同则合并，不同则报错，不比较与收益无关的借款 `rate`。下一页最小时间必须严格向过去推进。最多请求 8 页，仍未到结束条件则报错，不把被截断结果当完整窗口。
6. 全部页面取完、校验完后才交给 writer，不逐页落库。

### Aave 历史

- V3、V4 固定请求 `LAST_WEEK`；不在同一路线交替写 `LAST_DAY`、`LAST_MONTH` 或 `LAST_YEAR`，这些窗口的聚合粒度可能不同。
- 本轮 V3 返回 168 个点，V4 返回 92 个点；只是实测样例，不能据此写死条数或认为都是每小时数据。
- 保留来源的 `date`，转 UTC 后排序；不截整点、不插值、不填前值，也不按日均 APY反推每日到账利息。
- 本期每条路线只有这一个来源历史曲线，**不再写采集时刻的当前 APY 快照**；不用 `tier_no` 区分即时值和平均值，因此不需要新增统计字段或修改唯一键。
- 当前身份信息仅用于确认没有选错市场；不能把当前额度、暂停标记、费用或区块位置回填到历史点。

### 共同校验

- GraphQL HTTP 200 不代表成功：`errors` 非空、`data` 缺失、身份或历史字段为 `null` 均整批失败。只校验使用的必填字段，允许来源新增无关字段。
- 每条 Aave 曲线允许 1–512 个点，时间不得重复；早于 `T-14天`、晚于 `T+5分钟` 的点报错。14 天只是防止异常响应范围的宽松保护，不是官方对 `LAST_WEEK` 边界的承诺。OKX 在窗口过滤前也拒绝超过 `T+5分钟` 的异常未来时间。
- 入库前至少有一个有效历史点，最新点不得旧于 `T-6小时`。空响应、过期响应不能靠更新 `collected_at` 伪装正常。
- 来源真实存在的时间缺口可以保留，不要求每小时连续；解析失败、翻页中断或关键字段缺失不能静默跳过后声称成功。
- 利率为 0 是有效数据；负数、空值、非法数字则失败。不要把收益率超过 100% 当作通用解析错误。
- 所有数值由字符串或 `json.Number` 进入 `shopspring/decimal`，禁止经过 `float64`。利率处理顺序固定为：解析原值并拒绝负数 → 用 `Shift` 精确换算单位 → 检查结果非负且小于 `10^20` → 显式 `Truncate(18)` 写入 `Decimal(38,18)`。不能先截断再判负，否则微小负数会变成 0；不用默认只保留 16 位除法精度的 `Div` 做上述缩放。
- 利率字符串复用现有 `model.ParseStrictDecimal`，只接受普通十进制，不接受科学计数法或空白；V3 另要求 `raw` 全为数字，避免异常指数进入缩放运算。

这是“完整取得所请求窗口中的可用记录”，不是“来源保证七天每小时都齐全”。停机超过窗口造成的缺口如实保留，本期不实现更早历史的补查工具。

## 6. 如何填现有观测表

| 字段 | 本期填写规则 |
|---|---|
| `observation_time` | OKX 的 `ts` / Aave 的 `date`，不是本机当前时间 |
| `collected_at` | 本轮统一的 `T` |
| `tier_no / tier_min_amount / tier_max_amount` | `1 / 0 / NULL` |
| `tier_mode` | Aave 为 `none`；OKX 为 `unknown`，公开指标不能证明所有余额档位同利率 |
| `rate / rate_kind` | 按第 4 节；OKX 是 APR，Aave 是 APY，不强行互转 |
| `rate_origin / rate_mode` | `reported / variable` |
| `reward_asset_keys / reward_component_rates` | OKX 为 `[C] / [rate]`，Aave 为 `[W] / [rate]`；这里只表达该基础利率的计价资产 |
| 所有费用、固定罚金、固定费用币种 | `NULL`，历史接口未给出，不推断为 0，不计算净 APY |
| `lock_seconds` | `0`，路线限定无固定锁期的活期/供给产品；不是“未知”的代号，也不保证立即全额退出 |
| `unbonding_seconds` | `NULL`，本期没有可对应历史日期的退出等待信息 |
| `exposure_ratio` | OKX 为 `1`（每单位账内 AVAX 权益）；V3 为 `1`（余额增长型 aToken 的面值）；V4 为 `NULL`（历史接口未给 supply share 换算率） |
| `capacity / remaining_capacity / tvl` | `NULL` |
| `availability` | `unknown`；不能根据利率非零判断当前可存入或历史可退出 |
| `block_height / block_hash / finality` | 全部 `NULL`；本期是未提供链上锚点的官方 API，不额外读一个当前区块冒充历史位置 |
| `source_payload_hash` | 每条必填，按下文计算 |

Aave 按已约定的“只存不借的基础借贷”口径，`rule_principal_loss_mode=none`、`rule_eligibility=candidate`。这不代表合约、坏账处置或治理风险已审查。OKX 公共利率本身不足以确认个人适用条款，先写 `unknown / unknown`，原因 `public_lending_rate_only`；不因此阻止记录利率。三条路线的 `fixed_principal_loss_rate` 都为空。

哈希使用现有 `yield.HashPayloads`，顺序固定：

- Aave：`request`（实际发送的请求体），`response`（完整原始响应字节）。
- OKX：按照实际分页次序，依次加入 `page-1-request`（完整请求 URL）、`page-1-response`，再到下一页，包括用于确认结束的空页。

同批每行可以使用同一个批次哈希。哈希覆盖原始输入，不按解析后的字段重建 JSON，不混入本机时间；不得用哈希充当主键。哈希不能还原响应，本期不建设原始响应归档系统。

## 7. 写入、重试和查询

沿用 `WriteYieldBatch`：先登记/复用路线 ID，再批量写观测。逻辑键仍为：

```text
(yield_route_id, observation_time, tier_no)
```

- 每批先通过来源校验及 `NormalizeAndValidateForLiveCollection`，包括历史点也必须有 payload hash。
- 写入失败由现有 Runner 保留同一 `Batch` 重试；不得重新采集或重新生成时间、ID、哈希后再把它叫作同一次重试。
- 两张表之间没有数据库事务。可以先有路线而暂时无观测；重试必须复用路线 ID，不能当作全部写入成功。
- 历史窗口重抓可能重复插入，现有 `ReplacingMergeTree` 配合 `FINAL` 得到一个逻辑结果；不是承诺物理行永不重复。
- 现有表没有按 `collected_at` 比较的 version 列。因此坚持单进程、每条路线一个串行写入者，不另外开回补进程同时写这三条路线。
- 同一日期的指标可能被来源修订；本期只保留去重后的版本，不保存修订历史，不能据此还原当时已知的数据。
- 不修改盘口或资金费率 writer；不做定期 `OPTIMIZE FINAL`、独立索引或物化视图。

## 8. 最少的代码与配置改动

已新增：

```text
internal/yield/okxearn/collector.go + collector_test.go
internal/yield/aave/v3.go + v3_test.go
internal/yield/aave/v4.go + v4_test.go
internal/yield/aave/common.go          # 只放两者实际共用的解析/校验
```

V3/V4 的结构和单位分别处理，不设计通用市场 registry。可用 `NewV3Collector(endpoint)` / `NewV4Collector(endpoint)`，测试注入 `httptest` 地址；不增加只供单元测试使用的环境变量。

配置只新增 `AVAX_YIELD_ENABLED`，默认 `false`；OKX 复用 `OKX_REST_URL`，Aave 使用上述官方默认地址。本期不把历史窗口、三条路线地址和每个校验阈值都做成配置项。

`internal/app/app.go` 需要同时改三个位置：

1. “没有任何启用数据源”的判断应包含 AVAX 开关，允许只运行这三条收益采集；
2. 加载收益 registry 的启用条件包含 AVAX；
3. 为三条路线各装配一个 Runner，Source 分别为 `okx-avax-flexible`、`aave-v3-avax`、`aave-v4-avax`。

不复制 Runner、writer 或 route ID 分配器。实现完成后再更新 README 的启用示例与架构“已实现”清单，不在设计阶段修改实际运行环境或重启服务。

## 9. 测试与完成标准

离线单元测试使用小型真实形状 fixture 和 `httptest`，至少覆盖：

1. 三条固定身份；错误 chain、pool、spoke、reserve ID、underlying 或 aToken 拒绝整批。
2. OKX 的借款 `rate` 故意设得很高，仍只写 `lendingRate`；窗口外旧记录缺该字段不污染新数据；窗口内缺失必须失败。
3. OKX 多页、边界重复、重复冲突、游标不推进、混入大于 `after` 的记录、中途请求失败、空尾页和达到页数上限；相同 `ts` 的利率即使只在小数第 18 位之后不同也必须报冲突。任一非正常中断都不向 sink 写半批。
4. V3 的 27 位缩放、V4 的百分数缩放与 18 位截断；零利率有效，缺失和负数无效，特别测试 V4 `normalized=-0.000000000000000001` 必须拒绝而不是写 0；V4 请求明确包含 `includeRewards:false`。
5. GraphQL HTTP 200 携带 `errors`、身份或历史 `null`；重复时间、乱序排序、非等距历史、来源缺点、未来时间、全批过期。
6. 来源 `ts/date` 与本机取数时间分开；历史费用/额度/状态不被当前值填充；V4 的 `exposure_ratio` 保持为空。
7. 多响应哈希顺序稳定；修改任意使用的原始响应后哈希改变；每条都有哈希且不伪造区块锚点。
8. 复用现有重试测试方式验证：sink 第一次失败后再次收到同一批身份、时间与哈希；一个来源失败不停止其他来源；仅开 AVAX 时可以启动。

数据库验收使用隔离测试库，不删改现有生产历史：

- 第一次采集登记恰好三条新路线，各有非空历史，利率没有百倍或 `10^27` 倍的单位错误。
- 相同 fixture 连续写两次，`FINAL` 查询的逻辑行数不变、路线 ID 不变；模拟登记后写观测失败，重试不新增路线。
- 每条路线分别检查最近采集时间、最新来源时间和缺失哈希数。查询示例：

```sql
SELECT r.product_code, max(o.collected_at) AS last_collected,
       max(o.observation_time) AS last_source_time,
       count() AS history_points,
       countIf(isNull(o.source_payload_hash)) AS missing_hashes
FROM yield_observation AS o FINAL
INNER JOIN (SELECT yield_route_id, product_code FROM yield_route FINAL
    WHERE product_code IN ('simple-earn-flexible-avax',
        'avalanche-v3-wavax-supply', 'avalanche-v4-main-wavax-supply')) AS r
    USING (yield_route_id)
GROUP BY r.product_code
ORDER BY r.product_code;
```

应有三行，`missing_hashes=0`；缺少整条路线也算异常。来源窗口重抓不会让 `history_points` 固定不变，因此不把历史条数写死为 168/92；最近成功采集超过 2 小时应查日志，来源时间超过 6 小时不会被当作本轮成功。保留日志即可，不为三个来源另建告警平台。

编程完成的标准是三条路线持续采集、可恢复写入且可查询历史，不是已经能算净套利收益。

## 10. 文档审阅记录

2026-08-26，新建独立 Agent 完整审阅正文及通用字典的对应改动。第一轮指摘与处理：

- 先截断可能吞掉微小负数：改为截断前校验，并增加回归用例。
- 分页只检查最小时间不足以证明游标有效：增加逐条 `after` 边界校验，重复利率按未截断值比较。
- 不把实测历史点数/边界当永久接口契约：保留固定 `LAST_WEEK`，放宽过旧数据保护；最新来源时间仍严格校验。

评审结论：修订后，同一 Agent 第二轮完整复审通过，主 Agent 复核后双方认可本设计可直接实施。范围保持“三条固定来源曲线、每小时轮询、两张现有表”，无新增框架或服务。本节不是代码实现或部署完成标记。

## 11. 实现与验收记录

2026-08-27（北京时间）：

- 三个 collector、`AVAX_YIELD_ENABLED` 与三个独立 Runner 已实现；现有两张收益表、route registry、writer 和 Runner 生产逻辑均直接复用，没有 DDL 变更。
- 编程 Agent 增加固定身份、原精度分页冲突、历史完整性、APR/APY 单位、负值/科学计数法、来源时间、哈希及历史未知值的离线测试；加强已有 Runner 测试，按值快照比较重试批次的身份、时间与哈希，不保留可被原地修改的切片或指针别名。
- 主 Agent 独立调用真实公开接口取得 OKX 168、V3 168、V4 93 个历史点；写入独立临时库并重复写三次、中间重载 registry，`FINAL` 仍为三条路线、429 个观测。利率、毫秒时间、奖励数组、哈希及空字段回读一致；该临时库已删除。这些条数仅是本次响应，不是固定验收阈值。
- 新增可重复运行的隔离库 fixture 测试，覆盖登记后观测写入失败、重载 registry、重试及重复写；不会读取 `CLICKHOUSE_DATABASE`，也不删改生产库。该测试和既有收益两表集成测试均通过。
- 主 Agent 独立执行完整 `go test ./...`、`go test -race ./...`、`go vet ./...`、`go build ./...`，全部通过；还在独立临时库真实运行仅启用 AVAX 的 `app.Run`，确认三个 Runner 均采集并写入后可干净退出。实测临时文件和临时库已清理。
- 代码审阅的两项指摘均已修订：严格普通十进制解析拒绝异常科学计数法；重试测试使用解引用后的值快照，避免别名掩盖批次变化。主 Agent 复审及编程 Agent 自查均认可当前实现，没有剩余阻断项。

定向运行数据库测试，无需执行一天盘口压缩测量：

```bash
CLICKHOUSE_INTEGRATION=1 go test ./internal/storage/clickhouse \
  -run '^TestClickHouseAVAXYieldCollectorsHistoryAndRetry$' -v
```

本轮不修改生产启用配置、不重启服务。上线后仍需观察连续小时采集；停机超过近期窗口的更老缺口不会自动回补。
