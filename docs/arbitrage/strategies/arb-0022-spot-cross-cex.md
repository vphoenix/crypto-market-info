# ARB-0022 跨 CEX 同币种现货盘口价差套利

> 迁移说明：本文件来自 `crypto-arb-observer`。文中的 Redis、PostgreSQL、机会生命周期和“已实现”状态均描述来源项目；`crypto-market-info` 当前只负责行情采集、ClickHouse 存储和回放。

状态：已实现只读观察系统
证据等级：E2
当前代码位置：`~/crypto-arb-observer`
当前覆盖 venue：`binance_spot`、`okx_spot`

## 1. 策略定义

`ARB-0022` 关注同一现货交易对在不同中心化交易所的可执行订单簿价格短时偏离。理论执行路径是在低价交易所买入 base asset，并在高价交易所卖出 base asset；实盘中通常需要预置库存和后续慢速库存再平衡。

当前项目只实现观察系统，用于证明价差可观测、可计算、可复盘，不执行交易。

## 2. 当前已实现能力

| 能力 | 当前实现 |
|---|---|
| 交易场所 | Binance spot、OKX spot |
| 行情采集 | WebSocket order book depth |
| 标的归一化 | `BTC-USDT`、`ETH-USDT`、`SOL-USDT`、`BNB-USDT`、`XRP-USDT` 等 |
| Redis 状态 | latest-only order book、metadata、universe、health |
| 机会计算 | 通用 BuyLeg / SellLeg，计算可执行价差、容量、手续费后 spread |
| 生命周期 | candidate / active / max / periodic / close |
| PostgreSQL 复盘 | metadata、opportunity_events、opportunity_snapshots |
| 诊断 | `arb-doctor` 只读健康检查 |
| 注入测试 | `arb-test-injector` 构造 fake opportunity 验证写入链路 |

## 3. 当前不做

- 不下单。
- 不接 API Key。
- 不读取真实账户余额。
- 不做库存再平衡。
- 不验证真实成交率。
- 不处理跨交易所转账到账时间。
- 不声称扫描结果可以直接实盘交易。

## 4. 与机会库的关系

完整机会定义见 [机会库 ARB-0022](../opportunities/opportunity-library.md#arb-0022跨-cex-同币种现货盘口价差套利)。

来源项目的机会库中 `ARB-0022` 已标为 `E2`，原因是来源项目已经实现只读行情采集、盘口深度计算、机会生命周期记录和复盘数据写入。尚未进入 `E3`，因为还没有历史回放或模拟执行证明扣除真实执行失败率、库存再平衡成本后仍为正收益。

## 5. 相关文档

以下配套资料仍保留在来源项目中，本次不迁入，以免与本项目架构混淆：系统设计、PostgreSQL 数据字典、Redis 数据模型、代码地图、运行手册和 TODO。

## 6. 后续升级方向

`ARB-0022` 后续若要从 E2 走向 E3，需要补充：

1. 更长时间窗口的历史回放。
2. 账户库存模型和库存再平衡成本。
3. 成交失败率和部分成交模型。
4. 交易所限频、限额和风控异常模型。
5. 小资金或沙盒级模拟执行，不直接进入大额实盘。
