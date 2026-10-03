# 套利机会与策略资料

本目录保存从 `/home/ubuntu/crypto-arb-observer` 迁入的 `ARB-xxxx` 套利资料，方便本项目在设计行情采集范围和历史分析能力时直接查阅。

## 文档结构

| 路径 | 内容 | 在本项目中的作用 |
|---|---|---|
| [`../dex-arbitrage-implementation.md`](../dex-arbitrage-implementation.md) | Ethereum 协议兑换数据采集（已上线） | 同块金额报价、Sky 规则、五张专项表和只读报告；不含交易执行 |
| [`../reserve-data-implementation.md`](../reserve-data-implementation.md) | Reserve 拍卖／篮子申赎（已上线首版） | 完整篮子、拍卖权限、三档金额、共享池标记；历史回补及联合模拟仍有缺口 |
| [`../across-stablecoin-data-implementation.md`](../across-stablecoin-data-implementation.md) | Across 稳定币中继（已上线首版） | 订单、成交、退款与实时 probe；报告提供费用空间 |
| [`../lst-redemption-data-implementation.md`](../lst-redemption-data-implementation.md) | LST 折价与官方赎回（已上线首版） | 定额买入、赎回队列、退出报价及对冲数据；未知成本保留 unknown |
| [`../justlend-keeper-data-implementation.md`](../justlend-keeper-data-implementation.md) | JustLend keeper（已上线首版） | 清理事件、收据、只读模拟和资源费；成功奖励模拟尚未认证 |
| [`opportunities/opportunity-library.md`](opportunities/opportunity-library.md) | 来源项目迁移时的 `ARB-0001` 至 `ARB-0022` 机会库 | 判断各种套利需要采集哪些市场数据 |
| [`strategies/arb-0002-perp-funding-rate.md`](strategies/arb-0002-perp-funding-rate.md) | 来源项目的跨平台永续资金费率错位套利设计 | ARB-0002 的策略、计算和风险参考 |
| [`strategies/arb-0002-settlement-window-collection.md`](strategies/arb-0002-settlement-window-collection.md) | ARB-0002 结算窗口采集规则 | 旧项目的专项窗口采集方案参考 |
| [`strategies/arb-0009-options-collection-design.md`](strategies/arb-0009-options-collection-design.md) | Deribit 期权采集简化设计 R4（已实现） | 现有机器、固定C/P及期货清单、每秒10档与必要元数据，保存历史供以后分析 |
| [`strategies/arb-0009-options-live.md`](strategies/arb-0009-options-live.md) | 期权实时采集与查询（已上线） | 配置、固定28流自动选择、真实 REST/WS 到数据库验证；接入现有 collector |
| [`strategies/arb-0009-options-phase-1.md`](strategies/arb-0009-options-phase-1.md) | 期权离线基础实现记录（已完成，保留） | 协议解析、精确规格/规则、10档模型、分钟写入/回放及测试；剩余工作以R4为准 |
| [`legacy/arb-0009-options-collection-design-r3.md`](legacy/arb-0009-options-collection-design-r3.md) | 期权原R3设计审核存档 | 仅用于核验历史审核；原四阶段部署和全量前置要求已由R4取代 |
| [`strategies/arb-0016-yield-data.md`](strategies/arb-0016-yield-data.md) | 可对冲本金的收益数据采集 | ARB-0016 的两表数据模型、理论筛选和历史采集规则 |
| [`strategies/arb-0016-trx-yield-implementation.md`](strategies/arb-0016-trx-yield-implementation.md) | TRX 收益采集实现设计 | JustLend 与 TRON 原生质押的采集、校验和写入规则 |
| [`strategies/arb-0016-sol-yield-phase-1.md`](strategies/arb-0016-sol-yield-phase-1.md) | SOL 收益采集第一阶段实现设计 | 通用 Stake Pool、JitoSOL、mSOL、原生验证者与 Marinade Native |
| [`strategies/arb-0016-sol-yield-phase-2.md`](strategies/arb-0016-sol-yield-phase-2.md) | SOL 收益采集第二阶段实现设计 | laineSOL、JupSOL、hSOL、Kamino SOL 与 Save SOL |
| [`strategies/arb-0016-avax-yield-phase-1.md`](strategies/arb-0016-avax-yield-phase-1.md) | AVAX 收益采集第一阶段（已实现） | OKX、Aave V3/V4 的近期历史利率、校验与两表写入；运行状态见运行说明 |
| [`strategies/arb-0016-avax-yield-phase-2.md`](strategies/arb-0016-avax-yield-phase-2.md) | AVAX 收益采集第二阶段（已部署） | BENQI sAVAX、Ankr ankrAVAX、BENQI AVAX 借贷；同块兑换率、基础 APR、池内现金和退出窗口 |
| [`strategies/arb-0022-spot-cross-cex.md`](strategies/arb-0022-spot-cross-cex.md) | 来源项目的跨 CEX 同币种现货盘口价差套利设计 | ARB-0022 的策略和数据需求参考 |
| [`legacy/arb-0002-settlement-window-implementation-plan.md`](legacy/arb-0002-settlement-window-implementation-plan.md) | 旧项目的 ARB-0002 窗口实现说明 | 仅用于理解可复用旧代码，不是本项目实现规范 |

## 使用边界

- 机会定义和策略逻辑可以作为本项目确定采集范围、回放能力及分析字段的依据。
- 来源文档中关于 Redis、PostgreSQL、旧服务名、旧代码目录和部署状态的描述，只属于 `crypto-arb-observer`。
- 本项目不使用 Redis，数据库和行情存储结构以 [`../market-data-storage.md`](../market-data-storage.md) 为准，实现边界以 [`../implementation-design.md`](../implementation-design.md) 为准。
- 本项目当前只负责公开数据的采集、标准化、校验、存储和查询。这里的公开数据既包括盘口、成交和资金费率，也包括 ARB-0016 使用的公开收益率、产品规则与链上状态；不因迁入策略文档而增加下单、账户、私钥或自动交易能力。
- 来源文档中的证据等级和“已实现”状态是迁移时的历史记录，不自动表示本项目已经实现对应套利分析。

## 来源

迁移日期：2026-08-20

来源目录：`/home/ubuntu/crypto-arb-observer/docs`

本目录保留必要的来源说明，但不复制旧项目的 Redis、PostgreSQL、运维和发布文档，避免与本项目架构混淆。
