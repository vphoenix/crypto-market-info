# BB、QNT、ONE 异常价差独立验证

**BB 与 QNT 是已确认的跨资产身份合并错误，不能交易该“价差”；ONE 的约 4% 价差能由当前官方盘口复现，但它是特殊指数规则下的跨永续基差，不能当作锁定利润。** 本次只读，不改采集代码、数据库、映射或服务，不访问账户或下单。

## BB / QNT：不是定价异常，是不同资产同名

来源一为当前 run 的 `soak-current-books.json`，取 **2026-09-12 19:12:00 UTC**、第 0 秒有效盘口。来源二为本次 **19:18–19:19 UTC 左右**新请求的交易所公开 instrument、ticker 与 book API，响应、来源时间与 SHA256 记录在 [market-anomaly-public.json](market-anomaly-public.json)、[market-anomaly-followup.json](market-anomaly-followup.json)。本地原文件 SHA256、涉及 8 条 instrument 的完整字段及价格精确换算见 [market-anomaly-validation.json](market-anomaly-validation.json)。

| 本地 instrument | 价格编码与第 0 秒买/卖价 | 当前官方身份 | 判断 |
|---|---|---|---|
| 201 Binance `BBUSDT` | tick 0.00001；0.00810 / 0.00811 | `underlyingType=COIN`，`underlyingSubType` 含 `Crypto`；2024 上市 | 加密币 BB |
| 202 Bybit `BBUSDT` | tick 0.000001；0.008103 / 0.008109 | `fullName=BounceBit`；2024 上市 | 加密币 BB |
| 203 OKX `BB-USDT-SWAP` | tick 0.001；7.752 / 7.761 | `instCategory=3`，`listTime=1780304400000` | 股票类 BB |
| 1067 Binance `QNTUSDT` | tick 0.01；64.10 / 64.12 | `underlyingType=COIN`，`underlyingSubType` 含 `Crypto`；2022 上市 | 加密币 QNT |
| 1068 Bybit `QNTUSDT` | tick 0.01；64.09 / 64.11 | `fullName=Quant`；2023 上市 | 加密币 QNT |
| 1069 OKX `QNT-USDT-SWAP` | tick 0.01；49.29 / 49.31 | `instCategory=3`，`listTime=1780655400000` | 股票类 QNT |

当前官方盘口再次返回 BB：Binance 约 0.00810/0.00811，OKX 约 7.751/7.760；QNT：Binance 约 64.11/64.12，OKX 约 49.31/49.32。由此排除“只因为本地旧 tick 解释出错”的假设：**本地数值与官方数值一致，错误在于把资产认成相同**。

两份 OKX 官方上市公告分别明确 BB 于 **2026-06-01 09:00 UTC**、QNT 于 **2026-06-05 10:30 UTC**作为股票永续上市。两时刻与本地 `venue_contract_version` 及当前 API `listTime` 一一相符。这不是凭 ticker 或价格量级猜测资产类型。[BB 股票永续官方公告](https://www.okx.com/en-gb/help/okx-to-list-perpetual-futures-for-bb-rdw-and-lunr-equities)、[QNT 股票永续官方公告](https://www.okx.com/en-gb/help/okx-to-list-perpetual-futures-for-qnt-equity)

公告 HTML 原文已保存为 `official-okx-to-list-perpetual-futures-for-bb-rdw-and-lunr-equities.html` 和 `official-okx-to-list-perpetual-futures-for-qnt-equity.html`；各自请求开始/结束 UTC 时间、URL、原始响应 SHA256 在 `market-anomaly-followup.json` 的对应条目中。不能把 BB 股票与 BounceBit、QNT 股票与 Quant 配成价格对冲；相同错误也会污染跨所资金费率差值。

## 可读取的资产类别字段与本地缺口

OKX 官方 `instCategory` 明确为：`1` 加密货币、`3` 股票、`4` 商品、`5` 外汇、`6` 债券，空字符串表示不可用。它可用来阻止本例跨类别合并；仍不能单靠类别与 ticker 证明同类别资产身份相同。[OKX 官方 API 文档](https://app.okx.com/docs-v5/en/)

`instType=SWAP` 仅表示永续，`ctType=linear` 仅表示线性结算；本例股票与币都满足。`groupId` 是费率组，BB/QNT/ONE 都可为 4，不是可靠身份字段。`ctVal × ctMult` 解决每张合约数量，不解决标的资产类别。

本地 [OKX 元数据结构](/home/ubuntu/crypto-market-info/internal/exchange/okx/metadata.go:20)没有读取 `instCategory`；[筛选条件](/home/ubuntu/crypto-market-info/internal/exchange/okx/metadata.go:64)只筛 SWAP、linear、USDT，之后在 `baseCcy` 为空时从 symbol 首段补 `base_asset`。[默认 canonical 映射](/home/ubuntu/crypto-market-info/internal/universe/aliases.go:133)据 `base_asset + "-USDT-PERP"` 构造同名组，因此 BB/QNT 两个不同身份被合并。已存价格和原始 instrument 不必因本发现被改写；历史查询首先需要排除错误的跨标的映射。此次没有实现修复。

范围计数也已独立核验：当前第 0 秒有效样本包含 **425 条 OKX**，其中官方分类 **272 加密、149 股票、4 商品**。149＋4 非币流不能全部叫污染：另一交易所可能是同一股票或商品。本次与 Binance 官方 `underlyingSubType` 含 `Crypto` 的产品严格交叉，**确认恰有 BB、QNT 两组股票/币冲突**；没有凭 ticker 把其他组判错。明细见 [market-identity-conflicts.json](market-identity-conflicts.json)。这个计数仅针对本次有效盘口样本，并非整个运行计划的完整成员审计。

## ONE：单位正确、盘口新鲜，指数却不相同

本地 Binance 955 `ONEUSDT` 与 OKX 956 `ONE-USDT-SWAP` 都是加密类 ONE；本次官方元数据的 tick 都是 **0.0000001**，与本地一致。OKX `ctVal=100`、`ctMult=1`、`lotSz=1`，本地 `contract_multiplier=100` 正确；100 张 OKX 合约等于 10,000 ONE，不能直接将 lot 数当币数。Bybit 的 `ONEUSDT` 当前官方状态为 `Closed`，ticker 为空，不能把该腿加进当前套利。

当前 REST 再次观测 OKX 约 0.0006436 卖、Binance 约 0.0006716 买。随后取得两家 100 档盘口，**来源撮合时间相差约 357 毫秒**（OKX `1789240798659`、Binance `1789240798302`），用 Decimal、相同 ONE 数量、100 ONE 数量公约数逐档计算：

| 名义规模（约） | OKX 多头开仓名义额 | Binance 空头开仓名义额 | 初始基差金额，非已赚利润 | 基差/多头名义额 |
|---|---:|---:|---:|---:|
| 100 USDT | 100.01634 | 104.35128 | 4.33494 | 4.334% |
| 1,000 USDT | 1,000.59081 | 1,043.0642553 | 42.4734453 | 4.245% |
| 5,000 USDT | 5,005.12895 | 5,210.4561677 | 205.3272177 | 4.102% |
| 10,000 USDT | 10,017.38549 | 10,411.455717 | 394.070227 | 3.934% |

这证明不是仅卖一不足 1 USDT 所制造的全部幻影，较深档也有明显价差；它依然只是一对异步静态盘口，不保证实际两腿能同时成交。**两腿永续开仓时并不会把该价差记成已实现现金收益**。同基础币数量的持仓价差损益为 `数量 ×（入场两所价差 − 离场两所价差）`，还需扣开平仓费用、资金费、滑点。若退出价差不缩小，就没有这里的毛收益。

分母同样必须讲清楚：约 1 万多头加约 1.04 万空头，如果按两腿均充分备付的约 2.04 万资金计算，假设完全收敛的毛收益只是约 **1.93%**，而不是 3.93%；且未含任何费用或等待时间。用保证金杠杆放大这个数也同时放大单所强平风险。

本次公开指数和标记价也证实两腿不是共享同一锚点：Binance `indexPrice=0.00067330`、`markPrice=0.00067238`；OKX `idxPx=0.0006399`、`markPx=0.0006442`。OKX 指数组成接口返回 OKX、Kucoin、Binance、Gate、Mxc，组成快照另有自身时间，不能把组件简单加权结果当成完全同步的最后指数。

Binance 官方 2026-08-14 公告解释：Harmony 安全事件造成多所充提受限与现货价格偏离，ONEUSDT 启用 Latest Price Protection（LPP）特殊保护规则。因此此差价应归类为**安全事件与不同指数机制下的基差头寸**。开放永续成交不证明现货可自由搬运，也不保证两所最终按同一价格收敛。[ONEUSDT LPP 官方公告](https://www.binance.com/en/support/announcement/detail/1127f937c5fe49acb976d4a2dd272d27)

同项目外部验证 Agent 正在补充 LPP 是否结束、迁移/充提状态和近 7 日实际资金费。我的一次官方采样已含 OKX 100 次、Binance 30 次资金费历史以及下一期估算，保存在 `market-anomaly-followup.json`。当前资金费为预估且结算频率不同，不能用一条当前差值代替持有成本。

**研究结论：删除 BB/QNT 假机会；ONE 保留为有显著可见基差、需要调查收敛机制的专项候选，当前不满足“风险小”的筛选条件。** 不应把其规模表解释成已验证的净收益或建议建立仓位。
