# Ethereum DEX：3%获利门槛与Sky退出参考

口径：用户全本金1,000,000 USDT、无持仓；仅采用canonical、committed、finalized且完整live报价。结论：已验证的Sky PSM/Uniswap V3八个已采路线方向没有毛正观测，无法支持全本金净简单年化3%。报价没有实际交易成交证据，不累加重复报价，也不把瞬时价差乘365。

## 四个独立窗口

| UTC区间 | finalized且canonical区块 | 完整live块 | 毛正报价/窗口 |
|---|---:|---:|---:|
| 2026-09-28 00:00–02:00 | 598 | 592 | 0 / 0 |
| 2026-09-29 06:00–08:00 | 595 | 414 | 0 / 0 |
| 2026-09-30 12:00–14:00 | 599 | 482 | 0 / 0 |
| 2026-10-01 15:00–17:00 | 597 | 461 | 0 / 0 |
| 合计 | 2,389 | 1,949 | 0 / 0 |

每完整块为八路线方向×七金额档（1,000至1,000,000 USDC）及两个成本参考共58条报价。独立整数汇总共1,949×56=109,144条完整finalized策略报价，毛正数为0。现有`dex-check`研究副本同时验证每批成员数量、完整quote digest、manifest、链hash和预算身份，四个窗口全部exit=0。该程序按100万USDT换得的USDC预算筛选，百万USDC档未必可由100万USDT提供，因此独立整数汇总额外包含七个原始金额档，不能误认为USDT等于USDC。

百万USDC档各窗口最好毛损失分别是−255,997.649527、−31,769.709945、−272,773.300473、−51,516.464530 USDC，反映已采池大单深度不足，不能把1,000USDC小单表现放大至全本金。所有量级都毛负，加入gas、排序付款、出入场、失败或滑点不会转成正利润。

2026-09-28至10-01每日覆盖见`coverage_recent.jsonl`，其中存在partial、backfill/missing以及orphaned历史；它们均保持未知或排除，不把断档及重组数据变成收益。上述四个两小时窗口只是抽样，不能外推全部历史或所有链上池。

## 当前finalized Sky容量及PT退出参考

当前保存且经原工具完整digest核验的参考为区块 **26,098,873**，时间 **2026-10-01 17:10:59 UTC**（北京时间2026-10-02 01:10:59），hash **0xf47925358bb6f2017d6f5753d8a492b6088e2d55bbd5b992ac641296200a8db1**。该块canonical、committed、finalized、完整58/58；这是一份明确历史锚点，不冒充查询时刻所有head都已finalized。

| 同块已采字段 | 值 |
|---|---:|
| LitePSM tin / tout | 0 / 0 |
| DAI现金 | 801,033,767.276841270548465826 DAI |
| USDC pocket现金 | 4,167,514,719.910482 USDC |
| pocket allowance | UInt256最大值 |
| vat/join存活及ward，identity_ok/state_complete | 全部true |
| 已采100万USDT→USDC输入 | 1,000,000.000000 USDT |
| 同块输出 | 999,494.501626 USDC |
| 以稳定币1:1作显示参考的兑换损耗 | 505.498374，即0.0505498374% |

这支持“此锚点下PSM无需swap费且现金规模远大于百万”的条件性观测。官方[LitePSM文档](https://developers.skyeco.com/protocol/liquidity/litepsm/)确认兑换模块及费用可由治理改变；[Uniswap费率文档](https://developers.uniswap.org/docs/get-started/concepts/fees)解释报价池swap费与费用档。参考报价已经含池内swap费，不能另把同一swap费重复扣一次。

现有58条只采1 ETH→USDC gas_reference和100万USDT→USDC capital_entry，没有**100万USDC→USDT反向精确报价**。不得把正向输出求倒数当成反向可成交报价，也不能把PSM无费用当成USDT退出无成本。对于PT到期退出，还缺到期时PSM规则与容量、PT赎回/转换费、USDC→USDT精确可成交金额与gas/滑点。此锚点的PSM现金及当前正向报价不证明未来容量，也不锁定未来退出价。

最新参考块八路线全部仍毛负。`verified_exit_reference/report.json`核验单块完整digest，最高小单报价为dai_usdc_100_psm_first输入1,000 USDC，毛利−0.053628；百万USDC同路线毛利−55,134.710855，尚未计gas等。

## 复核命令与证据

`capture.py`保存四窗口完整区块状态、按路线和金额的整数汇总及SQL。`exit_reference.py`保存同hash的Sky与报价原始JSON、payload hash；抓取UTC时间和响应SHA-256记录在`capture.jsonl`。所有查询POST、readonly=1、max_threads=2、max_execution_time=60，金额保留整数token atoms，需要显示时转Decimal。

`../options/build_checks.py`复制当前`cmd/dex-check`及存储包，在研究副本收紧连接查询预算；未改服务、DB或生产源文件。`run_checks.py`给出四个有界窗口精确命令，结果是`check_manifest.json`与`verified_windows/*`。单块补充命令为：

```text
dex-check --from 2026-10-01T17:10:59Z --to 2026-10-01T17:11:00Z --capital-usdt 1000000 --report-dir verified_exit_reference
```

未额外访问RPC补报价，未建表、写DB、启采集、签名或下单。费用或执行补证不能把已观测负毛利路线变成3%机会；新路线和PT收益应按自身typed模型与现金流另评估。
