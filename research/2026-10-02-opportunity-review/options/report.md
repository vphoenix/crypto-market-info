# Deribit：已采期权及同到期期货复核

判断口径：用户当前有1,000,000 USDT、无持仓；门槛是全本金的净简单年化3%，年化公式只适用于已锁定现金流与已确定资本占用，不能以保证金代替全部资本。

结论：本次已验证的期权box、conversion/reversal全部毛亏，不能提供3%机会。最新有效样本为2026-10-01 17:25:59 UTC（北京时间2026-10-02 01:25:59），当时28个已采合约每分钟60/60有效，四个指数完整；当前BTC/ETH、币本位/USDC四族均到期于2026-10-03 08:00 UTC。不是误把已到期期权当作当前产品。

## 可执行价差及历史独立窗口

仅以买入ask、卖出bid计算，上沿结果不假定maker成交，也不把报价按无限数量使用。对同一方向的更多数量，吃深盘口不会优于最优买卖价，因此毛利上沿为负即可排除该组的全部正数量，无须给它编造保证金年化。空买卖方或无效秒对应的组合排除。

| UTC分钟 | 28腿同时有效的秒数 | 可评估组合秒 | 毛正组合秒 | 标准入场费后正组合秒 |
|---|---:|---:|---:|---:|
| 2026-09-28 18:00 | 60 | 2,880 | 0 | 0 |
| 2026-09-29 12:00 | 60 | 2,880 | 0 | 0 |
| 2026-09-30 14:00 | 59 | 2,832 | 0 | 0 |
| 2026-10-01 08:00 | 60 | 2,231 | 0 | 0 |
| 2026-10-01 17:25 | 60 | 2,880 | 0 | 0 |
| 合计 | 299 / 300 | 13,703 | 0 | 0 |

完整的每秒结果是`all_second_bounds.json`（最新）及`windows/*/all_second_bounds.json`；汇总见`all_second_summary.json`。2026-10-01 08:00组合数少是部分方向无挂单，不是把缺失报价填零。上述五个独立分钟不代表连续全历史扫描，也不能排除未采产品的机会。

最新第59秒最接近零的USDC conversion，是卖ETH_USDC-3OCT26-2700-C、买同strike put、买同到期期货：毛利−2.200000 USDC / ETH；两期权加期货标准taker入场费2.565548 USDC / ETH；入场后−4.765548 USDC / ETH，尚未扣任何结算费、USDT/USDC兑换、出入金或执行损耗。最新这一分钟最好瞬间上沿仍是−1.35 USDC / ETH（第29秒），入场后−3.9145045 USDC / ETH。币本位结果保留BTC/ETH单位，展示用USD指数不是可成交的USDT兑换腿。

## 公式和资本边界

线性conversion一单位标的的毛利：长synthetic、短future为`F_bid − K − C_ask + P_bid`；反向为`K − F_ask + C_bid − P_ask`。同到期线性长box终值是`K_high − K_low`；以四条可成交bid/ask净付款对比，不按mark。

币本位C−P的终值是`1−K/S_T`；匹配K USD数量的inverse future，长synthetic/短future毛利为`1−K/F_bid−C_ask+P_bid`，反向相反。币本位box需匹配宽度USD数量的同到期期货，锁定的是币数量；未额外证明换回USDT和资本币敞口，不能声称锁定USDT收益。数量单位已读取`derivative_contract_spec`，inverse futures的USD数量不误作BTC/ETH数量。

标准费用参考期权`min(0.0003×相应标的单位, 12.5%×premium)`及期货taker0.035%。用户1,000,000 USDT不自动证明Deribit账户权益费率档，故没有把VIP折扣当事实；毛利已经全部为负，折扣为零也不改变排除结论。不能将一个很小的box payoff本金、某个保证金数字或只部署的资金作为100万USDT的收益分母。

## 规则核验及可复核性

2026-10-02核验Deribit官方[费用](https://support.deribit.com/hc/en-us/articles/25944746248989-Fees)、[线性USDC期权](https://support.deribit.com/hc/en-us/articles/31424932728093-Linear-USDC-Options)、[线性期货](https://support.deribit.com/hc/en-us/articles/31424954805405-Linear-Futures)、[币本位期货](https://support.deribit.com/hc/en-us/articles/31424938981533-Inverse-Futures)及[币本位期权](https://support.deribit.com/hc/en-us/articles/31424939096093-Inverse-Options)。期权为欧式，到期以07:30–08:00 UTC指数TWAP结算；ITM期权先生成同到期期货再现金结算，已有期货可能先净额抵销，不能重复计费用。官方费用表对日到期/周度合约另有豁免，故本报告没有把统一0.015%/0.025%粗糙当成所有合约精确结算费；负毛利判断不依赖这一步。

`capture.py`、`capture_windows.py`把SQL、原始JSONEachRow、UTC抓取时间与响应SHA-256保存在各目录。所有HTTP查询为POST、readonly=1、max_threads=2、max_execution_time=60，`settings.jsonl`实测该三项。价格和数量从整数tick/lot恢复，财务计算全程Python Decimal，未使用Float64。

`build_checks.py`复制现有`cmd/options-check`和存储包到本研究目录，只把连接预算收紧为上述限制，不修改服务、生产源文件、表或配置。`run_checks.py`对五个完整分钟调用原校验路径，全部exit=0；它核验run、全部盘口与质量成员hash、索引hash、batch digest、metadata规则和回放不变量。结果见`verification_manifest.json`及五份`verified_minute.json`。重建源身份保存在`checker_build_manifest.json`。生成的构建缓存、二进制及代码副本在复核后删除，可由脚本重建。

未测算杠杆保证金收益、未执行交易。本研究是已有数据筛选；其他更长到期、更多strike、组合成交及实际USDT出入场报价尚缺。
