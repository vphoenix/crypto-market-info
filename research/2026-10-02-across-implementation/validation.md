# Across 实现验收（2026-10-02）

## 交付范围

独立 Go 命令、七张 ClickHouse 表、历史回溯、实时 probe、独立收据补采、离线四文件报告已实现。正式研究库为 `crypto_market_info_across`；90秒实时验收使用 `crypto_market_info_across_livecheck`。未安装常驻 Across 服务、未完成30日历史或7日在线观测。本记录不作获利结论。

配置：Base/Arbitrum 原生 USDC，币安 `https://api.binance.com`。公共 RPC 单批实测 Base 10 / Arbitrum 20 个成员，每链至多2并发，批量 header 共享5秒截止。65个连续 header 的实测耗时分别为984/859 ms；相邻 parentHash 检查通过。[协议原始证据](protocol-sources.json)、[实网 relayHash 验证](live-protocol-validation.json)。

## 测试

- `go test ./...` 全仓库回归通过；[输出](all-tests.log)。第一次在受限沙箱运行时，既有 httptest 测试因禁止监听本地端口失败；允许本地测试监听后全量通过。
- 最终 `go test -race ./internal/across ./cmd/across-data ./internal/storage/clickhouse -count=1` 通过；[输出](targeted-race-tests.log)。包括严格ABI/quantity、完整relayHash、uint256大ID、更新/slow fill、独家和截止等号、空/失败日志、实际计划时间、重组所有类型/尝试、来源失败隔离、失败Filled状态不终结、source错误不引用全零hash、revision写失败不提前修改内存状态、报价定点数和过期处理。
- `CLICKHOUSE_INTEGRATION=1 go test ./internal/storage/clickhouse -run '^TestAcross' -count=1 -v` 真实本地临时库通过（1.018秒）；覆盖UInt256/NULL/Decimal18/数组、重试去重、成员缺失、latest revision、孤块不复活。临时库自动删除。
- 初次正式幂等建表暴露SQL注释中的分号被当作语句终止符；已改为先剥离行注释。随后七表建表成功，现有三个已建表通过幂等执行保留。
- 采集写失败与证据文件失败均终止，避免错误地拆分范围并重新抓取；reorg revision先成功写库再更新内存，失败可以稳定重试。最终采集后重新核对区间两端hash。

## 两条真实历史窗口

| 链 | finalized 区块范围 | 原始窗口 | 存款事件 | 成交事件 | 退款事件 | 已存收据 |
|---|---|---|---:|---:|---:|---:|
| Base | 52077937–52078448 | 12:47:01–13:04:03 UTC | 56 | 116 | 3 | 65 |
| Arbitrum One | 510991304–510991815 | 13:00:02–13:02:21 UTC | 9 | 4 | 0 | 2 |

共69个已提交capture、65存款、120成交、3退款事件、67收据；两段日志均complete、unknown_event_count=0。七张表均存在，其中update和probe在历史窗口无行，不能将无分区误认为未建表。事件数包括其他路线，不是可参与的机会数。

[数据库测量](database-validation.json)记录物理行、压缩列与磁盘字节、UTC范围和查询耗时；[四文件报告](report-first-window/summary.json)按原生USDC路线另筛6个原始空message订单。9条退款Transfer核到账，逐单归属仍unknown。65份Base收据总费用因缺operator fee证据保持NULL，未补零。未匹配成交及缺收据/覆盖保留unknown；报告净收益null，negative_conclusion_allowed=false。

## 90秒实时验收

[采集日志](watch-livecheck.log)、[报告](report-livecheck/summary.json)。配置目标间隔1秒，实际完整循环6.258–13.728秒；包含公共RPC超时和维护工作。不能称已达到每秒采集，也不能据这段样本否决更快接入方式。

验收库保存9个存款事件，2个目标路线live首见订单，2次成功状态probe和1次来源查询失败；成功观察时已成交，没有验证可参与的开放窗口。2/5/10秒采样调度和边界经夹具验证，本段实网没有取得可用于该三档开放容量评估的正样本。

币安ETHUSDT与USDCUSDT各2次HTTP200；响应和原始时间保存在验收证据目录，未使用 `.vision`。原始首轮一次网络失败把全零占位hash误列为RPC证据引用，报告正确将该capture排为missing_rpc_evidence，没有参与机会计算。已修正零hash过滤并增加取消网络context的回归测试；初始冻结样本原样保留，以免改写历史。

## 日规模合成容量

[测量JSON](capacity.json)，[复现程序](capacity/main.go)。从上述两个真实512块窗口平铺24小时，重新生成身份并裁去超出24小时的行；两套临时库均已清理。所有金额仍为原始整数。

| 合成24小时样本 | 事实行数 | 压缩列字节 | 磁盘字节 | 批量写入 |
|---|---:|---:|---:|---:|
| Base较活跃窗口 | 20,275 | 3,088,769 | 3,205,804 | 634 ms |
| Arbitrum较低流量窗口 | 9,261 | 1,451,513 | 1,538,519 | 142 ms |

三类按资产身份分组的查询暖运行中位约2.6–7.6 ms，全部原始微秒耗时已存。**这是合成容量验收，不是实测日流量/日收益，也不是生产空间承诺**：重复内容压缩偏乐观、每个合成日只有一个capture、没有probe/update样本、只复制已取得的收据；gzip只测真实窗口，未虚构日原始证据量。

## 运行边界

报告先回答量级、费用空间和公开状态，不计算未知退款/失败竞价/调仓成本后的净利。公共RPC接入时延较高；完整30日覆盖、跨日稳定性、真实抢单成功率和逐单资金回收时间仍未实证。操作见[实现说明](../../docs/across-stablecoin-data-implementation.md)，独立审核见[审核记录](../../discuss/0013-across-stablecoin-code-review.md)。
