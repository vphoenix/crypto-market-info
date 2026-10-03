# DEX 实现验收记录（2026-09-22）

代码入口和用户命令见 [实现说明](../../docs/dex-arbitrage-implementation.md)，独立审核见 [0010](../../discuss/0010-dex-arbitrage-code-review.md)。生产库只创建了五表，没有写入研究样本、启用DEX分支或重启collector。

## 正确性

- 完整 `go test ./...` 通过。
- DEX定向 `-race` 与 `go vet` 通过；最后的缓存变化另测，canonical状态每次重查，不缓存尚可能重组的状态。
- ClickHouse隔离库验证：UInt256/NULL精确读写、相同批次重复提交、事实先到而commit缺失的不可见性、旧canonical插入晚于orphan revision、只读连接禁止DDL、缺日志补采、回执补齐退出pending、原quote时间与成员hash保持。临时库在测试后清理。
- 真实样本离线重算使用保存的Quoter返回值，重新按PSM整数规则传播56条路线/金额，输入输出和dust逐条相等。它不是全路径EVM执行验证。
- 三份样本的93个gzip文件经过SHA-256核对，含最初失败请求的证据。大地址过滤被公共RPC拒绝后改成6地址一组，任何组缺失都不能记成空日志成功。

| 样本 | 区块 | 成功报价 | 日志/完整回执 | 日志与回执覆盖 | 完整采集耗时 |
| --- | --- | --- | --- | --- | --- |
| sample-with-events | 26,026,770 | 58/58 | 3/3 | complete | 1.962秒 |
| sample-finalized | 26,027,049 | 58/58 | 2/2 | complete | 2.098秒 |
| sample-no-events | 26,027,046 | 58/58 | 0/0 | complete | 1.498秒 |

这些历史重采的capture均为research；文件夹名称不能替代记录里的finality。最佳策略毛利分别为-0.082749、-0.082835、-0.082835 USDC，不证明其他区块或协议无机会，也不支持任何正净年化结论。真实有事件样本写入隔离库并完整读回，五表单块压缩列数据为12,521字节。

## 日规模合成容量

为满足采集改动的容量/查询验收，`TestDEXDailyCapacity`分别将有事件、无事件样本衍生成7,200个合成区块（12秒/块）及417,600条报价。只写自动清理的测试库，capture=research，不作为历史行情或收益研究数据。

最终代码上的测量结果：

| 固定种子 | 报价行 | 日志/回执行 | 压缩列数据字节 | 整日区块+报价读取（含Go解码） | 单块58报价读取P95 |
| --- | --- | --- | --- | --- | --- |
| 有事件样本 | 417,600 | 21,600/21,600 | 40,849,739 | 4.244秒 | 536.758毫秒 |
| 无事件样本 | 417,600 | 0/0 | 34,423,988 | 4.876秒 | 57.917毫秒 |

口径和局限：

- 每档报价与Sky数值重复相应单个真实样本；区块、batch、交易、payload身份和时间随块变化。它测量日规模行数与身份开销，**不能代表真实变化行情的压缩率**。
- 压缩值为各表 `OPTIMIZE FINAL` 后的 `system.parts.data_compressed_bytes`，不含marks、索引及其他磁盘开销，也不含gzip原文证据。
- 每种单块查询取20个位置，P95取排序后的第19个样本。未控制冷/热缓存和宿主机并发负载，不把两轮结果换算为加速倍数，也不从两类样本推断活跃度与耗时关系。
- 首轮单块查询只按batch过滤会扫描过多历史；最终实现同时使用已知chain、manifest和高度范围约束，利用现有排序键，没有增加索引或物化视图。
- 原始指标见 `capacity-sample-with-events.json` 与 `capacity-sample-no-events.json`。真实24小时持续占用、RPC配额与7天重复机会统计仍需启用采集后测量。

复现合成容量（必须显式启用隔离测试，不会写主库）：

```bash
CLICKHOUSE_INTEGRATION=1 DEX_CAPACITY=1 \
DEX_CAPACITY_SAMPLES=/home/ubuntu/crypto-market-info/research/2026-09-22-dex-implementation \
go test ./internal/storage/clickhouse -run TestDEXDailyCapacity -count=1 -v
```
