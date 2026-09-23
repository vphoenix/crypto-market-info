# Ethereum DEX 采集与套利候选判断

2026-09-22 实现并部署。对应 [R2设计](dex-arbitrage-mvp-design.md)，独立审核见 [0010](../discuss/0010-dex-arbitrage-code-review.md)。代码、五表和只读命令已完成；现有 `crypto_market_info` 库已实际建表。应用配置默认关闭，生产unit已显式启用，并继续使用原有单一collector进程。

## 已交付

- Ethereum主网，4个策略池、2个成本参考池，Sky LitePSM/wrapper/converter及必要依赖；地址、代码SHA-256、代理实现槽、ABI和7档金额固化在 `internal/dex/ethereum/manifest.json`。发现代码/实现/身份变化时，本块策略停止判断，不能自动信任新版本。
- 每个新块扫描8个方向×7档USDC金额，另取1 WETH→USDC显示参考、100万USDT→USDC预算参考；全部状态与报价固定原blockHash。金额为整数原子值；PSM费用、库存、方向停机、有限授权、dust及重复访问库存按顺序处理。
- 每2秒检查新头；单请求5秒、每批最多20个RPC成员、最多2个HTTP请求并发。一次数据读取预算10秒，结束后另做一次5秒内的canonical复核，available_at包含真实延迟。白名单日志按每组6地址查询，全部组完整才标complete；公共节点实测会拒绝更大的过滤组合。
- 五表批写、证据先落盘、最后提交block标记；重组追加状态，默认报告只认finalized；暂时落后的head/finalized标签重试，既有finalized高度的hash冲突才暂停DEX分支。DEX错误在分支内处理，CEX等其他来源继续运行。
- 漏高度每次空闲补一个块，低于设计100块上限，历史补采不扫描报价；缺日志和缺回执也有有界恢复。完成的回执退出pending集合；补采时间留在证据中，不改原报价时间。
- `dex-check`输出确认新毛正窗口、连续性未知片段、正候选出现日期、最大持续时间、最佳金额和毛利。连续100块同一价差只构成一个窗口；换金额档和普通事件不拆窗口，共用PSM的候选不能相加。

## 命令

仅建表（幂等，不启动任何采集器）：

```bash
go run ./cmd/collector --init-dex-schema
```

查询最近24小时的finalized数据，默认100万USDT总预算；第一次启动前空表会明确输出0个观测，而非“已证明没有套利”：

```bash
go run ./cmd/dex-check --report-dir var/dex-reports/latest
```

使用UTC范围，或查看明确标为head的最新候选：

```bash
go run ./cmd/dex-check --from 2026-09-21T00:00:00Z --to 2026-09-22T00:00:00Z \
  --capital-usdt 1000000 --report-dir var/dex-reports/2026-09-21
go run ./cmd/dex-check --include-head
```

显式成本情景（示例数值不是实际执行成本的保证）：

```bash
go run ./cmd/dex-check --gas-units 400000 --priority-fee-gwei 1 \
  --ordering-usdc 5 --other-cost-usdc 0 --quote-costs-rpc \
  --report-dir var/dex-reports/cost-scenario
```

省略 `--quote-costs-rpc` 时完全不访问RPC，只显示已存毛利与未知成本。启用时仅为最多50个选中正窗口补报，每个窗口最多4个RPC成员，报告内按hash和完整参数缓存。所有金额始终固定原hash，无历史state则unknown，不退回latest。报告保留cost_scenario_hash、请求金额/方向、原始证据、来源及采集时间。

准备金检查后会重新在同一候选块的已有金额档中选择：100万档若占满本金，仍检查50万等较小金额。CSV使用实际选中档的金额和毛利，不能混配。

报告输出：`report.json`保留定类型完整结果与情景；`windows.csv`每个观察窗口一行；`candidates.csv`保存每块每路线最佳金额，不表示每行都能赚钱。离线预算参考固定100万USDT；其他本金没有匹配参考时明确预算未知，不按USDT=USDC换算，也不计确认新窗口。

只取一个公开样本、核验程序且不写任何数据库：

```bash
go run ./cmd/dex-check --sample-block finalized --report-dir var/dex-reports/sample
```

该模式使用 `capture=research`，生成 `sample.json`与证据。历史高度可写十六进制，如 `--sample-block 0x18d2312`。它不是启动持续采集。

## 启用持续采集时的配置

只增加三项collector环境变量：

```text
DEX_ENABLED=true
DEX_ETH_RPC_URL=https://ethereum-rpc.publicnode.com
DEX_EVIDENCE_DIR=/持久目录/dex-evidence
```

默认 `DEX_ENABLED=false`，RPC默认上述公开节点，证据默认相对工作目录 `var/dex-evidence`。当前生产unit已使用上述RPC，并把证据目录设为 `/home/ubuntu/.local/share/crypto-market-info-dex/evidence`。后续部署按 [运行说明](runtime-operations.md) 更新同一个collector；同库仍只允许一个collector，不要另起 `go run` 向生产库写数据。RPC URL可以由本机环境提供，不能提交凭据。此版本没有独立DEX服务、钱包、交易下单或自动执行。

## 判断口径

- `unknown`：状态、数据、预算或成本证据不足。
- `quoted_nonpositive`：已观测且可比较的金额档报价不正；不外推未采到的状态。
- `gross_candidate`：同块金额报价毛正，尚未建立完整成本情景。
- `scenario_positive`：显式gas/tip/排序支付/其他费用和本金准备金下，**USDC库存情景**仍正。

USDT→USDC入场报价用于总预算，已在输出中包含池费。回到USDT的独立同状态报价只做结算参考；入场和退出共享USDT池，未经顺序模拟不能拼成已验证USDT闭环利润。本版不输出USDT净年化，也不把观察窗口毛利加总为可获利金额。4.5%对应100万本金全年45,000 USDT，是后续完整经济核验门槛。还需真实纳入、完整路径gas、失败及竞争成本等证据后才能讨论可达到的净年化。

## 本次验收

- 完整 `go test ./...`通过；独立审核运行DEX定向race测试及ClickHouse隔离库集成。
- 覆盖精确舍入/溢出/停机/dust、候选私有库存与两次PSM访问；RPC响应id/ABI位宽/同hash/分组缺失/确定性重试；重组最新revision、半批不可见、UInt256/NULL读写、日志与回执恢复、原quote时间保持；窗口与资金准备金退选、stale head恢复、DEX不取消其他collector。
- 主库五表实际DDL保存在 [created-tables.jsonl](../research/2026-09-22-dex-implementation/created-tables.jsonl)。样本只在本地证据和自动清理的隔离测试库验收，主库不混入研究行情。
- 历史固定区块26,026,770：58/58报价、3日志、3完整回执，采集约1.962秒；56条策略用已存Quoter输出离线重算PSM现金流逐条一致。最佳毛利为 **-0.082749 USDC**。单块隔离库五表合计实测压缩12,521字节，不含gzip证据。
- 另一固定区块26,027,049：58/58报价、2日志、2回执，约2.098秒；最佳毛利 **-0.082835 USDC**。样本文件见 [研究目录](../research/2026-09-22-dex-implementation/)。这些是少量功能验收样本，不是持续收益调查。
- 完整无事件区块26,027,046：58/58报价，日志与回执均为0且coverage=complete，约1.498秒；证明成功为空与RPC失败缺失可区分。
- 已完成两份7200块日规模合成容量测试：有事件/无事件种子各417,600报价，压缩列数据40,849,739 / 34,423,988字节，整日读取4.244 / 4.876秒。固定seed、合并后列数据、不含证据与marks、缓存未控制等完整口径见[验收记录](../research/2026-09-22-dex-implementation/validation.md)。
- 生产部署后验收连续高度26,027,452至26,027,477：25/26区块为58/58完整报价，完整块耗时约3.9至7.3秒；26,027,460因一次RPC/Sky状态读取不完整保留为partial，后续自动恢复。继续观察到69个连续区块时服务零重启，早期区块已从head推进为safe；当时finalized锚点尚未进入启动后的高度。同期五路CEX盘口继续写入60/60有效秒。
- 首份 `--include-head` 实时报告覆盖24个观察高度、23个完整报价高度，8条路线均为 `quoted_nonpositive`，确认窗口为0；报告保存在 [`var/dex-reports/initial-live`](../var/dex-reports/initial-live/)。head仅用于上线验收，默认查询仍只认finalized。
- 尚未进行24小时持续运行和7天重复性统计。上线69个区块时，gzip证据为2,101个文件、逻辑大小39,251,056字节、磁盘分配约43 MiB；按短样本线性外推约4.1 GB和21.9万个文件/日，只作为容量预警。日占用、RPC持续配额、补采追赶速度及查询延迟以启用后的实测为准；本次不把该外推冒充实测日量。
