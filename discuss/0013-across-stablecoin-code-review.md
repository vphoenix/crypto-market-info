# Across 稳定币中继采集程序代码审核

日期：2026-10-02。独立审核 Agent：`/root/across_design_reviewer`。审核依据为已复审的 [设计](../docs/across-stablecoin-data-mvp-design.md) 与 [DDL](../docs/across-stablecoin-data-schema.sql)。

状态：已完成最终复审。在首版公开数据研究范围内通过，未发现未关闭的 P1；这不是净盈利、抢单能力或可投入资金的认证。

范围：只审核本线程新增的 Across 采集/存储/查询/CLI，不修改程序、数据库、资金或运行服务。实现工作目录为 `/home/ubuntu/crypto-market-info`，不会改动另一 `/tmp` 工作树或 Reserve 代码。

## 审核结论与边界

`cmd/across-data`、`internal/across` 与专项 ClickHouse writer/schema 已构成可使用的采集、重启恢复与离线报告链路。金额和 ID 使用整数，价格使用十进制定点数，时间使用 UTC；七张定类型表、一个命令和本地证据目录符合已审设计的最小范围。没有交易执行、资金操作、全链索引或 Dataworker 前置要求。

可以继续扩大数据窗口并收集公开可达性证据。要回答“能否稳定赚 200 美元/日”，仍需更长实采、足够及时的公开订单观测、完整成本与真实资金周转证据；当前 report 会明确输出 `net_profit_usdt=null`、`negative_conclusion_allowed=false`。不能把小窗口费用空间或没有观测到开放订单直接变成赚钱/不赚钱结论。

## 已核对的实质修正

以下问题均曾向作者提出；最终结论依据当前文件与回归测试，未按作者回复直接关闭。

| 审核事项 | 最终实现与最小修正 |
|---|---|
| 实际部署身份与 ABI | manifest 加载核对 verified 源文件 SHA-256、实现 runtime bytecode 和嵌入事件/只读 ABI；启动核验 chain ID、USDC 元数据、代理实现与代码 hash。未知历史实现和升级边界保持 raw/partial，不套用当前 ABI。 |
| 完整 relay 与实时可见性 | RelayHash 包含完整动态 tuple 与目标链，原始条款和 speedup 分开；`AvailableAt` 在解码后记录，live 还须通过新鲜头检查。重启订单标为 restart，backfill 不冒充 live。 |
| 证据与部分写入 | capture evidence 校验六表数组、成员计数/摘要与文件 hash；事实先写、capture 最后提交。冻结批次的 DB/证据写失败直接停止，不能被当作 RPC 范围失败重新抓取；failed capture 不携带未完成的事实行。 |
| 恢复与重组 | 日志游标使用区间并集，二分成功的多段共同覆盖原范围，缺口不跳过；收据队列从已提交日志恢复。采完重查 from/to 端点 hash，重组检查涵盖相关 capture 类型与失败尝试，finalized hash 冲突停止；finalized 收据明确标记。revision 写成功后才更新内存，写失败停止且后续可重试，已有失败状态回归。 |
| probe 时间与失败 | +2/+5/+10 秒有实际任务和计划/实际时间；RPC 预算 context 与持久化 context 分离。失败查询中的 Filled 不终结订单，本地过期仅记未验证取消；source canonical header 有独立原始证据引用。 |
| 逐单费用空间与 coverage | 分别按原始条款、实际 fast fill、live 开放观测计算逐单非负费用空间；多条更新/probe 不重复计收入。跨链时间倒序只标可能 prefill/时钟顺序反转，无匹配成交保持 unknown。 |
| 退款与 gas | 退款地址/批次仅以独立收据中唯一 USDC Transfer 核验；逐单和路线归属 unknown，不把退款叶全额当本路线收入。Arbitrum 不重复叠加 L1 gas；Base 缺适用 operator fee 信息则总成本 unknown。共享交易 gas 有交易身份与 scope，summary 去重。 |
| 失败 source probe 的零 hash | 最后发现的 32 字节零 hash 不再写成不存在的 RPC 引用。`TestAcrossFailedSourceProbeHasNoZeroEvidenceReference` 强制网络失败，核验错误仍持久化且没有虚构引用。 |

## 独立检查与实际验收证据

独立执行 `GOCACHE=/tmp/crypto-market-info-go-cache go test -race ./internal/across ./cmd/across-data ./internal/storage/clickhouse`，退出 0；包含最后 revision 写失败修正的最终执行 Across 包用时 1.910 秒。CLI 没有专门测试文件。检查部署证据时，两个 verified 文件 SHA-256 均与 manifest 相符，嵌入协议 ABI 与两份实际部署 ABI 对应，两个 RPC bytecode 与 explorer 的 deployed bytecode 逐字节一致。Base explorer 仍标为部分源验证，该事实不被描述成完整源验证。

作者执行的全库测试日志 [all-tests.log](../research/2026-10-02-across-implementation/all-tests.log)、专项测试日志和实际 ClickHouse 验收记录已读取；它们是作者执行的验收，独立审核没有再次向运行中的数据库写入。实际只读 `getV3RelayHash` 对照的记录见 [live-protocol-validation.json](../research/2026-10-02-across-implementation/live-protocol-validation.json)，两链固定 finalized 块上的合成 tuple 对照通过，没有发送交易。

最终 [验收记录](../research/2026-10-02-across-implementation/validation.md) 已补齐并核对，与测试、两段真实窗口、90 秒循环延迟、旧零 hash capture 排除和合成容量限制一致；不存在等待该文件补齐的未完成项。代码冻结，最终审核结论与下列代码摘要保持不变。

核对 [database-validation.json](../research/2026-10-02-across-implementation/database-validation.json) 与两个报告的 JSON/CSV：

| 样本 | 实际覆盖与结果 |
|---|---|
| [两链 finalized 小窗口](../research/2026-10-02-across-implementation/report-first-window/summary.json) | 每链 512 块；65 deposits、120 fills、3 refund events、67 receipts。69 accepted captures；原始空 message 路线候选 6 单，原始费用空间合计 0.013791 USDC。9 个退款地址实付通过 Transfer 核验，逐单归属仍 unknown；65 份收据费用 unknown。 |
| [独立 90 秒 live 验收](../research/2026-10-02-across-implementation/report-livecheck/summary.json) | 28 captures 中 27 accepted；9 deposits，2 个原始条款候选，2 个成功 probe。原始费用空间 0.056001 USDC，live 开放费用空间 0。旧样本中 1 个失败 probe 的零 hash 引用被 report 明确排除为 `missing_rpc_evidence`；冻结历史保留，代码已修并有回归。 |

两个报告的 coverage/orders/refunds CSV 行宽均与表头对应。报告选择的是存款区块时间 cohort，允许使用后续已存证据，并非过去某时刻的可见信息回放。

90 秒日志实际轮次为 6.258–13.728 秒；一次 source 查询失败轮次为约 11.416 秒。`poll_millis=1000` 是目标间隔，实采不能宣称每秒完成；该样本没有证明 +2/+5/+10 的准时覆盖或抢单能力。没有启动 30 日回补或常驻服务。容量文件 [capacity.json](../research/2026-10-02-across-implementation/capacity.json) 是重复真实小窗口形成的合成 24 小时数据，重复内容使压缩偏乐观，也不含 probe/update 压力；其中占用与查询耗时不能当作真实每日流量或持续负载结论。

## 未解决的研究限制

没有未关闭的代码 P1。保留的限制包括：公共 RPC 实测延迟、样本短与缺测、未知历史 ABI、其他源链/路线与用户退款成员覆盖不足、逐单退款和资本周转 unknown、Base 成本缺项、gas 共享交易分摊与库存恢复成本未核实。它们限制获利结论，已经在程序输出与使用说明中保留；不要求首版增加全链索引。

## 最终审核版本

19 个代码、测试、ABI、配置与专项 DDL 文件的 SHA-256 汇总为 `b336757b11c48f5c01298244b433f5a764bff94357940746d4b6c6f50304f7cb`。算法：对以下文件相对路径按字典排序，依次输入 `UTF-8 相对路径 + NUL + 文件原始字节 + NUL`，计算 SHA-256；不包含文档、原始证据、Go 全局依赖或本审核记录。

纳入：`internal/across/*.go`、`internal/across/*.json`、`cmd/across-data/main.go`、`config/across-research.json`、`internal/storage/clickhouse/across.go`、`internal/storage/clickhouse/across_schema.sql`、`internal/storage/clickhouse/across_integration_test.go`。

| 关键文件 | SHA-256 |
|---|---|
| `internal/across/collector.go` | `188e3eff439fe1d1d767494115438c38a46bcc681cf9da9edb52b51e269a9908` |
| `internal/across/runner.go` | `48656554515beefac6dab7d2a8275ef8d8f3e212095a9232a4f175ddedba7ea9` |
| `internal/across/core.go` | `00b623cc36c996d75d76a28c07324980358f69f8c1b337ec85a4e02b8ef1b904` |
| `internal/across/report.go` | `c5c115904796d70d27d2420c8bbcc0a7a0c92dabbbdba6ebb9ab691c41165a48` |
| `internal/across/manifest.go` | `ee7f9e7147b983fc226647fc0ba9c3c8eebd2dab0ebbb8afab74a56fe653e38b` |
| `internal/across/rpc.go` | `36388de883a4127ff1a2631cf5514cfbf2bf4e21d9d0ae8dfa71923df850b1b5` |
| `internal/across/abi.json` | `255ce2fbc91c61774a25d746ac490e3794bef89db26b0b0c7bfdf6a5b2816ea8` |
| `internal/storage/clickhouse/across_schema.sql` | `f5ceb1147277c5c2ee2ae574addaf9d6762dc26ed3ce73364189a56d926bd3c0` |
| `cmd/across-data/main.go` | `87c16d18cafafe58e9d1a2705a68edd3a3c10b6b5d92d4f6fcffd4b1bb89e08e` |
| `config/across-research.json` | `448beae42c03717c4d6a0b3b4ece149e9580aa8ba5555df814bbf52cf2ca609e` |
