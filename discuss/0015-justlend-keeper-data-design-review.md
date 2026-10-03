# JustLend 清理 keeper 数据采集设计审核

日期：2026-10-02。审核者：独立 Agent `/root/keeper_design_review`。

状态：已复审通过，初审 P1/P2 均已关闭。仅审核数据设计；审核者未实现采集器，未建生产表、部署或交易。

## 首版边界

一个 TRON 能源租赁代理、一种 resourceType、一个独立 Go 命令、定类型 ClickHouse 小表与原始响应归档足够。已有 SDK/HTTP 重试、TRON 地址处理和轻量链头读取可复用，不需要全链索引、消息队列、策略引擎、历史 TVM、私钥或自动下单。

设计已经区分“协议实际付了多少钱”“获奖地址付出多少资源”“我们能否事先看到机会”。这三个问题不能互相替代；成功交易成本也不能替代所有机器人失败成本。首版以有限样本判断是否值得继续投入，合适，但采样不能系统性跳过快到期订单。

## 初审问题

### P1-1：模拟 caller 与租单身份冲突会系统性制造假阴性

本轮 `simulation.raw` 的 API `result.result=true`，TVM `transaction.ret[0].ret=FAILED`，revert 表示不允许清理自己的订单。请求 caller 与 receiver 相同。它证明了只看 HTTP/API 成功不够，也表明“任意已激活公开地址”不是充分条件。

最小修正：固定两个与研究者账户无关的已激活公开 EOA，模拟时排除与 renter/receiver 相同者；冲突单列 caller_conflict，不归入未到期或不可清理。直接 liquidate 的返回 reward、模拟事件、内部转账和 TVM 成功需一致，缺证据保持 unknown。无需创建账户或获取私钥。

### P1-2：持续只选最近活动的 50 个身份可能在到期前淘汰订单

RentResource 包含续期和增押金。始终取“最近出现”的未关闭订单会不断偏向新近补款者，旧单可能在最接近清理时被样本上限排除。每 30 秒轮询 5 个、样本 50 个，普通身份约 5 分钟才重看一次；零成功可能只是选样和时间分辨率造成。

最小修正：固定一组跨活动时间段的有界样本，定义明确的入选、退出和替换规则，已入选活跃身份不因新租单出现而静默淘汰。保存选择/淘汰原因、候选和排除数量、实际复查间隔。无需复杂到期预测器，但报告只评价该公开数据与采样策略；没有成功 probe 不能否定整个业务。短时优先队列只提高已看见机会的分辨率，不能纠正首次发现前的漏采。

### P2-1：30 日三类事件逐笔补收据可能把候选建设做成主体工程

完整的 Liquidate 奖励、收据和付款核验是历史核心。为建立最多 50 个 live 候选而全量抓取 30 日 RentResource/ReturnResource 并逐笔拉收据，缺少必要的请求量论证；高频租赁行为可能远多于清理事件。

最小修正：历史 Liquidate 单独完整抓取；候选种子使用有界 Rent/Return 回扫和明确预算，覆盖分开报告。候选生命周期可复用已取得事件，ReturnResource 的剩余 amount 大于 0 是部分归还，不能一律关闭。窗口之前建立但窗口内没有活动的订单继续标未覆盖。不需要为了凑全量候选做全链扫描。

### P2-2：严格解析应拒绝错误语义，不必拒绝无关新增 JSON 字段

“未知字段变更报错”若解释为供应商 HTTP 响应新增任意字段即停采，容易造成无意义中断。

最小修正：必需字段缺失、金额格式/范围错误、ABI 长度错误、未知事件签名或已固定实现改变必须失败或 quarantine；无关新增 JSON 字段保存在 raw 后可忽略。不要把全部来源 schema 做成僵硬的手写镜像。

### P1-3：订单生命周期缺少同区块内交易执行顺序

首份 DDL 的事件包含 block_number、tx_id 和日志下标，但 tx_id 字典序不是交易执行序。相同身份在同一区块可能先关闭后重新租赁，也可能相反，顺序会改变候选状态及所属租期。

最小修正：在已用于验证交易成员关系的区块响应中取得 transaction_index，不增加请求或表。生命周期按 block_number、transaction_index、已核实的同交易事件顺序处理；无法确定时标 unknown，不用 tx hash 排序。仅 indexed_only 的候选数据不得混入已收据核验的 canonical 奖励计数。

### P2-3：费用观测失败与本地可用时间需要在 DDL 表达

首份 cost 表允许 error，但 received_at 非 NULL；没有响应的 timeout 不能填本地失败时间冒充来源响应收到时刻。报价只有源/收到时间而没有解码完成时间，也不足以检查它在一次 live probe 作出判断时是否已可用。

最小修正：收到时间 Nullable，保留独立 available_at；实际开始了请求才填写请求时间。行情/费用引用只使用当时已经 available 的观测，另检查源时间年龄。失败可用 manifest hash 保存本地错误证据，不虚构来源 payload 或链头。

## 已核验并应保留

- 当前代理的 `implementation()` 返回一个可解码地址，可直接记录实现地址与代码 hash；不需要套用通用 EVM 代理存储槽。当前官方 ABI 与实际返回不一致：rentals/getRentInfo 的只读响应都是三 word，其中 getRentInfo 官方文档及 ABI 是两 word。首版不依赖自行解释这些字段或复制计息状态机，直接 liquidate 模拟更简单。
- 官方文档支持非只读函数的无广播模拟。`/wallet` 是节点请求当时可见状态，可能包含正在处理的区块及 pending 变化，前后链头相同仍不能当成固定区块末状态；初稿的 `node_latest_unpinned` 语义正确。
- Liquidate 奖励只能取 liquidateFee，usageRental 和 sendBack 不属于 keeper。多事件共享一次整笔费用；混合 helper 交易不能按事件数量随意分摊成直接调用成本。原生内部转账应匹配付款方、收款方与总额，并排除 rejected。
- fee、energy_fee/net_fee、energy_usage_total 与 penalty 必须按协议语义避免重复相加。实际 TRX burn 小并不证明能量免费；全燃烧是保守场景，租能可承受单价是另一条研究输出。
- 不从成功事件假造失败尝试覆盖，不把统一当前币价重估叫历史已实现利润，不把补采链上时间叫本地 first_seen，不把多次成功 probe 累加成多笔机会。
- 供应商分页完整不等于独立证明链上没有漏事件。完整和不完整窗口、迟到事件复扫、固化 hash 冲突及有条件零值应保留。
- 项目已有 `internal/yield/tron/client.go` 的 `getblock` + `detail=false` 轻量固化链头读取。watch 应优先复用这类轻量方法；只有为验证交易成员关系才拉完整区块并缓存，不必每次 probe 前后归档大块完整交易列表。
- cohort 成员和选择原因可放入已有 evidence manifest 并由 cohort_id 引用，避免为了重启恢复新增表。跨 capture 同一 canonical 身份的不可变事实冲突应明确失败，不按最后一次采集覆盖；原始页包装不同导致 hash 变化则不算事实冲突。

核验来源：[JustLend 官方能源租赁文档](https://docs.justlend.org/developers/energy_rental/)、[TRON 无广播模拟接口](https://developers.tron.network/reference/triggerconstantcontract)、[TronGrid 事件分页](https://developers.tron.network/reference/get-events-by-contract-address)。实际接口原始证据位于 [本轮只读探针](../research/2026-10-02-keeper-design/)。

## 复审

已重新读取修订后的完整正文及五表 DDL，没有仅按作者反馈关闭问题。复审所读版本 SHA-256：

- 正文：`32ecd52cca9d882c2703b4cc87b61bb55ce58f1dd50cfd5c4463cecbbbd137f2`（审核时首页仍写等待审核；随后仅更新审核状态不改变以下技术结论）。
- DDL：`3c49163efe9cc2cc90040104a8c19388dce4290ee6f7c1a3bc4bebca8ac609a0`。

逐项结果：

1. **P1-1 已关闭。** 使用两个经 getaccount 核验的公开普通地址，排除 renter/receiver；均冲突时只记 skipped。API 成功和 TVM 失败、返回数据、reward 日志/转账一致性分别校验。成功 reward fixture 尚未取得，被明确列为实施验收条件，失败样本不会伪装成有利可图的模拟。
2. **P1-2 已关闭。** 首版按已发现活动年龄分层，固定最多 50 个身份；旧样本不因新单到来被淘汰，只有已验证关闭才补位，cohort 和选择原因由现有证据 manifest 保存并恢复。实际轮转间隔、未发现/排除身份和短机会漏采有明确口径，零成功只评价该观察方式，不否定全市场。离散成功点不再被称为中间全程可执行。
3. **P1-3 已关闭。** 事件增加 transaction_index，receipt_verified 要求交易及日志次序齐全。同块生命周期按真实顺序处理，indexed_only 不参与精确奖励或同块状态推导；部分归还、新租期和状态未知分开。
4. **P2-1 已关闭。** 30 日完整主线只抓 Liquidate 和去重收据；Rent/Return 三个年龄层各最多 10 页，总计最多 60 页，仅入选身份补必要证据。coverage_scope 区分清理事件、抽样租单和已选任务，不把候选上限写成完整覆盖。
5. **P2-2 已关闭。** 必需字段、数值范围、ABI 和实现身份严格校验，无关新增 JSON 留存 raw 后可忽略。
6. **P2-3 已关闭。** cost received_at 与没有来源响应时的 payload_hash 可空，另有本地 available_at；价格、费率引用必须先可用再检查年龄，失败保持 unknown。完全跳过的任务记录 capture，不能填造来源时刻。

另外核对了提交与读取语义：每个有界批次独立 capture_id，真实 HTTP 尝试有独立证据，数据库重试才复用冻结内容；所有事实按同一 capture_started_at 分区。报告校验四类成员计数/摘要后才使用，跨 capture 的 canonical 事实去重，金额/地址/已知资源量冲突隔离，NULL 补全与 raw 包装改变分别处理。初始未提交事实不能被 report 当成覆盖完成。

费用口径也一致：奖励与整笔 burn、非 burn 资源、代付关系、未知失败尝试分别报告。全自付燃烧只是一种情景；资源采购盈亏平衡式使用相同 sun 单位且对未知/零 Energy 不作除法，不把开发者补贴当作必得。手续费和参数未知不填零，不从公开模拟生成实测胜率。

独立重新计算 15 份来源 raw/meta 的 SHA-256，全部匹配；读取 MarketProxy/MarketG1 和两个失败模拟响应，确认实际输出与正文一致。轻量最新/固化链头均约 542 bytes，省掉了循环保存完整区块的流量。另读取 [官方 protobuf](https://raw.githubusercontent.com/tronprotocol/protocol/master/core/Tron.proto)，确认 Transaction.Result.code 的 SUCESS=0 与 contractResult.SUCCESS=1 是不同枚举，不能混用；成功默认值也不能覆盖实际 revert 或缺失输出。

作者已经在隔离 `/tmp/justlend-keeper-design-ddl-r1` 用 ClickHouse local 接受完整 DDL；审核者查看了该目录实际生成的五表 ATTACH 元数据，字段、约束、ReplacingMergeTree 和冻结分区与方案一致。此检查仅验证 DDL 被接受，不替代实现后的金额往返、部分提交恢复和真实采集覆盖测试。验证详情见 [记录](../research/2026-10-02-keeper-design/validation.md)。

**结论：在首版有限样本、只读采集和条件成本分析范围内通过，没有仍阻止最小程序实现的设计问题。** 保持一个命令、五张小表、固定采样和离线报告即可，不必追加全链失败交易索引、历史 TVM、租单计息器或资源商执行接口。设计通过不表示 keeper 已适合实盘赚钱；后续采集仍需回答实际资源成本、成功可见性和可得份额。
