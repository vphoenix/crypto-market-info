# 0006：期权采集设计独立审核

日期：2026-09-21。审核者：独立 Agent `options_design_reviewer`。范围仅为设计审核，不代表生产实现、部署或盈利验证。

历史审核对象为ARB-0009的R1–R3版本，最终[R3原文已归档](../docs/arbitrage/legacy/arb-0009-options-collection-design-r3.md)。R1 文件 SHA-256：`408cb1bc1824f902386f540b1b708b85d751480b5d3f3700e6adc38605c23864`。

范围更新：用户随后要求按“采集数据供日后分析”简化未实现部分，现行要求见[R4](../docs/arbitrage/strategies/arb-0009-options-collection-design.md)。下列问题与结论保持为历史记录，不表示本Agent已审核R4，也不要求继续执行R3的部署阶段。

## R1 结论

暂不通过，需修改后独立复审。专项空书/有符号价格模型、每秒 10 档、全深序列校验及隔离试采的总体方案可行；下列事项仍会使实现产生语义冲突、启动等待或不确定的历史观测。

### R1-01 [P1] 不可变规格与动态交易规则尚未真正分开

位置：设计 §3.2、§4.1、§7.1。

`derivative_contract_spec` 以 `instrument_id` 为唯一键且声明不可变，却含 `min_trade_amount`、`source_tick_size` 和 `tick_bands`。与此同时，身份摘要及换 ID 条件主要覆盖经济定义/编码单位，§4.1 又要求在固定编码单位下保存交易规则改变前的遗留挂单。一次真实下单 tick 或最小量调整后，要么同键写不同内容违反不可变约束，要么沿用旧规则，让历史查询无法判断当时交易约束。

规格内的 `source_payload_hash` 也须明确是首次定义证据，而非每轮完整目录响应的当前 hash；一次新增其他合约就可能改变整份响应 hash，不能因此改写每条旧 spec。

整改要求：独立定义有生效/知悉时间的交易规则 revision，包含 tick bands、最小量/增量及来源证据；将不可变经济/编码 spec 与规则观测分开。catalog/member、分钟或每秒质量必须能定位当时规则，规则在分钟内改变也不能被一个分钟级旧引用掩盖。补充“同 instrument 的 tick/min amount 改变”“同定义不同源响应 hash”“旧细价挂单仍存在”的验收。

### R1-02 [P1] 秒边界屏障缺少接收侧完整水位

位置：设计 §6.4、§8.1。

“屏障与应用更新串行化”只保证不会同时改书，尚不保证所有已接收且 `received_at <= T` 的消息都排在屏障前。接收线程先记时间、尚未入队时，定时线程可能先投递/执行屏障；也可能工作线程还有已接收消息未应用。这样会冻结一个漏更新的状态，并错误标成完整有效秒。

整改要求：明确接收、入队与每秒 watermark/barrier 的排序所有者。屏障必须携带该连接截止时已接收消息的序号水位，只有此前消息全部应用且无错误后才能完成；定时屏障不得越过水位前消息。无流量连接也需可证明的水位推进路径；250ms 截止失败应明确无效。补充“接收已标记 T 前时间、入队被抢占”“静默连接”“多个连接不同积压”的并发测试。

### R1-03 [P1] 缺少首次 quote 引导与已提交计划的启动闭环

位置：设计 §2、§5.1、§5.2、§12。

重点集合依赖 future BBO 选行权价，但当前描述的订阅目标由 universe member 决定；首次无 universe 时从何处获得这些参考 BBO 没有定义。§5.2 的“未准备好保持旧计划”在首次启动也没有旧计划可保持。若全链订阅分批耗时较长，早取得的静态参考 BBO 还会超过 30 秒门槛，使首次选择一直未就绪。

整改要求：给出确定启动顺序，例如先提交仅 quote/index 的 discovery revision，持续记录明确缺失，再取得参考并预热完整 L2 组，最后在分钟边界切换。明确预热目标计入容量但不属于已承诺的 book 采样范围；新 revision 的准备、持久化、未来生效边界和激活前失败顺序。30 秒“确认”须注明来自何种合约级证据，不能用连接心跳给无序列 BBO 续期；没有确认可保持 pending，但应能通过受限重订/定向核对恢复。

### R1-04 [P2] 辅助观测类型与缺失/静默编码不完整

位置：设计 §6.1、§7.4、§8.1、§9。

`option_analytics_observation` 把 `underlying_index` 包含在“均 Nullable Decimal”中，但该字段是名称/标识字符串。文中要求每分钟包含明确缺失的 analytics/index 状态，表契约却未完整定义“从未收到”“本采样点没有新消息但同代次持有”“断线后的旧值”的字段、必需行数和 NULL 组合。`AnalyticsAtOrBefore` 的 `max_age` 也没有说明检查真实来源/接收时间还是人为每 5 秒重写的 sample_time，后者会使静默旧值看起来持续更新。

整改要求：将名称/ID、Decimal、单位版本、时间分别列明；定义每个计划采样点必写缺失行还是通过已提交缺失位图表达，并列出 observed/held/missing/disconnected/invalid 的取值约束。index 与 estimated price 各自状态、时间和 NULL；analytics 静默持有也必须保留原始 provenance，max_age 不得由新采样时间刷新。补充完全无源消息和缺少单字段的回放验收。

### R1-05 [P2] 范围过滤和目录完整性规则有冲突

位置：设计 §3.1、§4.2、§5.1、§7.1。

USDC instrument scope 会先筛掉非 BTC/ETH，而三个 currency 的 `get_combos` 返回含其他资产/期货价差的组合。§3.1 随后要求每个组合腿都解析到同批已知定义，可能把明确不在首期范围的组合当作整批目录错误。未知数量单位的 combo 又被要求“保留完整目录”，但 spec 注册要求数量单位已验证，这两种状态缺少明确落点。

整改要求：先定义原始 scope 响应完整性，再按明确可复现的产品/资产规则分类 accepted、out_of_scope、unsupported；只有 accepted 对象才进入 instrument/spec 的严格可采集验证。保留排除对象的源身份、理由、scope hash/计数，不强制给未解析结构分配可采行情 instrument ID。真实目标中的未知腿或缺失规格仍应阻止该对象/相关组进入有效计划，不能通过“过滤”掩盖。

### R1-06 [P2] 公共 instrument 数量语义及交割类型映射缺字段

位置：设计 §3.2、§4.2、§5.1、§7.1。

amount 的文字说明正确，但新 instrument 的 `contract_multiplier` 究竟写 `1` 还是 source `contract_size` 没有定死。现有通用查询会以 multiplier 换算，使用后者会把已是基础币的 amount 再乘一次。新 derivative spec 也没有明确的 future linear/inverse 字段，§5.1 却需要据此匹配对冲品种；不能只给 option payoff enum 或解析 symbol 推断 future 类型。

整改要求：明确各产品 `instrument.contract_multiplier` 的赋值及禁止调用的旧通用换算路径，原始 contract_size 单独保留；在 derivative spec 中存可验证的 instrument_type/quantity_semantics，未来来源增加时仍按类型分派。用现有 raw fixture 验证 BTC 期权 amount=0.1、contract_size=1 与 USDC 线性交割合约的数量，不因字段名“合约乘数”重新解释 native amount。

## 已核实的设计基础

- 全深 book 首包包含所有价位，随后通过 `prev_change_id` 链校验；grouped schema 没有该前驱字段。使用全深内存、每秒截取 10 档符合项目约束。[全深协议](https://docs.deribit.com/subscriptions/orderbook/bookinstrument_nameinterval)、[grouped 协议](https://docs.deribit.com/subscriptions/orderbook/bookinstrument_namegroupdepthinterval)
- `incremental_ticker.<instrument>` 首 snapshot 后只发变更字段，最多每秒一次；R1 首期选择完整 ticker，可以暂时避免未验证的嵌套 patch 语义。[官方增量 ticker](https://docs.deribit.com/subscriptions/market-data/incremental_tickerinstrument_name)
- combo 腿比例和交易规格来自不同对象；目录支持 `option_combo`，`contract_size` 与数量增量不能互换。[合约字段](https://docs.deribit.com/api-reference/market-data/public-get_instruments)、[combo 结构](https://docs.deribit.com/api-reference/combo-books/public-get_combos)
- 现有代码拒绝空边与非正价格，并把回放 SourceTime 设成查询秒。专项模型和专用 replay 比放宽旧表更易保住已有 10/50 档历史不变量；仍需既有回归测试，设计审核不是测试通过声明。
- 每分钟批次先数据、后 commit，并按固定 batch 精确关联，配合单 writer 与同步 insert，可以提供所述业务可见性；不等于 ClickHouse 跨表事务。应测试 commit 超时重试，并确保查询缺表行时返回 incomplete 而非无变化。

## 后续复审

当前仅完成 R1 审稿，尚未判定通过。设计修改后，独立审核者重新读取实际文件，逐项记录整改证据与剩余限制；不能仅凭作者口头确认关闭问题。

## R2 独立复审

独立重新读取完整 R2 文件，SHA-256：`5fbc81482a1230a8de7bb612bf9d6c8104a50b62bfccc68d2063438b9c7baf69`。R1 的六项问题均已实质整改，以下状态仅适用于本次读取的文件。

| 问题 | 复审结论与实际设计证据 |
| --- | --- |
| R1-01 动态交易规则 | 关闭。§3.2/4.1/7.1 新增独立 `derivative_trading_rule`，经济 spec 只保留首次定义证据；同 instrument 规则变化可逐秒引用，经济/编码变化才换身份。`known_from` 是解析完成的证据时间，独立 source commit 成功后再发布运行时规则，无需把尚未发生的提交时间预写入事实行。 |
| R1-02 接收水位 | 关闭。§6.4 将接纳时间、序号和非阻塞 FIFO 入队放入同一排序临界区；timer 必经同一 ingress，T 后消息入队前补 T 屏障，工作线程确认水位后冻结，250ms 超时不补写有效槽。接纳时间定义和内核收包时间已区分。 |
| R1-03 首次引导 | 关闭。§5.2/5.3 明确 discovery-only revision、future quote/受限 REST 核对、完整组预热及未来分钟持久化确认；首次失败保留 discovery，预热消耗完整预算，确认超时不激活，实际采用 revision 由分钟 commit 判定。 |
| R1-04 辅助缺失编码 | 关闭。§7.4 将 underlying_index 改为 Nullable String，并规定12个 analytics 行、60个 index 行及quote槽；来源和接收时间不随 held 刷新，missing/disconnected/invalid 清空市场值，index/estimated 独立，max_age 检查真实时间。 |
| R1-05 目录范围 | 关闭。§3.1/7.1 先核实九 instrument 与三 combo 原始 scope，再分类 accepted/out_of_scope/unsupported；无 instrument 的排除/不支持证据仍有目录行，未知目标组合不能进入有效计划。 |
| R1-06 数量类型 | 关闭。§4.2 固定本链路 multiplier=1、单列 source_contract_size，future/option均有linear/reversed及native量纲；给出了BTC option、线性future、inverse future的精确换算样例。 |

额外检查：§8 的分钟 hash 排除了自引用字段，批次与规则引用的可见性未发现环状提交依赖；规则/spec 可以先作为孤立定义登记，规则独立 commit 后再使完整 catalog 可见。固定行数、身份集合及 delta_bitmap 能区分真实无变化与丢失差量。单writer约束明确只支持单宿主机；全链、预热过渡和全深状态均计入资源门槛，不把设计默认值当实测容量。

### R2-01 [P2] 质量内的规则与生命周期也必须按 T 截止

位置：设计 §6.4、§7.1、§7.3。

WS book 连接的水位已经明确，但每秒质量还读取交易规则及 `market_open`，其更新可来自独立规则 Runner 或另一生命周期连接。若规则在 T 前解析、T+100ms 才提交发布，而书的 T 屏障在 T+150ms 处理，冻结时读取共享最新规则会把 T 后运行时才采用的引用写进 T。另一连接 T 后到达的 lifecycle 事件也有同样问题。区分 known_from 与 commit 解决了时间循环，但仍需定义实际发布事件的采样截止。

整改要求：规则运行时发布记录单调顺序及发布时刻，冻结 T 时只能选择已提交且 published_at<=T 的版本；不得按 captured_at 读取共享 latest。生命周期状态按自身接纳时间及水位选取 T 前状态，并和书版本一起冻结；水位未就绪则标相应 metadata/state uncertain，不能使用 T 后值。发布时刻可仅用于运行时版本选择和诊断，实际历史已由每秒 rule ID 引用表达，不要求循环依赖提交时间的写入协议。

R2 结论：R1 问题已关闭，剩余 R2-01 需补清后再给最终通过结论。无生产代码或测试被本审核修改或运行。

## R3 最终独立复审

复审对象为实际 R3 文件，SHA-256：`d6bdfbe05c9f252cd33b11409cb26083213d5a5ee8ec2e04c8071581b5349f73`。重新检查 §6.4、§7.1、§7.3、§11 及其与已审 R2 的一致性。

R2-01 关闭：规则/catalog 在独立 commit 确认后才产生 control 发布事件，T 秒只能采用 `published_at<=T` 且已生效的版本；known_from 不能代替运行时发布时间。生命周期连接先完成自身 T 水位，采样器合并书、control 和 lifecycle 同一截止版本，不在较晚 captured_at 读取共享 latest。水位不足的状态显式标 metadata_uncertain、market_state_known=0，不能生成看似有效的 replay 秒。新增质量字段和并发验收用例与此规则一致，发布时间归档没有引入事实行与提交时间的循环依赖。

最终结论：**设计审核通过，当前没有未关闭的 P1/P2 设计阻塞项。** 可据 R3 开始模型、离线回放和协议 fixture 实现。用户要求的新数据每秒买卖各 10 档、旧 50 档完整回放，以及公开数据采集边界均已保留。

通过范围限于设计的内部一致性与所核实协议基础。combo 真实 WS/数量单位、公共限频、截止屏障性能、分表压缩占用和 typed 规则解析仍是主设计明确列出的实施验收项目；该结论不表示这些实测已完成，也不表示允许跳过既有回归或直接上线。本审核仅新增/维护本审核记录，未修改生产代码、数据库或运行服务。
