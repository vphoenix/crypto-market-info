# Keeper 有界机会探测设计（2026-10-24）

用户已要求完成设计并修改程序。本设计落实此前讨论的低写入方案；交付源码、测试及测量，不发送交易、不更换生产二进制或运行数据库迁移。

## 问题与目标

现有固定 cohort 为研究抽样，不代表机会搜索。现场 43 成员仅 27 verified/open，16 成员同版本核验失败三次后长期保留。新设计保留 cohort 审计，但不为无效槽位反复创建空 probe。机会搜索使用同一 owner 和预算，不能另启采集进程。所有金额整数 sun/UInt256、UTC、来源哈希和 latest 前后块范围保持原语义。

## 方案取舍

只替换 cohort 会破坏固定研究口径且仍没有机会优先级；全池高频 RPC 会放大请求和 fsync。选择保留研究 lane、增加有界动态 lane，共享单一检查/核验时隙。用户已明确要求实施，不重复询问执行方式。

## 调度与恢复

- SchedulerState 是独立紧凑状态；保存版本、下一时隙、研究轮转、1:3 lane phase、候选稳定 cursor、generation-keyed ledger。候选池仍最多 1500，不声称全市场完整覆盖。
- 每次只允许一个 seed/probe/rental_state 检查链；完成后至少 30 秒才开下一条，不补发错过轮次。seed 的四次 RPC 也占该时隙。HTTP 重试仍走原额度/冷却；额外所有研究/机会链 RPC（含重试）最少间隔 5 秒，以持久化时间限制实际调用量。事件、身份、报价请求保留原 gate 和 40000/日总上限。
- 每四次 admission 研究 lane 一次、机会 lane 三次；空 lane 可借用。研究只选 verified/open/non-ambiguous。动态 lane 用当前已采候选作公平轮转，verified且上次 liquidatable 者优先，但每四个机会槽至少一个公平槽，避免饿死其他候选。
- 动态项首次必须核验来源事件。复用现有 seed hydration（solid head/block/receipt/body）；重试上限3、退避30分钟，按 origin generation 记录且不因轮出重置。新origin或解析revision可重试。ledger上限为当前候选+cohort的去重集合，不存RawEvent，过期键清理不影响仍在池内generation。
- generation用有序事件身份，和实际租约生命周期区分。旧seed迟到结果、旧事件不能覆盖更新后的pair；发出状态/清算RPC前再次检查generation。清算/全额归还关闭旧状态，新Rent重新准入。Liquidate与Rent/Return同样参与5分钟新鲜度门槛。
- 不通过模拟revert推定关闭。旧研究重试记录保留。启动可恢复旧checkpoint，旧未完成操作先走既有冻结提交语义，新请求也受新共享速率限制。

## 当前订单观测与条件模拟

getRentInfo(address,address,uint256) selector c449d2a9，energy=1，严格解码96字节：当前押金sun、当前rentIndex、bool（仅0/1）。接口输出命名以官方开发者说明为准，不能误当租赁amount。
新链：before head → getRentInfo → 若可信且liquidatable=true则liquidate模拟 → after head。false只表示本次未满足条件，不表示订单关闭。未读取的值保持NULL，失败为unknown，禁止伪造零。尾部块头失败则保留已经严格解码的tuple并降unknown，不能用它认证正收益。来源实现地址/代码hash固定核验；观测绑定node_latest_unpinned，不冒称历史固化状态。

## 定类型存储与兼容

新增 RentalStateObservation 与 RentalStateBatchProof 两张专表，不更改冻结125表映射、旧Capture字段或旧成员摘要。
Batch增加RentalStates和RentalStateProof；新capture_kind=rental_state必须有proof，计数与FactBytes摘要完整核验（包括0行失败批次），新表成员与proof先写、capture最后提交。旧kind不能携带新成员。
观察字段包括capture/index、合约/pair/resource、origin、caller、请求/接收/可用时间、前后区块头、实现身份、API/TVM/status/error、Nullable押金/rentIndex/liquidatable、energy及请求/响应哈希。proof保存capture身份、row_count及digest。
MySQL使用独立扩展mapping/幂等schema安装入口，ClickHouse新建两表；运行检查拒绝缺新schema的机会模式。导出增加rental_states与rental_state_batches，按capture认证。历史七表导出继续可用；新schema未部署时不能静默丢失新成员。同步storage文档。

## 正奖励逐笔认证

取消永久success_fixture_not_certified静态封锁，改为每笔数据认证：已验证实现、API/TVM成功、正32字节返回、唯一匹配合约/renter/receiver/caller的Liquidate日志、合约到caller实际原生TRX支付，金额三者一致；必须有前后头、来源哈希和一致性标记才可提交success_reward。缺失为unknown/incomplete，矛盾为unknown/mismatch，保留已解析数值。
资源委托撤回note（unDelegateResourceOfEnergy等）不可当作付款；只在新的奖励认证路径过滤，不能悄悄改变旧receipt NativeTransfers的持久摘要。测试合成响应明确标synthetic；新真实节点ABI为空，官方说明和已核验相同实现bytecode支持签名/单word返回，不能冒称已获取真实positive常量响应。

## 写入控制

调度ledger与频繁订单状态不放进Cold Candidates。native runtime v2及新legacy typed v3检查点按独立不可变引用保存scheduler；继续读取旧native v1/legacy v2状态，仅改变时重写；无变化不写。纯选择/排序不保存；原请求额度、冷却、未决批次持久屏障不削弱。不新增逐轮历史SQL扫描或原文归档。以固定请求序列测有/无新调度的written bytes/fsync与候选对象hash，生产175KiB/s短窗仅为旧现场参考，不是性能承诺。

## 验收

无效16槽不阻塞、不造空capture；未核验零模拟；300稳定候选全部被访问；同pair新租约与迟到结果；所有链共享限额和重启恢复；旧冻结批次摘要不变；strict ABI/bool/null/error解析；真实转账与资源操作区分；完整批次、来源哈希/头、写重试稳定；单源失败隔离；SQLite等替代数据库不作为MySQL真实验证。运行Go/race/vet相关包、存储兼容测试、隔离MySQL可用时真实schema/roundtrip、固定输入I/O基准。新功能不保证能抢赢其他Keeper。

实现将拟议的128项活跃队列简化为一个持久在途capture和有界generation ledger，避免复制原始事件；稳定cursor保证全池公平轮转。旧未冻结研究链共享该owner顺序排空；旧冻结SQL提交恢复不新增RPC。
