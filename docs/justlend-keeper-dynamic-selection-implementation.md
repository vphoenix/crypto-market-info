# Keeper 动态 40＋10：实现与运行

2026-10-24（UTC+8）。实现依据[设计](justlend-keeper-dynamic-selection-design.md)。代码和离线验证已完成；实际切换时间、进程和数据检查以本文末尾运行验收为准。

## 调度与数据

`watch --dynamic-selection=true` 使用 `dynamic-40-10-v1`，替换固定样本及旧 1:3 lane。每轮最多 50 个不同订单，按四个 focus、一个 explore 交错准入，每次重新选择。集合不足时记录补位；失败和撤销占已准入名额。seed 与 getter/条件模拟属于同一 slot，重启不重新领取。

候选由所有已提交、认证的 typed 索引事件和事实重建，旧缓存/cohort 只作未验证线索。订单与检查状态分别使用 typed `PagedMap`，不按 hash 丢弃第 1,501 个及之后的成员。关闭位置保留，迟到结果按 generation 隔离。同块顺序冲突一起核验；单项最多 32 个同块竞争 origin，超出后隔离并统计。

重点优先最近可信 true，再按押金消耗代理排序。公共 `_rentalRate(0,1)` 每五分钟最多一条三请求链。估计用整数及交叉乘法，不宣称可清算截止时间。实际 getter 返回 `(0,index,false)` 时，旧 amount 不能证明它仍有资源；该订单保留探索资格，暂停零时间风险估计，避免疑似空订单占满重点。false 不被用来宣布关闭。

探索按上次尝试/首次发现时间轮转。失败保留 unknown 和最后有效状态，适用逐订单退避；同 origin/revision 的 seed 最多三个 admission。所有发送意图共享原 40,000/UTC 日预算、源冷却、研究请求五秒 gate 及每链后三十秒间隔。slot 的 RPC 次数随预算预留保存；崩溃在发送边界时，可能包含尚未发出的预留，避免少计。

后台从 `min_timestamp=0` 到首次启用时固定水位，分别分页发现三类历史事件。最多每分钟新增一个历史源页，三类轮换；实时覆盖落后五分钟时暂停。旧在途任务继续恢复，不再继续创建旧的逐日 Liquidate 任务。分页穷尽只标 `all_available_contract_history_provider_indexed`，不冒称独立证明供应商保留了部署以来全部事件。[官方事件 API](https://developers.tron.network/reference/get-events-by-contract-address)允许毫秒最小时间，默认 0，分页须保持过滤参数。

## 专表和恢复

依赖两张租约状态表，另新增四张表：

- `jl_keeper_rental_rate` / `jl_keeper_rental_rate_batch`：租金率及 proof，保留来源 hash、UTC 时间、前后块头和实现身份。
- `jl_keeper_selection_member` / `jl_keeper_selection_round`：准入输入、选择原因、重点/探索、子 capture、RPC 预留数；轮次保存成员摘要、候选/隔离数、最长未尝试/未成功年龄和终止原因。

每轮最多 50 条成员，输入在准入时固定，结束时批量提交 proof 和最终 capture。中途停止从 checkpoint 继续；无准入不生成空轮，不保存全体逐轮排名。原七表、capture 摘要和冻结 125 表迁移 catalog 不变。

MySQL 专用映射使用 8 KiB 压缩页；ClickHouse 使用 `ReplacingMergeTree`。旧、新及预期零成员均认证，重试比较扩展 proof，拒绝额外行和相同 ID 不同内容。MySQL `KeeperBatches` 改为有界一致性快照批量读取全部成员表，避免转换时逐 capture 多次查询。

native snapshot 为 v3，新字段仅追加到原布局末尾；部署 v1 的字段位置和已知零/false 指针有专门恢复回归。订单页、检查页及不可变轮成员引用随 current/previous checkpoint 保存；继续读取 v1/v2，旧程序拒绝 v3。重启用安装的 snapshot 初始化比较基线，空闲保存不重写。检查结果不改写全体订单或旧冷候选，页根描述符的规模成本单列测量。

导出为 `jl-keeper-data-export-v4`，增加四份 typed CSV；原数值保留整数/NULL，观测与估计口径明确。

## 安装与切换

管理命令使用既有私有 socket 和凭据文件；collector 只检查、不执行 DDL：

```text
storage-migrate install-keeper-rental-state --family crypto_market_info_justlend_keeper --target <目标库> <管理连接参数>
storage-migrate install-keeper-dynamic --family crypto_market_info_justlend_keeper --target <目标库> <管理连接参数>
```

正常停止 Keeper，完整备份 runtime、legacy state、原 binary、unit 和配置指纹。保持账户、producer、恢复世代和来源端点，运行：

```text
justlend-keeper-data dynamic-rebuild --config config/justlend-keeper-tron.json --database crypto_market_info_justlend_keeper --state var/justlend-keeper/state --runtime-state-dir var/justlend-keeper/runtime
```

转换只读 SQL、不发 HTTP，按页保存进度，可继续原转换。完成后在原 unit 追加 `--dynamic-selection=true --opportunity-probes=true`。缺 schema、转换未完成或非 MySQL native 模式拒绝启用。

v3 启用后不能直接换回旧 binary 或恢复旧备份重置预算。临时关闭动态调度使用能读 v3 的兼容新版 binary；已有 slot 仍正确收尾，重新启用继续原轮。

## 验证

- 相关 short 回归、动态/恢复/条件模拟 race、相关包 vet 通过；历史补采、零押金和最后一个 slot 等边界有定向回归。
- 隔离 MySQL 实际建表、NULL/false/零/最大 UInt256、冻结重试、空 proof、额外成员拒绝及批量认证通过。ClickHouse 独立测试库实际读写、预写成员后重试、零/false 和冻结冲突通过。
- native 恢复保留全集、round/slot 和发送前计数；时钟推进不破坏分页摘要；第 50 个 slot 的 seed 后仍可继续 getter。
- 1,226/10,000/100,000 个内存摘要各做一次重点与探索选择，单次测量约 14.5 ms / 128 ms / 2.25 s；大集合分配成本明显，不作为生产吞吐保证。
- 1,226 订单、十次检查状态更新的固定序列，约 350,710 字节写入、30 次 file fsync、50 次 directory fsync；旧冷候选/订单页重写和空闲写入均为零。该测量不含 SQL/网络/WAL，不代表永久增长或整盘负载。

## 运行验收

切换与持续观察完成后在此记录实际结果。getter 的可信 false、未知和失败必须如实保留；程序正常上线不等于当前一定存在可获奖机会。
