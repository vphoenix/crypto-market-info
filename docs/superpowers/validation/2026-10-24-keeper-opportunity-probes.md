# Keeper 机会探测实施验收（2026-10-24 UTC+8）

实现位于 `codex/keeper-opportunity-probes` 隔离分支，基线 `5ea4744b8ef3cf4f75cea65b2d6768407097b86b`。保留原工作区未提交修改。本轮未运行生产DDL、安装二进制、重启服务或发送交易。

## 交付

- [设计](../specs/2026-10-24-keeper-opportunity-probes-design.md)、[实施计划](../plans/2026-10-24-keeper-opportunity-probes.md)。
- 单链持久owner、1:3研究/机会准入、5秒实际请求gate、完成后30秒间隔；候选公平轮转、有限核验重试和事件代际防串单。
- 状态查询成功且可清算才追加只读清算模拟；实际正奖励依靠本次返回/事件/原生付款三证认证。真实负例fixture和synthetic正例明确分开。
- 独立状态与proof表、完整批次和冻结重试认证；Capture、旧成员摘要、冻结125表映射未改。
- Scheduler独立检查点、native v2/legacy typed v3；旧格式仍可读，新格式旧程序拒绝，false/zero、限额和待办可恢复。

详细工作与回归证据：[存储](2026-10-24-keeper-storage.md)、[解析](2026-10-24-keeper-parsing.md)、[调度](2026-10-24-keeper-scheduling.md)。

## 验证

最终相关包普通回归（`go test -short ... -count=1`）退出0：Keeper、runtimecheckpoint、MySQL、ClickHouse、Keeper CLI、storage-migrate。短模式仅跳过本次单独运行的固定序列I/O测试；数据库集成测试另受环境开关约束。[完整输出](2026-10-24-keeper-tests.log)。

`go vet` 覆盖 Keeper、两后端及两个CLI，退出0。两个候选二进制构建成功：`/tmp/justlend-keeper-data.20261024`、`/tmp/storage-migrate.20261024`，未安装。Task2阶段Keeper/CLI全包race通过；最终新增调度、恢复、解析、认证用例race通过（5.919s），见[日志](2026-10-24-keeper-race.log)。独立全变更评审发现并复现一项P2：候选池淘汰后的固定成员部分归还会永久退出研究轮转；已推进已认证Origin并保留原租约生命周期，新增回归通过，独立复核确认修复、无剩余已确认审查项。见[独立审查报告](2026-10-24-keeper-independent-review.md)。最后修复后Keeper/CLI全包短模式回归再次通过（16.823s/0.007s），[日志](2026-10-24-keeper-final-review-tests.log)；相邻调度/恢复race再次通过（3.930s），[日志](2026-10-24-keeper-final-review-race.log)。

已验证的恢复场景包括：旧真实gob与冻结批次、旧摘要跨进程一致性、known false/zero、限速预约重启、损坏scheduler拒绝、旧多待办顺序排空、过时generation不发query/probe、源错误保留真实尝试、尾部块头失败/延迟不丢失此前已知值。

## I/O测量

`TestRuntimeSchedulerFixedSequenceIO` 使用1,226个候选，机会组另有1,226个紧凑ledger成员。两组都是60次相同逻辑请求进度更新、20条三请求检查链；机会组同时模拟每条链的准入与完成状态变化。初始化不计入。仅记录本地检查点Store的逻辑写入和同步调用；不含HTTP/SQL事实、提交WAL、Redis或物理盘忙碌率，不作为生产吞吐/日占用保证。

| 指标 | 无机会调度状态 | 含机会调度状态 |
|---|---:|---:|
| written bytes | 815,151 | 3,250,988 |
| file fsync | 160 | 200 |
| directory fsync | 240 | 320 |
| 当前checkpoint bytes | 4,223 | 4,300 |
| 冷候选对象重写 | 0 | 0 |
| 60次无变化保存写入 | 0 | 0 |

[最终测量日志](2026-10-24-keeper-io.log)。此夹具每条链增加约119 KiB逻辑写入、2次文件与4次目录同步。额外对象只在状态变化时写，保持有界；这些字节是写入量，不是永久磁盘增长。早期仅计算完成时变化的2.04MB数字未包含准入更新，以本表为准。

限速降低研究请求频率，但不代表整机I/O必然低于现场旧175 KiB/s短窗；不能把二者的不同工作负载直接比较。真实总负载仍须在后续部署观测数据库和采集进程一起测量。

## 尚未执行的运行验收

隔离MySQL socket不存在，真实MySQL建表/回读/事务测试明确skip；相应集成测试已实现。ClickHouse也没有在本轮执行真实DDL/roundtrip。schema生成、SQL codec、完整性和历史兼容测试通过，不替代真实数据库验证。

尚未取得真实正奖励constant响应，也未部署观察实际命中率/连续稳定性；不能宣称已可赚取奖励或优于其他Keeper。新模式仅扩大已有候选池内的可验证观测机会，候选上限1,500、索引确认延迟120秒等边界仍在。

最终候选二进制位于 `/tmp`，未安装到服务路径：

- `justlend-keeper-data.20261024`: `e7dacd1a274d58da2e3167797d8d8fb855b3f16ee201922088d7b63a88daf171`
- `storage-migrate.20261024`: `13d7cebd06adeecab68c4b3fa79eb5285e4e8ed418069d8bbd917f3e576ab5bb`
