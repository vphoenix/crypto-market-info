# Keeper 动态 40＋10 验证（2026-10-24）

实现见 [运行说明](../../justlend-keeper-dynamic-selection-implementation.md)，设计见 [40＋10 设计](../../justlend-keeper-dynamic-selection-design.md)。

- 相关 short 回归最后一次在旧字段布局兼容修复后全部通过（Keeper 31.730s、runtimecheckpoint、MySQL、ClickHouse、两个命令包）。
- 动态/恢复/条件模拟 race 通过（280.235s）；新增历史/恢复/记账定向 race 12.919s 通过。
- MySQL 独立实例 UUID 按项目已有工具验证；新旧扩展实际安装、NULL/false/零/最大 UInt256、冻结重试、批量认证、额外成员拒绝通过。ClickHouse 独立临时库实际重试与往返通过（53.734s），测试库由测试清理。
- 新增部署 v1 字段位置/已知零/false 指针恢复回归；首次转换入口提前调用及字段前插问题已修复。
- 旧来源的发现时间修正发生在版本顺序过滤之前；不能用 capture UUID 顺序替代源 available_at。全表聚合及随机 join 的校正尝试触及生产语句时限，最终采用已经验证的逐页批次认证重放，不保留失败聚合路径。
- 离线 1,226 / 10,000 / 100,000 项两类选择单次约 14.5ms / 128ms / 2.25s，仅为本地调度输入，不代表 RPC 模拟耗时。
- 1,226 项十次状态更新最后测得 350,710 bytes、30 file fsync、50 directory fsync；冷候选/订单页重写 0、空闲写入 0。不含 SQL/网络/WAL 或全天增长。

生产切换已完成，04:09:37（UTC+8）原 Keeper unit 已启动 `ad8f540cc` 对应发布，实际进程 SHA-256 与发布文件一致，保持 enabled；04:21 再核验自动重启为零。六张新增专表均已实际安装为 8 KiB 压缩页。完整成员转换及发现时间校正重放 26,794 个非空事件批次、恢复 1,447 个订单身份；UTC 日 2026-10-23 的预算 24,753 前后相同，没有发 HTTP。

实际首组准入为四个 `focus/pressure_verified` 加一个 `explore/oldest_waiting`，5 次 getter 均完成；探索还实际完成来源事件核验。只读正式客户端认证 5 个 rental_state、2 个 rental_rate、1 个 events 批次，成员和 proof 一致，成功记录必要的区块与来源证据缺失为零。5 个 getter 均为可信 false，没有生成正奖励模拟；这不证明可赚取奖励。

04:21:27 候选集合 2,268 个身份、2,000 个未标记关闭，证明不再受旧 1,500 上限限制。全历史补采仍在推进，不宣称完整覆盖；后台核验有 missing_block_header 的 partial，保留实际失败。runtime 恢复错误为零，完整 50 槽轮次仍执行中，尚未写入封口轮次表。该验收为约十二分钟和首组 4＋1，不能替代整轮或全天稳定性。

Keeper 进程约 375 秒采样平均写 306 KiB/s、读 30 KiB/s，运行目录约 3 MiB；包括实时/历史任务，非新旧对照，不包含数据库和共享磁盘。完整部署路径、哈希、准入时间与边界见[运行说明](../../justlend-keeper-dynamic-selection-implementation.md#运行验收)。隔离 MySQL 测试实例已正常停止，生产 MySQL、Redis 和其他采集服务未因本次部署重启。
