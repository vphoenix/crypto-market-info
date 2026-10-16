# 限定启动准备工具

2026-10-14 范围纠正：下述 59 表流水线是旧的整表预置流程，已经停止，不再作为上线前提；`startup-scope-review.json` 存在时接续器会拒绝重新运行。当前按[实际启动调用](../../../docs/redis-mysql-migration/startup-dependencies.md)选择记录。`tools/storage/prepare-startup-dex.py` 仅复制审计确认的 DEX 恢复成员；已复制的身份和独立低频库继续使用，其余历史在切换后迁移。`recovery` 保留内部启动恢复检查，不要求所有旧表先达到历史验证完成状态。

本工具只执行用户已授权的启动依赖导入和恢复验证，不管理服务、不激活 producer、不打开完整历史。

1. `tools/storage/snapshot-bootstrap.py ABSOLUTE_PREPARATION_DIR` 将保守清单中的 59 张表复制到独立 ClickHouse 快照库。互斥锁禁止重叠运行；完整清单封存后才能导入。快照保留原引擎和版本语义，排除全部高频盘口及对应提交表。
2. 编译 `./tools/storage/bootstrap-prepare`。`register` 只登记 2026-10-13 已明确放弃的六个 DEX 和两个 Keeper 旧批次；核对原提交摘要和声明，并保留完整定类型原行。正常查询完整性不变。
3. `copy` 先计划全部固定范围，随后单连接逐表导入；`verify` 独立重读原字段并通过正常恢复读者检查闭包；`check` 核对逐表固定快照行数、验证状态、缺口、producer 未激活及盘口表为空。
4. `recovery` 以 query 账户只读验证主服务、Reserve、LST、Keeper 的关键读取及原状态身份。`recovery-lowfrequency` 允许在主库仍导入时先检查已完整复制的 Reserve/LST/Keeper，并单独生成低频结果；不能替代最后的完整恢复检查。指定 `CMI_BOOTSTRAP_PREPARATION` 后，Across/LST 的 `TestPreparedMySQLStartupRecovery` 调用实际内部恢复路径：不创建网络读者、不写正式数据库，LST 仅修改准备目录中的状态副本。

目录须为绝对路径且保持私有。源地址和目标 socket/账户固定为本机已准备实例，密码从仓库外私有文件读取。59 表导入可能耗时较长；源错误或新发现的成员不一致立即退出并保留检查点，不扩大既有缺口集合。`copy` 可在相同已封存清单及 run 上重跑，不能换清单后沿用 run。

复制和独立字段校验的 MySQL 有界读取使用两分钟 socket 等待，最多 64 个完整主键组一段、500 行一个普通事务；CLI 可显式指定 `--target-read-timeout 2m`，大于两分钟拒绝。生产 collector 和实际启动恢复检查仍使用原 30 秒等待；源读取、字段摘要、事务与检查点规则保持。

单表验证通过不等于服务已切换，登记缺口也不等于历史完整。切换前必须排空旧端并补齐快照后增量；新服务暂存单元和二进制应另行检查并按用户后续授权应用。

本次长任务可由 `tools/storage/finish-bootstrap-preparation.py ABSOLUTE_PREPARATION_DIR --wait-pid OWNED_COPY_PID` 接续。它先等待当前拥有的复制进程退出，再在同一封存清单和 run 上执行 copy/resume、verify、check、recovery，失败即保留证据并停止；互斥锁禁止两个接续器并行。结束后恢复原 512 MiB MySQL buffer pool，阶段写入 `workflow-status.json`。该脚本不管理服务、不激活 producer，成功状态也仅表示数据验证完成，仍须最终运行状态和报告检查。等待阶段被中断时若原复制仍运行，暂不调整其资源，由操作人员停止复制后恢复。
