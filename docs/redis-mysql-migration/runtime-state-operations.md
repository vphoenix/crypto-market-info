# 运行状态工具与恢复协议

本页描述源码中的运行状态接口。实际转换、二进制和上线状态以[实施记录](../../research/2026-10-15-runtime-state-improvement/report.md)为准；没有转换清单与部署记录时，不能视为已上线。

## 本地格式

`internal/runtimecheckpoint` 只负责本地持久化，不证明 MySQL 提交。领域对象使用定类型编码，保留二进制身份、精确整数/十进制、UTC 时间和已知零值指针；不使用通用 JSON 事实表。

运行目录包含 `checkpoint.current`、`checkpoint.previous`、不可复用的 `issued` 序号、按 4 MiB 分段的原操作记录，以及按内容摘要命名的冻结对象。原操作必须先完成文件同步、原子安装及目录同步，随后才能交给数据库。当前与上一份 checkpoint 的全部引用和恢复尾部均保留；损坏或缺失的已发布记录拒绝恢复。

大量收据任务以原身份分页保存；每页至多 128 项。调度索引保留缺 transfer 优先、最早尝试和身份排序，工作线程只取得选中的任务。分页对象仍属于 checkpoint 的恢复引用，不是另一份永久事实日志。无变化的页不重写。原始 payload、历史事实和 checkpoint 的含义不同，不能凭状态文件存在宣布历史完整。

绑定包括服务、目标数据库、server UUID、配置摘要、schema 摘要、状态版本、producer generation/holder，以及数据库恢复世代。世代文件放在数据库备份和数据目录之外；普通启动不能创建或轮换它。

## Across 显式入口

命令使用与对应服务一致的 backend、producer、manifest 和来源配置。涉及状态转换/核验/回退的命令必须在该服务停止、同范围其他写者停止或被 fencing 后执行；保留现有 pending 和真实暂停时间。命令持有进程锁并核对 generation，不能把文件锁当作数据库 fencing。

| 命令 | 行为 |
| --- | --- |
| `across-data runtime-convert` | 对冻结输入按主键分页；在 `.conversion` 保存进度，用旧恢复逻辑对照；校验成功后原子发布状态目录。失败不发布半份状态 |
| `across-data runtime-verify` | 恢复原身份未决操作，核验原事务，再输出当前状态；可用 `--runtime-export` 导出完整当前恢复链 |
| `across-data runtime-rollback` | 必须指定新的 `--runtime-export` 目录。先处理未决操作，备份当前链，再以当前数据库执行受控旧逻辑重建核验 |
| `across-data watch` | MySQL 模式下需要已核验状态；不自动扫描历史或从当前链头重置起点 |

共同参数：`--runtime-state-dir`（默认 `var/across/runtime`）、`--runtime-epoch-file`（也可通过 `ACROSS_RUNTIME_EPOCH_FILE` 设置）。`--legacy-history-recovery` 只供明确安排的旧逻辑 watch 恢复使用，不是普通启动的自动兜底。

`runtime-verify` 会恢复已接纳的原操作，因此不是纯只读检查。只检查本地格式时使用下述独立工具。回退不能用升级前状态覆盖当前状态，也不能仅替换旧二进制后让它忽略新未决操作。不能恢复的停机实时观测仍记为缺失。

Across 转换保留已按存储故障协议撤回的日志登记：只有 `committed=false`、具有完整区块锚点且原因以 `storage_integrity_loss: ` 开头的记录，才进入转换报告的 `WithdrawnDeclarations`。原身份、范围、成员声明和损坏原因完整保留，不进入有效覆盖或事实恢复。普通未提交登记和已提交但成员不可读的批次仍阻止发布。旧版转换中断后，可从已冻结并核对摘要的登记页恢复这种分类，再执行原有完整新旧对照；不改写 MySQL 记录，也不把历史缺失标成完整。

## 独立恢复工具

`runtime-state verify|export|recover-previous` 使用 `--state-dir`、`--binding`；导出或恢复上一份时另给尚不存在的 `--destination`。`binding` 是已核验转换清单中的完整绑定对象。上一份恢复总是写到新目录，保留损坏来源。`verify` 和 `export` 也接受未完成维护交接，输出其完整计划和进度；不执行 SQL、不解除采集启动屏障，保持源恢复文件不变。工具成功只证明本地恢复链可以解析，随后仍须领域提交核验。

`runtime-state epoch-init|epoch-show|epoch-rotate` 使用 `--epoch-file` 和精确 `--target`（`server_uuid/database`）。首次初始化要求文件不存在；轮换必须给当前 `--expected-epoch` 与维护原因 `--reason`。

`tools/storage/runtime-state/prepare-restore.py` 将该协议落到一个可续跑操作：要求目标服务全部停止并取得对应 `runtime.lock`，验证全部预期世代后才逐项轮换，记录工具二进制哈希与完整恢复世代清单。它不执行数据库恢复，不删除 checkpoint，不改 producer，不启动服务。结果通知丢失时按同一维护 job 的前序世代和原因核对后续跑，不能重复轮换或把不相关的世代重新标成自己的。

该工具参数为 `--inventory`、`--runtime-tool`、`--runtime-tool-sha256`、`--output` 和 `--reason`。输入 JSON 为 `version: 1`、`targets` 数组，每项列出 `target`、`epoch_file`、`expected_epoch`、`services` 和 `state_directories`；目录必须使用绝对路径。输出 `restore-fence.json` 保存在独立维护证据目录，不进入数据库备份。

恢复数据库备份、更换数据目录、人工删除/改写事实前，停止受影响采集服务并先轮换世代。恢复后保留原状态与原未决引用，按明确的维护输入清单完成核验和显式重建；不得修改旧 pending 的 generation 或把库 UUID 不变当作没有回退。此阶段历史导入仍暂停。

对影响活跃覆盖、canonical/finality、旧缺口或待跟踪对象的人工修复，使用同样的冻结交接和明确 ID/范围清单；纯最终历史的导入仍受原迁移容量门槛约束。禁止等待采集器周期扫表自行发现外部变化。

## Reserve 显式入口

`reserve-data runtime-convert|runtime-verify|runtime-rollback` 与 Across 使用相同的恢复世代和维护边界。参数为 `--runtime-state-dir`（默认 `var/reserve/runtime`）、`--runtime-epoch-file`（或 `RESERVE_RUNTIME_EPOCH_FILE`）、`--runtime-export`。绑定包含当前/related manifests 和原 `--from` 起点；转换与 watch 必须一致。普通 MySQL watch 要求已核验状态，显式旧逻辑恢复才使用 `--legacy-history-recovery`。

转换沿 `(chain_id, manifest_hash, capture_id, batch_id)` 联合主键分页，游标保留 capture 与 attempt 两个二进制键。在同一冻结输入上对照原连续日志游标、覆盖、最新有效 attempt 及收据待办。每个 manifest 保持独立 finalized 锚点，related manifest 不与当前配置身份合并。转换中断保留 `.conversion`，未通过核验的 `.ready` 不作为运行目录。

调度页只持有必要 capture 字段和冻结对象引用，大型 `ReceiptRefs` 不常驻调度状态；修订时按引用加载完整原对象。重试继续使用原 `batch_id`、available 时间和摘要。存储忽略旧修订时读取实际修订，不能把成功返回误当成旧值已覆盖新值。回退先恢复未决提交、导出当前链，再核对当前数据库的旧逻辑重建结果。

Across 和 Reserve 保留原路径锁，并额外按真实 server UUID、数据库及 producer 加锁。该锁与 manifest、evidence 路径无关；它不替代 SQL producer generation fencing。

## LST 显式入口

`lst-data runtime-convert|runtime-verify|runtime-rollback` 继续取得原 `state-dir/lst.lock`，并锁定实际数据库/producer。参数为 `--runtime-state-dir`（默认 `var/lst/runtime`）、`--runtime-epoch-file`（或 `LST_RUNTIME_EPOCH_FILE`）、`--runtime-export`。转换时使用与 watch 一致的 manifest、`--from-block`、日志模式、市场间隔、每轮路线数和日志暂停设置。

原 pending 文件保留原编码，通过硬链接进入恢复链。转换在写库前保存其引用，按原身份完成不确定提交，再按 capture 主键遍历全部适用登记，按需读取 31 天 followup 和 30 天 gas 窗口明细。核验与旧逻辑使用同一冻结输入，保留原日志起点/来源阻断，比较全部最终性待办、followup 到期顺序及 gas 样本。`.conversion` 可断点续跑，未通过核验的状态不发布。旧模型允许的 revision=0 不被新状态格式排除。

普通 MySQL watch 必须先有经过核验的 runtime checkpoint。`runtime-rollback` 要求导出当前恢复链，再用当前数据库进行受控旧逻辑对照并写回当前游标；随后才允许显式旧逻辑恢复。不得拿升级前状态覆盖当前待办，也不得删除预算目录或从当前链头重置。命令及隔离恢复测试已实现；生产使用前应查实施记录中的验收与转换状态。

## 转换输入清单

`tools/storage/runtime-state/input-inventory.py --service across|reserve|lst|keeper|dex --server production|isolated --out <新文件> [--evidence <项目内证据文件>]` 只读现有 migration metadata。它核对固定 server UUID，在只读一致性事务中记录迁移计划的上下界、已核验覆盖和原不可读记录；不遍历业务事实，不启动历史迁移，不创建状态或轮换世代。

清单保留 `verified_rows_only`（状态 5）与完整表核验的区别，未列出的源范围一律未知。每类元数据最多 4,096 条，额外哨兵发现溢出时拒绝发布，不拿截断清单宣称完整。输出不覆盖已有文件，文件和目录同步；外部证据只记录路径、长度和 SHA-256。五个领域的转换入口均要求该清单：低频命令使用 `--runtime-input-inventory`，DEX 使用 `--input-inventory`。第一次分页读取前将原始清单摘要和内容纳入 checkpoint；续跑拒绝更换，后续 checkpoint/导出保留原引用。工具本身不证明运行依赖齐备，仍须按冻结范围完成领域对照。未知范围不被标成完整历史；2026-10-16 首次生产转换已执行，具体范围和原输入摘要见实施记录对应的部署清单。

## Keeper 显式入口

`justlend-keeper-data runtime-convert|runtime-verify|runtime-rollback` 保留原命令进程锁，并锁定真实数据库/producer。共同参数为 `--runtime-state-dir`（默认 `var/justlend-keeper/runtime`）、`--runtime-epoch-file`（或 `JUSTLEND_KEEPER_RUNTIME_EPOCH_FILE`）以及核验/回退使用的 `--runtime-export`。MySQL 的 watch、preflight、resume-source 使用同一份已核验状态；resume-source 只解除指定来源的 blocked 标记，保留预算和冷却。

状态仍由原单线程 Collector/Step 修改。扩展的 State 包含未完成窗口和 enrichment 工作页，已有 Ops 保留冻结批次；SQL 前将已同步的原 checkpoint 硬链接到恢复链，避免再次序列化 pending payload。候选对象保留单独冷文件；当前与上一份 checkpoint、未覆盖原操作的引用一起保留。领域状态和原 Ops 使用定类型编码，保留已知零值指针；旧 SQL 事实摘要不变。

转换先按旧 pending 协议完成已经冻结的 Ops，不重复请求来源。之后保留原 State、预算、冷却、分页和两类游标，分父/子阶段按主键遍历当前 config 的已提交 capture，用同一冻结输入逐项比较 enrichment 待办、排序和严格分页链；`.conversion` 保存每页进度。bootstrap 不参加 enrichment 选择；其他优先级、三次失败和 30 分钟冷却按原 MySQL 查询执行。失败三次不等于该页证据完整。

正常循环从提交结果更新窗口，缺页、重复输入 token、混合 scan、断开的空结束页或未完成子批次都不能推进 EvidenceCursors。回退先导出当前恢复链，核对当前工作依赖，再生成旧版可读的当前 State；旧格式无法无损表达未决状态时拒绝回退，不能静默把已知零改成未知。外部 backfill/migrate-pages 发现已有 runtime 目录时要求显式维护交接，不等待正常循环重扫历史。

## 主 DEX 显式入口

`dex-runtime-state convert|verify|rollback` 使用主采集器的 backend/producer 配置与 Ethereum manifest。参数为 `--state-dir`（默认 `var/dex/runtime`）、`--epoch-file`（或 `DEX_RUNTIME_EPOCH_FILE`）、`--export`。转换应在主 collector 停止并冻结所有相关 DEX 写者后执行；真实目标锁和 producer 检查继续保留。该工具不构造 RPC 客户端。首次转换遍历全部适用的区块登记，以可续跑页游标和流式输入摘要保存进度，再次遍历核对冻结输入，并按旧高度相邻规则独立计算缺口。

主采集器 MySQL DEX 分支需要 `DEX_RUNTIME_STATE_DIR` 和 `DEX_RUNTIME_EPOCH_FILE` 对应的已核验状态。`DEX_LEGACY_HISTORY_RECOVERY=true` 只供明确安排的旧逻辑恢复，不能作为缺少 checkpoint 时的自动回退。正常缺口查询由连续区间与实际未最终确认工作页回答；原始区块成员、待补日志/收据、链上 header/finality 检查保持。

实时和维护网络循环继续独立；区块批次、修订与日志补齐先冻结原操作，经原 MySQL 事务和准确主键读取确认后，由唯一状态 owner 应用。旧修订 SQL 成功但未更新时返回实际版本；前次结果未知的同区块操作先恢复，再接纳新计划。新版产生的未决操作随 checkpoint/恢复尾部保留。升级前旧进程的不可恢复实时采样与实际停机区间必须明确记缺失，不能用新状态补造过去报价。

回退先处理未决原操作并导出当前恢复链，再核对当前数据库的旧缺口结果；旧版仍从当前数据库恢复，不恢复升级前快照。首次转换必须提供输入范围清单；生产转换、完整负载容量和持续运行验收尚未执行，不能仅凭本地测试宣布可以切换。


## 有限维护交接（尚未部署）

低频四个命令提供 `runtime-handoff --runtime-handoff-plan <plan.json>`；DEX 提供 `dex-runtime-state handoff --handoff-plan <plan.json>`。沿用原进程锁、真实目标锁、producer 和恢复世代核验。必须先停止该范围的采集与其他写者，处理原有在途；Keeper 还要求原 Ops 排空。此入口只执行清单中的原批次或原修订，不进行来源请求，不扫描全历史。新清单中的 Batch 必须尚未存在于目标库，以精确主键预检；已经入库的原批次只能续跑其原交接清单，不能换清单重新生成已结束的待办。绕过交接发生的外部写入走恢复世代与显式重建协议。存在性预检不用于判定未知 COMMIT 已结束。

清单格式为 `Version: 1`、完整 `Binding`、非空 `Reason`、`Operations` 数组。每个操作列出 `File` 与 `SHA256`，文件路径相对清单目录或为绝对路径。文件是领域的定类型冻结操作，不能把包含二进制字符串的 Batch 直接 JSON 化。低频领域提供 `EncodeRuntimeHandoffBatch` / `EncodeRuntimeHandoffRevision`（Keeper 只有 Batch），DEX 提供 `EncodeHandoffBatch` / `EncodeHandoffRevision` / `EncodeHandoffLogs`；LST Batch 使用原 pending 编码。清单本身不含密钥或源凭据。每次最多 4,096 项，冻结 payload 合计不超过 64 MiB，超限应明确拆分有限维护批次。

工具先验证全部操作形状、原文件摘要、原记录身份和修订前序。清单按依赖顺序排列：新批次在其修订之前、Keeper 父索引批次在 enrichment 子批次之前；错误前序在第一项 SQL 之前拒绝。新事实落入 Across 已完成且最终确认的解码范围时，要求显式重建，不静默忽略新增待办。随后将文件硬链接到已有恢复对象目录（要求同一文件系统），同步后保存整个清单和进度。首次 SQL 前，当前和上一份 checkpoint 都必须包含完整有限清单；两份安装之间中断时，维护恢复补齐这一屏障后才继续，避免回退时忘记未发起的项。每项沿原 MySQL 事务提交并核验实际记录，再将运行状态变化、派生待办和交接进度一起 checkpoint。COMMIT 结果未知时保留原操作；重启只能经同一交接命令恢复。普通 watch 不接纳未完成交接。已完成清单重复执行不重复提交，最后完成状态不取代数据库提交证明。

中断后冻结文件即使被原目录移除，已接纳操作仍从恢复链中的原引用读取。续跑要求原清单字节一致，拒绝换成另一份清单。原状态、上一份 checkpoint 和未决引用保留，不能通过删交接记录解除阻断。对已完成覆盖的破坏性改写和不明来源外部库变化，仍按恢复世代协议显式重建。
