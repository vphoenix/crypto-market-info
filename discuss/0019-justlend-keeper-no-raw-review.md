# JustLend keeper 去除原文归档依赖审核

日期：2026-10-04。独立审核者：`/root/keeper_code_review`。

状态：代码独立复审、本地回归及正式七表迁移／无归档落库／非空后台核验／导出验收通过，无剩余审核阻断。最终服务记录为active/running、enabled、PID2243716、NRestarts=0，验证文档已闭环。结论仅覆盖列明的短期运行范围。审核者不修改实现、正式数据、服务或路由，不访问外部 API。

用户要求正常采集仅解析、校验、落库，不永久归档 RPC/API request/response，后台补查和导出也不依赖原文文件。本轮保持来源响应严格解析、UTC／整数／Decimal、payload hash、固化锚点、批次成员摘要、持久化限速与提交后推进游标。

## 设计与代码范围

读取 AGENTS.md，以及 client、evidence、collection、export、report_evidence、collector、state、indexed、index_page、canonical、解析／观察路径、CLI 和 ClickHouse keeper writer／DDL。

新 jl_keeper_index_page 仅保存 capture 身份、扫描ID、输入／输出token、请求／可用时间和来源hash，共8个定类型字段。独立表使旧 Capture 和旧成员编码无需改变。正常 Send 只在内存计算 request／response SHA；Finish 只计算兼容摘要hash，不写摘要或原文文件。后台从已提交索引行恢复已解析事件，核对类型／ABI／必填NULL和往返值后按原链上收据匹配逻辑补查。分页链从新表读取，空输入token串到终页且全部页有成功子批次才推进样本证据游标。

export／兼容report命令直接核对数据库成员数量和稳定摘要、父capture身份／配置／合约／窗口／发现数，输出七份typed CSV及metadata。metadata明确原响应不保留、不重新验证；旧probe状态按原行导出，不靠已删原文重新分类。原 Archive 读写工具仅供显式旧页迁移和历史研究兼容，不在正常采集、补查或导出路径调用。

## 已反馈的恢复边界

第一轮发现 MigrateIndexPages 仅读 capture 和旧manifest，未将迁移分页元数据与现有定类型索引行交叉校验。作者已加批量读取成员、capture身份比较、加入IndexPage后Validate；来源hash／请求时刻／可用时刻不一致会拒绝。第二处为 WriteKeeperBatch 在比较已有capture之前先写分页表，可能让被拒绝的错误重试留下新sidecar；作者已改为先比较已有capture，再写独立分页记录。新增批次仍先写元数据／成员，最后写capture提交标记。

旧 Frozen 不重Seal，不修改原 Capture／成员摘要或可用时刻；只从已保存分页状态补单独IndexPage。旧已提交页通过显式本地 migrate-pages 从SHA认证的旧manifest一次读取元数据，禁止重新请求并替换历史内容；迁移之后的正常路径只需要数据库和恢复状态。旧未冻结页从已解析Discoveries恢复，已有预算／冷却不清空。最终通过需要在归档路径不存在或为不可写普通文件时，独立验证发送／冻结恢复／后台补查／完整分页链／纯数据导出，并确认旧迁移幂等和拒绝错误重试。

## 独立测试与代码结论

作者同步的 `/tmp/keeper-no-raw-build` 快照中30份 keeper／CLI／keeper writer／DDL／集成测试文件与工作区逐文件字节一致。审核者在此快照独立执行完整 `go test -race -count=1 ./internal/justlendkeeper ./cmd/justlend-keeper-data`，keeper通过8.356秒，CLI编译通过、无测试文件。另执行仅本机随机隔离库的 SevenTableRoundTripAndFrozenRetry 与 IndexFrontierRetryAndLegacyDefaults，两项race通过7.242秒，测试库由测试清理，不修改正式库。首次工作区无监听定向测试另通过1.127秒，作为早期检查保留，不替代同步快照的完整测试。

本轮新增测试验证普通文件作为不可写归档路径时HTTP200／503仍采集相同请求／响应hash且不改文件；旧manifest来源hash与已保存typed成员不符时迁移失败、写入0页；正确迁移保持原capture编码，删除归档后重复迁移0行且导出仍成功；原成员被修改但摘要不变时导出拒绝。空能源索引页保留payload hash／分页终点／空成员摘要、不创建归档目录，随后仍可导出。必填NULL缺失时typed恢复拒绝，原ordinal与来源hash保持；后台恢复单个child无需原响应。

真实隔离库回归包括七表读回、旧Capture默认／Frozen恢复、UInt256和Decimal、源页元数据与成员关系、257父页完整窗口、逆序补齐及30分钟重试。专门验证已有cap但缺sidecar时，修改cap的错误重试被拒绝且没有写入分页元数据。完整token链缺页／重复输入／非终页及旧前缀恢复测试仍通过。两份DDL字节相同。

源码检索确认 client／collector／collection／export 正常路径没有 Archive.Get／Put、readManifest／verifyManifest／reportEvidence 调用；唯一新路径的 readManifest 位于显式 MigrateIndexPages。仍保留旧研究报告库的归档辅助函数，仅作历史兼容，不是采集或CLI导出依赖。新证据摘要hash是未保存原文的兼容字段，metadata不再承诺原API／RPC响应重放认证。

代码复审通过，可按已授权的维护流程部署；此结论不提前代替正式迁移与清理后的运行结果。应先完成全部旧已提交索引页元数据迁移，再清归档；旧Frozen通过原状态补独立进度，预算／冷却／配置／路由不重置。实际部署后需确认新的索引和后台child继续落库，并从已清理或不存在的归档路径完成新进程export。

当前审核指纹包含上述30份keeper源码／CLI／CH源码及DDL／测试，加 config/justlend-keeper-tron.json，共31文件；排序后逐条拼接path+NUL+文件SHA256十六进制+LF再SHA256，结果为`f94b4f6a4658ff1e66b9c009d09d7c1af6472eda08c2940a1ebd4e015449476b`。本轮文档收尾及实际运行证据不混入此代码范围指纹。

## 正式迁移与无归档运行抽查

已读 research/2026-10-04-keeper-no-raw 的 deployment、service-started、database-before／after-migration、state-before／after-start、migration、page-coverage、cleanup及两份导出metadata。实际部署文件SHA独立读取为`480b8d916a21bf2bf013c55a2b954444fbf2d85ed2cac1865dba0c58295a7b47`。服务2026-10-03 18:09:06 UTC（10月4日02:09北京时间）恢复，启动记录PID2243716、active/running、enabled、NRestarts=0；用户unit启动参数与路由未由审核者修改。

旧六表迁移前后行数及全行fingerprint逐表相同，审核者程序比较两份JSON一致：capture20791、indexed875、event526、receipt526、probe7843、cost1572。3244个旧成功index页补入新分页表，page-coverage为missing0。迁移只补分页元数据，没有重写旧capture／成员摘要。冻结历史终点仍为2026-10-03 00:00 UTC。

state前后UTC day仍为2026-10-03，Used从27858延续至27873，没有清空预算。source与evidence三类游标从原18:04:45／18:04:10推进到18:08:06 UTC，审核者比较均无回退。来源Next继续推进、Cooldown／Blocked／Limited未被重新初始化。state JSON的observed_utc是读取快照时间，不作为旧state实际采集时间使用。

正式本机只读SQL抽查新Rent父34aa70ba-1741-4985-aa4a-3ed73f6f7c64保存1条索引，Return父fcd13c8a-6975-475b-a9c2-1e9cf2d22603保存2条索引。对应新分页表的payload hash、request_started_at、available_at与每个索引行逐项相同；ordinal分别为0及0／1，ABI为v2，block hash保持NULL，finality=provider_claimed_confirmed、position_status=indexed_only。child9e60b353-c81d-4461-be96-60492e0b9203及378529ec-a93d-4fd3-841b-2bac7e628cdf分别发现1／2、选中0、complete，事件／收据行数均0，没有将非样本索引冒充收据认证。本轮此时未出现新的非空已核验child，真实选中事件路径仍依据已通过的fixture／恢复回归，不将零成员批次扩大为全部链上核验运行验收。

原归档路径不存在时，启动前新进程30天export成功，20791审计capture、875索引、526事件及526收据、7843probe、1568cost、3244分页记录；四条旧撤回报价仍作为capture审计保留、成员排除。作者记录约3.45秒，为本次快照耗时，不承诺固定速度。

export-live窗口为18:09:06至18:11:09 UTC，实际31审计capture、14分页、4索引、4cost，probe／事件／收据均0。审核者读取全部4索引CSV与分页CSV，来源hash及两个来源时间字段全部一致；读取全部12个child，所选范围均0且事实／收据均0，已包含窗口内父页者的配置／事件种类／时间窗／发现数与父索引数对应。唯一窗口外父39e216e2-bef5-41b6-a705-2b8c910e2ae6通过独立只读SQL核对：18:06:58.585083开始、同配置、committed complete、ReturnResource、索引0、窗口18:04:10至18:04:40，与child相符。父页开始时间早于导出下界不是漏采；导出实现通过数据库关系核验，不再读取其旧原文。

审核者确认 var/justlend-keeper/evidence、本轮legacy-archive-to-delete及上轮before/evidence三个路径都不存在。cleanup记录18:13:08 UTC只删除两个指定旧归档目录，原test fixture保留。正常采集已在缺少原路径时持续产生新index／enrichment，没有重建归档目录。无外部请求、无正式库写入、无服务或路由变更由审核者执行。本阶段运行验收通过，最后短期probe／成本及文档闭环由后续快照补充。

## 最终非空核验、模拟与文档闭环

已读最终 runtime、state、service、export metadata，以及[validation](../research/2026-10-04-keeper-no-raw/validation.md)最终持续运行小节。18:20:03 UTC的SQL快照有65个新成功index页、12索引观测，65个成功enrichment含1事件／1收据，另1 seed含1事件／1收据；38个完整probe、14成本观测，另10个skipped probe capture。完整源页累计3310，分页进度missing0。service-final仍为PID2243716、active/running、enabled、NRestarts=0；运行／部署二进制SHA保持480b8d916a21bf2bf013c55a2b954444fbf2d85ed2cac1865dba0c58295a7b47。

审核者对清理后的export-final全部12索引与65分页记录重新核对来源SHA和两个来源时间字段，全部相符。非空enrichment98b760fa-5c6b-4bf2-977a-331d11cb34bd引用父7eefd2f2-e520-468d-a719-b43649283fa1，发现1／选中1，真实Return v2：块86792853、receipt_log_index3、transaction_index3。父索引的ABI、租单身份、全部相关UInt256金额、payload hash及request_started_at与核验事实一致，事件／收据tx、高度、block hash及solid状态相同，body_complete／receipt_complete均true。新事实／收据AvailableAt均为child完成时刻18:16:24.147934 UTC。

另实际seed a5ca34da-4ac9-426f-b0d4-4ba8a8f06fb0保存Rent事件及收据，块86791201、receipt_log_index3、transaction_index280；两表tx／高度／hash相同，明确receipt_verified且位置非NULL，source／body／receipt payload hash非空。这是无原文文件条件下完成的正常补链核验。此抽查只验证定类型结果、批次摘要及字段关系，不宣称已经重放删除的供应商响应。

最终export窗口18:09:06至18:20:03 UTC，实际197审计capture、65分页、12索引、39probe、2事件／2收据及14cost。全部39条probe保留revert及非空response hash，before／after高度、hash、时间六字段均非NULL；全部14cost有payload hash，11个BBO和3个chain_resource均ok，后者保存energy_fee=100 sun/unit、bandwidth_fee=1000 sun/byte、user_resource_percent=10、origin_energy_limit=90000。另10个skipped的实际reason均为caller_conflict_or_unverified_candidate，不混为stale或已执行失败，不能声称全部计划都已模拟。SQL的38 probe与稍后导出的39是读取／提交时刻不同；时间窗按capture_started_at选择，不假装同刻快照或available_at截止。

state-final观察时刻18:20:10.286948865 UTC：UTC day仍10月3日，Used=28090，三类source／evidence均推进到18:17:36，包含既有120秒索引延迟；预算未被重置。三处原归档目录继续不存在。最终0.425秒小窗口导出和启动前约3.45秒30天导出数据量及查询条件不同，均为本次实测，不外推固定耗时。

验证文档全部21处本地链接存在，正文与迁移／清理／服务／预算／窗口／不同读时刻的计数差异一致；明确不将十余分钟外推全天稳定或完整非样本收据覆盖。本轮代码、实际无归档恢复／核验／导出及文档闭环审核通过，没有剩余审核阻断；审核者仅修改此审核文档。

最后独立读取实际 `/proc/2243716/exe` SHA和只读systemctl状态，与上述二进制、PID及active/running、enabled、无重启一致。初次沙盒进程视图未暴露正式PID，使用只读本机进程权限后核对成功，未执行服务控制。最终31文件代码范围指纹保持`f94b4f6a4658ff1e66b9c009d09d7c1af6472eda08c2940a1ebd4e015449476b`；最终验证文档SHA256为`35d7c6ce840a4ce059dbd1a2bc0daca8cc5dd0796c86bfd0bb7721962ab9f235`，单独记录，不混入代码指纹。
