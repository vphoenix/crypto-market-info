# ARB-0009 R5：期权自动发现实施与验证

日期：2026-10-04。代码已实现，复用同一独立Agent审核；[设计及代码审核记录](../../../discuss/0018-options-lifecycle-design-review.md)。初次实施阶段未修改常驻二进制、unit或生产数据库；后续已完成[部署及真实全量DB验证](arb-0009-options-lifecycle-deployment.md)。首次部署出现持续运行故障，最新修订及验证见[持续运行修复](arb-0009-options-sustained-repair.md)和[0019复审](../../../discuss/0019-options-lifecycle-sustained-review.md)。以下保留初次验证边界；生产状态以最新修复记录为准。

## 最终行为

`OPTIONS_SYMBOLS=auto` 持续发现BTC/ETH币本位及BTC/ETH USDC线性四族全部未到期期权、交割期货。新到期日或行权价都可独立进入采集，不要求C/P、同到期期货配齐。到期、终止或关闭分别保存真实质量；到期取消订阅，Supervisor继续运行。显式清单仍走原最多32成员的固定模式。

生命周期13频道先确认，再抓六个完整目录；每30分钟和重连后核对。完整creation可以直接确认身份，缺定义时请求单合约。目录失败一分钟后重试，单合约失败5/15/60秒退避。单合约失败只影响目标symbol；完整目录失效后，单合约成功不能放开其他未经本代确认的成员。

定义、规则、状态和原文证据落盘后立即预订阅。不可变计划默认在当前UTC分钟之后第二个边界生效，保留1–2分钟落盘与预热时间；这是完整历史的起点，源订阅先于它发生。晚到计划顺延，缺第0秒锚点不补造。计划以全局前驱串联，跨session生效分钟严格递增；每个instrument一分钟只有一个owner。

T边界新run立刻采样，旧writer在后台排空T之前的冻结分钟。旧分钟排空失败记录缺失，不阻塞T的新锚点。所有新数据仍用整数tick/lot、买卖各10档、按价格的秒差量，原50档历史完整回放。

## 关键实现与约束

| 路径 | 职责 |
| --- | --- |
| `internal/exchange/deribit/lifecycle.go`、`session.go` | 严格解析creation/state/platform/status；增量订阅、精确ACK、源epoch、心跳及限速 |
| `internal/optionslive/catalog_runtime.go` | 目录与状态Supervisor、负向屏障、持久化后发布、不可变计划、预热与故障恢复 |
| `internal/optionslive/catalog_hub.go`、`catalog_engine.go` | 物理连接共享、单书重建、有序冻结、分钟owner切换与60槽证据 |
| `internal/options/catalog.go` | 定类型观测/计划/证据及独立v2摘要；旧LiveRun序列化保持不变 |
| `internal/storage/clickhouse/options_catalog_*` | 新五表、内容冲突检查、证据核验、commit最后写、全计划查询 |
| `internal/storage/clickhouse/options_writer.go` | 独立8连接池，分钟验证/写入限三路并发，控制数据保留额度 |

原始响应按SHA-256命名的gzip先归档，再允许正向引用。locked、maintenance、协议错误先在有序入口失效，不等待数据库。epoch、接纳序号、状态时间和平台generation阻止旧响应或写入重试放开新屏障。未知maintenance不伪造false；实际public/status的locked为字符串，非法布尔值不能作为基线。

真实platform_state订阅会推送大量非本任务资产。所有合法消息仍归档，非四族的单指数锁通知不触发四族全局恢复；维护、相关锁或非法消息保持保守处理。public/status使用单飞槽直到证据持久化，过期响应合并为最新一代；非法响应或预算拒绝释放匹配槽并有界重试。初版生命周期证据队列为256条；持续运行修订改为`2*MaxBooks+512`，最多128条一批持久化，并为拒绝工作保留主动确认屏障。慢REST目录队列仍为64条，共享32MiB证据预算。

每条物理连接仅有一个待ACK订阅批次，新成员等待时合批。订阅限速由独立有界preparer等待，socket writer仍优先发送心跳；队列等待90秒与实际发送后的ACK等待15秒分别计时。同频道取消后不在原epoch复用，防止旧消息污染新snapshot。

默认4096个书、20条物理WS（含生命周期和指数）、每条256个书频道；逻辑run最多32成员。所有WS读取与book入口共享64MiB字节预算；每分片另有256条/16MiB上限。全任务完整L2最多200万个价位，单书每侧最多20,000；保存深度仍各10档。分片分钟队列两个批次、最老积压45秒，资源或写入故障明确失效并替换分片。

五张表及精确字段见[存储字典10.4](../../market-data-storage.md#104-期权自动发现模型r5)。新 `catalog_v2` 使用独立提交域和60槽来源证据，不改变原R4摘要。`options-check -profile live` 默认按完整生效计划检查预期/有效/缺失成员，不能用最新一个run代表全量。

## 已验证结果

最终公开源探测使用真实REST/WS、临时原文目录和内存sink，未写生产或测试ClickHouse。`2026-10-03T19:13:00Z` 同一完整分钟的四族结果如下；期货计入各族。

| 合约族 | 计划成员 | 第59秒可回放 | 全60秒可回放 |
| --- | ---: | ---: | ---: |
| BTC | 1015 | 1015 | 1015 |
| ETH | 849 | 849 | 849 |
| BTC_USDC | 671 | 671 | 671 |
| ETH_USDC | 571 | 571 | 571 |
| 合计 | 3106 | 3106 | 3106 |

其中3062个期权、44个交割期货。保存[公开探测日志](../../../research/2026-10-04-options-lifecycle-implementation/public-final-budget.log)及[原文hash清单](../../../research/2026-10-04-options-lifecycle-implementation/public-evidence-latest/manifest.json)：六个目录、155条platform_state和一份字符串locked基线。该次没有maintenance推送，其异常/恢复通过模拟协议回归验证；没有等待真实新挂牌或真实到期发生，动态准入与退出由确定性测试覆盖。

正式回归包括旧合约到期后新单边期权先于计划预热、状态负向屏障、断序重建、取消频道隔离、过期元数据、闭市后fresh snapshot、准确T切换、旧writer慢排空、计划ACK丢失、证据重试身份、scope/单合约失败隔离、平台初始消息合并、限速等待时心跳以及进程锁。盘口/采样/回放测试覆盖同秒最终数量、零数量删除、有效/无效位图和旧50档起点。

真实隔离ClickHouse验证新DDL、部分写入不对查询可见、重试身份稳定、证据缺失拒绝、计划分叉拒绝、全计划计数、逐秒精确回放及R4/50档兼容，见[数据库回归](../../../research/2026-10-04-options-lifecycle-implementation/clickhouse-regression.log)。所有临时库已清理。相关包race、CLI编译和vet单独记录；独立Agent另行运行自己的回归和race。

保留的显式模式另以真实公开C/P/期货三成员写入隔离ClickHouse，`2026-10-03T19:21:00Z` 三书全部60秒可回放，见[公开源到数据库兼容测试](../../../research/2026-10-04-options-lifecycle-implementation/legacy-public-clickhouse.log)。此测试不代表R5全量DB吞吐。

## 容量与实际限制

3728本书（公开全量加20%）、117个逻辑分片、每侧20层、20%活跃流每秒更新的合成采样测试，最慢同步一秒为71.17ms，堆约203MB，低于250ms采样预算，见[容量日志](../../../research/2026-10-04-options-lifecycle-implementation/capacity.log)。完整分钟hash与证据验证已移到有界writer，避免分钟边界集中阻塞采样。这是合成CPU/堆测试，不是全量网络或DB吞吐承诺，也不是峰值RSS。

完整1440分钟单流事实/质量/提交测量见[整日测量](../../../research/2026-10-04-options-lifecycle-implementation/day-measurement.log)：活跃流压缩2,282,942字节，静默流1,120,977字节；100次分散完整读取及回放P95分别24.22ms和22.73ms。此测量复用基础事实编码，不含R5额外质量证据、指数、目录和计划。

R5额外成本另用每组3书、12分钟、完整证据与指数/提交测量，见[R5窗口测量](../../../research/2026-10-04-options-lifecycle-implementation/catalog-measurement.log)：活跃41,873字节，静默30,467字节；完整读取、证据核验和回放P95为63.94ms/65.10ms。按该小窗口直乘的每书每日估算为1,674,920/1,218,680字节，**不是整日实测**；压缩块大小、活跃度及共享元数据会影响结果，不能与基础整日值直接相加。

初次实施阶段未验证全量真实DB吞吐；后续部署前已以3108成员完成公开源到真实DB连续两个完整分钟的写入及回放，生产亦验证两个完整分钟，见部署记录。尚未运行长期断线/限流验收。没有以真实挂牌事件测得“接纳至订阅P95≤5秒”；初始化全目录的限速和证据落盘需要时间。后续已在独立验收库验证公开源及全量分钟提交，补齐批量定义/规则路径并经同一Agent复审，随后替换常驻程序。短窗口成功不等于长期稳定性承诺。
