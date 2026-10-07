# ARB-0009 R5：期权自动发现生产部署

2026-10-04 23:59:51（Asia/Shanghai）已用通过新一轮独立复审的持续运行修复替换本记录中的首次部署版本。当前SHA为`41059f71a06f0064dbb05db7ecbea9380aec350fd2871c3da9e6c8bb42265c0c`，unit参数沿用；修复及新验收见[持续运行修复记录](arb-0009-options-sustained-repair.md)。以下保留首次部署及其失败边界。

2026-10-04 15:43:18（Asia/Shanghai）已替换并启动现有 `crypto-market-info-collector.service`。`OPTIONS_SYMBOLS=auto` 现在运行持续目录发现、独立新合约准入及到期退出。生产库 `crypto_market_info` 已幂等创建五张R5专项表。

2026-10-04 22:41持续运行复查发现严重退化：16:00真实到期/状态变更后出现重试队列溢出，随后反复状态证据校验失败；22:40计划2977个成员，缺129个，只有6个全分钟有效，另113个已开放成员仍等待准入。新挂牌发现及到期退出已有真实记录，但长期采集尚未通过验收。下文“生产验证”仅记录部署初期窗口；当前问题见[部署后状态与数据复查](../../../research/2026-10-04-postdeploy-status-2224/report.md)。

## 实际版本和配置

collector SHA-256：`3a39581b13a8abe0dcf03383d1d4ae8337a29bf350bb11aec44f5787f98a0ee4`。

从提交 `422fac3f12185e64616508eac42725e137d3e6a4` 的隔离源码快照构建，覆盖此前审核的R5文件、应用接入及本次批量写入修复。构建没有包含工作区其他尚未提交的策略改动；完整路径/hash和构建元信息见[构建清单](../../../research/2026-10-04-options-lifecycle-deployment/build-source-manifest.json)。同一独立Agent复审新增修复，无P1/P2，race和真实ClickHouse回归通过，见[审核记录](../../../discuss/0018-options-lifecycle-design-review.md)。

当时安装unit新增 `OPTIONS_EVIDENCE_DIR=/home/ubuntu/.local/share/crypto-market-info-options/evidence`。2026-10-06 该参数改为兼容保留，期权响应只在内存校验并写定类型数据库，不再依赖或生成原文文件；以下原文核验记录描述历史验收，不代表原文永久留存。其余已安装采集环境参数沿用，4096书、20条WS、256频道等预算使用已审默认值。仅重启主collector；ClickHouse以及其他独立采集服务不随部署重启。

旧二进制及unit保存到 `/home/ubuntu/.local/share/crypto-market-info-collector/deploy-backups/options-r5-20261004T074317Z/`。旧SHA为 `dd55a3a80fbba9bec3b61a18401f41b1ba436322d4de8012f4889fdca6859a21`。部署记录包含启动时间、备份路径及原日志偏移，见[部署身份](../../../research/2026-10-04-options-lifecycle-deployment/deployment-record.json)。

## 部署前验收与必要修复

在同一宿主ClickHouse的独立库运行候选collector，仅启用期权。第一轮发现逐合约定义/规则查询和单条insert使完整目录超过30秒处理期限；生产仍运行旧版。随后改成批量读取、完整校验后批量插入，保留FIRST证据、同批重复、规则来源时间、不可变ID和逐秒引用限制。1100合约定义、刷新、规则及重复写入测试约219ms，独立Agent约326ms；旧R4及失败提交/回放回归通过。

修复后的真实REST/WS→实际OptionsWriter→ClickHouse验收：`07:39`、`07:40 UTC` 连续两个全量分钟均为98分片、3108书、每书60有效秒。默认完整计划查询耗时13.01秒，无缺失；第二分钟全部186,480个秒级状态实际回放通过，完整读取和回放合计18.76秒。162份引用原文解压重算hash一致。两个临时验收库已清理，未删除生产历史。

证据：[真实数据库批量回归](../../../research/2026-10-04-options-lifecycle-deployment/batch-database-tests.log)、[全量计划查询](../../../research/2026-10-04-options-lifecycle-deployment/accept-full-plan-query.json)、[全部秒回放](../../../research/2026-10-04-options-lifecycle-deployment/accept-all-seconds-replay.json)、[四族及原文核验](../../../research/2026-10-04-options-lifecycle-deployment/accept-raw-and-families.json)、[验收库清理](../../../research/2026-10-04-options-lifecycle-deployment/acceptance-cleanup.json)。

## 生产验证

生效全量计划 `5fcd10ad-9901-4667-91c1-98d62f71bd6b` 自 `2026-10-04T07:46:00Z` 起包含3108成员、98个逻辑分片，pending=0。此前 `07:45` 的首个部分目录计划如实包含1002期权。表内、证据及以下验证时间均为UTC；`07:46`对应北京时间15:46。

| 合约族 | 期权 | 交割期货 | 全量成员 |
| --- | ---: | ---: | ---: |
| BTC | 1002 | 13 | 1015 |
| ETH | 838 | 13 | 851 |
| BTC_USDC | 662 | 9 | 671 |
| ETH_USDC | 562 | 9 | 571 |
| 合计 | 3064 | 44 | 3108 |

`07:46`、`07:47 UTC` 两个全量分钟每书均60/60有效，缺失分片为0。`07:46` 全部3108书×60秒实际回放通过，完整读取约12.04秒、连同逐秒回放约19.95秒。163份生产来源证据均在持久目录存在，gzip解压hash一致。核验时主collector与ClickHouse均active/running、NRestarts=0；未见部署后ERROR，初始化更换预热路线的10条 `context canceled` 连接退役记录未导致分钟缺失。

五路CEX盘口（instrument 1、3、5、6、7）最新 `07:47` 分钟均60秒有效、stored_depth=10。DEX继续推进到26117598，部署后28个live区块中26个报价complete；另两个保持真实partial，不以部署成功扩大为所有DEX报价完整。146条收益路线已有部署后观测、来源hash均存在。以上为约五分钟的现场窗口，没有以此证明长期运行、新挂牌订阅P95或未来真实到期事件；动态加入/退出逻辑另有确定性回归。

证据：[生产分钟提交及质量](../../../research/2026-10-04-options-lifecycle-deployment/production-final-probe.json)、[生产全部秒回放](../../../research/2026-10-04-options-lifecycle-deployment/production-all-seconds-replay.json)、[生产原文核验](../../../research/2026-10-04-options-lifecycle-deployment/production-raw-validation.json)、[现有分支和服务状态](../../../research/2026-10-04-options-lifecycle-deployment/production-health-summary.json)。

## 历史数据限制与回滚

对期权质量表的无时间范围查询发现9月历史分片 `crypto_market_info.derivative_book_quality_minute/202609_1_5549_21` 读取 `replay_valid_bitmap.cmrk2` 时出现 `UNKNOWN_CODEC (family code 0)`。这是本次未写入的旧月份分片，当前R5按具体分钟读取与回放成功。保存[原始异常](../../../research/2026-10-04-options-lifecycle-deployment/legacy-september-quality-read-error.txt)，没有detach、删除或重写该分片，也没有宣称全部历史健康。R5现场质量统计明确限定到本次计划起点；该限定不能修复或掩盖旧数据缺陷。历史损坏应单独核查及恢复。

需要回滚时先停止主collector，恢复上述备份目录中的 `collector` 和同名unit至原安装路径，执行 `systemctl --user daemon-reload` 后启动主collector。回滚恢复R4固定选约行为，五张新增表及已写R5历史保留。数据库仍有R5表，读取回滚后的R4分钟应显式指定其实际run ID，不能把停止更新的旧R5计划视为当前完整覆盖。此次未实际执行回滚。
