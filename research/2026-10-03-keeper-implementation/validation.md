# JustLend keeper 实现验收

日期：2026-10-03（Asia/Shanghai），存储时间 UTC。对应[实现说明](../../docs/justlend-keeper-data-implementation.md)和[独立代码审核](../../discuss/0017-justlend-keeper-data-code-review.md)。本记录保留失败预检，不将失败数据覆盖为成功。

## 自动验证

- `GOCACHE=/tmp/keeper-go-cache go test -race ./internal/justlendkeeper ./cmd/justlend-keeper-data` 通过。真实归档解析；HTTP 503 后 probe/cost 两次事实独立保存；429 只暂停对应来源；TLS 实际协商 HTTP/1.1；重复日志跨页及重复 provider index；冻结状态恢复；报告 canonical 去重、NULL fee 和固化冲突均有测试。
- 排队 6×200 个本地 HTTP 工作项，观察前 70 秒及闲置恢复，实际发送间隔不小于 5 秒；这是模拟时序验证，不是完成了真实 60 页 bootstrap。
- `KEEPER_DB_TEST=1 ... go test ./internal/storage/clickhouse -run TestKeeperFiveTableRoundTripAndFrozenRetry -count=1 -v` 通过（0.304 秒）。隔离临时库仅五表；UInt256 最大值、Decimal 18 位、binary address/hash、NULL、嵌套 UInt256 转账及 UTC 微秒精确往返；模拟事实先写但提交标记丢失，重复写入 FINAL 不重复；跨月变更身份拒绝、只读 writer 拒绝、离线报告通过。临时测试库已删除。
- `go vet`、DDL 源文件一致性和用户 systemd unit 校验通过。

## 实际数据库与预检

`crypto_market_info_justlend_keeper` 已建立五表。只写研究库，没有更改既有盘口、收益或其他独立采集库。

初次 Go 预检失败：`db7f523b-f5c4-4a41-b827-3641bb39f112`，可用时间 `2026-10-02T18:50:16.617462Z`，status=error，reason=transport_error。复制的默认 TLS Transport 遗留 h2 ALPN，服务端协商 h2 后 Go 按 HTTP/1 解析帧。诊断确认默认 HTTP/2 和 curl 都可连通，随后明确只协商 HTTP/1.1 并清空 inherited h2 handler。没有以大量重试排查，也没有通过改节点隐藏失败。

修复后实际六请求限速预检成功：`477ea65a-9da5-4c6c-8a8b-49f99b87393d`，可用时间 `2026-10-02T18:55:14.134489Z`，status=complete。两个公共正常 EOA、目标 implementation 和代码 hash 核验一致。实现地址 `418cb94f0ccb41cc8de909e00b01edef6a717ade8c`；预检没有签名、广播或调用写接口。

部署二进制 SHA-256：`c7c55a405dda346430e412fe7e1f90dd9006b8898d2104070471ec5588e46340`。后续真实服务启动与首批统计补在本记录末尾。

## 首批验收修复

初次服务于上海时间03:02:31启动，真实TronGrid分页和Binance报价可读，03:06暂停。累计23个capture、4条报价事实，其他三张事实表为空；15个bootstrap页的能源事件原始响应已归档。来源费率响应中`getRemoveThePowerOfTheGr=-1`合法，原unsigned解析错误已修为signed int64；两项费用为正数100/1000 sun，目标缺失仍报错。

跨进程report发现gob成员摘要不能认证：gob的全局类型注册ID依赖进程遇到类型的顺序，另有源Decimal八位与数据库十八位表示差异。此前同进程集成测试漏掉该边界。修复用`jl-keeper-fact-v1`稳定二进制摘要，Decimal固定18位，不用浮点数；manifest显式记录编码。新增不同子进程/不同gob注册顺序四类事实摘要一致性、NULL与零、1/8/18位真实来源数据库往返测试通过，race3.947秒、DB0.983秒。

全部23个旧capture逐一认证，仅以下四个非空quote失败：

```text
ebe8916f-0ea6-4921-99dd-cd6ce99053dd
11807f7f-5e38-4356-b428-00be4ec3a910
5126cce8-c0d2-45db-a09c-f8e10aa92c20
7c2ed946-c9fc-4851-9a5b-656213edd3a6
```

修复前完整五表Native备份（其中event/receipt/probe零行）、capture TSV、state.gob及79个原始证据SHA-256清单均在本目录`before-repair-*`。状态6个待办均未冻结，日预算54保留。仅上述四个cap修改`committed=false,status=error,reason=invalid_legacy_gob_member_digest`；保留所有事实、capture_id/time、成员数、原摘要与manifest。前后23行逐字段比较确认仅这四行的提交/状态/原因改变。对应after-repair-capture.tsv可复核；没有删除记录或重算原摘要。

修复后二进制独立CLI只读report成功，`first-report/coverage.csv`保留四行uncommitted；它们不计入已认证成本、收益或完整窗口。19个原空事实capture可继续认证。已明确追加不同进程测试，不以这份仍为空事实的报告替代后续非空正式数据验证。

服务03:26:14恢复后，真实新quote `725efc3a-5b35-42dc-b6ea-937490bb3380`与资源`237896d6-e507-432e-acef-e4801e8057b4`提交。资源费率100/1000 sun，proxy用户资源比例10、origin limit90000，status=ok。独立新进程CLI report已成功核验这些非空事实，关闭之前同进程测试的缺口。

随后检查实测请求时刻发现预留额度之后的fsync耗时波动使最小Started间隔4.962758秒；未将该偏差当允许误差。末修在收到headers/transport结束后再次向后延期，用当次Reserve的gap，跨warmup边界不降为一秒；所有期限取max，不改额度/冷却。另将组合资源行可用时刻统一为Finish/capture完成，参数source_time不变；旧行只在报告中采用保守effective availability，原事实不改。新增跨边界、落盘延迟、传输失败、组合可用时刻测试通过，最终race3.702秒，DB1.016秒；独立审核者再次完整-race通过。

当前二进制`629d19e0edd4eed038cc04ff1ae592bf0d31ecffce183caf375a9ae18bb1e5fc`于上海时间03:43:18原子安装并恢复同unit，active/running、NRestarts=0；已有预算、游标、原始证据与Frozen批次全部保留。

## 后续数据门槛

03:46起真实 seed 出现 `event_receipt_log_mismatch`，没有写为有效事件。根因是官方旧参考页的 Rent/Return 签名漏掉真实日志中的 `securityDeposit`、`rentIndex`。索引响应同时给出完整事件声明与具名值，且可与指定 implementation hash 下的 receipt 精确互证。新增两版精确 topic/word 数解码；扩展字段完整解析并以 Nullable UInt256 定型保存，缺一项或多/少 word 拒绝，参与 EventEqual 和 RawFromRow。真实租赁及部分归还的索引页、块、body、receipt 和 SHA-256 provenance 保存在本目录 `rent-v2-*`、`return-v2-*`。

迁移前生产event/probe均零行；`init-schema`仅给已存在五表的event增加两列，没有修改其他事实或摘要。旧失败capture保持partial及原证据。原state备份为`before-event-v2-state.gob`；未删进度或预算。恢复种子按身份一次只排一个，失败后30分钟、每个decoder revision最多3次，次数和下次时间落状态及manifest。恢复不会阻塞bootstrap完成后的已验证probe/live，也不把失败标关闭或换样本。

作者完整-race通过4.231秒；独立审核完整-race通过4.324秒；vet、diffcheck、DDL源文件一致均通过。修复二进制`4637bf2cfdd97c572e0b5188674f8d3d8b17e9547466b28f6b1e7d42ed6eb9ce`于03:58:11恢复同unit，active/running、NRestarts=0。实际初段17次已提交请求均HTTP200，最小Started间隔5.54603秒；这是部分启动窗口，尚不能替代全部五分钟核验。后续正式事件与probe验收追加在下方。

两列非空UInt256集成测试补齐：SecurityDepositSun使用UInt256上界、RentIndex=1051630712545733418，旧Liquidate两项NULL。中间fixture第二行漏给不同provider_event_index，被ReplacingMergeTree正确去重而使成员校验失败；测试身份改为1后，作者通过1.069秒，审核者独立-race DB通过2.119秒。不是生产数据修复或重算摘要。

03:58:11..04:03:11完整启动窗口可回读的51次请求（含当时pending证据）均HTTP200，最小Started间隔5.54603秒，见`event-v2-request-spacing.json`。旧state当日已用433次，恢复后的snapshot已用540次，未重置；50个固定样本中6个已验证，其余继续经gate一次一个恢复，不改变采样身份。

实际seed `b1e78646-ebdd-4d18-8a57-1a6f610aa3f1`、`cb3ae403-3e4d-40b1-96c5-eb13411249d8`各提交一条事件与收据；真实增量Liquidate `f7dae608-89a5-492e-9c48-600f7cf459f4`也提交一对。源页面/收据/typed v2字段逐值相同，Return的securityDeposit=0仍是非NULL。独立新进程report在251个capture时读取真实Rent/Return/Liquidate+cost完成成员及raw核验。

04:05:38起已提交四条真实只读清理模拟，均为TVM revert，并非采集失败；有前后latest头范围和实际响应SHA。例capture `69454f65-dfa8-4bf7-bbbe-b944b75e4e40`、`93f66225-7212-4a37-b6ab-8ad8bfc83a5c`、`bacd8d07-1670-4538-bb2a-4a8578344a48`。正式离线报告位于仓库`var/justlend-keeper/reports/latest`。这只说明对应采样时刻未通过模拟，不代表整个市场没有清理机会。

04:05左右压缩空间基线（system.parts活跃physical parts，含早期失败事实）：capture45430、cost33598、event24390、receipt24846字节；当时probe尚未纳入该查询。不外推为每日空间。24小时后按相同口径重新测量。

04:07正式验收：event9、receipt9、probe5、cost45行（各表FINAL，独立CLI报告截至20:07:09 UTC，320个capture）。probe5条均revert；审核者逐条源锚点抽查其中一条，API true但TVM FAILED/REVERT，typed保持revert、奖励NULL，energy18173与前后头86766298/86766302一致。服务enabled、active/running、NRestarts=0、用户Linger=yes，退出终端后继续。原始证据约7.08MB、本地状态约0.91MB；仍为首批实测，不认证全天容量。固定样本继续缓速恢复，旧partial和uncommitted在coverage中保留。

成功奖励模拟 fixture 尚未认证，当前所有正返回值保留 unknown/uncertified，禁止 success_reward；没有短期优先队列或可认证 episode 净收益。成本以最近已可用观测费率和 400 字节带宽为研究情景，能源实际报价、维护边界、其他成本 K、失败成本未知。

完整 24 小时实际请求数、稳定性、压缩空间及查询耗时应持续采集后补测，不能据首批外推为全天容量或盈利。历史奖励、首批租单样本和真实胜率不是同一件事。

## 文档补齐验证（2026-10-03 UTC）

README及architecture补入独立keeper链路，目标设计新增实施状态表。实现说明补齐参数／配置绑定、report锁例外、真实CSV字段、健康SQL、状态与证据备份要求、测试命令和后续验收条件；运行说明纠正“停watch即可backfill”的不完整操作。当前有watch待办时CLI会拒绝切换，尚无自动队列排空命令；本次仅据代码记录限制，没有修改采集程序、状态或启动新回补。

核验五个文档中的88个相对链接均存在，代码围栏与行尾格式通过；涉及文档的`git diff --check`通过。新增三条健康SQL从文档提取后在本机研究库用`readonly=1`执行成功，包含已提交批次、cost分来源／状态以及最近一小时probe，不访问外部数据源。配置键、CLI参数、错误码、CSV列名均与源码逐项核对；没有因文档改动重复跑外部采集或改动现有服务。

完整24小时运行测量、30日历史覆盖、获奖励成功fixture及实际获利成本仍是待验收项。本次文档补齐不把这些项目标作已完成。
