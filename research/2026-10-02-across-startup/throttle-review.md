# Across 历史 RPC 节流复审

2026-10-02，独立审核 Agent `/root/across_design_reviewer`。结论：代码与测试复审通过，未发现新增未关闭 P1/P2，可以将已构建新二进制用于历史任务。实时进程继续使用旧二进制，本次审核不宣称实网限流问题已经解决；须看限速续跑是否实际跨过停点。

## 本次改动

新增 CLI `--rpc-min-interval`，默认 0 保留实时批量与并发路径。负值在访问数据库之前拒绝。history unit 两条固定区间命令明确传入 `500ms`，没有给 watch 增加限速参数。

正间隔下，Reader 将请求拆为单成员，以共享 context-aware gate 串行执行，持有 gate 到响应完成，再冷却指定间隔。Header、日志、合约状态和收据均走同一 Reader 调用入口。已检查排队/冷却取消、发起前 context 检查、失败/null 后剩余成员不发送、原始 payload hash 保存、Members/proof 访问锁。未发起成员为错误与零 payload，不伪造来源响应。

Header 总预算按每成员 `interval + 5s` 计算，防止溢出，父 context 更早截止仍优先；默认路径仍为整个批次 5 秒。默认 0 的 Base 每批 10 / Arbitrum 每批 20 与共享 transport 两请求并发保持原实现。

## 验证

独立执行 `GOCACHE=/tmp/crypto-market-info-go-cache go test -race ./internal/across ./cmd/across-data ./internal/storage/clickhouse -count=1`，三包全部通过，退出 0；原输出见 [throttle-tests.log](throttle-tests.log)。新增测试覆盖并发调用真实发起间隔和证据、预先取消、冷却取消、等待活动请求时取消、来源限流/null 后停止、预算与防溢出、CLI 负值访问 DB 前拒绝。原有实时分组和协议/恢复测试也同时运行。

作者另报告 ClickHouse/shared Ethereum 包兼容测试与 `git diff --check` 通过；本次独立审核未重复这些命令，也未向运行中数据库写入。

## 既有限制与部署边界

`collectSplit` 保持既有逻辑：错误的已提交日志尝试可拆小，RPC 错误也可能进入这一分支，未区分纯范围超限与限流。每段最大 512 块，单条持续失败路径最多经历 10 个区间层级（9 次二分）再退出；每个网络请求同样受新冷却限制。取消、存储/证据写失败和不可拆分单块错误仍退出，history service 失败后等待 60 秒重启。本轮没有修改共享 `ethereum.Client` 的错误分类或增加调度器。

500ms 是单个 history Reader 的冷却间隔，不是整个机器/IP 对来源的请求上限；live 的请求仍共享公共节点额度。通过夹具和 race 回归证明限速/取消代码行为，不证明来源必定接受该速率、历史状态可用或完整 30 日任务完成。Arbitrum 的历史状态缺失仍按 raw/partial 保留。本次不改实时进程与路由。

## 审核版本

21 个代码、测试、ABI、配置及专项 DDL 文件汇总 SHA-256：`73d0c8379740bba9e42816fb7b9d6973e7549c914bcd78146cf7a3b8e32dfaae`。

算法：相对路径按字典排序，对每文件依次输入 `UTF-8 相对路径 + NUL + 原始文件字节 + NUL`，计算 SHA-256。纳入 `internal/across/*.go`、`internal/across/*.json`、`cmd/across-data/*.go`、`config/across-research.json`、`internal/storage/clickhouse/across.go`、`internal/storage/clickhouse/across_schema.sql`、`internal/storage/clickhouse/across_integration_test.go`。不包含运行 unit、文档、证据或全局依赖。

| 文件 | SHA-256 |
|---|---|
| `internal/across/rpc.go` | `e2d7efd6d1c098a810919dd70d33eab4e6166caed0c54220feda395e49610967` |
| `cmd/across-data/main.go` | `a183744fd991f1e8f77378a07d4097fd2fa04d396221dafe01621b8988493c8e` |
| `internal/across/rpc_throttle_test.go` | `19b0a00bf8333ff49d018b16229031c1d1933cba3d17bf3fdb76172d6bf7c4a0` |
| `cmd/across-data/throttle_test.go` | `360edaff18c499d84ef1ed7c7ee8695631e173d114935d1f96871949bcdda2fd` |

独立读取的待部署 `var/across/bin/across-data.next` 二进制 SHA-256 为 `60831b15d1d22cd4f410c7c4402f2fc6212dd25b98e1635b71e090108064e9df`。当前实时旧二进制 SHA-256 为 `48e000e554f7a75758a0cb6fec95e9fe28d0456a94ade3c78a9ab6f08cb2e8ed`；二进制 hash 与源代码汇总 hash 是不同口径。
