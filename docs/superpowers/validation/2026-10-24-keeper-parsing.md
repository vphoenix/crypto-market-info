# Task 2 — 严格订单状态解析与逐笔奖励认证

实现范围：`observations.go`、`rental_state_parse.go`、`reward_validation.go`、对应两个测试文件及 `testdata/keeper-rental-state/`。未修改旧 `Transfers`、`NativeTransfer`、`Probe` 或 `Capture` 的持久字段，未提交 Git，未操作生产服务、数据库 schema 或交易。

## 行为

- `ParseRentalState` 严格解析一个 96 字节返回值：当前押金 sun、uint256 租金指数、规范 bool（完整 word 仅 0/1）。截断、尾随数据、坏十六进制及无效 bool 不制造零。未知实现保留已解出的元组，但状态不为 `ok`。新的订单状态表将有效 TVM 成功统一标为 `SUCCESS`。
- 与 `ParseProbe` 共用 API/TVM 判定和来源身份提取，保留已有合法 protobuf 缺省成功的语义；显式 `ret:[null]` 不当成功。矛盾的 API 错误信息不认证；普通 uint256 恰好以 Error selector 开头不再误判为 revert。
- 正奖励由该笔响应认证：API/TVM 成功、已核验实现、唯一匹配的清算事件、正整数返回值，以及市场向 caller 的实际原生现金支付；三方金额精确一致。重复/错误事件、币种不符、付款被拒绝、未知/缺失 trace 都不能认证。资源委托撤回只在新的 probe 支付路径排除，旧收据摘要不变。
- `validatePositiveProbe` 在最终提交前要求来源哈希、有效时间顺序、匹配实现与代码指纹、新鲜身份检查、合法独立 caller、完整且一致的前后头，以及相等的三项正 UInt256 金额。解析阶段的 provisional `success_reward` 仍需 root 的 finalizer 补全头并通过此检查。
- 支付 trace 解析失败时保留已经解出的 return/log 金额，降为 `unknown/reward_evidence_invalid`，不再因返回 error 而使 collector 丢掉已知数值。

## TDD 与验证

先观察到真实行为红测：完整三证仍被静态门禁拦住、资源操作将 20,000,000 sun 错算成 119,000,000 sun、null trace 当零、未知 note 当现金。随后观察到并修复新增边界红测：显式 null rejection、矛盾 API 错误、数值的 selector 前缀、支付解析异常导致已知金额丢失。元组解析和提交校验也先通过接口骨架观察行为失败，再实现。

最新普通完整回归：`GOCACHE=/tmp/cmi-keeper-go-cache go test ./internal/justlendkeeper ./cmd/justlend-keeper-data -count=1` 通过（40.251s、0.005s）。同两包 `go test -race ... -count=1` 通过（257.928s、1.882s），未报告数据竞争；`go vet` 通过，本任务文件 `git diff --check` 通过。root 负责最后统一集成验证，本任务不再追加全包测试。

## 来源与测试边界

`testdata/keeper-rental-state/rent-info.raw.json` 是未改动的 2026-10-02 公开 constant 响应，SHA-256 已由测试核对。实际返回 `(0, 1054569691767344913, false)`，不是正奖励清算。

`abi-audit.json` 保存三次公开只读查询的 URL、请求、UTC 时间、响应哈希和当前实现指纹。两个节点 ABI 均为空；官方下载的精简 ABI 有 `getRentInfo`，仍没有 `liquidate` 函数条目。后者签名/奖励返回由官方说明支持，同字节码的有限 dispatcher 检查支持非零 native CALLVALUE 拒绝和单 word 返回；不是完整 TVM 回放。

所有正奖励测试响应明确是 **synthetic**。没有把实际交易 receipt 拼接成或宣称成真实正 constant fixture；也没有声称取得真实正 constant 响应或完成本地 TVM 复现。生产正记录必须自身满足严格认证条件。latest 前后头只界定观测范围，不保证同块状态或未来抢单成功。

## root 接口只读复核

检查了 `collector.go` 与 `research_observation.go`：失败继续后置头、订单观测先落批再报告解析错误、发送查询/模拟前再次检查 generation、最终正奖励验证失败降级，均符合解析接口。已知 return/log 在支付错误时丢失的唯一发现已在本任务解析路径修复。提醒 root 更新 `config.go` 关于永久成功 fixture 门禁的过时注释。
