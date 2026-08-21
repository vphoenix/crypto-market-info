# ARB-0002 结算窗口采集实现说明

> 历史实现说明：本文件记录 `crypto-arb-observer` 的 Redis/PostgreSQL 窗口采集实现，仅用于评估旧代码复用，不是本项目的实现计划。

状态：代码已按确认的采集规则完成改造；尚未做真实交易所窗口或Production验收。

业务主源：[ARB-0002结算窗口数据采集规则](../strategies/arb-0002-settlement-window-collection.md)

当前状态以来源项目的 `crypto-arb-observer/docs/ai/current-state.md` 为准；该文件未迁入本项目。

本文记录代码怎样实现业务主源，不新增业务规则。本文不涉及机会事件生命周期、下单、账户、API Key、发布流程或Production部署。

## 1. 已实现的窗口流程

### 启动排期

- 启动时读取Binance和OKX合约metadata、最新funding及`next funding time`。
- Binance和OKX按同名币种直接匹配，并应用`funding.symbol_blocklist`排除已知错配。
- 任一交易所在`T`结算，就为该币生成一个以`T`为中心的窗口；双方同一`T`只生成一个窗口。
- 启动时若已进入`T-5`到`T+2`，整窗跳过，不从中间启动、不补采。
- 跳过窗口已有的`scheduled`或`sampling` batch会标为`partial`；不写新的深度状态。
- 跳过窗口结束后只刷新下一次结算时间并重新排期，不启动本窗口盘口和正式采样。

### T-5：一次性选择窗口深度

- 启动当前窗口需要的Binance和OKX funding/mark WebSocket session。
- 根据双方当前funding差和上一完整窗口的`top1`/`top100`状态，分别使用进入阈值或维持阈值。
- funding缺失或过期的币不参加`top100`排名，使用`top1`。
- 所有合格币按funding差降序、币种名升序统一排序，只取可配置的前`K`个使用`top100`；默认`K=20`。
- 同一币在Binance和OKX使用相同深度；`top1`和`top100`目标互斥。
- 深度在本窗口固定，不在135个采样点重新判断。

### T-3到T+2：固定135点采样

- 复用deterministic 135点sample plan。
- 每个采样点从latest cache读取funding、mark price和盘口，不在采样调用里同步请求交易所。
- funding、mark price和book分别保留自己的交易所源时间与本地接收时间。
- 盘口可用性按publisher最新`RedisWriteTS`判断；`LocalReceiveTS`只计算并保存`book_age_ms`，低活跃币没有盘口变化本身不判过期。
- 每个capture wave使用`pgx.Batch`批量写入。
- 数据缺失或过期时如实记录，不补抓、不切换深度。
- 保留同一进程内已经抓取数据的pending write retry；这不是进程重启恢复。

### T+2：关闭并保存窗口状态

- 完成最后一个正式采样点后关闭窗口funding/mark session。
- 对四个盘口Manager下发空目标，停止本窗口盘口订阅。
- OKX Manager收到空目标时先停止本地worker/publisher，再整体关闭并清空当前shard，不逐币发送unsubscribe；Manager保留运行，供下一窗口重新创建shard。单币resync仍沿用unsubscribe+subscribe。
- 只有按当前进程完整结束的窗口才写入该币本窗口最终`top1`或`top100`状态。
- 使用窗口内获得的新`next funding time`安排后续窗口。

## 2. 数据源与运行时

- OKX窗口期funding和mark使用公共WebSocket；REST只用于metadata、启动排期和必要的一次性刷新。
- Binance窗口期mark/funding使用现有WebSocket能力。
- Binance和OKX盘口继续复用现有四个perp-book Manager；Manager只接收当前窗口固定目标。
- 窗口外不为settlement collection持续抓funding、mark price或盘口。
- `settlement_window_runtime.go`、`settlement_window_controller.go`和`settlement_window_run.go`负责窗口生命周期。
- `settlement_window_depth_plan.go`负责`T-5`深度选择和四个Manager目标。
- `settlement_sampler.go`及其fixed-window辅助文件只负责已激活窗口的135点读取与写入。

## 3. 窗口间状态与字段

Redis只保存上一完整窗口的最小深度状态：

- `normalized_symbol`
- `last_depth_level`
- `last_window_t`
- `updated_at`

状态固定TTL为7天；缺失或过期时按上一状态`top1`处理。运行中断、启动跳过或未完整结束的窗口不覆盖它。

`FundingLatest`和对应Redis JSON分别保留funding与mark的时间：

- funding source event time和local receive time
- `mark_source_event_ts`
- `mark_local_receive_ts`

固定窗口深度原因是：

- `window_top1`
- `window_top100`

缺失或过期的数据继续使用已有诊断原因。replay和pair evaluation兼容历史数据中的旧reason，但新窗口不再产生旧adaptive reason。

## 4. 配置

当前窗口选择使用：

- `settlement_top100_max_pairs`：默认20，可配置。
- `settlement_depth_entry_min_rate_diff`：从上一状态`top1`升级的进入阈值。
- `settlement_depth_maintain_min_rate_diff`：从上一状态`top100`继续维持的阈值。

旧candidate threshold、record threshold、downgrade grace和per-venue max targets仅在raw YAML解析层接受并忽略，以兼容旧配置文件；它们不参与当前运行逻辑。历史冻结的`config/releases/*`未因本次改造而修改。

## 5. 明确不存在的采集机制

当前settlement collection没有：

- 采集层`candidate`或`active`状态；机会扫描器的candidate/active是另一条独立生命周期。
- 长期全市场`top1`采集。
- adaptive depth controller或采样点深度重算。
- depth checkpoint、restart debt或当前窗口重启恢复。
- 周期schedule refresh、authoritative reconciliation或历史补采。
- 容量hard gate、prewarm release gate或动态调参。
- 盘口批量unsubscribe状态机。
- 跨venue经济标的身份认证；当前按同名匹配，发现错配后删除错误数据并加入例外名单。

## 6. 已完成的代码验证

已运行并通过核心采集相关包的普通测试：

```text
go test ./src/arb-core/config ./src/arb-core/store ./src/arb-core/strategies/arb0002 ./src/arb-market-data ./src/arb-doctor
```

已运行并通过核心采集相关包的race测试：

```text
go test -race ./src/arb-core/config ./src/arb-core/store ./src/arb-core/strategies/arb0002 ./src/arb-market-data ./src/arb-doctor
```

完整`go test ./...`以及独立的`go test ./src/arb-funding-replay -count=1`、`go test -race ./src/arb-funding-replay -count=1`均被同一个既有且环境敏感的`TestSnapshotDirectoryProtectionAndNoChmod/symlink_directory`阻塞（symlink target mode changed）。本次没有修改release脚本规避该失败，因此不能声明全仓或replay package普通/race测试通过。

replay业务入口targeted测试`go test ./src/arb-funding-replay -run '^TestRun' -count=1`通过；store和pair evaluation对新`window_top1`/`window_top100` reason的兼容也随相关包测试通过。

已通过的测试覆盖窗口排期边界、启动落窗跳过、进入/维持规则、统一排序和`K`截断、`K=21`、四Manager互斥目标、OKX funding/mark WebSocket、固定深度135点采样、同进程写入重试、窗口关闭和状态写入等路径。

## 7. 尚未验证

- 尚未连接真实Binance/OKX跑完一个新实现的`T-5`到`T+2`窗口。
- 尚未核对真实窗口的135点完整性、实际WebSocket订阅规模、断线表现和数据库写入结果。
- 尚未部署或切换Production配置。
- 旧版本历史Production证据不能替代新窗口实现的live验收。

在用户明确授权真实运行环境操作前，只能把当前状态表述为“代码实现和本地测试完成”，不能表述为“live已验证”或“Production已完成”。
