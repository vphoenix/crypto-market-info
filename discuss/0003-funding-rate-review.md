# 0003 资金费率改动审查

日期：2026-08-20

## 结论

整体实现方向正确：估算费率已改为 WebSocket，实际费率由每个交易所一个 REST worker 延迟串行查询，`funding_time` 也保留了毫秒精度。

目前还有两项数据正确性问题需要修改。

## 1. OKX 不能把预计费率当作实际费率

文件：`internal/exchange/okx/funding.go`

当前代码在 `realizedRate` 为空时回退使用 `fundingRate`，然后将结果设置为 `is_actual = 1`。

OKX 对两个字段的定义不同：

- `fundingRate`：预计资金费率；
- `realizedRate`：实际资金费率。

因此，`realizedRate` 为空时不能使用 `fundingRate` 代替，否则预计值会被保存为实际值，并停止后续重试。

修改要求：

- `realizedRate` 为空时返回 `found = false`，由现有 worker 按计划继续重试；
- 增加一个 `realizedRate` 为空的解析测试，确认不会生成实际费率记录。

参考：[OKX API 文档](https://app.okx.com/docs-v5/zh/)

## 2. 启动时补查最近未确认的实际费率

文件：`internal/app/app.go`、`internal/funding/worker.go`

确认任务目前只保存在内存中，并且只根据本次进程收到的 WebSocket 推送创建。若程序在结算后、实际费率确认成功前重启，上一结算时点的任务会丢失。重连后的 WebSocket 通常已经指向下一结算周期，因此上一时点的估算值可能一直无法被实际值纠正。

修改要求：

- 程序启动时，只检查最近一个有限时间窗口内仍为估算值的资金费率记录；
- 将这些记录的原始 `funding_time` 交给现有的交易所串行 worker 补查；
- 补查继续遵守每个交易所请求串行、相邻请求至少间隔 1 秒；
- 不增加 Redis、Kafka、新状态表或持久化任务队列。

该要求与 `docs/implementation-design.md` 中“进程启动时只需对最近的有限时间窗口做低频补查”一致。

## 已通过的检查

- `go test ./...`；
- `go test -race ./...`；
- `go vet ./...`；
- ClickHouse 完整集成测试；
- 已有 `DateTime('UTC')` 到 `DateTime64(3, 'UTC')` 的迁移实测；
- 一日代表性数据写入、压缩及盘口回放测试。

除上述两项外，本次未发现新的关键问题。
