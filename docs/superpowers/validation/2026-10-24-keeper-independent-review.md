# Keeper 有界机会探测独立审查（2026-10-24）

复核结论：本次发现的 P2 问题已修复并通过定向回归；当前没有未解决的已确认审查问题。

审查对象为 `codex/keeper-opportunity-probes` 相对基线 `5ea4744b8ef3cf4f75cea65b2d6768407097b86b` 的未提交改动及新增文件，对照 `docs/superpowers/specs/2026-10-24-keeper-opportunity-probes-design.md`。审查关注调度、恢复、生命周期、来源证据、定类型存储和历史兼容，没有操作生产环境。

## 已发现并修复：P2 固定 cohort 的部分归还导致永久停采

原路径：一个已验证、仍开放的固定 cohort 成员已被有界 `Candidates` 缓存淘汰。后续已认证的正余额 `ReturnResource` 令 `observeResearchEvents` 将旧 generation 标记为 `Superseded`，而原 `ApplyEvents` 只在 Rent 时更新成员 `Origin`。因此 `selectResearchMember` 此后持续选到旧 generation 并跳过该成员，即使租约仍开放，也没有可用的新 generation 恢复研究采样。

独立审查用临时 Go overlay 复现失败：cohort-only 成员完成一次 admission → 接收已认证的部分 Return → 再次调度生成零个 operation。该复现随后固化为 `internal/justlendkeeper/cohort_generation_test.go` 的 `TestReviewCohortOnlyPartialReturnRemainsSchedulable`。

修复位于 `internal/justlendkeeper/collector.go` 的 `ApplyEvents`：机会模式或已有 scheduler 时，已认证的部分 Return 同样通过 `RawFromRow(r)` 推进当前 `Origin`，保留 `Lifecycle`、`Closed` 状态及 cohort 成员。该更新位于既有时间和链上位置检查之后；旧事件仍不能覆盖新状态，完整归还和 Liquidate 仍进入关闭分支。独立复核确认修复消除了上述永久停采，未发现该修复引入的新问题。

## 其他审查范围

- 共享实际请求预算与持久化五秒间隔、单条在途检查链、完成后至少三十秒间隔、lane quota、公平轮转和 generation retry ledger。
- 固定研究成员与动态机会成员的来源认证、迟到 seed、同区块事件顺序和关闭状态。
- native v2 / legacy typed v3 检查点、独立 scheduler 对象、冷候选引用复用、已知 false/zero 的保留及旧冻结批次兼容。
- `getRentInfo` 严格 96 字节 ABI/bool 解析、未知值保留为 NULL、可信 true 才触发条件模拟。
- 正奖励 return/log/native payment 一致性、资源操作与付款区分、前后区块和实现身份要求。
- RentalState proof 的空批次认证、MySQL/ClickHouse 新表读写及重试、旧 Capture/member digest 和冻结迁移映射、导出认证。

上述范围未发现其他可确认缺陷。存储及奖励路径另由独立子审查复核；本报告不将源码审查等同于真实 MySQL 部署验证或真实正收益节点响应验证。

## 本轮复核验证

以下两次命令均通过，使用 `GOCACHE=/tmp/cmi-keeper-go-cache`：

```text
go test ./internal/justlendkeeper -run '^TestReviewCohortOnly' -count=1
go test ./internal/justlendkeeper -run '^Test(ReviewCohortOnlyPartialReturnRemainsSchedulable|OpportunityLateSeedCannotVerifyNewGeneration|OpportunityClosedGenerationIsNotReopenedByLateSeed|OpportunitySameBlockCloseCannotCloseLaterRent|OpportunityResearchMemberFollowsVerifiedPartialReturn)$' -count=1
```

没有运行全包测试或重复 I/O 基准。独立审查未修改实现文件；本文件为最终审查记录。
