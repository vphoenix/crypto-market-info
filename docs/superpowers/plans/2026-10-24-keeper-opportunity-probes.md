# Keeper 机会探测 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development or superpowers:executing-plans. Steps use checkbox tracking.

**Goal:** 有界机会探测、严格奖励认证及完整存储，不扩大为交易执行。
**Architecture:** 单owner共享调度和原HTTP gate，新增订单状态定类型表与独立小型检查点对象。
**Tech Stack:** Go、MySQL/ClickHouse、既有runtimecheckpoint。
**Spec:** docs/superpowers/specs/2026-10-24-keeper-opportunity-probes-design.md

## Global Constraints
- UTC、UInt256/sun；NULL不是零；latest观测有前后头/hash。
- 不改生产服务、状态、schema；不发送交易。
- 保留固定cohort和125表迁移mapping/旧Capture/旧摘要。
- 单检查链、完成后30秒、所有seed/probe/state请求间隔至少5秒、总请求40000/日原冷却保留。
- 新状态不得让整个Candidates随每个probe重写。

## Review Focus
- 新generation发生于排队/seed失败/冻结重试之间，旧结果不得污染新状态（Task3）。
- 节点缺省TVM字段、缺日志/内部转账/资源note，不得伪认正奖励（Task2）。
- 新proof缺行/重复/篡改、旧七表历史和未迁移schema（Task1）。
- 重启未决操作、所有重试共享预算、全池公平性（Task3）。
- checkpoint/no-op/冷候选引用复用与存储读写放大（Task4）。

### Task 1: 独立typed订单状态与存储
Files: model.go, rental_state_model.go, evidence.go, export.go; storage/mysql and clickhouse keeper adapters/schema; schema install/check CLI; storage docs.
Interfaces: RentalStateObservation, RentalStateBatchProof; Batch.RentalStates []RentalStateObservation, Batch.RentalStateProof *RentalStateBatchProof. Capture不改。
- [x] 先写新/旧批次摘要、缺proof、导出及codec映射失败测试并运行。
- [x] 新schema及extension mappings、读写、Seal/Validate/export接入；成功probe改调用validatePositiveProbe函数（Task2提供）。
- [x] 跑Keeper+storage定向测试，记录旧映射/hash兼容与真实隔离DB验证条件。

### Task 2: 严格解析与逐笔奖励认证
Files: observations.go, rental_state_parse.go, reward_validation.go, parser tests；不改旧Transfers持久化形态。
Interfaces: ParseRentalState(body,ev,can,caller,scheduled,c) (RentalStateObservation,error); validatePositiveProbe(Probe) error; root.finalizeRentalStates(*Operation,time.Time)；具体模型见Task1定义。
- [x] 测96-byte元组、bool、溢出/截断/API/TVM、NULL；测试完整三证positive、资源操作排除、wrong addresses、missing/mismatch。
- [x] 实现解析与正奖励认证，合成fixture显式标记；保留未知金额/缺证据。
- [x] 运行相关测试，记录官方/公开bytecode核验的边界。

### Task 3: 生命周期与共享有界调度
Files: opportunity.go, state.go, watch.go, collector.go, client.go, runtime_owner.go, checkpoint.go, command flags; lifecycle/scheduler/request-gate tests.
Interfaces: SchedulerState独立对象；纯选择返回seed/check/probe；root负责Task1/2接口接入。
- [x] 写16无效槽、300公平候选、同generation重试上限、新租约/旧结果、5s实际gate/30s链间隔与重启测试并跑红。
- [x] 实现1:3 lanes、compact retry ledger、freshness三事件、generation guards、条件RPC与队列完成。
- [x] native runtime v2与legacy typed v3 checkpoint分离Scheduler引用，旧v1/v2可读，旧state默认恢复；main增加机会模式schema要求。
- [x] 测freeze/unknown commit/重启、coldCandidates引用稳定、失败不伪造当前状态。

### Task 4: 集成验收与独立评审
Files: integrated tests, docs/justlend-keeper-data-implementation.md, docs/market-data-storage.md, validation results.
- [x] 同输入基准记录idle/active写字节、fsync、请求数量、checkpoint尺寸；不宣称生产负载通过。
- [x] Go相关包、race、vet、diff检查；真实隔离DB不可用，roundtrip测试已实现但明确skip，非生产验收。
- [x] 独立全变更评审，复现并修复固定成员部分归还代际问题；完整设计/实施/测试证据交付。

验证结果、数据库环境限制与逐项证据见 [验收记录](../validation/2026-10-24-keeper-opportunity-probes.md)。不包含生产部署或真实获利验证。
