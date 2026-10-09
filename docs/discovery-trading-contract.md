# OKX 找币数据衔接

本轮实现公共行情采集与 `/home/ubuntu/crypto-arbitrage-trading` 的严格只读选币。数据进入采集项目的公开 ClickHouse；订单前的私人费用、币级实际杠杆、借币额度和账户余额由交易项目 API 重验，保存在交易私有 SQLite。本项目不读交易密钥、不下单、不保存私人账户资料。

| 输入 | 来源与单位 | 时间与拒绝条件 |
| --- | --- | --- |
| 下一期独立预测 | public/funding-rate 的 fundingRate、fundingTime，费率小数 | 原生 ts，须未来结算且来源/观察相差不超过90秒；不取 settledFundingRate、不回退 hourly 实际资金费 |
| 标记价格 | public/mark-price，USDT/基础币 | 单独 ts，不用本地收包时间刷新旧价格 |
| 抵押估值指数 | BASE-USD / USDT-USD，向上取18位，USDT/基础币 | 保留两个原生 ts，原始两个价格及单位；不假定 USDT=USD |
| 上一完整分钟成交量 | market/candles，vol×原生合约面值/乘数，与volCcy交叉核对 | 上一UTC整分钟、confirm=1；数量为基础币，不拿更早分钟顶替 |
| 公开费用 | 固定已核对官方费率公告 Regular 行，产品API核验group12/13/14和4/5 | 公告来源日期和生效日保留；本地复核截止2026-10-15，不自动续期 |
| 公开借币利率与额度 | public/interest-rate-loan-quota，daily rate与基础币额；Basic/Regular/override/config分项 | 无原生观察时间，明确标记请求观察；不冒充账户实际条款 |
| 抵押与风险 | public/discount-rate-interest-free-quota及position-tiers | typed marginal discount/liqPenaltyRate/imr/mmr/maxLever；reference borrow lever5明确为假设，执行换成私人实际值 |

永续档查询 SWAP/cross/instFamily=BASE-USDT，原生合约区间转为基础币。多币种借币档必须 MARGIN/cross/ccy=BASE，按 minSz/maxSz基础币区间，不能用instId/baseMaxLoan替代。实际币级响应可无ccy字段，必须把请求URL范围固定在来源元数据，拒绝非空币对身份或显式错币。缺失/null collateralRestrict不视为抵押许可。

表定义在 `internal/storage/clickhouse/discovery_schema.sql`，定类型模型在 `internal/model/discovery.go`，API采集在 `internal/exchange/okx/discovery.go`，循环在 `internal/app/discovery_runner.go`，writer/query在 `internal/storage/clickhouse/discovery*.go`。OKX_PAIRED_ENABLED 配对运行中自动启用；每币采集失败不刷新旧观察的时间，重试不会把上期已结算数据改成预测。公开请求共享限流，代际切换取消并等待旧 worker。

2026-10-08 全量部署补充：抵押折扣接口使用客户端共享的 1100ms 请求间隔，仓位档接口使用独立共享的 210ms 间隔，其他发现请求沿用 100ms 门限。门限在每一次实际 HTTP 发送前执行，包含重试和冷却后的请求，避免八个 worker 在冷却结束后同时发送。无原生时间的来源时间取成功物理尝试经过限流后的发送时刻，不把排队时间或失败尝试当作本次响应的来源时间。原有 Retry-After 冷却继续生效。

source_publication_intents 在任何事实写入前耐久保存不可变身份和首次writer知悉时点；原始事件时刻、观察时刻、首次知悉、实际完成分别保存。source_batch_status 只在事实、分钟差量、全部来源成员已耐久写入后 complete=1。失败后整调用重试沿用首次 intent，不回填旧记录的“当时已完整发布”。source_count/provenance_hash承诺排序的源URL、响应摘要和时钟；读者核对数量、角色、官方域、产品、源时间，全部成员读取后最后重新核对完整发布记录。新增列在旧行为空，不能因此获得交易许可。只使用必要元数据，不恢复原始JSON备份。

新盘口保留现货/期货5档、期权10档；旧10/50档仍回放原尺度，不改价格、数量或旧产品ID。私有观察不能写此公开库。

隔离真实公开采集验证：

```bash
GOCACHE=/tmp/collector-go-cache go run ./cmd/discovery-probe \
  --database discovery_validation_manual --assets BTC,ETH
```

命令强制 discovery_validation_ 前缀，从完整目录注册产品，只订阅选中币种，等待真实完整分钟及实际发布后结束，无账户和订单能力。不能把此临时库或临时写入用户冒充生产配置。常驻服务的正式替换、迁移及全币覆盖需按既有运维流程执行，避免双writer。交易项目的可重复只读验证脚本及证据见该项目 `docs/continuous-trading-delivery.md`。
