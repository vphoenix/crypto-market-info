# JustLend keeper 采集设计验证

2026-10-02。仅验证设计、DDL 和公开接口，未实现／部署采集器，未改变运行中的数据库或服务。

## 数据库结构

使用本机官方 ClickHouse local 26.8.1.1825，在独立 `/tmp/justlend-keeper-design-ddl-r1` 中执行完整 [五表 DDL](../../docs/justlend-keeper-data-schema.sql)，退出码0，得到 `jl_keeper_capture`、`jl_keeper_cost_observation`、`jl_keeper_probe`、`jl_keeper_rental_event`、`jl_keeper_tx_receipt`。

执行方式：将 DDL 复制至临时 SQL 文件，末尾附上查询 system.tables 的语句，再运行：

```text
/home/ubuntu/.local/share/crypto-market-info-clickhouse-bin/clickhouse local \
  --path /tmp/justlend-keeper-design-ddl-fresh \
  --multiquery --queries-file /tmp/justlend-keeper-design-ddl.sql
```

该检查说明列类型、Nullable、数组元组、约束、分区及引擎语法被接受，不替代实现后的读写、分页完整性、实际费用归属和失败恢复测试。未连接生产 ClickHouse。

## 官方来源实查

[verified-sources.json](verified-sources.json) 收录本轮15份原始响应及URL、UTC时间、SHA-256。已逐份重算并全部匹配。公开地址字段为协议／公开链上账户，不涉及用户私人账户。

1. 官方 energy-market ABI 缺少 liquidate；rentals 只声明1个返回值、getRentInfo声明2个。当前实际调用均返回96字节，不能按下载ABI静默截断。首版因此直接模拟 liquidate，省去计息器和截止时间预测。
2. 入口 getcontract 名称 MarketProxy，ABI空。`implementation()` 可调用，返回21字节TRON地址 `418cb94f0ccb41cc8de909e00b01edef6a717ade8c`。其 getcontract 名称 MarketG1、ABI空，bytecode SHA-256为 `c66f4e441f88f9bc0b2cf31be8417c5ea220596cfc49dd9abfebf85cf92c3782`。这是本次节点当前状态观察，不能替代30日历史实现身份。
3. 以租单receiver作为caller的模拟，API result=true，TVM ret=FAILED，解码revert为 `liquidate: not allowed to liquidate own orders`。更换为已存在的其他普通账户后，API仍为true而TVM为FAILED，revert为 `liquidate: no resource to liquidate`。这两份反例用于证明分类与caller要求，**没有取得可获奖励的成功模拟**。
4. 代理 consume_user_resource_percent=10、origin_energy_limit=90,000；实现自身对应值为100和100,000。不同入口/内部资源承担不能混用，也未验证开发者可用资源。首版用“全部模拟能量由自己承担”的燃烧情景，并保留实际收据中 origin_energy_usage 等字段。
5. `/wallet/getblock` 和 `/walletsolidity/getblock` 使用 `detail=false` 均返回约542字节完整头。先前 getnowblock 响应约0.5–0.7MB，正式设计采用轻量头；仅事件归属验证取完整块。
6. `/wallet` 不提供本设计可用的固定历史blockHash模拟参数，文档还说明会受pending与正在处理状态影响。因此模拟保存前后头与本地请求时间，标 `node_latest_unpinned`，不假装同一块的完整回放。[官方状态说明](https://developers.tron.network/reference/triggerconstantcontract)
7. TRON protobuf 中 AccountType.Normal=0，Transaction.Result.code.SUCESS=0；JSON省略默认枚举与缺完整对象须分别解析。尤其成功默认枚举不能覆盖revert内容或缺失输出。[协议定义](https://raw.githubusercontent.com/tronprotocol/protocol/master/core/Tron.proto)

验证辅助为 [probe_sources.py](probe_sources.py)、[inspect_implementation.py](inspect_implementation.py)。它们只发公开GET／查询或不广播的constant-call POST，不是常驻采集器；已保存的 unsigned transaction 仅为节点模拟返回，未签名／广播。

默认沙箱下首次联网因网络限制失败；随后只读联网经自动审批通过。该限制没有被当作协议无数据。本轮没有回补新30日历史、抓取完整资源商报价或运行实时策略观测。

## 审核

独立 Agent 审核方案、五表 DDL、原始响应与必要官方文档，结果见 [审核记录](../../discuss/0015-justlend-keeper-data-design-review.md)。主分析已纳入 caller冲突、固定抽样与恢复、按需收据、同块执行次序、无响应NULL、时间可见性、默认枚举和离散episode等修订。

## 来源分流补充（原复审之后）

2026-10-02 用户确认 `tron-rpc.publicnode.com` 可直连，选定 PublicNode 承担节点查询／只读模拟，TronGrid 仅保留事件分页。已同步方案第2.1节及数据字典；五表 DDL 不变，未实现或部署程序。本补充不属于上述独立 Agent 已复审版本。

当次公开只读请求用 curl 实测 PublicNode：`/wallet/getblock`、`/walletsolidity/getblock`（detail=false）、`/wallet/gettransactioninfobyid` 和 `/wallet/triggerconstantcontract` 均 HTTP 200，成功请求约0.9–1.2秒，两个头响应各542字节。收据样本为 `5c63519663ae37058868ed2d171d9d0edb272acf2be534ccb8f9d941a44e1823`，返回 SUCCESS 及资源消耗。模拟沿用原独立 caller 请求，API result=true、TVM FAILED、energy_used=18173，只证明接口可调用，未取得获奖励成功 fixture。

同 host 的 `/v1/contracts/TU2MJ5Veik1LRAgjeSzEdvmDYx7mefJZvd/events?event_name=Liquidate&only_confirmed=true&limit=1` 返回404，因此不能用 PublicNode 替换事件分页来源。初次默认 Python 请求头得到403，随后 curl 请求成功；实现应核验真实客户端请求头及其余所需方法，不把403误判成无数据。本次为连通性补查，原始响应未加入15份 verified-sources manifest；延迟只代表当次当前机器请求，不是稳定服务指标。

## 启动突发检查与设计修订

2026-10-02 按用户要求检查启动后集中请求风险。原方案虽有全局1请求／秒及日预算，但未明确启动检查是否经过 gate、是否积攒发送额度、底层重试／自动重定向、60页 bootstrap 恢复和来源冷却跨重启等约束，不能仅凭原文字宣称启动没有突发。

静态读取 `internal/exchange/http.go`，确认 HTTP 工具每次重试都有 Cooldown／BeforeRequest 入口；但默认5xx重试延迟为250ms起步，不自动满足 keeper 的缓速规则。`internal/yield/tron/client.go` 的完整 Fetch 会遍历见证人，不适合为此方案启动复用；设计仅允许复用必要的单接口方法。未修改这些既有客户端。

方案第7.1节已补启动前5分钟至少5秒间隔、其后全局1秒／单在途、TronGrid及后台补采5秒间隔、逐次attempt gate、不积攒额度／错过轮次不集中补发、有限重试、持久冷却与日预算／进度及本地mock验收。数据字典同步登记，五表DDL未改。属于设计静态检查及要求修订，尚未实现真实发送时序测试，也未对公开节点压测；不作免封IP保证。本修订不属于原独立 Agent 复审版本。
