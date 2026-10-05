# Reserve 公开数据采集首版

当前七表及响应留存政策见「2026-10-04 数据库存储修正」，请求优化见文末「2026-10-05 完整性优先的请求修正」；前文运行记录保留其历史日期。
2026-10-02 已建立独立库 `crypto_market_info_reserve` 并实现 `cmd/reserve-data`。一个 Go 进程、五张定类型表、一个 SHA256/gzip 原始证据目录；不调用交易接口、不使用钱包。正常运行只访问 [Ethereum PublicNode RPC](https://ethereum-rpc.publicnode.com)，使用 HTTPS JSON-RPC POST。官方目录只在手选地址时使用，运行不依赖目录网站。

首个白名单是 Ethereum DFX `0x188d12eb13a5eadd0867074ce8354b1ad6f4790b`，链上 r5.0.0，完整六成分，share 与六成分 decimals 均为18。配置见 `config/reserve-ethereum.json`：15条经实际 factory/pool 查询核验的有限路径，包括六成分、share、WETH 与 USDT 参考，每资产最多两条、每条最多两跳。白名单固定地址和 decimals，转账语义明确标为待联合交易模拟验证；没有将 Quoter 成功当成交易执行证明。新资产、代理实现或 decimals 不匹配时仍保留完整状态，停止相应经济报价。

规则固定官方 r5 commit `241f0d244c45a311a4cd2af2c383a4f3bbe56265`。有效 manifest 身份包含配置原文件 SHA256、内嵌 ABI SHA256 和 `reserve-r5-mvp-1` 采集语义版本；配置与身份对象一起写入证据目录。改变解码、金额或采样语义时必须升级版本。implementation 与 Quoter runtime bytecode 采用 Keccak256 校验；池地址由固定 factory、token0/token1、fee 核验。查询全部绑定 `blockHash`、`requireCanonical=true`，采集结束再复查 canonical。

## 数据与完整性

实际 DDL 见 [reserve-data-schema.sql](reserve-data-schema.sql)，与 Go 内嵌 DDL 同步。五张表为：

| 表 | 内容 |
| --- | --- |
| reserve_capture | 日志范围或状态批次；区块锚点、最终性、覆盖、数量、成员摘要、提交修订 |
| reserve_folio_state | proxy/version、费用、待分配份额、完整篮子与余额、rebalance/auction 权限及状态 |
| reserve_route_quote | 三档 USDC 申赎/拍卖、实际金额与所有腿、路径、共享池、成本参考、失败原因 |
| dex_log | Folio 全部原始日志，逐事件区块位置，按既有定型结构保存 |
| dex_tx_receipt | 对应交易及完整收据，包括 Folio 白名单之外的全部转账日志和实际 gas |

金额为 `UInt256`，未知为 NULL；Go 使用 `big.Int`。时间为 UTC 微秒。嵌套 Tuple 的原生驱动转换只在 `big.Int` 与 `*big.Int` 间做精确转换，不经过 JSON 或浮点。二进制地址/hash 的成员摘要使用确定性二进制编码，避免 JSON 替换无效 UTF-8 的碰撞。

事实先写、capture 提交最后写。DB 重试保留整批 ID、时间和字节；新的 RPC 尝试用新 batch。`receipt_refs` 是 capture 中定类型的 `(block_hash, tx_hash, receipt_hash, calldata_hash)` 数组，只引用当时实际验证成功的收据。后来补齐收据不会改变旧批回放。共享收据保留首次成功观测时间；完整原始收据还必须包含选中的全部日志。相同链上日志身份的内容冲突报错。

读取先 `FINAL` 解析每个批次的最新修订，再过滤 canonical/committed/finalized。重组撤销旧分支所有尝试，包括部分失败批，日志游标从首个撤销范围之前恢复。确认过的 finalized hash 发生冲突则停止。历史事件块会读取 proxy/version；升级发生在同块或版本不可核验则标 unknown，保留原始日志；首版不重建历史投标权限。

## 采样和报价

每2秒检查 head；每个遗漏高度都抓日志。新拍卖/篮子/费用/权限等事件立即触发状态采样，尚未开始且未结束的拍卖保持逐块监控。最终性按唯一高度批量复核，只在状态变化时追加revision。安静期间每60秒状态/报价一次，活动拍卖逐块报价；观察到篮子毛正片段后临时逐块观察至少10个区块。单轮状态/报价限制300个 RPC 成员、10秒，HTTP 每批最多20成员、最多两并发。拍卖先批量探测完整有向成分对，再轮转最多六个全局可投标对，未覆盖数量记录在 `skipped_routes`。三档目标为5,000/20,000/50,000 USDC，批量分阶段读取，实际花费和容量独立保存。

最终性复核的区块头请求专用五成员小批，全部成功才处理修订。Watch 主循环遇到明确的传输/读取超时、截断、HTTP429/5xx时保留游标，两秒后重新取链头并复核，再继续采集。事件触发标记在采样失败后保留。非法响应、证据或存储错误、最终性冲突仍停止；启动预检失败仍可由 systemd 重启。重试不补造失败时刻的报价。

完整篮子向量保留零余额成员，但零金额成员不生成 AMM 腿。mint 使用 `toAssets(Ceil)` 并逐步复现 checked 费用乘加、DAO 最低3bp和净份额；redeem 使用 Floor，弃用 Folio 仍可赎回。`totalSupply` 已含待分配份额，不重复相加。活动状态与 bids 权限独立检查；rebalance 截止时间不会提前关闭仍运行的拍卖。

独立腿共享任何池时标 `indicative_overlap`，缺失任何必要腿时标 `incomplete`。`quoted_complete` 仅表示完整独立报价，执行和联合价格影响仍须后续验证。Quoter 已含池费，不能再扣一次。

同批成本参考保存 USDT↔USDC 的100k/1m金额档，以及 USDC→WETH exact-output 的0.001/0.01/0.1 ETH档。假设 gas 情景只采用匹配档或更大档，标明上界；缺档为 unknown，不线性缩放、不取小档。Quoter gas estimate 不等于执行 gas。

## 使用

```bash
go build -o var/reserve/reserve-data ./cmd/reserve-data
var/reserve/reserve-data init-schema
var/reserve/reserve-data watch --once
var/reserve/reserve-data watch
var/reserve/reserve-data backfill --days 30
var/reserve/reserve-data backfill --from FIRST --to LAST --chunk 64
var/reserve/reserve-data report > var/reserve/report-finalized.json
var/reserve/reserve-data report --finalized-only=false > var/reserve/report-head.json
var/reserve/reserve-data report --gas-units 500000 --tip-wei 1000000000
```

`report` 连接 readonly=1，不初始化库。默认只读 finalized，包含 head 必须显式设置。报告输出 JSON 摘要与最多20个毛正观测，带最终性、源区块时间、实际输入/输出和成本假设。完整事实可通过 `ReserveBatch` 或 SQL 查询。首版不输出设计中的三个 CSV，不计算窗口持续时间、已确认新机会数、实际对手利润或年化；这些报告扩展暂缓，不能把观测数求和当日利润。

默认数据库/native 地址是 `crypto_market_info_reserve` / `127.0.0.1:9000`，证据为 `var/reserve/evidence`。凭据可通过 `RESERVE_RPC_URL`、`RESERVE_CLICKHOUSE_USER/PASSWORD` 环境变量提供，不写入仓库。写命令必须使用同一 evidence 目录；文件锁防止常驻 watch 与回补同时写。回补时先停止服务，完成后再启动；日志会从已覆盖位置续采。

部署 unit 为 `deploy/systemd/crypto-market-info-reserve.service`，单独启动，不重启现有 CEX 服务。

## 只读查询与报告口径

从 `report` 的 `manifest_hash` 取得有效配置身份，去掉 `0x` 后替换下面的 `<manifest_hex>`；它包含 ABI 和 collector version，不能拿配置文件自身的 SHA256 代替。连接独立 Reserve 库，查询账户设置 `readonly=1`。地址/hash 是二进制 FixedString，展示用 `hex`，查询用 `unhex`。

最近完整采样及覆盖分类：

```sql
SELECT capture_kind, capture_mode, finality,
       log_coverage, state_coverage, quote_coverage, receipt_coverage,
       count() AS attempts,
       min(from_block) AS first_block, max(to_block) AS last_block,
       max(available_at) AS last_available_utc
FROM crypto_market_info_reserve.reserve_capture FINAL
WHERE manifest_hash = unhex('<manifest_hex>')
  AND canonical AND committed
GROUP BY capture_kind, capture_mode, finality,
         log_coverage, state_coverage, quote_coverage, receipt_coverage
ORDER BY capture_kind, capture_mode, finality;
```

`attempts` 是采集尝试数。最小/最大高度只表示边界，不能证明中间连续覆盖；完整日志范围应做区间并集，不能把重叠范围长度直接相加。日志、状态、报价、收据覆盖分别检查，`not_requested` 表示该批未请求，不等同于成功。

需继续处理的日志／收据缺口：

```sql
SELECT capture_mode, from_block, to_block, finality,
       log_coverage, receipt_coverage, reason,
       available_at, lower(hex(batch_id)) AS batch_hex
FROM crypto_market_info_reserve.reserve_capture FINAL
WHERE manifest_hash = unhex('<manifest_hex>')
  AND canonical AND committed AND capture_kind = 'logs'
  AND (log_coverage != 'complete' OR receipt_coverage != 'complete')
ORDER BY from_block, available_at;
```

这里列出失败尝试，后来的成功尝试不会删除它；判断尚未填补的缺口须再与成功范围核对。当前 Watch 可在日志完整、收据不完整时推进日志游标，没有独立的收据补采队列；这种缺口需用显式 `backfill` 对相同固定范围重抓，不保证仅靠运行 Watch 自动补齐。

已最终确认的申赎／拍卖报价，按同一 capture 的最后一次可用尝试选择批次：

```sql
WITH selected AS
(
    SELECT chain_id, manifest_hash, capture_id,
           argMax(batch_id, available_at) AS batch_id
    FROM crypto_market_info_reserve.reserve_capture FINAL
    WHERE manifest_hash = unhex('<manifest_hex>')
      AND canonical AND committed AND finality = 'finalized'
      AND capture_kind = 'snapshot'
    GROUP BY chain_id, manifest_hash, capture_id
)
SELECT q.block_number, q.block_time, q.available_at,
       lower(hex(q.folio)) AS folio_hex, q.route_kind,
       toString(q.requested_budget_raw) AS budget_usdc_raw,
       toString(q.amount_in_raw) AS input_usdc_raw,
       toString(q.amount_out_raw) AS output_usdc_raw,
       q.quality, q.status, q.reason
FROM crypto_market_info_reserve.reserve_route_quote AS q FINAL
INNER JOIN selected USING (chain_id, manifest_hash, capture_id, batch_id)
WHERE q.route_kind IN ('mint', 'redeem', 'auction')
ORDER BY q.block_number DESC, q.route_kind, q.requested_budget_raw
LIMIT 30;
```

三类路线的预算、输入及输出为 USDC 原子单位（6 decimals），换算采用十进制精确除以1,000,000。实际输入可能因容量缩量而小于目标预算；NULL 保持未知。其他 token／份额数量按白名单各自 decimals 解释。SQL 仅供检查事实和筛选，不验证成员摘要；用于研究的批次还须经过 `ReserveBatch`／`report` 的成员校验。毛差额使用带符号的大整数计算，不直接做可能下溢的 UInt256 减法。

报告字段的计数口径：

| 字段 | 含义 |
| --- | --- |
| `captures`／`canonical_captures`／`finalized_captures` | 当前 manifest 的尝试及修订筛选计数；在内容的 finalized-only 筛选和 capture 去重之前计算 |
| `live_snapshots`／`complete_states`／`quote_quality` | 经过所选最终性和批次去重后的内容；quality 不计七条成本参考 |
| `unique_logs`／`unique_receipts` | 按链上身份去重的已引用事实；首尾高度不是历史覆盖证明 |
| `positive_observations` | 最多20条毛正观测，按毛差额排序；不是全部正样本数或独立交易数 |
| `net_usdc`／`cost_status` | 仅在明确假设 gas 且参考档覆盖时计算；其他成本仍未知 |

`available_at` 是首次事实可用时间，finality 修订不刷新它；历史补采的可用时间不能改成过去区块时间。比较后续报价或对冲数据时，同时约束链上位置和实际可用时间。

## RPC 切换与固定范围续跑

`RESERVE_RPC_URL` 是唯一端点覆盖项，没有自动换节点。历史节点需能提供目标范围的日志、交易／收据和历史实现身份；实时经济报价还需支持指定 blockHash 的 EIP-1898 只读调用。单个近期空日志响应不代表通过30日能力验收。

前台命令读取当前 shell 环境；已运行的 systemd 服务不会继承新 export。长期切换时，在仓库外的本机权限为0600的环境文件保存端点，并给 `crypto-market-info-reserve.service` 增加 `EnvironmentFile` drop-in；例如引用 `%h/.config/crypto-market-info/reserve.env`。安装／修改 drop-in 后 `daemon-reload`，再重启这一研究 unit。该文件与 drop-in 尚非默认部署配置；带凭据的内容不放入 manifest、仓库或验收输出。

回补步骤：

1. 停止 `crypto-market-info-reserve.service`，保留同一库、manifest 和 evidence 目录。
2. 冻结要补的 `--from`／`--to`，结束高度须已 finalized；先用 `--max-ranges 1` 验证一片的日志和收据覆盖。
3. 对固定范围执行 `backfill --from N --to M --chunk 64`。遇到首个不完整范围会保存缺口后退出；当前 Backfill 不自动递归二分或无限重试。
4. 修复来源后使用相同范围与 chunk 续跑，完整且收据齐全的相同分片会跳过。`--days 30` 每次按当时 finalized 时间重新算窗口，不能用它承诺续跑原冻结区间。改变 chunk 可能重抓重叠范围，查询仍需去重。
5. 保存只读覆盖报告，恢复这一 unit 的 `watch`。回补不恢复过去的实时报价；报告中的历史完整性和实时完整性分别核验。

## 首版的证据备份方式（历史记录，已被2026-10-04替代）

2026-10-02首版没有自动保留期限、证据垃圾回收或已验收的自动备份。以下记录当时的方式，当前只需备份七表及静态规则，不需要原始响应文件。

备份时停止这一独立 writer，取得整个五表库的一致 ClickHouse 备份，同时保存 `var/reserve/evidence`、公开 manifest、匹配的 ABI／collector version、构建身份和 unit 配置，再恢复 watch。备份方式需匹配本机 ClickHouse 已配置的目标；本文没有假设某个 backup disk 已可用。凭据单独管理。

恢复先在新的隔离库和目录核验：使用匹配 manifest 运行 `report --database <restore_db>`，检查完整批次、缺口和收据引用；抽查证据 DAG 的根和所引用对象。证据路径为 `<前两位>/<SHA256>.json.gz`，文件名摘要针对**解压后的原始字节**，包含 calldata 等非JSON字节对象。`report` 验证数据库成员，不遍历原始证据文件；报告通过不能替代证据恢复检查。恢复库和线上库也不能同时当作两份独立机会累加。

## 尚待数据与实现验证的项目

仍待完成可靠的历史覆盖、真实活动拍卖／投标收据的实网核验，以及安静／活动时各自的持续负载测量。联合多腿模拟、资产实际转账语义、候选窗口去重与持续性、真实 gas／失败摊销、USDT 转换及对冲成本也未认证。CSV／窗口分析和历史权限复原属待实现能力；自动备份恢复属待验收运维能力。记录这些缺项不能替代真实验证，也不能据此给出日赚200美元或年化3%的结论。

## 实网验收与已知缺口

最早实网样本完整读取六成分及13条计划报价（六条申赎、七条成本参考），批量化后全部13条成功；共享池分类与只读回放均验证。早期开发 manifest 的样本保留在库里，新语义 manifest 不与其混合；其只读摘要及原配置位于 `research/2026-10-02-reserve-implementation/initial-report.json`、`initial-manifest.json`。

2026-10-02 11:33 UTC只读验收：当前 manifest 已有21轮完整状态采样、126条申赎观测，全部为共享池的 `indicative_overlap`，毛正观测为0；其中10轮采样已提升为finalized。这段短样本未遇到DFX事件，所以实网日志/收据行数仍为0；完整收据链路由专项数据库测试核验，不能声称已采到真实拍卖交易。最新一版于11:29 UTC部署，后续短时观察没有自动重启且持续写入；此前版本确实曾因公共节点读取超时重启。首版没有长期可用率或24小时容量验收结论。只读报告、覆盖检查和服务记录分别为研究目录中的 `live-report.json`、`finalized-report.json`、`database-acceptance.json`、`service-acceptance.txt`、`service-journal-final.log`。

11:34 UTC数据库检查已覆盖实时高度 `26104215..26104371`，157个高度无缺失；当前 manifest 增至22轮完整采样。历史403缺口另列，未混入实时连续覆盖。包含早期开发尝试与反复最终性复核的gzip证据目录当时约90 MiB；这是本次实测占用，不是稳定日增量。报告和SQL查询为依次执行的只读观测，采集仍在继续，计数可能略有差异。

免费节点对30天首个日志分片返回 HTTP403，失败锚点/覆盖已入库；独立预检还曾收到 `Archive requests require a personal token`。部分近期大分片也被拒绝，小分片成功与失败均实际保留。不能据此称30天没有拍卖。要完成完整30天回补，需要能够可靠提供该范围日志、收据和历史状态的 RPC；接口保持 `RESERVE_RPC_URL` 可替换，程序不会默默缩短请求天数。历史状态仅供核验事件身份，绝不假造过去的 Quoter报价。

验证：全项目 `go test -race ./...`、`go vet ./...`，及隔离 ClickHouse 的 Reserve 集成测试通过。专项覆盖 checked fee 及舍入、同 hash 整篮子、零金额、六对容量、RPC 混合失败、共享池、gas 桶、重组所有尝试/游标、UInt256超过200bit、NULL、提交顺序、重试、旧revision、共享收据历史引用和日志内容冲突。测试和实网证据位于 `research/2026-10-02-reserve-implementation`；独立代码审核见 [0012](../discuss/0012-reserve-data-code-review.md)。

## 2026-10-03 修复版（以此节覆盖前文过时运行参数）

代码版本 `reserve-r5-mvp-2`，DFX manifest identity 为 `0x04af64a255a41d2e2182eb44cc8148338ee62e7fbc5e37a43eac86a73ef93abc`，旧 v1 identity 保留。六表（原五表加 `reserve_route_simulation`）自动通过幂等 CREATE 增补；没有改写旧事实、删除证据或更改交易接口。

### 来源控制、错误与恢复

- HTTP 默认每批最多 10 成员、最多两并发。按 hostname 在 `var/rpc-state` 共享持久来源配额：请求准入最小间隔 500ms、每成员 100ms，不积攒闲时突发额度。这只能协调使用同一目录/政策的新进程，不能确定旧进程或其他应用是否占用了同一 IP 额度。
- HTTP 超时从配额准入后计起，默认 8 秒；snapshot wall budget 从 10 秒增为 35 秒，仍限 300 成员。`rpc_source_wait_exceeds_deadline`、`rpc_member_budget_exhausted` 和来源失败分别记录。
- 429 响应头一到达即写持久冷却，读取正文超时/截断也不能丢掉它；最低 60 秒，连续失败指数增加（最多 16 倍），尊重更长 Retry-After，安静 10 分钟后重置递增计数。HTTP200 中明确的 RPC 限流也触发冷却。重启不能清空冷却。主循环等待来源就绪后再轮询，不再两秒密集试。
- head 默认每 6 秒检查；安静采样仍为上一轮完成后约 60 秒。未 finalized 端点每分钟批量复核，观察到 rollback/hash/parent 冲突则立即复核。复核失败保留 pending 状态，成功前不能绕过并继续采集。活动拍卖/毛正 burst 仍尝试逐 head，实际吞吐受预算限制；没有实网活动拍卖负载验收。
- 重启按 canonical、committed、日志 complete 的连续范围并集续采；不会用最大端点跳过中间缺口。`watch --from` 可固定迁移起点，`--reconcile-manifest` 明确复核旧 manifest 尾部。
- 日志完整但收据 partial 不阻挡新日志游标；每分钟最多重试一个当前 manifest 的不完整收据范围，用新尝试及真实可用时间保存。backfill 依据已 finalized、日志/收据 complete 的区间并集跳过，不依赖旧 chunk 边界。已识别暂时来源错误最多重试三次，每次保留尝试；历史授权拒绝立即停止（exit 3），不通过切小范围绕过授权。
- 新 RPC 证据增加 allowlist 响应头（Retry-After、Date、Content-Type、rate-limit 字段）、DNS/connect/TLS/first-byte/write 时间和脱敏错误类别。`Sent=false` 表示未进入 HTTP 的本地等待/取消；`Sent=true` 表示 HTTP 尝试，实际写入另看 `wrote_request`。不保存带凭据的 URL、原始 net error 字符串、Authorization 或 Set-Cookie。
- 明确 DNS not-found/error、TLS certificate、connection-refused、network/host-unreachable 会停止此进程（exit 4），user unit 禁止自动重启此退出码。不修改路由。单次 deadline、读取超时或 reset 不能单独证明 DNS/TLS/路由故障；按证据分别记录。

可选参数：`--rpc-state`、`--rpc-interval`、`--rpc-per-member`、`--rpc-batch`、`--rpc-timeout`、`--rpc-cooldown`、`--snapshot-budget`。保持配额目录稳定；不能换证据/数据库目录就当作获得新的来源额度。

### 历史任务与研究输出

实时仍使用 `https://ethereum-rpc.publicnode.com`。其历史方法的 personal token 授权限制没有被节流修复。公开替代 `https://rpc.mevblocker.io` 已通过原失败首分片、历史身份调用、交易和完整收据实采，作为独立历史任务的显式来源；没有隐式节点回退或路由改动。

本轮冻结原 30 日窗口 `25889149..26104214`，历史库 `crypto_market_info_reserve_history`，证据 `var/reserve/history-evidence`，与 realtime 单 writer 隔离。历史任务停在 partial 时，按同一范围重跑会自动跳过完整并集，补齐收据缺口；不得用浮动 `--days` 代替冻结窗口。独立来源与目录须同时备份，跨库事件按 blockHash/txHash/logIndex 去重。验收结果和任务身份见 [修复记录](../research/2026-10-03-reserve-repair/report.md)。

2026-10-03 06:52:59 UTC 固定窗口已正常完成，215,066 高度的日志及所需收据完整并集均无缺口；保留 1,946 条原始 DFX 事件和 957 笔收据。源时间为 2026-09-02 10:56:35 至 2026-10-02 11:02:47 UTC。这是日志/收据覆盖，不是过去 30 日的可执行报价或权限复原。原 v1 首分片的 403 失败事实仍在原库中，新历史库保留其成功替代和后续重试尝试。

```bash
RESERVE_RPC_URL=https://rpc.mevblocker.io var/reserve/reserve-data backfill \
  --database crypto_market_info_reserve_history --evidence var/reserve/history-evidence \
  --from 25889149 --to 26104214 --chunk 512 --rpc-timeout 20s
var/reserve/reserve-data report --out var/reserve/reports
var/reserve/reserve-data report --manifest-hash 0x98cf5cc757bbbe69d9cc8219210cad0490ae70e6a19a4531f6f6aa74afc3a4a9
```

`report` 批量读取 256 captures 一组，仍读取全部成员表并验证计数、摘要及固定收据引用；SQL 查询时间不再随数千 captures 逐个增加。JSON 增加按 mode 的成功区间并集与实际 gaps、失败腿观察数及有正确 finality/mode 的联合模拟记录。`--out` 新增三份 CSV：coverage 保留所有尝试和 selected 标记；activity 去重原始事件并列完整收据/gas 事实；candidates 输出所有选中经济报价（含非正、失败、共享池、联合结果），不只导出 top20。

CSV/JSON 各自执行只读查询；后台继续写入时生成时刻与计数可能略有差异。引用 `quote_id`/batch identity 可核对。历史公开权限、赢家净收益、采样缺口之间的持续性仍未知，CSV 明确写 unknown；没有用今天权限或区块末报价反推历史机会。没有收益率/日赚认证。

兼容字段 `missing_log_ranges` 计数的是选中但不完整的范围尝试，不能直接当作当前缺失高度。范围不同的后续成功批次可以覆盖该失败范围；当前缺口以 `log_coverage_scopes.missing_blocks`、`receipt_missing_blocks` 和 `gaps` 为准。模拟结果读写均校验定类型字段、quote/payload 绑定摘要及资金/预算标记；JSON 与 CSV 都要求 manifest、quote、Folio、kind、金额、区块高度及 hash 严格对应。模拟 canonical/finality 由所选 canonical committed quote/capture 派生，不由模拟表自己宣称。

### 联合只读验证

`simulate` 在指定隔离库采一个 `research` snapshot，然后验证最小两条完整经济路线；`watch --simulate-every 5m` 每五分钟最多验证最小两条（当前 DFX 为 5,000 USDC mint/redeem）。研究 actor 的 Solidity runtime、ABI、compiler/source hash 均随证据存档，源码见 `internal/reserve/simulator/RouteProbe.sol`。无链上部署、签名、发送交易或私钥。

```bash
var/reserve/reserve-data simulate --database crypto_market_info_reserve_simtest \
  --evidence var/reserve/simtest-evidence --simulation-limit 2
```

选中白名单路径的交换、实际 Folio 操作与退出在一次 EIP-1898 eth_call 中顺序执行；不是 Quoter 数量简单相加。合成余额只代表资金假设，不能把 1,000,000 USDC 等同为用户实际 USDT 资金。`gas_internal` 不是整笔 gas；MEV、竞争、实际 inclusion、USDT 转换仍未认证。只读 bid 路径已实现，但本轮没有实际活动拍卖正例可验收。

增加 `config/reserve-ethereum-activity.json` 作为限定三候选的独立研究配置（DFX、ixEdel、DGI），没有扩生产报价白名单。实采已区分：ixEdel 为 r5、完整七成分，但资产/venue 尚未白名单；DGI 代理为 r4 实现，不套 r5 状态/经济 ABI，日志保持 unknown_version 原文。目录与链上身份只是研究范围证据，短窗口不能冒充三基金完整 30 日，也不能以 DFX 安静样本否定 Reserve 市场。

## 2026-10-04 数据库存储修正（现行政策）

按用户要求取消 Reserve API/RPC 响应归档。`reserve-r5-mvp-3` 的共享 RPC client 显式使用 `Archive.HashOnly`：计算原有 SHA256，然后丢弃正文，不产生 gzip 文件；其他采集器未通过此开关改变。collector 从内存中的响应解析、校验并写定类型事实，写入失败重试同一批，不用文件进行日常恢复。来源限速、持久冷却及明确网络故障停止逻辑仍生效，路由没有修改。

新增第七表 `reserve_receipt_data`，将过去只在文件中的完整交易调用参数（calldata）和所有回执日志转为数据库明细。它严格绑定不可变回执摘要、区块/交易身份、原 receipt/calldata 哈希、日志数量及二进制内容摘要；零日志有显式行。补采与查询直接从此表读取，摘要冲突、明细缺失、截断或选中日志不匹配都报错，不能生成看似完整的批次。原五表及模拟表的身份、旧 receipt_refs、可见时间和成员摘要不变。

错误在收到响应时直接分类，无需回读文件。服务日志仅记录有界 HTTP 状态、RPC code、允许列表响应头、方法与网络阶段时间，不记录正文、provider 文本、请求参数或完整 URL。每项响应头最多256字节、最多20方法/code、方法名最多64字节；诊断最多每10秒一条，由现有 journald 管理。来源的429冷却独立执行，不受诊断抽样影响。

`--evidence` 目录目前只需要 writer.lock；名称保留以兼容既有命令。小型静态规则保存到 `--rules`（默认 `var/reserve/rules`），包括版本身份、公开配置、ABI、只读模拟源码/编译产物；这些不是返回数据。报告明确 `raw_response_policy=not_retained`，来源 hash 只表示收到过的内容摘要，不能声称删除原文后仍可逐字核验原始响应。

旧库升级先停对应写进程，运行一次离线迁移；此命令不创建RPC client、不访问外部来源，不改变旧采集时间或引用。覆盖各版本、失败尝试或孤立尝试已写入的全部回执，验证SHA及所有明细后写新表；已迁移的行再次执行只校验，不覆盖。

```bash
var/reserve/reserve-data migrate-receipts \
  --database crypto_market_info_reserve --evidence var/reserve/evidence
var/reserve/reserve-data migrate-receipts \
  --database crypto_market_info_reserve_history --evidence var/reserve/history-evidence
```

所有库迁移完成、旧规则单独保留并实测无文件复用后，旧 `??/<sha256>.json.gz` 可以删除。不要删除 writer.lock、`var/rpc-state` 或数据库。备份七表、配置/规则与运行参数；已删除原文后不要回滚到依赖文件的 v1/v2 程序。本轮迁移、清理、实际服务和落盘验收见[记录](../research/2026-10-04-reserve-storage/report.md)。

## 2026-10-05 完整性优先的请求修正

用户要求采集可以延迟，但不能因此减少数据。本轮保留原报价规则（安静期上一轮完成后约60秒、事件触发、活动拍卖逐head及毛正burst）和部署的5分钟联合模拟；不实施5分钟报价或30分钟模拟。`latest`仍每轮至少等待6秒，`safe/finalized`改为每分钟复核需要时读取；观察到分支冲突或存在pending复核时也读取新tag。失败复核保持pending，成功前不继续采集。普通轮询由3个RPC成员减少到1个，复核周期仍不超过原规则，不降低业务采样密度。

日志从数据库连续成功覆盖的下一块起抓，每片最多512块，逐条保留片内所有白名单事件及所需完整收据。成功提交后才推进日志游标。授权、429或超时不会再缩片；仅明确的日志范围/结果数量限制会二分，成功后逐步恢复至512，避免永久退成单块。单块范围复用同一份起止header，后续canonical校验保留。`rpc_archive_auth_required`尝试仍保存missing，保留原游标，每5分钟才重新检查一次；不通过细分、跳过范围或服务反复重启处理授权失败。收据补采和来源429持久冷却沿用原规则。

这减少请求浪费，但不能解除来源权限，也不能把缺失报价补造为当时已知。完整性以canonical/committed成功区间并集、事件/收据成员校验及真实可用时间验收，不以最高区块或失败次数代替。未改变表结构、数据粒度、manifest身份、RPC来源或路由；实际二进制身份与实网观察见[本轮记录](../research/2026-10-05-reserve-cadence/report.md)。
