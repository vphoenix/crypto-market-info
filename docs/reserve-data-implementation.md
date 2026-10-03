# Reserve 公开数据采集首版

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

## 证据保留与备份恢复

当前首版没有自动保留期限、证据垃圾回收或已验收的自动备份。状态／报价和最终性修订持续积累；数据库压缩 bytes 与 gzip 证据占用分别测量。保留共享收据、旧分支和旧尝试，有助于核验重试及当时可用性；删除不能只按 capture 的当前状态决定。

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
