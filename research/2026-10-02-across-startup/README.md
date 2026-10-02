# Across 启动验收（2026-10-02）

用户已明确授权启动并开始抓取。2026-10-02 13:57:18 UTC安装、启用并启动 `crypto-market-info-across.service`；原先没有 Across 常驻进程。主库为 `crypto_market_info_across`，证据为 `var/across/evidence`，启用 user linger、开机启动与失败自动重启。既有行情、DEX、Reserve 服务未重启或修改。

启动前后记录见 `baseline.json`、`report-initial/` 和 `report-live/`。主库从 Base 52078449、Arbitrum 510991816恢复，逐段追赶；旧块不会伪装为实时首见。所有事件计数含这两个SpokePool其他路线，不代表可赚钱订单数。读报告时必须同时看coverage；没有作净收益结论。

30日回补使用独立 `crypto_market_info_across_history` 与 `var/across/history-evidence`，一个oneshot writer顺序执行两条链：

| 链 | 固定起点 | 固定终点 | 起点时间下界 UTC |
|---|---:|---:|---|
| Base | 50783591 | 52079591 | 2026-09-02 13:42:09 |
| Arbitrum | 500994686 | 511000892 | 2026-09-02 13:42:29 |

窗口按各链当时finalized头向前30日的真实时间二分定位，并检查起点前一块。完整公开响应在 `window-8453.json`、`window-42161.json`，复现代码 `freeze_window.py`；冻结的unit不会重跑该脚本或滑动窗口。history执行期间systemd显示`activating (start)`，两条命令均结束后`active (exited)`。失败重启重新进入第一条命令，已提交连续覆盖被跳过。两个库可能有重复事件，不能直接相加订单、收入或费用空间。

## 实际遇到并保留的缺口

- Arbitrum默认 `arb1.arbitrum.io` 部分历史state不可用：原始日志和锚点已保存，但缺实现校验的范围只标partial。`arbitrum-default-history-errors.json`摘录原始证据引用；不把它当成无交易或收益为零。
- 独立审核Agent试过Arbitrum PublicNode、dRPC、OnFinality：均未找到可直接替换的匿名历史入口。PublicNode历史请求明确需要个人token；dRPC还限制免费batch最多3成员；OnFinality有429。见 `rpc-alternatives.json`。没有更改运行RPC域名、购买服务或申请凭据。
- Base默认 `mainnet.base.org` 的30日前实现槽、bytecode及512块日志已实际验证，见 `base-history-capability.json`；PublicNode的Base历史也需要个人token，见 `base-publicnode-history-capability.json`。
- 回补首次已写入真实日志/收据，随后默认Base节点返回`-32016 over rate limit`，自动重试仍停在同一块。`history-start-errors.json`保留具体错误。为避免影响实时采集，短暂停止history，加上仅历史生效的请求节流后续跑。实时watch保持运行。
- 币安仍是 `api.binance.com`；ETHUSDT/USDCUSDT的HTTP200、来源时间和证据hash见 `binance-com-observations.json`。

## 审核与运行方式

`/root/across_design_reviewer`已核对实时unit恢复起点、writer锁、history固定高度及oneshot顺序执行。宿主 `systemd-analyze --user verify`通过。原始代码审核记录位于 `discuss/0013-across-stablecoin-code-review.md`；本次请求节流属于后续运行修正，单独测试和复审。

实际运行命令、库和证据目录见 `docs/runtime-operations.md`。此记录仅证明启动和实采，不证明30日覆盖已完成、长期在线稳定性、每秒轮询、抢单能力或获利。

## 限速修正验收

节流变更经独立Agent最终复审，无新增P1/P2；race覆盖Across/CLI/ClickHouse通过，另验证共享Ethereum适配器与ClickHouse包回归通过。见 `throttle-review.md` 与 `throttle-tests.log`。部署二进制SHA-256：`60831b15d1d22cd4f410c7c4402f2fc6212dd25b98e1635b71e090108064e9df`。原已审核二进制另存 `var/across/bin/across-data.pre-throttle`；实时PID未更换，继续原0间隔实现，下次服务启动将加载新版本的同一默认路径。history已应用500ms节流后续跑。

实网复核：14:20:53 UTC恢复history，14:23:25成功提交Base `50784359..50784870`，跨过原先限流停点。限速后的前220份原始HTTP响应均为单成员、HTTP200、无RPC错误；观察到的最小响应间隔705469微秒，见 `throttle-live-evidence.json`。这只证明当前短窗口恢复推进，不代表以后不会限流。

14:24:49 UTC最终快照见 `running-snapshot.json`：实时unit仍是原PID、0重启，主库558存款事件、733成交事件、403收据；最新日志来源时间Base14:24:43、Arbitrum14:24:30 UTC。history正在Base阶段，306存款事件、211成交事件、20收据；Arbitrum30日阶段尚未开始。history累计3次自动重启均发生在限速修正前，恢复后PID保持。30日回补未完成，实时与历史不得直接合计。

14:24 UTC后仍出现3次收据RPC错误和1次日志RPC错误，后者原始响应再次为`-32016 over rate limit`，见 `throttle-later-errors.json`。因此500ms只降低突发并恢复了推进，没有彻底消除共享公共节点限流；此时history进程未重启，继续较小范围重试。失败原样保留，不能把“前220份成功”外推为后续全部成功。
