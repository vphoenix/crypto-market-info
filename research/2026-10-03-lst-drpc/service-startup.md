# LST 常驻服务启动记录

按用户要求于 2026-10-03 02:36:08（北京时间；2026-10-02 18:36:08 UTC）启动用户级 `crypto-market-info-lst.service`，并启用开机启动。用户管理器已有 `Linger=yes`；核验时服务 `active/running`，PID `1831314`，重启次数 0。

[unit 源文件](../../deploy/systemd/crypto-market-info-lst.service) 经 `systemd-analyze --user verify` 验证，安装到 `/home/ubuntu/.config/systemd/user/`。只运行已有 `lst-data watch`，固定 `eth.drpc.org` / `fapi.binance.com`，沿用 `var/lst/state` 和 `var/lst/evidence`，失败后等待 60 秒重启。没有修改采集代码、请求预算或表结构，也没有执行 `backfill --days`。现有 ClickHouse 与其他采集器未重启。

启动前研究库共有 18 个 capture，其中 11 个市场批次；最新市场观测在 17:51:54 UTC。启动后的只读查询与 journal 确认：

| 市场 capture | 开始时间 UTC | 协议状态 | 报价观测数 | 完整且及时的报价数 |
| --- | --- | --- | ---: | ---: |
| `17315180-47d0-4e22-b67e-359fd1bb6cf6` | 18:37:30 | unknown：`transport_send_reservation_expired` | 8 | 0 |
| `a53664e0-45cf-41c1-898f-9017acd8d947` | 18:38:30 | ok | 8 | 3 |
| `9672d114-98e0-4da0-834a-4bfdaacfa6f0` | 18:39:30 | ok | 8 | 1 |

首轮本地发送预约过期后数据明确保留 unknown，后两轮自然恢复。三个市场批次均为 partial/canonical，不能把全部 24 条观测解释为完整报价或机会。

另已写入资金费 capture `5f18769a-6cb7-42b0-904d-8ad00b1a054e`，complete，6 条实际资金费结算观测。实时日志游标尝试补 `26105616..26106127` 时仍被 dRPC HTTP 400 / code 35 拒绝；capture `e0d82a04-20d0-4137-aa61-27dc22ee516e` 保留 failed、零事件，不推进游标。来源错误没有停止市场或资金费采集，断线日志补缺能力仍受此端点限制。

18:39:55 UTC 的进程累计统计为 RPC 121 次、Binance 9 次，来源限流计数均为 0、未禁用。这是短窗口实测，不是长期免费额度保证。

服务操作与日志查询见[运行文档](../../docs/runtime-operations.md)。本记录写入后服务继续运行，不按验收时限退出。
