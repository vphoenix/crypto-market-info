# DEX 新增流量分流提示词

```text
请只为 crypto-market-info 本次新增的 Ethereum DEX 流量配置路由器分流，使其走新加坡 Oracle VPS。现有 CEX 交易所分流已经完成，禁止修改。

代码核对结果：

- 新增 DEX 的唯一公网配置是 DEX_ETH_RPC_URL。
- 当前默认值：https://ethereum-rpc.publicnode.com
- 目标域名：ethereum-rpc.publicnode.com
- 协议：HTTPS，TCP 443，JSON-RPC POST。
- 没有 Ethereum WebSocket，没有 UDP，没有第二个备用 RPC 域名。
- collector 内的 DEX 持续采集和 dex-check 的 --sample-block、--quote-costs-rpc 都使用同一个 DEX_ETH_RPC_URL。
- ClickHouse 127.0.0.1:9000、健康检查 127.0.0.1:8123和本地证据目录不属于公网分流。
- DEX与CEX在同一collector进程，不能按整个进程、运行用户、systemd unit或cgroup分流，否则会把CEX一起改掉。应按下面的目标域名分流。

请在路由器加入域名规则：

  ethereum-rpc.publicnode.com -> 新加坡 Oracle 出口

2026-09-22查询到的当前A记录是：

  172.66.150.162
  104.20.24.117

当前没有原生AAAA记录。上述IPv4的DNS TTL约286秒，而且属于CDN地址，随时可能变化，也可能与其他域名共用，所以优先使用路由器的域名规则、动态DNS集合/ipset/nftset，不要只永久写死这两个IP。如果路由器只能按IP分流，就必须让它按DNS结果和TTL自动刷新地址集合；同时说明按共享CDN IP分流可能连带影响其他域名。

实施前先检查collector实际生效的DEX_ETH_RPC_URL。如果以后将它改成另一个RPC域名，路由规则也必须改成那个新域名；不要同时保留旧RPC作为直连回退。检查配置时不要输出URL内可能存在的凭据。

验证要求：

1. 使用和collector相同的DEX_ETH_RPC_URL执行一次有界只读测试：

   go run ./cmd/dex-check --sample-block finalized --report-dir /tmp/dex-egress-check

2. 确认该测试访问的目标只有 ethereum-rpc.publicnode.com:443，并从新加坡Oracle出口出去。
3. 确认现有CEX规则、连接和数据新鲜度没有变化。
4. 确认路由规则未命中或Oracle出口不可用时，DEX请求失败，不回落到原公网直连。
5. 报告实际使用的规则、命中的域名和当时解析IP；不要修改DEX_ENABLED，不要启动第二个生产collector，不要写生产数据库。

项目目录：/home/ubuntu/crypto-market-info
配置位置：internal/config/config.go
DEX调用位置：internal/app/dex_runtime.go、cmd/dex-check/main.go、internal/dex/ethereum/rpc.go
```

当前解析结果只用于现场核对。长期规则应以域名为准。
