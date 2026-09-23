# Deribit 协议固定样本

这些文件截取自 2026-09-20 的公开只读探测；原始响应、请求时间及哈希在 `research/2026-09-20-options-arbitrage/`。不包含账户或凭据。

- 六份 metadata 文件保留原始 JSON 数值字符，只筛出对应的 12 个合约：BTC/ETH × 币本位/USDC × call、put、同到期期货。筛选后的文件哈希与原始整份响应不同。
- `books.jsonl` 保留每个合约首个全量 WS snapshot 和紧随的首个 change，共 24 条，保留原始接收时间与 payload。
- 这些样本只验证协议、单位与序列，不证明 combo 单位、长期稳定性或可成交收益。
