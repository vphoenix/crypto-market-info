# Ethereum V3 + Sky：合约、事件及状态最小清单

2026-09-21，仅公开数据只读核验。未发送交易，未证明套利利润；实际 V3 池发现由同目录的独立研究记录负责。

## 地址与核验状态

Sky 地址来自[官方 active chainlog](https://chainlog.sky.money/api/mainnet/active.json)，并以固定 finalized 区块的代码及身份 getter 核对。Uniswap 基础合约来自[官方 Ethereum 部署表](https://developers.uniswap.org/docs/protocols/v3/deployments/v3-ethereum-deployments)。不能跨链复用这些地址。

| 对象，chain_id=1 | 地址 | 用途 |
|---|---|---|
| USDC，decimals=6 | `0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48` | 同币闭环和 gas 折算的资产身份 |
| DAI，decimals=18 | `0x6B175474E89094C44Da98b954EedeAC495271d0F` | PSM 中间资产 |
| USDS，decimals=18 | `0xdC035D45d973E3EC169d2276DDab16f1e407384F` | 转换资产，代理合约需跟踪实现升级 |
| DaiUsds converter | `0x3225737a9Bbb6473CB4a45b7244ACa2BeFdB276A` | DAI↔USDS |
| LitePSM | `0xf6e72Db5454dd049d0788e411b06CfAF16853042` | DAI↔USDC |
| USDS LitePSM wrapper | `0xA188EEC8F81263234dA3622A406892F3D630f98c` | USDS↔USDC，调用底层 LitePSM |
| LitePSM pocket | `0x37305B1cD40574E4C5Ce33f8e8306Be057fD7341` | 实际持有 USDC，不能用 PSM 地址的 USDC 余额代替 |
| DAI Join | `0x9759A6Ac90977b93B58547b4A71c78317f391A28` | 转换依赖 |
| USDS Join | `0x3C0f895007CA717Aa01c8693e59DF1e8C3777FEB` | 转换依赖 |
| Vat | `0x35D1b3F3D7966A1DFe207aa4514C12a259A0492B` | 内部记账及补充库存额度 |
| Uniswap V3 Factory | `0x1F98431c8aD98523631AE4a59f267346ea31F984` | `getPool` 和 `PoolCreated` 发现池 |
| QuoterV2 | `0x61fFE014bA17989E743c5F6cB21bF9697530B21e` | 按量只读报价核验，不能替代完整路径仿真 |
| TickLens | `0xbfd8137f7d1516D3ea5cA83523914859ec47F573` | 已初始化 tick 读取辅助 |

旧 `MCD_PSM_USDC_A=0x89B78CfA322F6C5dE0aBcEecab66Aee45393cC5A` 仍在目录中；本次目标是上表的 **LitePSM**，不能混淆。

## 本次固定区块读数

RPC：`https://ethereum-rpc.publicnode.com`；查询使用 EIP-1898 `{blockHash, requireCanonical:true}`。finalized 区块 **26,026,763**，hash `0x4029e600a608549b6afc7772ada713b4f7bcda69780a5b4970e882240f397e20`；区块 UTC 时间 `2026-09-21T15:25:11Z`；采集完成 `2026-09-21T15:39:48.078494Z`。

| 字段 | 原始整数值 | 含义 |
|---|---|---|
| PSM `tin()` / `tout()` | `0` / `0` | 当时两个方向收费为 0；可治理修改 |
| PSM `buf()` | `800000000000000000000000000` | 8 亿 DAI 目标缓冲，不是当前可成交库存 |
| DAI `balanceOf(PSM)` | `805366824141739270548465826` | 805,366,824.141739270548465826 DAI |
| USDC `balanceOf(pocket)` | `3961578635940529` | 3,961,578,635.940529 USDC |
| PSM `rush()` | `0` | 当时没有可通过 `fill()` 新增的 DAI 数量 |
| `wrapper.live()` / `vat.live()` / `DAIJoin.live()` | `1` / `1` / `1` | 各自实际接口结果 |
| PSM `bud(wrapper)` | `0` | wrapper 没有专用 NoFee 白名单资格，普通入口当时收费恰为 0 |
| USDS `wards(USDSJoin)` / DAI `wards(DAIJoin)` | `1` / `1` | Join 的币合约权限仍存在 |

Pocket 对 PSM 的 USDC allowance 已查询且大于库存。三个转换合约均有代码，身份 getter 与目录一致；这是部署与接口核验，不是编译后逐字节证明 GitHub 当前分支等于部署源码。所有 getter 原始响应、包括失败，保存在 [sky-snapshot.json](contract-sources/sky-snapshot.json)。

## Sky 具体字段与事件

| 对象 | 必须采集的实际 getter / 依赖 | 不应虚构的字段 |
|---|---|---|
| DaiUsds | `daiJoin()`、`usdsJoin()`、`dai()`、`usds()`；依赖 Join、token 代码/实现/权限状态；按实际调用模拟转换 | converter 没有自己的 `vat()`、`fee()`、`paused()` |
| LitePSM | `ilk()`、`vat()`、`daiJoin()`、`dai()`、`gem()`、`pocket()`、`to18ConversionFactor()`；`tin()`、`tout()`、`buf()`、`rush()`；按功能读取 `gush()`、`cut()`、`vow()`、`wards(address)`、`bud(address)` | 没有普通 `paused()`；方向停止由 `tin/tout == 2^256-1` 表达 |
| 库存及补充能力 | `DAI.balanceOf(PSM)`、`USDC.balanceOf(pocket)`、`USDC.allowance(pocket,PSM)`；研究 fill 时另读 `vat.ilks(ilk)`、`vat.Line()`、`vat.debt()` 等 | `buf`、TVL、债务上限均不能替代当前可执行库存 |
| Wrapper | `psm()`、`usdsJoin()`、`usds()`、`gem()`、`vat()`、`ilk()`、`pocket()`、`dec()`、`to18ConversionFactor()`；转发 `tin/tout/buf/vow`，`live()` 返回 Vat 状态 | **`wrapper.dai()` 返回 USDS**，不是旧 DAI；wrapper 没有专属成交事件 |

本次 `USDSJoin.live()` 调用 revert；[其官方源码](https://raw.githubusercontent.com/sky-ecosystem/usds/dev/src/UsdsJoin.sol)没有该 getter，不能把失败解析成 `live=0`。正式 collector 不应请求不存在的字段。

事件 ABI，以源码为准：

```solidity
// Converter
event DaiToUsds(address indexed caller, address indexed usr, uint256 wad);
event UsdsToDai(address indexed caller, address indexed usr, uint256 wad);
// LitePSM
event SellGem(address indexed owner, uint256 value, uint256 fee);
event BuyGem(address indexed owner, uint256 value, uint256 fee);
event Fill(uint256 wad);
event Trim(uint256 wad);
event Chug(uint256 wad);
event File(bytes32 indexed what, uint256 data);
event File(bytes32 indexed what, address data);
event Rely(address indexed usr);
event Deny(address indexed usr);
event Kiss(address indexed usr);
event Diss(address indexed usr);
```

PSM `value` 是 USDC 最小单位，`fee` 是 DAI 的 18 位最小单位；SellGem 的接收人可能是 wrapper，不是最终交易者，需把 token Transfer 和 call trace 串起来。精确费用：`g18 = gemAmt * to18ConversionFactor`；卖 USDC 获 DAI/USDS `g18 - floor(g18*tin/1e18)`，买指定 USDC 所需 DAI/USDS `g18 + floor(g18*tout/1e18)`。归一化后做整数运算，不经浮点。

| 函数签名 | selector |
|---|---|
| `daiToUsds(address,uint256)` | `0xf2c07aae` |
| `usdsToDai(address,uint256)` | `0x68f30150` |
| `sellGem(address,uint256)` | `0x95991276` |
| `buyGem(address,uint256)` | `0x8d7ef9bb` |
| `tin()` / `tout()` | `0x568d4b6f` / `0xfae036d5` |
| `buf()` / `rush()` | `0x15232515` / `0xcbf0bfac` |

官方源码：[DaiUsds](https://github.com/sky-ecosystem/usds/blob/dev/src/DaiUsds.sol)、[LitePSM](https://github.com/sky-ecosystem/dss-lite-psm/blob/main/src/DssLitePsm.sol)、[wrapper](https://github.com/sky-ecosystem/usds-wrappers/blob/dev/src/UsdsPsmWrapper.sol)。

## Uniswap V3 最小状态与事件

池身份：Factory `getPool(tokenA,tokenB,fee)`（`0x1698ee82`），随后核池 `factory/token0/token1/fee/tickSpacing` 与非空代码。费档是否有效用 `feeAmountTickSpacing`/`FeeAmountEnabled`，零地址池或零流动性不能当可交易池。

价格与规模模拟：`slot0()`（`0x3850c7bd`）的 `sqrtPriceX96,tick,observationIndex,observationCardinality,observationCardinalityNext,feeProtocol,unlocked`；`liquidity()`（`0x1a686502`）；路径可能跨越的 `tickBitmap(int16)` 与 `ticks(int24)`，尤其 `liquidityGross/liquidityNet/initialized`。只读当前 tick 和总 liquidity 不能恢复跨 tick 成交成本。token 余额和协议手续费状态用于核对真实资金变化；完整 EVM 重放还需对应归档状态，不能仅靠报价字段恢复全部存储。

至少采 `PoolCreated`、`Initialize`、`Swap`、`Mint`、`Burn`；现金流核验还要 `Collect`、`Flash`、`CollectProtocol` 及 token 转账；配置变化采 `SetFeeProtocol`、`IncreaseObservationCardinalityNext`。关键 ABI：

```solidity
event PoolCreated(address indexed token0, address indexed token1,
                  uint24 indexed fee, int24 tickSpacing, address pool);
event Swap(address indexed sender, address indexed recipient,
           int256 amount0, int256 amount1, uint160 sqrtPriceX96,
           uint128 liquidity, int24 tick);
event Mint(address sender, address indexed owner, int24 indexed tickLower,
           int24 indexed tickUpper, uint128 amount, uint256 amount0, uint256 amount1);
event Burn(address indexed owner, int24 indexed tickLower, int24 indexed tickUpper,
           uint128 amount, uint256 amount0, uint256 amount1);
```

`Swap.amount0/amount1` 是池余额有符号变化，不是机器人利润；Mint/Burn 要更新范围两端和当前有效流动性。顺序键必须到 `(block_hash, transaction_index, log_index)`，保留 removed/reorg/finality、接收 UTC 时间和 payload hash。

V3 官方接口：[PoolEvents](https://github.com/Uniswap/v3-core/blob/main/contracts/interfaces/pool/IUniswapV3PoolEvents.sol)、[PoolState](https://github.com/Uniswap/v3-core/blob/main/contracts/interfaces/pool/IUniswapV3PoolState.sol)、[Factory](https://github.com/Uniswap/v3-core/blob/main/contracts/interfaces/IUniswapV3Factory.sol)。本地源码快照见 [contract-sources](contract-sources/)，函数 selector 及全部已整理事件的 topic0 见 [signatures.json](contract-sources/signatures.json)，文件 SHA-256 见 [sha256.json](contract-sources/sha256.json)。[核验脚本](verify_sky_contracts.py)只使用公开 `eth_call/eth_getCode` 等只读方法。
