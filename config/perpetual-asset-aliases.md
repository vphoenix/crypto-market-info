# 永续资产别名字典依据

本文件说明 [perpetual-asset-aliases.json](perpetual-asset-aliases.json) 的初始资产身份和单位口径。目录身份取自 2026-09-09 UTC 的交易所公开 API；补充的人工资料核对及价格量级检查完成于 2026-09-10（Asia/Shanghai）。实时选集以启动时的完整合格目录为准，不以本文件的交易状态为准。

每条规则精确绑定 `exchange / market_type / exchange_symbol / venue_contract_version`，并断言原始 base、quote、settle。这里的简称不是可以应用于其他交易所、历史版本或其他合约的通配规则。没有显式规则时，不会自动剥离数字、改写名字或按价格接近建立映射。

## 1. 单位规则

`canonical_base_units_per_venue_base_unit` 表示一单位交易所 base 含有多少单位规范 base。它与交易所合约乘数是两个不同的量：

```text
规范基础币数量 = qty_lot × qty_step × contract_multiplier × alias_factor
每单位规范基础币价格 = price_tick × price_tick_size ÷ alias_factor
```

原始盘口 tick、lot、合约乘数均不修改。不得再把 OKX 的 `ctVal` 重复算入 alias factor。

| 规范资产 | Binance 显式 base / factor | Bybit 显式 base / factor |
| --- | --- | --- |
| BABYDOGE | `1MBABYDOGE / 1000000` | `1000000BABYDOGE / 1000000` |
| MOG | `1000000MOG / 1000000` | `1000000MOG / 1000000` |
| BONK、CAT、FLOKI、LUNC、PEPE、RATS、XEC | 对应 `1000` 前缀 base / `1000` | 对应 `1000` 前缀 base / `1000` |
| SATS | `1000SATS / 1000` | `10000SATS / 10000` |
| SHIB | `1000SHIB / 1000` | `SHIB1000 / 1000` |
| CHEEMS | `1000CHEEMS / 1000` | 无规则 |
| BTT、TAG、TOSHI、TURBO | 无规则 | 对应 `1000` 前缀 base / `1000` |
| BOB.BNB | `1000000BOB / 1000000` | 无规则 |
| BEAM | `BEAMX / 1` | 无规则，当前 `BEAM` 为 identity |
| LUNA | `LUNA2 / 1` | `LUNA2 / 1` |
| NEIRO | 无规则，当前 `NEIRO` 为 identity | `1000NEIROCTO / 1000` |
| NIULAI | `牛来 / 1` | 无规则，当前 `NIULAI` 为 identity |

上表只概括 JSON 中明确枚举的合约。尤其不能把 `1INCH` 当成倍率，也不能把任意 `1000...` 名字视作已经确认的同一资产。

Binance 明确说明 `1MBABYDOGE` 代表一百万 BABYDOGE；SATS 公告明确其为 BRC-20 SATS，并说明 1000 倍面额。SATS 不是把 BTC 的最小计量单位作为另一种资产接入。[BABYDOGE / NEIRO 公告](https://www.binance.com/en/support/announcement/detail/4336ae4908154736acff8302509f7a05)、[SATS 公告](https://www.binance.com/zh-CN/support/announcement/detail/67d516a4ecec4c3ca17760d9afe04570)。

其他批量面额以完整合约目录中的 base 名称、交易所合约页面和跨站规范化价格量级相互检查；这属于显式人工判断，不是运行时启发式。可核验页面包括 [Binance 1000BONK 上线公告](https://www.binance.com/en-PH/support/announcement/detail/7b3f500e0eec40f380023b4ff0ccca18)、[Binance MOG 上线公告](https://www.binance.com/es-LA/support/announcement/detail/209355888f0042f788899dd1a04a0052)、[Bybit 1000TOSHI 上线公告](https://announcement.bytick.com/article/new-listing-1000toshiusdt-perpetual-contract-in-innovation-zone-with-up-to-12-5x-leverage-bltd85e64a669c9ae27/)、[Bybit SHIB1000 合约页](https://www.bybit.global/trade/usdt/SHIB1000USDT)。

## 2. 易混淆身份

### BOB：保留两个不同资产

Binance `1000000BOBUSDT` 的底层是 Build On BNB，BNB Smart Chain 地址为 `0x51363F073b1E4920fdA7AA9E9d84BA97EdE1560e`。本字典使用 `BOB.BNB`，不映射到普通 `BOB`。[Binance 1000000BOB 上线公告](https://www.binance.com/en-IN/support/announcement/detail/9d12cbfb72bb46e6835313471b6b3c85)。

Binance 后来上线的 `BOBUSDT` 是 Build on Bitcoin，地址为 `0x52B5fB4B0F6572B8C44d0251Cc224513ac5eB7E7`。两者不能因删除数字前缀而合并。核对时后者处于 `SETTLING`，未进入合格实时目录；这不改变必须保留的身份区分。[Binance BOB 上线公告](https://www.binance.com/es-LA/support/announcement/detail/36c4f838c82b4f81920d5adc958d9a53)。

### CAT 与 CHEEMS：名称不能代替资产身份

当前两家的 `1000CATUSDT` 按 Simon's Cat 映射为 `CAT`，一单位为 1000 CAT。Binance 官方列出的 BNB Chain 地址为 `0x6894CDe390a3f51155ea41Ed24a33A4827d3063D`；Bybit 的现货公告曾使用 `CATBNB` 表示 Simon's Cat。因此不能仅凭不同栏目 ticker 不同就认定为不同币。[Binance CAT 项目公告](https://www.binance.com/uz-UZ/support/announcement/detail/b60b629a7396476ba3c3391b016074e1)、[Binance 1000CAT 交易页](https://www.binance.com/en/trade/1000CAT_USDT)、[Bybit CATBNB 公告](https://announcements.bybit.com/en/article/new-listing-catbnb-simon-s-cat--blt0fa0d9750d78d072/)。

另一个 Binance `CATUSDT` 属于股票类：核对时公开目录返回 `contractType=TRADIFI_PERPETUAL`、`underlyingType=EQUITY`，而 `1000CATUSDT` 返回 `PERPETUAL`、`COIN`。前者不满足当前适配器的精确 `PERPETUAL` 条件；Bybit 股票使用 `CATSTOCKUSDT`。不能根据 mark-price 列表中出现 `CATUSDT` 就把它纳入 CAT 代币组。以后扩展股票或其他合约类型时，必须先建立独立资产身份规则。[Binance 原始公开目录](https://fapi.binance.com/fapi/v1/exchangeInfo)。

`1000CHEEMSUSDT` 对应 cheems.pet 的 BNB Smart Chain 资产，地址为 `0x0df0587216a4a1bb7d5082fdc491d93d2dd4b413`，不能与其他链上同名 CHEEMS 任意合并。当前其他合格目录没有本字典确认的对应合约，所以只记录映射、不因此满足“两家共有”。[Binance CHEEMS 上线公告](https://www.binance.com/en-PH/support/announcement/detail/3dd2c1e3f5f040ac9f7a94c6597ef842)、[Binance 官方资产名称页](https://www.binance.com/en-GB/markets/coinInfo-Meme)。

### BEAM、LUNA 与 NEIRO

`BEAMX` 是 Binance 对游戏网络 Beam（原 Merit Circle）使用的 ticker，一单位仍是一枚 BEAM，不再额外乘以历史 MC 迁移比例。Bybit 的 BEAM 也是此游戏网络资产，不是较早的隐私币 Beam。[Binance BEAMX 永续公告](https://www.binance.com/en/support/announcement/detail/25c05ab82d674c7396eab887d1384b05)、[Bybit Beam 身份说明](https://www.bybit.com/zh-TW/learn/gamefi/what-is-beam-crypto-by-merit-circle-dao)。

`LUNA2` 对应 2022 年新 Terra 的 LUNA，factor 为 1；旧链 Terra Classic 的 LUNC 独立存在。Binance 永续公告的 underlying asset 明确为 LUNA，Bybit 资料也把 LUNA 2.0 与 LUNC 区分。[Binance LUNA2 永续公告](https://www.binance.com/en-ZA/support/announcement/detail/3b7184c80d544586993045a0e6e36e57)、[Bybit LUNA 2.0 说明](https://www.bybit.com/en/learn/post/luna-2-0-jumped-following-warp-partnership-blt7e9a81b686c452e7)。

Bybit `1000NEIROCTO` 对应 First Neiro on Ethereum（Community Takeover），与 Binance `NEIRO` 同一口径；不是所有名为 Neiro 的 Ethereum/Solana 代币。Bybit 官方说明明确区分这些项目并链接 `1000NEIROCTOUSDT`。[Bybit Neiro 说明](https://www.bybit.com/en/learn/memes/what-is-neiro-crypto)、[Binance NEIRO 公告](https://www.binance.com/en/support/announcement/detail/4336ae4908154736acff8302509f7a05)。

### 中文 ticker

Binance `牛来` 与 Bybit `NIULAI` 使用 factor 1。Bybit 上线公告同时使用 NIULAI 合约名和底层资产名“牛来”，并提供其代币地址；不根据拼音猜测对应关系。[Bybit NIULAI 永续公告](https://announcements.bybit.com/en/article/new-listing-niulaiusdt-perpetual-contract-in-innovation-zone-with-up-to-20x-leverage--art0740ce60bd90/)。

以下名称只建立可保存的 ASCII 标识，不声称与另一交易所的英文 ticker 相同。标识中的十六进制部分是原名字逐字符的 Unicode code point：

| Binance base | 保留标识 |
| --- | --- |
| 币安人生 | `CN.5E015B894EBA751F` |
| 哈基米 | `CN.54C857FA7C73` |
| 龙虾 | `CN.9F99867E` |
| 我踏马来了 | `CN.62118E0F9A6C67654E86` |

只有补充底层身份依据、精确合约版本和人工规则后，才能将它们匹配到其他交易所。暂时缺少对应关系会少采一组，不会伪造“两家共有”。

## 3. 价格量级交叉检查

读取 Binance `/fapi/v1/premiumIndex`、Bybit `/v5/market/tickers?category=linear` 和 OKX `/api/v5/public/mark-price?instType=SWAP`，以十进制运算除以 alias factor。以下为一次非同步观测，只用于排除数量级错误，不证明跨站资产身份、报价同步或可套利性：

| 规范资产 | Binance 每枚 USDT | Bybit 每枚 USDT | OKX 每枚 USDT |
| --- | --- | --- | --- |
| BABYDOGE | `0.00000000039332` | `0.0000000003935` | — |
| SATS | `0.00000001113` | `0.000000011138` | `0.000000011139` |
| SHIB | `0.00000535379` | `0.000005354` | `0.000005356` |
| NEIRO | `0.000087` | `0.00008725` | `0.00008715` |
| LUNA | `0.04799` | `0.04795` | `0.04793` |
| CAT | `0.000002085` | `0.000002086` | — |

同一次检查中，BONK、FLOKI、LUNC、MOG、PEPE、RATS、TAG、TOSHI、TURBO、XEC、BEAM 和 NIULAI 的已匹配合约也没有倍率造成的数量级偏离。`BOB.BNB`、CHEEMS、BTT 和四个 `CN.*` 标识在该次合格目录中仍未构成两家共有，不因有 alias 就强制订阅。

## 4. 维护规则

新增或修正规则时，先用 `collector -print-perp-catalogs` 读取当前精确身份，再核验项目、链及必要的合约地址；价格检查只作辅助。保留原始 base/quote/settle 断言，不把旧版本规则直接改成通配。某个名字重新上市时，应重新判断新版本是否仍为同一资产。

修改字典生成新的 `mapping_revision`，重新执行 `collector -print-perp-universe`，检查组成员、不同交易所数量和容量预算。历史盘口和旧映射保留；查询以运行记录指定的 revision 解读，允许以后人工修正错误并回滚。选集、订阅和存储规则详见 [共同永续选集设计](../docs/perpetual-common-universe.md)。
