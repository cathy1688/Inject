# Inject — Bittensor 子网区块排放监测

**所有子网，追踪每个区块的真实排放**

## 产品目标

- 直接从 Bittensor Subtensor 读取 finalized 区块，不依赖 Taostats API
- 动态发现全部活跃子网，不写死 128 个子网
- 逐区块保存链上 `SubnetTaoInEmission` 与 `SubnetExcessTao`
- 汇总 Injected / Chain Buys / Total Emission
- 支持 24 小时、7 天、30 天、自定义时间范围
- 明细完整展示每个区块并支持搜索、排序、CSV
- 仅保留最近 30 天，超过 30 天自动清理
- GitHub + Cloudflare Workers + D1 + workers.dev，目标使用免费额度长期运行

## 数据口径

- **链上实际注入**：`SubtensorModule.SubnetTaoInEmission`
- **Chain Buys**：`SubtensorModule.SubnetExcessTao`
- **Total Emission**：页面当前口径为 `Injected + Chain Buys`
- **理论注入**：逐区块理论计算；严禁用猜测值制造偏差
- 数据库存储单位：RAO 整数；展示层换算为 TAO
- 只使用 finalized block 作为永久记录

## 架构

```text
Bittensor Subtensor WebSocket RPC
              │
              ▼
       Cloudflare Worker
       ├─ Cron 每分钟追块
       ├─ 缺块自动补采
       ├─ 子网动态发现
       ├─ 汇总/查询 API
       └─ 30 天自动清理
              │
      ┌───────┴────────┐
      ▼                ▼
  inject-meta       4 个区块分片 D1
  子网目录/状态      每片 8 天滚动窗口
  分钟/小时/日汇总   合计覆盖 32 天
              │
              ▼
       Static Assets 前端
       *.workers.dev
```

Cloudflare Free 的 D1 单库上限为 500 MB，因此使用 **1 个元数据库 + 4 个区块分片数据库**，避免 30 天原始区块数据把单库撑满。

## 当前代码状态

已实现第一阶段工程：

- Cloudflare Worker + Static Assets
- Subtensor WebSocket JSON-RPC 客户端
- finalized block 扫描与区块游标
- `NetworksAdded` 动态子网发现
- `SubnetIdentitiesV3` 名称读取
- `RegisteredSubnetCounter` 跟踪
- 实际 Injected / Chain Buys 采集
- 1+4 D1 滚动分片
- 分钟 / 小时 / 日准确汇总
- 30 天清理
- API：status / subnets / summary / blocks / chart / CSV / sync
- 已确认 V17 页面改造成真实 API 前端

> `src/theory.ts` 是理论计算的唯一入口。该模块目前不会伪造数值；精确公式启用后，所有汇总、明细和偏差会自动使用同一套逐区块理论结果。

## Cloudflare 一次性配置

需要创建以下 5 个免费 D1 数据库：

- `inject-meta`
- `inject-blocks-0`
- `inject-blocks-1`
- `inject-blocks-2`
- `inject-blocks-3`

然后将 5 个 `database_id` 写入 `wrangler.jsonc`，再应用：

- `migrations/meta/0001_init.sql` → `inject-meta`
- `migrations/blocks/0001_init.sql` → 四个区块分片

完成后，Cloudflare 连接 GitHub `cathy1688/Inject`，部署到自动分配的 `*.workers.dev` 地址。
