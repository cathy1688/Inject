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
       ├─ 区块游标 / 失败重试
       ├─ 子网动态发现
       ├─ 精确区间汇总 API
       └─ 30 天自动清理
              │
      ┌───────┴────────┐
      ▼                ▼
  inject-meta       4 个区块分片 D1
  子网目录/状态      每片 8 天滚动窗口
  闭合小时精确汇总   合计覆盖 32 天
              │
              ▼
       Static Assets 前端
       *.workers.dev
```

Cloudflare Free 的 D1 单库上限为 500 MB，因此使用 **1 个元数据库 + 4 个区块分片数据库**。完整小时使用可重复重建的精确小时汇总；任意自定义时间的首尾零散区间直接读取原始区块，因此汇总与明细保持一致。

## 当前代码状态

已实现：

- Cloudflare Worker + Static Assets
- Subtensor WebSocket JSON-RPC 客户端
- finalized block 扫描与区块游标
- `NetworksAdded` 动态子网发现
- `SubnetIdentitiesV3` 名称读取
- `RegisteredSubnetCounter` 跟踪
- 实际 Injected / Chain Buys 采集
- 1 + 4 D1 滚动分片
- 可重复重建的精确小时汇总
- 任意时间范围的精确汇总：完整小时走汇总，边缘时间走原始区块
- 30 天清理
- API：status / subnets / summary / blocks / chart / CSV / sync
- V17 页面已接真实 API
- 首次运行自动初始化 D1 表结构

> `src/theory.ts` 是理论计算的唯一入口。当前不会伪造理论值；精确公式启用前，页面理论值与偏差显示 `—`。

## Cloudflare 部署

`wrangler.jsonc` 已声明 5 个 D1 绑定但不写死数据库 ID，优先使用 Cloudflare 的自动资源配置能力。首次 GitHub 部署时若自动配置成功，会自动创建并绑定所需 D1；Worker 第一次调用 API 或 Cron 时会自动创建数据表。

部署入口：Cloudflare → Workers & Pages → Create application → Import a repository → 选择 `cathy1688/Inject`

建议：

- Worker / Project name：`inject`
- Production branch：`main`
- Root directory：`/`
- Deploy command：`npx wrangler deploy`
- 使用自动分配的 `*.workers.dev` 地址

如果 Cloudflare 控制台未自动创建 D1，按 `docs/DEPLOY.md` 的备用步骤手动创建并绑定即可。
