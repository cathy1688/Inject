# Cloudflare 部署步骤

## 推荐：GitHub 直接部署

1. 登录 Cloudflare
2. 打开 **Workers & Pages**
3. 选择 **Create application** / **创建应用**
4. 选择 **Import a repository** / **连接 GitHub**
5. 选择仓库：`cathy1688/Inject`
6. Project / Worker name：`inject`
7. Production branch：`main`
8. Root directory：`/`
9. Deploy command：`npx wrangler deploy`
10. 保存并部署

`wrangler.jsonc` 已声明：

- Static Assets：`public/`
- Cron：每分钟采集 + 每日清理检查
- 5 个 D1 绑定：`META_DB`、`BLOCKS_0`、`BLOCKS_1`、`BLOCKS_2`、`BLOCKS_3`
- workers.dev 开启

当前配置优先使用 Cloudflare 自动资源配置。部署后第一次访问 API 或第一次 Cron 会由 `src/schema.ts` 自动创建所需表。

## 部署后检查

打开自动分配的：

`https://inject.<你的 workers.dev 子域>.workers.dev`

然后检查：

- 页面能打开
- 顶部“链上连接”状态
- `/api/status`
- 当前活跃子网数量
- 最新同步区块
- D1 中开始出现区块数据

## 如果 D1 自动资源配置没有成功

在 Cloudflare D1 手动创建 5 个数据库：

- `inject-meta`
- `inject-blocks-0`
- `inject-blocks-1`
- `inject-blocks-2`
- `inject-blocks-3`

分别绑定到：

- `META_DB`
- `BLOCKS_0`
- `BLOCKS_1`
- `BLOCKS_2`
- `BLOCKS_3`

然后将 Cloudflare 提供的数据库 ID 写入 `wrangler.jsonc` 对应绑定，再重新部署。

## 当前上线测试重点

1. Cloudflare Worker 能否连接 `wss://entrypoint-finney.opentensor.ai:443`
2. Subtensor RPC 是否允许 `state_getKeysPaged` 与 `state_queryStorageAt`
3. `SubnetTaoInEmission` / `SubnetExcessTao` 是否正确解码
4. 子网名称是否能从 `SubnetIdentitiesV3` 正确解析
5. Cron 连续运行是否稳定
6. 24 小时累计值是否与逐区块明细求和完全一致

理论计算模块暂未启用，属于刻意设计；在公式锁定前理论值与偏差显示 `—`。
