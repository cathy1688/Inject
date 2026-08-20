# Inject｜Bittensor 子网区块排放监测 PRD

## 1. 产品目标

建立独立于第三方统计平台的 Bittensor 子网排放核验工具，直接从 Subtensor finalized 区块采集链上真实数据，并提供全网汇总、具体子网查询、逐区块明细与理论偏差核验

核心原则：**所有页面数据必须来自同一套原始区块数据，汇总与明细必须强一致**

## 2. 数据范围

- 从正式上线开始采集，不追溯上线前历史
- 只保存最近 30 天
- 超过 30 天自动删除
- 只记录 finalized block
- block_number 唯一，同时保存 block_hash

## 3. 自动子网发现

系统不得写死 128 个子网。每次同步链上子网目录，自动识别：

- 新增子网
- 注销子网
- 重新注册子网
- 当前活跃状态

已注销子网在 30 天保留期内仍允许查询历史记录

## 4. 全部子网监控

对所选时间范围内每个子网准确汇总：

- 子网名称 / NetUID
- 链上实际注入
- 理论计算注入
- 偏差
- Chain Buys
- Total Emission
- 状态

汇总必须由同一时间范围内原始区块数据或由其确定性重建的准确汇总缓存生成，不允许近似估算

## 5. 具体子网查询

支持：

- 24 小时
- 7 天
- 30 天
- 自定义开始 / 结束时间

正常情况下数据延迟目标为 0～1 个 finalized block。Cron 每分钟自动追块，页面查询时可触发有限追块

## 6. 区块明细

每个区块显示：

1. 序号
2. 区块高度
3. 区块时间
4. 链上实际注入
5. 理论计算注入
6. 单区块偏差
7. 累计偏差
8. 累计实际注入

24 小时约 7200 个区块必须完整可查看；7 天、30 天同样连续滚动，不使用分页 UI，前端使用虚拟列表

## 7. 查询与排序

- 子网：输入 SN / 名称实时过滤
- 区块：输入区块高度 / 时间实时过滤
- 区块明细点击任意列标题排序；再次点击切换升序 / 降序
- CSV 导出字段与页面数据口径一致

## 8. 数据口径

- 实际注入：链上 `SubnetTaoInEmission`
- Chain Buys：链上 `SubnetExcessTao`
- Total Emission：当前页面口径为 `实际注入 + Chain Buys`
- 存储单位：RAO 整数
- 展示单位：TAO

### 理论计算

理论值必须逐区块使用批准后的精确公式计算。**在精确公式未锁定前，系统显示 `—`，不得使用估算值生成虚假偏差**

## 9. 同步可靠性

- 每分钟读取最新 finalized block
- 数据库保存最后同步区块游标
- 发现落后时按区块顺序补采
- 失败时不跳过失败区块
- 重复扫描不得重复累计
- 汇总缓存必须可从原始区块确定性重建

## 10. 技术栈

- GitHub：代码管理
- Cloudflare Workers + TypeScript：后端与定时任务
- Cloudflare Static Assets：前端
- Cloudflare D1：数据库
- workers.dev：免费访问地址
- Subtensor WebSocket JSON-RPC：链上数据

不使用 VPS、R2、Taostats API 或付费域名

## 11. D1 设计

Cloudflare Free 单个 D1 数据库最大 500 MB，为保证 30 天原始区块数据有足够空间，采用：

- `inject-meta`：子网目录、同步状态、准确小时汇总
- `inject-blocks-0`
- `inject-blocks-1`
- `inject-blocks-2`
- `inject-blocks-3`

4 个区块库按 8 天窗口滚动映射，总覆盖 32 天；清理任务保证实际仅保留最近 30 天

## 12. API

- `GET /api/status`
- `GET /api/subnets`
- `GET /api/subnets/summary`
- `GET /api/blocks`
- `GET /api/chart`
- `GET /api/export.csv`
- `POST /api/sync`

## 13. 验收标准

- 所有活跃子网自动识别
- 新增 / 注销无需改代码
- 连续运行无主动跳块
- 24 小时区块完整可查
- 汇总与明细相加结果一致
- 实际 Injected / Chain Buys 来自链上
- 理论值启用后逐区块准确计算
- 30 天数据保留及自动清理正常
- 搜索、排序、CSV 正常
- Cloudflare 免费架构可稳定运行
