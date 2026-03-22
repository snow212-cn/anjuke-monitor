# anjuke-community-monitor

用于在青龙面板等定时任务环境中，定期拉取安居客（Anjuke）二手房源列表，并在**房源新增/下架**（可选：价格等字段变更）时输出变更结果（通知能力预留为可插拔接口，默认仅控制台输出）。

同时会在 [`./data/`](data/) 目录生成**人类友好可读**的“表格文件”（默认 CSV），方便灵活浏览与分析房源。

> 本仓库代码不内置任何第三方依赖，默认使用 Node.js 18+ 自带的 `fetch`。

## 功能

- 拉取接口：`/esf-ajax/property/info/list`
  - 支持按小区 `comm_id` 抓取
  - 支持按地图视野 `min_lat/min_lng/max_lat/max_lng` 抓取
  - 支持自动翻页拿到“全量”房源
- 房源去重：按可配置的房源特征先分组，再结合主图或价格容差，尽量把不同经纪人发布的同套房合并为一条
- 状态持久化：将上次抓取结果保存到本地文件，便于下次比对
- 差异检测：
  - 新增房源（added）
  - 下架房源（removed）
  - （可选）字段变化（updated，比如价格/标题）
- 可扩展：
  - 支持多个小区 targets
  - 通知模块可插拔（青龙里接 `sendNotify` / Telegram 等都行）
- HAR 离线回放：可使用你保存的 `.har` 文件验证解析逻辑（不请求线上接口）

## 快速开始

### 1) 复制示例配置

将 [`config/config.example.json`](config/config.example.json) 复制为 `config/config.json`（或直接使用环境变量）。

### 2) 运行（本地/青龙均可）

```bash
node anjuke_monitor.js
```

### 3) 离线回放（使用 HAR，不发起网络请求）

```bash
node anjuke_monitor.js --har anjuke.com.har --no-save
```

## 报告文件（便于人工浏览/分析）

默认会生成（位于 `data/`）：

- 当前全量列表（覆盖写，表格）：`listings_<targetKey>.csv`
- 发生变化时的 diff（覆盖写最新一份）：`diff_<targetKey>_latest.md`

其中：

- 小区模式的 `targetKey` 形如 `<cityId>_<commId>`
- 地图视野模式的 `targetKey` 形如 `<cityId>_viewport_<hash>`

> 默认不再为每次变更都新增一份 diff 文件，避免目录里文件越来越多。

可通过参数或环境变量控制：

- CLI：`--no-report`
- ENV：
  - `AJ_REPORT_ENABLED=0`
  - `AJ_REPORT_SAVE_LISTINGS=0`
  - `AJ_REPORT_SAVE_DIFF=0`
  - `AJ_REPORT_LISTINGS_FORMAT=csv`（或 `md` / `csv,md`）
  - `AJ_REPORT_DIFF_FORMAT=md`（或 `csv` / `csv,md`）
  - `AJ_REPORT_SAVE_DIFF_HISTORY=1`（开启后会生成带时间戳的 diff 历史文件）
  - `AJ_REPORT_CLEANUP_LEGACY=1`（默认开启：自动清理旧格式/不需要的报告文件）

## 通知扩展（青龙对接）

脚本支持加载一个“通知模块”（CommonJS），签名为：

- `module.exports = async function notify(diff, context) {}`

当发生变更时，脚本会传入：

- `diff.added / diff.removed / diff.updated`：结构化差异数据
- `context.text`：**已拼好的通知正文**（推荐直接发这个）
- `context.summary / context.fetchedAt / context.listHash / context.target`：辅助信息

使用方式二选一：

1) 命令行：

```bash
node anjuke_monitor.js --notify ./config/notify.example.js
```

2) 环境变量：

- `AJ_NOTIFY_MODULE=./config/notify.example.js`

你可以把该模块替换为你自己的 `sendNotify` 封装，并用 `context.text` 作为通知内容。

## 配置

优先级：命令行参数 > 环境变量 > `config/config.json` > 内置默认值。

- `config/config.json`（可选）
  - `targets`: 监控目标数组
  - `request`: 请求相关配置（超时、重试、headers 等）
  - `diff`: 差异检测策略

也支持环境变量快速覆盖（适合青龙面板）：

- `AJ_BASE_URL`（默认 `https://mudanjiang.anjuke.com`）
- `AJ_CITY_ID`（默认 `182`）
- `AJ_COMM_ID`（默认 `696590`）
- `AJ_ENTRY`（默认 `51`）
- `AJ_PAGE_SIZE`（默认 `20`）
- `AJ_ENABLE_UPDATE_DIFF`（默认 `0`，设为 `1` 会输出 updated）
- `AJ_DEDUPE_ENABLED`（默认 `1`）
- `AJ_DEDUPE_PRICE_TOLERANCE_WAN`（默认 `1`，单位：万元）
- `AJ_DEDUPE_PHOTO_FIELD`（默认 `default_photo`；留空可关闭按主图辅助并组）
- `AJ_DEDUPE_FEATURE_FIELDS`（逗号分隔；默认 `community_id,area_num,room_num,hall_num,toilet_num,floor_level_bucket,total_floor`）

`dedupe.featureFields` / `AJ_DEDUPE_FEATURE_FIELDS` 支持直接填写房源字段名，也支持以下派生字段：

- `floor_level_bucket`：把 `中层(共7层)` 归一成 `中层`
- `photo_key`：把图片 URL 归一成图片指纹
- `AJ_STATE_DIR`（默认 `./data`）
- `AJ_NOTIFY_MODULE`（可选，通知模块路径）

地图视野模式也可用环境变量覆盖单目标：

- `AJ_MIN_LAT`
- `AJ_MIN_LNG`
- `AJ_MAX_LAT`
- `AJ_MAX_LNG`

## 输出示例

当有变化时：

- stdout 默认会输出人类可读的通知正文（适合直接发通知/看日志）
- 若你需要结构化 JSON 调试，可设置：`AJ_NOTIFY_DEBUG_JSON=1`

## 青龙面板使用建议

- 定时：例如每 5~10 分钟一次
- 依赖：Node.js 18+
- 持久化目录：确保任务工作目录下的 `data/` 可写（青龙一般可写）

> 通知：本项目不内置通知发送；你可以通过 `--notify` / `AJ_NOTIFY_MODULE` 把 diff 结果交给你自己的通知逻辑。
