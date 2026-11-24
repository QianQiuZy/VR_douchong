# API 文档

本文档基于当前项目实现的 FastAPI，整理所有已对外暴露的接口请求方式与返回字段说明。

运行与缓存约定见 [runtime.md](./runtime.md)，接口开发与验证见 [development.md](./development.md)，项目入口见 [README](../readme.md)。

## 通用约定

- 基础地址：`https://vr.qianqiuzy.cn` `https://psp.qianqiuzy.cn` `https://vup.qianqiuzy.cn`
- 数据许可：上述三个 host 对外提供的直播统计数据集，在本项目有权许可的范围内采用 **CC BY-NC-SA 4.0**；使用与公开分享条件见 [数据许可说明](../LICENSE-DATA.md)。软件代码采用 GPL-3.0-or-later（GPL 第 3 版或由使用者选择的任何后续版本）。
- 返回格式：`application/json`。
- 错误响应：统一返回 `{"error": "错误信息"}`，并配合相应的 HTTP 状态码。
- 十个 API 按实际客户端 IP 合计限流，每滚动秒最多放行 20 次，超额返回 `429 {"error":"请求过于频繁"}` 和 `Retry-After: 1`。
- 默认六类 GET 从 Redis DB2 返回预热数据。未就绪、缺失、损坏、Redis 故障、当前快照超过 30 秒或在途请求过载时返回 `503 {"error":"接口缓存暂不可用"}` 和 `Retry-After: 1`，不直接回源 MySQL。
- 正常缓存响应支持 gzip，携带 `Vary: Accept-Encoding`；正常缓存响应与 429/503 均设置 `Cache-Control: no-store`。未知但合法的月份/房间保持原空结果语义。
- `API_CACHE_ENABLED=0` 是离线诊断旧路径，其报表并发容量耗尽仍返回 `503 {"error":"报表请求繁忙"}`；新增 `/gift/entry` 仍返回 `503`，不提供 MySQL 直读路径。
- 临时隐藏房间从所有业务 GET 结果中排除，包括历史月、按 UID 查询和最近访客窗口。直接查询隐藏房间返回 `404 {"error":"房间不存在"}`，不返回带房间号的空对象。可见性检查不受 `API_CACHE_ENABLED` 开关影响；Redis 状态不可用或损坏时返回 `503 {"error":"房间可见状态暂不可用"}`，不放行原始数据。管理接口仍可操作隐藏房间并返回鉴权后的结果。

## 1. POST /add/room

**说明**：新增监控房间。

**请求方式**：`POST`

**请求体（JSON）**

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| room_id | int | 是 | 房间号，必须为正整数。 |
| room_anchors | string 或 object | 是 | 主播名或映射。支持：`"主播名"` 或 `{ "<room_id>": "主播名" }`。 |
| api_key | string | 否 | API 密钥（也可通过请求头传递，见下）。 |

**认证方式（必填其一）**

- 请求头 `X-API-Key: <API_SECRET>`
- 请求头 `Authorization: Bearer <API_SECRET>`
- 请求体 `api_key`

**响应字段**（成功时）

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| ok | boolean | 是否成功。 |
| room_id | int | 房间号。 |
| message | string | 处理结果信息。 |

**错误响应**

| HTTP 状态码 | 说明 |
| --- | --- |
| 400 | 参数校验失败。 |
| 409 | 房间已存在或添加冲突。 |
| 401 | API 密钥缺失或无效。 |
| 500 | 服务器内部错误。 |

## 2. POST /delete/room

**说明**：删除监控房间。

**请求方式**：`POST`

**请求体（JSON）**

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| room_id | int | 是 | 房间号，必须为正整数。 |
| room_anchors | string 或 object | 是 | 主播名或映射（同 `/add/room`）。 |
| api_key | string | 否 | API 密钥（也可通过请求头传递，见下）。 |

**认证方式（必填其一）**

- 请求头 `X-API-Key: <API_SECRET>`
- 请求头 `Authorization: Bearer <API_SECRET>`
- 请求体 `api_key`

**响应字段**（成功时）

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| ok | boolean | 是否成功。 |
| room_id | int | 房间号。 |
| message | string | 处理结果信息。 |

**错误响应**

| HTTP 状态码 | 说明 |
| --- | --- |
| 400 | 参数校验失败。 |
| 404 | 房间不存在。 |
| 401 | API 密钥缺失或无效。 |
| 500 | 服务器内部错误。 |

## 临时删除与恢复（POST）

| 操作 | 路径 |
| --- | --- |
| 临时删除房间 | `/delete/room/temporary` |
| 临时删除房间恢复 | `/restore/room/temporary` |

两个接口的参数都放在 JSON payload 中，只需房间号，不使用 query 参数：

```json
{"room_id": 12345}
```

`room_id` 支持正整数或 ASCII 十进制字符串，必须小于 `2^64`；不接受布尔值、浮点数、零或负数。不需要 `room_anchors`。鉴权仅支持请求头 `X-API-Key: <API_SECRET>` 或 `Authorization: Bearer <API_SECRET>`。

成功示例：

```json
{"ok": true, "room_id": 12345, "hidden": true, "changed": true, "message": "房间已临时删除"}
```

恢复时 `hidden` 为 `false`，`message` 为 `房间已恢复展示`。两个操作均幂等：重复请求返回 200，状态无需改变时 `changed` 为 `false`。允许操作未配置但有历史数据的房间，也允许预先隐藏有效房间号；恢复不会添加监控房间或重新连接 blivedm。

鉴权失败返回 401，房间号不合法返回 400，请求 JSON 结构不合法时沿用 FastAPI 的 422，Redis 操作失败返回 503。成功及可见性错误响应使用 `Cache-Control: no-store`。

隐藏状态不设 TTL，持续至主动恢复；应用重启、缓存预热和月度刷新不会清除。操作成功后新发起的查询立即遵守新状态，已经开始的查询使用该请求读取到的状态。完整报表缓存继续更新，因此恢复无需重建所有月份；隐藏期间的数据依然按原有采集、归档和缓存新鲜度规则提供。已被外部调用方保存的数据不在源站接口控制范围内。

## 3. GET /gift

**说明**：当前月汇总。

**请求方式**：`GET`

可选查询参数 `light`。传 `1` 或 `true` 时返回轻量月报，每个房间省略 `danmaku`、`danmaku_user_count`、`danmaku_dependency`、`whale_dependency` 四个字段。省略参数或使用其他值时返回完整月报。`light` 仅适用于 `GET /gift`。

**响应字段**：返回数组，每个元素为房间汇总对象。

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| room_id | int | 房间号。 |
| anchor_name | string | 主播名。 |
| attention | int | 关注数（数据库记录）。 |
| status | int | 开播状态（1 开播，0 未开播）。 |
| gift | float | 礼物收入累计。 |
| guard | float | 守护收入累计。 |
| super_chat | float | SC 收入累计。 |
| payer_count | int | 当月去重付费人数。 |
| danmaku_user_count | int 或 null | 房间当月去重发言人数。完整覆盖且无人发言时为 0，旧数据或部分覆盖为 null。 |
| danmaku_dependency | object | 房间当月弹幕前排占比，字段与精度见下。 |
| steel_coin_count | int | 当月房间钢镚计数。 |
| blind_box_count | int | 盲盒数量累计。 |
| blind_box_profit | float | 盲盒盈亏累计（正负均可能，最小单位 0.1）。 |
| live_duration | string | 当月直播时长（`HH:MM:SS`）。 |
| effective_days | int | 当月有效开播天数。 |
| live_time | string | 当前月直播开始时间（历史月为默认值）。 |
| title | string | 直播间标题（历史月为空）。 |
| month | string | 月份（`YYYYMM`）。 |
| guard_1 | int | 舰长数量（当前状态）。 |
| guard_2 | int | 提督数量（当前状态）。 |
| guard_3 | int | 总督数量（当前状态）。 |
| fans_count | int | 粉丝团数量（当前状态）。 |
| current_concurrency | int 或 null | 当前同接，未开播或无采样则为 null。 |
| whale_dependency | object | 当月付费集中度及其数据状态，字段结构见下。 |
| danmaku | object | 当月弹幕统计二级菜单；历史数据无对应字段时各项为 `null`。 |

`whale_dependency` 中的比例以 JSON 数字返回，舍入至最多 4 位小数，例如 `0.0759`。

`whale_dependency` 字段：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| status | string | 数据状态：`live` 表示实时数据可用；`archived` 表示历史数据已完整归档；`partial` 表示历史数据仅部分归档；`unavailable` 表示暂无可用数据。 |
| source | string | 数据来源：`redis` 表示实时 Redis 数据，`mysql` 表示 MySQL 历史归档。 |
| top1 | float 或 null | 付费金额最高的 1 名用户占总收入的比例，范围为 `0` 到 `1`；无可用数据或总收入为 0 时为 `null`。 |
| top5 | float 或 null | 付费金额最高的 5 名用户合计占总收入的比例，范围为 `0` 到 `1`；无可用数据或总收入为 0 时为 `null`。 |
| top10 | float 或 null | 付费金额最高的 10 名用户合计占总收入的比例，范围为 `0` 到 `1`；无可用数据或总收入为 0 时为 `null`。 |
| top1_percent | float 或 null | 付费金额最高的前 1% 用户合计占总收入的比例，范围为 `0` 到 `1`；无可用数据或总收入为 0 时为 `null`。 |

`danmaku_dependency` 与 `whale_dependency` 都返回 `status`、`source`、`top1`、`top5`、`top10`、`top1_percent`。弹幕按有效 UID 发言条数排序，分母为该范围内全部已采集弹幕，包含无有效 UID 的条数。前 1% 按有效发言人数向上取整。比例返回 JSON 数字，最多四位小数，分母为零时返回 `null`。当前统计从 Redis 取值，结算后从 MySQL 取值。中途启用时返回 `partial`，比例仅覆盖已采集部分。若 DB4 排行明细丢失，也返回 `partial`，四项占比均为 `null`，已核实的 `danmaku_user_count` 仍正常返回。历史记录缺字段时返回 `unavailable` 和四个 `null`。

`danmaku` 字段：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| total | int 或 null | 当月总弹幕数。 |
| emoticon | int 或 null | 总数中以整条表情包发送的条数（`dm_type=1`）。带内嵌表情的文本（`dm_type=0`）按文本计。完整采集且没有表情包时为 0，旧记录缺字段或此前已有无法分类的弹幕时为 `null`。 |
| captain | int 或 null | 当月舰长弹幕数。 |
| admiral | int 或 null | 当月提督弹幕数。 |
| governor | int 或 null | 当月总督弹幕数。 |
| normal | int 或 null | 当月正常用户弹幕数。 |

**错误响应**

| HTTP 状态码 | 说明 |
| --- | --- |
| 500 | 数据库查询失败。 |
| 503 | 报表并发容量已满；响应包含 `Retry-After: 1`。 |

## 4. GET /gift/by_month

**说明**：指定月份或年份汇总。

**请求方式**：`GET`

**请求参数**

| 参数 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| month | string | 否 | 月份，格式 `YYYYMM` 或 `YYYY-MM`，与 `year` 互斥；两者均未传时默认当前月。 |
| year | string | 否 | 四位 ASCII 年份，范围 `0001` 至当前年，例如 `2026`；不能重复传入。 |

**响应字段**：返回数组，每个元素为房间汇总对象。

字段与 `/gift` 基本一致，区别如下：

- 历史月份返回 `live_time` 为 `0000-00-00 00:00:00`，`title` 为空，`status` 为 `0`。
- 历史月份的 `guard_1/guard_2/guard_3/fans_count` 返回 `null`。
- 月度返回顶层 `danmaku_user_count` 和 `danmaku_dependency`，字段与 `/gift` 相同。
- 返回 `payer_count`，表示该房间该月的去重付费人数。
- 返回 `whale_dependency`，结构和比例精度与 `/gift` 相同。当前月及归档状态为脏的历史月优先读取 Redis；已归档历史月从 MySQL 返回，暂无数据时 `status` 为 `unavailable` 且比例字段为 `null`。
- 返回 `danmaku` 二级菜单，结构与 `/gift` 相同；旧月份无对应字段时各项为 `null`。
- 不返回 `current_concurrency` 字段。

年度查询示例：`GET /gift/by_month?year=2026`。仍返回按房间组成的数组，年度字段约定如下：

| 字段 | 年度口径 |
| --- | --- |
| `year` | 字符串年份，替代 `month`；当年累计至当前月的最新快照，历史年份累计 1–12 月可用数据。 |
| `gift` / `guard` / `super_chat` | 按月累加金额。 |
| `blind_box_count` / `blind_box_profit` | 按月累加数量和盈亏。 |
| `live_duration` / `effective_days` | 累加月度秒数、有效天数，再格式化时长；小时数可超过 24。 |
| `attention` / `anchor_name` | 最新基础资料。 |
| `status` / `live_time` / `title` | 当年使用当前状态；历史年份分别为 `0`、`0000-00-00 00:00:00`、空字符串。 |
| `guard_1` / `guard_2` / `guard_3` / `fans_count` | 当年使用当前快照，历史年份为 `null`，不跨月累加。 |
| `payer_count` / `steel_coin_count` / `whale_dependency` / `danmaku_user_count` / `danmaku_dependency` | 固定为 `null`。 |
| `danmaku` | 各分类按月累加；该房间有统计活动的月份中，任一月份对应分项缺失时该分项为 `null`；没有统计活动也返回 `null`。 |

不额外返回 `total` 或时薪，客户端可用年度收入合计除以年度时长计算时薪。年份非法、晚于当前年、重复传入 `year`，或同时传入 `month` / `year` 时返回 `400`，不回退当前月。

年度结果由后台使用既有月度汇总和内存中的日统计小计生成，缓存到 Redis DB2。请求只读缓存，不查询 MySQL；未就绪、已知年度缓存缺失/损坏或当年缓存超过 30 秒时返回 `503`。没有数据的合法年份返回配置房间的年度空模板，不为请求动态创建缓存键。年度结果沿用原有 gzip、IP 限流和房间隐藏规则；关闭缓存的离线诊断模式也不会将年度查询降级到 MySQL。


**错误响应**

| HTTP 状态码 | 说明 |
| --- | --- |
| 500 | 数据库查询失败。 |
| 503 | 报表并发容量已满；响应包含 `Retry-After: 1`。 |

## 5. GET /gift/live_sessions

**说明**：查询指定房间 + 月份的单场直播清单。

**请求方式**：`GET`

**请求参数**

| 参数 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| room_id | int | 是 | 房间号，必须为正整数。 |
| month | string | 否 | 月份，格式 `YYYYMM`，默认当前月。 |
| stats_15m | string | 否 | 传 `1` 或 `true` 时返回 15 分钟统计。 |

**响应字段**

外层对象：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| room_id | int | 房间号。 |
| month | string | 月份（`YYYYMM`）。 |
| sessions | array | 直播场次列表。 |

`sessions` 元素字段：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| start_time | string | 开播时间（`YYYY-MM-DD HH:MM:SS`）。 |
| end_time | string 或 null | 下播时间，未下播为 `null`。 |
| title | string | 直播标题。 |
| gift | float | 单场礼物收入。 |
| guard | float | 单场守护收入。 |
| super_chat | float | 单场 SC 收入。 |
| payer_count | int | 单场去重付费人数。 |
| danmaku_user_count | int 或 null | 整场去重发言人数，跨区间的同一 UID 计一次。 |
| danmaku_dependency | object | 整场发言条数的前排占比，结构与月度相同。 |
| blind_box_count | int | 单场盲盒数量。 |
| blind_box_profit | float | 单场盲盒盈亏（正负均可能，最小单位 0.1）。 |
| danmaku_count | int | 弹幕数量。 |
| emoticon_danmaku_count | int 或 null | 整场弹幕中以整条表情包发送的条数，口径同月度 `danmaku.emoticon`。 |
| captain_danmaku_count | int 或 null | 舰长弹幕数量；旧记录无字段时为 `null`。 |
| admiral_danmaku_count | int 或 null | 提督弹幕数量；旧记录无字段时为 `null`。 |
| governor_danmaku_count | int 或 null | 总督弹幕数量；旧记录无字段时为 `null`。 |
| normal_danmaku_count | int 或 null | 正常用户弹幕数量；旧记录无字段时为 `null`。 |
| start_guard_1 | int 或 null | 开播时舰长数量。 |
| start_guard_2 | int 或 null | 开播时提督数量。 |
| start_guard_3 | int 或 null | 开播时总督数量。 |
| start_fans_count | int 或 null | 开播时粉丝团数量。 |
| start_attention | int 或 null | 开播时粉丝数（attention）。 |
| end_guard_1 | int 或 null | 下播时舰长数量。 |
| end_guard_2 | int 或 null | 下播时提督数量。 |
| end_guard_3 | int 或 null | 下播时总督数量。 |
| end_fans_count | int 或 null | 下播时粉丝团数量。 |
| end_attention | int 或 null | 下播时粉丝数（attention）。 |
| avg_concurrency | float 或 null | 平均同接，四舍五入至两位小数，未采样为 null。 |
| max_concurrency | int 或 null | 最大同接（若未采样则为 null）。 |
| current_concurrency | int 或 null | 进行中直播的即时同接，历史场次为 null。 |
| stats_15m | array | 仅在请求 `stats_15m=1` 或 `stats_15m=true` 时返回。区间以本场开播时间为起点，无统计时返回 `[]`。 |

`stats_15m` 元素字段：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| bucket_index | int | 本场相对区间序号，从0开始。 |
| start_time | string | 区间开始时间。 |
| end_time | string | 区间结束或最终落库时间。 |
| gift | float | 区间礼物收入。 |
| guard | float | 区间守护收入。 |
| super_chat | float | 区间 SC 收入。 |
| blind_box_count | int | 区间盲盒数量。 |
| blind_box_profit | float | 区间盲盒盈亏。 |
| danmaku_count | int | 区间弹幕数量。 |
| emoticon_danmaku_count | int 或 null | 本区间弹幕中以整条表情包发送的条数，口径同月度 `danmaku.emoticon`。 |
| captain_danmaku_count | int 或 null | 区间舰长弹幕数量；旧记录无字段时为 `null`。 |
| admiral_danmaku_count | int 或 null | 区间提督弹幕数量；旧记录无字段时为 `null`。 |
| governor_danmaku_count | int 或 null | 区间总督弹幕数量；旧记录无字段时为 `null`。 |
| normal_danmaku_count | int 或 null | 区间正常用户弹幕数量；旧记录无字段时为 `null`。 |
| avg_concurrency | float 或 null | 区间平均同接，四舍五入至两位小数。 |
| max_concurrency | int 或 null | 区间最高同接。 |
| sample_count | int | 区间同接采样次数。 |
| payer_count | int | 区间去重付费人数。 |
| danmaku_user_count | int 或 null | 本场该 15 分钟区间内的去重发言人数；旧记录缺列时为 null。 |

**错误响应**

| HTTP 状态码 | 说明 |
| --- | --- |
| 400 | 参数错误。 |
| 500 | 数据库查询失败。 |
| 503 | 报表并发容量已满；响应包含 `Retry-After: 1`。 |

## 6. GET /gift/attention

**说明**：查询指定房间在某月的每日粉丝数、舰长、提督、总督和粉丝团人数快照。

**请求方式**：`GET`

**请求参数**

| 参数 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| room_id | int | 是 | 房间号，必须为正整数。 |
| month | string | 否 | 月份，支持 `YYYYMM` 或 `YYYY-MM`，默认当前月。 |

**响应字段**

外层对象：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| room_id | int | 房间号。 |
| month | string | 月份（`YYYYMM`）。 |
| attention | array | 当月每日数据快照列表。无数据时返回 `[]`。 |

`attention` 元素格式：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| date | string | 日期，格式为 `YYYYMMDD`。 |
| attention | string | 当日粉丝数。 |
| guard_1 | int / null | 当日舰长数；历史归档表缺少该字段或字段为空时返回 `null`。 |
| guard_2 | int / null | 当日提督数；历史归档表缺少该字段或字段为空时返回 `null`。 |
| guard_3 | int / null | 当日总督数；历史归档表缺少该字段或字段为空时返回 `null`。 |
| fans_count | int / null | 当日粉丝团人数；历史归档表缺少该字段或字段为空时返回 `null`。 |
| gift | float | 当日房间礼物收入。 |
| guard | float | 当日房间守护收入。 |
| super_chat | float | 当日房间 SC 收入。 |
| payer_count | int | 当日房间去重付费人数。 |
| steel_coin_count | int | 当日房间钢镚计数。 |

示例：

```json
{
  "room_id": 1820703922,
  "month": "202603",
  "attention": [
    {
      "date": "20260301",
      "attention": "111",
      "guard_1": 10,
      "guard_2": 2,
      "guard_3": 1,
      "fans_count": 1234
    },
    {
      "date": "20260302",
      "attention": "112",
      "guard_1": null,
      "guard_2": null,
      "guard_3": null,
      "fans_count": null
    }
  ]
}
```

**错误响应**

| HTTP 状态码 | 说明 |
| --- | --- |
| 400 | 参数错误。 |
| 500 | 数据库查询失败。 |
| 503 | 报表并发容量已满；响应包含 `Retry-After: 1`。 |

## 7. GET /gift/sc

**说明**：查询 SC 日志。

**请求方式**：`GET`

**请求参数**

| 参数 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| room_id | int | 是 | 房间号，必须为正整数。 |
| month | string | 否 | 月份，支持 `YYYYMM` 或 `YYYY-MM`，默认当前月。 |

**响应字段**

外层对象：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| room_id | int | 房间号。 |
| month | string | 月份（`YYYYMM`）。 |
| list | array | SC 日志列表。 |

`list` 元素字段：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| send_time | string | 发送时间（`YYYY-MM-DD HH:MM:SS`）。 |
| uname | string | 发送人名称。 |
| uid | int | 发送人 UID。 |
| price | int | SC 价格。 |
| message | string | SC 内容。 |

**错误响应**

| HTTP 状态码 | 说明 |
| --- | --- |
| 400 | 参数错误。 |
| 500 | 数据库查询失败。 |
| 503 | 报表并发容量已满；响应包含 `Retry-After: 1`。 |

## 8. GET /gift/entry

只返回独立访客名单命中后采集的进房记录，主播进入自己的直播间不记录。所有房间均指程序实际连接的房间；名单删除或房间移除后，已存记录仍可查询。

| 参数 | 类型 | 说明 |
| --- | --- | --- |
| room_id | 正整数 | 房间模式，与 uid / cache 三选一。 |
| uid | 正整数 | 访客模式，与 room_id / cache 三选一。 |
| month | 字符串 | 月份模式可选，默认本月；接受 YYYYMM / YYYY-MM，返回 YYYYMM。 |
| cache | 整数 | 1 为最近 10 分钟，2 为最近 1 小时；不能同时传 month。 |

非法、重复、混用选择参数及未来月份返回 `400`。月份按上海自然月筛选，范围为月初（含）到下月月初（不含）；滚动窗口为请求时间减 600 / 3600 秒（含）到请求时间（不含），可以跨月。记录按事件时间倒序，同一时间按房间号、UID 倒序；返回全部匹配记录，不分页、不截断。

房间查询 `/gift/entry?room_id=1820703922&month=202610`：

```json
{
  "mode": "room",
  "room_id": 1820703922,
  "month": "202610",
  "items": [
    {"uid": 1203217682, "event_time": "2026-10-08T15:29:30+08:00"}
  ]
}
```

访客查询 `/gift/entry?uid=1203217682&month=2026-10`：

```json
{
  "mode": "uid",
  "uid": 1203217682,
  "month": "202610",
  "items": [
    {"room_id": 1820703922, "event_time": "2026-10-08T15:29:30+08:00"}
  ]
}
```

滚动查询 `/gift/entry?cache=2`：

```json
{
  "mode": "cache",
  "cache": 2,
  "items": [
    {"room_id": 1820703922, "uid": 1203217682, "event_time": "2026-10-08T15:29:30+08:00"}
  ]
}
```

本月或滚动窗口无记录时，HTTP `200` 返回 `items: []`。历史月在指定房间或 UID 下没有匹配记录时，HTTP `200` 的同一结构额外包含 `"status": "unavailable"`，`items` 为空。只有后台完成数据读取并确认无数据才返回 unavailable；缓存缺失/故障返回 `503`，请求超限返回 `429`，二者均包含 `Retry-After: 1`。

所有模式的访客 `items` 均读 Redis，月度缓存一般每 10 秒更新，历史数据启动时全量预热，随后默认每 5 分钟检查 Redis DB2 的变更月份，只刷新已标记的月份；归档和迟到写入在提交成功后按月标记。时间带 `+08:00`，有毫秒精度时保留小数；访客记录不包含用户名、事件来源、接收时间、计数或快照字段。


### 仅 cache=1 的红包字段

`GET /gift/entry?cache=1` 在原有字段之外增加顶层 `red_pocket`，没有有效红包时返回空数组。`cache=2`、按房间/UID 查询（包括历史月份），以及其他所有接口均不提供该字段。

```json
{
  "mode": "cache",
  "cache": 1,
  "items": [
    {"room_id": 1820703922, "uid": 671538942, "event_time": "2026-11-15T23:47:24+08:00"}
  ],
  "red_pocket": [
    {"room_id": 1820703922, "pocket_type": "guard", "guard_num": 5, "event_time": "2026-11-15T23:47:24+08:00"},
    {"room_id": 1820703922, "pocket_type": "battery", "battery_num": 100, "event_time": "2026-11-15T23:41:35+08:00"},
    {"room_id": 1820703922, "pocket_type": "gift", "gift_num": 20, "event_time": "2026-11-15T23:47:24+08:00"}
  ]
}
```

- 仅接收 `POPULARITY_RED_POCKET_V2_NEW`，按 `(room_id, lot_id)` 去重；发红包者不受访客名单限制，不要求房间正在直播。
- `rp_type=2` 返回 `pocket_type="guard"`，`price` 为 `9805/19610/39220/98050` 时，`guard_num` 分别为 `5/10/20/50`。其他上舰价格暂不返回。
- `rp_type=3` 返回 `pocket_type="battery"`、`battery_num=原始 price`；`rp_type=0` 返回 `pocket_type="gift"`、`gift_num=原始 price`。不额外做金额换算或乘以 `num`。
- `event_time` 固定取原始秒级 `start_time`，转换为上海时区；每条有效期为 `start_time <= 当前时间 < start_time + 600 秒`，达到边界即移除，重复消息不续期。数组按事件时间降序返回。
- 仅暂存当前采集进程内存，重启后清空；不写 MySQL、不进入 Redis 历史快照、不影响既有礼物和上舰统计。隐藏房间同样从红包数组过滤。
- 访客数据所需的 Redis 快照必须可用；缓存故障、过期、限流等仍按原有 503/429 策略响应，不单独降级返回红包。

## 11. GET /health/check

公开健康检查，无需鉴权、请求头或参数。独立于十个业务接口的 Redis 缓存、可见性过滤和共享限流，返回 `Cache-Control: no-store`；Redis 或业务缓存不可用时仍可返回诊断信息。接口只读取后台采样结果，不会因访问次数增加而重复探测数据库、Redis 或 B 站。

```bash
curl -sS https://vup.qianqiuzy.cn/health/check
```

所有当前依赖、采集循环、连接池、配置房间及启用的 API 缓存健康时返回 **HTTP 200**，`ok=true`、`status="ok"`；存在异常、启动尚未完成或采样过期时返回 **HTTP 503**，`ok=false`，响应仍为下面的诊断结构。

| 字段 | 说明 |
| --- | --- |
| `status` / `ok` | 总体状态：`ok`、`degraded`、`starting`、`unavailable`、`stale`；最近一小时发生过异常但已恢复，不会单独使总体状态降级 |
| `checked_at` / `snapshot_age_seconds` | 后台采样完成时间（上海时区）及采样年龄；首次采样完成前没有依赖明细 |
| `sample_interval_seconds` / `stale_after_seconds` | 每轮结束后等待 15 秒再采样；结果超过 45 秒未更新即标为 `stale` |
| `sample_duration_ms` | 一轮采样耗时；依赖失败时采样间隔会加上探测耗时 |
| `dependencies.database` | 独立短连接检查当前库的 `attention.attention` 列。缺列或不允许 NULL 时为 `schema_mismatch`，总体返回 HTTP 503。连接或查询失败时为 `unavailable` |
| `dependencies.database_pool` | 业务 SQLAlchemy 连接池的 `size`、`checked_in`、`checked_out`、`overflow`、`max_overflow`、`capacity`、取连接超时和回收周期；满载为 `exhausted` |
| `dependencies.redis` | `metrics`（配置的 Redis DB）、`whale_db1`、`api_db2`、`danmaku_users_db3`、`danmaku_rank_db4` 五类用途分别执行 PING，返回状态、耗时和异常类型；不读写业务键，不证明历史数据完整性 |
| `collector.loop_responsive` | 采集事件循环能否在两秒内返回状态快照；未启动、已停止或无响应均不健康 |
| `collector.http_pool` | 共享 aiohttp 连接池的状态、`limit`（0 为不设总上限）、`limit_per_host`、`acquired`、`waiting`、`idle` |
| `collector.rooms` | `configured` 为配置房间数，`connected` 为已连接且鉴权成功的房间数，`not_connected` 为其余房间数。其中处于每日计划重连的房间另计入 `daily_reconnecting`，该状态视为健康。逐房间状态包括 `not_started`、`connecting`、`authenticating`、`connected`、`stopped`、`daily-reconnecting` |
| `collector.rooms.items[]` | 房间号、直播状态、客户端运行状态、鉴权状态、最近鉴权成功/断联/收到消息时间及当前断联持续秒数；从未连接时相应时间为空 |
| `collector.queues` | 守护/粉丝、关注数及每日采集队列的当前长度 |
| `api_cache` | API 缓存的启用/就绪状态、最近刷新年龄和快照实际年龄；缓存关闭时为 `disabled` |
| `upstream_api` | 近一小时有观测的 B 站接口最近结果汇总及 `failing_endpoints`；最近结果仍为失败时总体状态降级，无观测为 `unknown`；不额外主动请求 B 站 |
| `last_hour` | 下述一小时历史及覆盖范围 |

每日 06:00 任务逐房间重连。轮到该房间时，从关闭旧连接到新连接首次鉴权成功，`status` 为 `daily-reconnecting`，`last_disconnected_at` 保留上次断联时间（无记录为 `null`），`disconnected_for_seconds` 为 `null`。新连接首次尝试失败或本次重连超过 60 秒时，恢复按实际连接状态判定健康。

`last_hour` 的统计口径：

- `window_seconds=3600`、`resolution_seconds=1`，使用单调时钟按秒聚合滚动窗口。`observed_since`、`coverage_seconds`、`full_window` 明确标注覆盖范围；历史只驻留当前进程内存，重启后重新累计，不扫描旧日志补齐。
- `totals.room_disconnect`：WebSocket 建立后发生的非主动断开次数。尚未建立连接的初始化失败不会算作一次断联，但会反映在当前房间状态及上游失败中。`room_planned_close` 单独统计调用 stop 的删除房间、主动重连等关闭。
- `totals.upstream_api`：共享 B 站会话遇到的 HTTP 4xx/5xx、连接/连接池等待/读取响应体超时、JSON 解析失败和非零业务错误码。同一请求只计一次。请求取消不计为故障。接口分类包括直播状态、同接、守护、粉丝、房间信息、弹幕配置、WBI 和 ticket 等；无法直接归属单房间的批量请求，其 `room_id=null`。
- `totals.local_api`：请求已到达本服务的十个业务 API 后产生的 5xx、未完成响应或异常中断；`totals.rate_limited` 单独统计 429。正常 4xx 参数/鉴权错误不算不可访问。健康接口本身不计入，避免自身 503 污染业务故障统计。
- `groups` 按类别、接口、房间、原因、HTTP 状态和业务错误码汇总；`recent_events` 最多提供最近 200 条明细，包含发生时间、耗时和连接池排队耗时，明细上限不影响聚合计数。`upstream_endpoints` 给出近一小时有观测的接口最近成功/失败时间与最后结果。
- 不返回数据库/Redis 地址、账号、密码、Cookie、请求参数、完整 URL、异常文本或上游响应体。公网 DNS、TLS、CDN、Nginx、主机断网以及进程完全停止等导致请求未到达应用的故障，需由外部监控探针判断；此接口无法在自身不可访问时报告该段历史。
