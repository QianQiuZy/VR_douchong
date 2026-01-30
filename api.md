# API 文档

本文档基于当前项目实现的 FastAPI，整理所有已对外暴露的接口请求方式与返回字段说明。

## 通用约定

- 基础地址：与服务运行地址一致（默认 `http://<host>:<port>`）。
- 返回格式：`application/json`。
- 错误响应：统一返回 `{"error": "错误信息"}`，并配合相应的 HTTP 状态码（常见为 400/404/409/500）。

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

## 3. GET /gift

**说明**：当前月汇总。

**请求方式**：`GET`

**请求参数**：无

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

**错误响应**

| HTTP 状态码 | 说明 |
| --- | --- |
| 500 | 数据库查询失败。 |

## 4. GET /gift/by_month

**说明**：指定月份汇总。

**请求方式**：`GET`

**请求参数**

| 参数 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| month | string | 否 | 月份，格式 `YYYYMM`。默认当前月。 |

**响应字段**：返回数组，每个元素为房间汇总对象。

字段与 `/gift` 基本一致，区别如下：

- 历史月份返回 `live_time` 为 `0000-00-00 00:00:00`，`title` 为空，`status` 为 `0`。
- 历史月份的 `guard_1/guard_2/guard_3/fans_count` 返回 `null`。
- 不返回 `current_concurrency` 字段。

**错误响应**

| HTTP 状态码 | 说明 |
| --- | --- |
| 500 | 数据库查询失败。 |

## 5. GET /gift/live_sessions

**说明**：查询指定房间 + 月份的单场直播清单。

**请求方式**：`GET`

**请求参数**

| 参数 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| room_id | int | 是 | 房间号，必须为正整数。 |
| month | string | 否 | 月份，格式 `YYYYMM`，默认当前月。 |

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
| blind_box_count | int | 单场盲盒数量。 |
| blind_box_profit | float | 单场盲盒盈亏（正负均可能，最小单位 0.1）。 |
| danmaku_count | int | 弹幕数量。 |
| start_guard_1 | int 或 null | 开播时舰长数量。 |
| start_guard_2 | int 或 null | 开播时提督数量。 |
| start_guard_3 | int 或 null | 开播时总督数量。 |
| start_fans_count | int 或 null | 开播时粉丝团数量。 |
| end_guard_1 | int 或 null | 下播时舰长数量。 |
| end_guard_2 | int 或 null | 下播时提督数量。 |
| end_guard_3 | int 或 null | 下播时总督数量。 |
| end_fans_count | int 或 null | 下播时粉丝团数量。 |
| avg_concurrency | float 或 null | 平均同接（若未采样则为 null）。 |
| max_concurrency | int 或 null | 最大同接（若未采样则为 null）。 |
| current_concurrency | int 或 null | 进行中直播的即时同接，历史场次为 null。 |

**错误响应**

| HTTP 状态码 | 说明 |
| --- | --- |
| 400 | 参数错误。 |
| 500 | 数据库查询失败。 |

## 6. GET /gift/sc

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
