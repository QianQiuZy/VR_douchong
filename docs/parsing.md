# 当前解析说明

核对日期：2026-10-21。本文描述当前检出代码的处理行为；[WebSocket 协议参考](./API.live_websocket.md) 包含更多平台事件和历史样本，不能据其 CMD 列表推断本项目已经入账。

## 处理链路

`AuthenticatedLiveClient → WebSocketClientBase 解包 → BaseHandler.handle → 消息模型 → MyHandler / room_lock_events → 统计、队列与持久化`。

- `app/blivedm/clients/ws_base.py` 解析 16 字节大端包头、JSON 消息、zlib/brotli 压缩包，处理心跳和鉴权响应；解压后的包继续按包头解析。
- `app/blivedm/handlers.py` 去掉 CMD 的冒号后缀并查回调表。新 V2 礼物会先整包解码，再逐条调用 `_on_gift`。
- `app/blivedm/models/web.py` 负责 Web JSON 和 V2 消息转换；`models/pb.py` 定义互动 V2 protobuf；`models/open_live.py` 为开放平台消息模型。
- `app/event_ingestion.py` 的 `MyHandler` 接业务；`app/room_lock_events.py` 接违规与强制下播。源码核对入口应同时看协议回调表和业务类覆盖方法。

## 实际接入范围

| CMD | 当前解析/分发 | VR_douchong 业务行为 |
| --- | --- | --- |
| `_HEARTBEAT` | 服务端心跳转换为内部消息 | 业务回调不额外统计 |
| `DANMU_MSG` | `info[] → DanmakuMessage` | 在播房间弹幕进入待写统计与 15 分钟桶，按身份分类 |
| `DANMU_MSG_MIRROR` | 标记 `is_mirror` | 跳过镜像弹幕，避免重复计数 |
| `SEND_GIFT` | JSON → `GiftMessage` | 礼物与盲盒金额、日/月/场次/统计桶、付费指标 |
| `SEND_GIFT_V2` | `command["data"]["pb"]` → protobuf 礼物列表 | 每条归一化为 `GiftMessage`，复用同一送礼回调 |
| `COMMON_NOTICE_DANMAKU` | 内容片段 → 通知模型 | 从首尾文本解析送礼者/礼物名，只处理既有价格映射 |
| `USER_TOAST_MSG_V2` | JSON 的 `pay_info` 等 → 上舰模型 | 上舰金额修正及日/月/场次/统计桶、付费指标 |
| `GUARD_BUY` | 可转为 `GuardBuyMessage` | `MyHandler` 未覆盖 `_on_buy_guard`，当前不由此入账 |
| `SUPER_CHAT_MESSAGE` | JSON → SC 模型 | 金额、文本日志、日/月/场次/统计桶、付费指标 |
| `SUPER_CHAT_MESSAGE_DELETE` | 可转为删除消息 | 未覆盖删除回调，当前不会删除已保存 SC |
| `INTERACT_WORD` / `INTERACT_WORD_V2` | 老 JSON / base64 protobuf → 互动模型 | 仅 `msg_type=1` 的名单访客进房，排除房主本人 |
| `WARNING` | 直接消费顶层 `roomid`、`send_time` | 作废当前片段的有效时长，不主动下播 |
| `CUT_OFF` | 顶层 `room_id`、`send_time` | 强制停止当前片段，进入 180 秒宽限流程 |
| `ROOM_LOCK` | 顶层 `room_id`、`send_time`、`expire` | 同强制下播并记录锁定到期，阻止滞后状态恢复 |
| `LIVE` / `PREPARING` | 没有业务回调 | 开下播由状态 API 轮询与生命周期模块确认 |
| `ONLINE_RANK_V3`、PK、点赞等 | 部分为已知忽略，部分首次记录未知命令 | 当前没有对应业务入账或榜单 protobuf 模型 |

内置库还注册开放平台 `LIVE_OPEN_PLATFORM_*` 回调，但本项目启动使用 Web 客户端，`MyHandler` 没有开放平台统计实现。不要把库具有的协议入口表述为本项目已启用功能。

## SEND_GIFT_V2 与金额归一化

原始事件路径是 `command["data"]["pb"]`，传给 `SendGiftV2Message.from_command` 后函数读取 `data["pb"]`。使用严格 base64 解码和 pure-protobuf；不是原始消息的 `command["data"]["data"]["pb"]`。

关键字段：顶层 UID/name 为 field 1/2，勋章为 field 8，盲盒为 field 9，礼物为 repeated field 10。礼物子消息中 `price=5`、`total_coin=6`、`discount_price=7`、`coin_type=8`、`tid=9`、`timestamp=10`、`rnd=12`。

解码结果是 `tuple[GiftMessage, ...]`。空礼物列表，或任一条 `gift_id <= 0`、`num <= 0`、礼物名为空，都会拒绝整包；BaseHandler 对解码错误告警并跳过，避免只入账前半包。

以下表格中的原始金额均指 protobuf 的字段值：

| 情况 | `GiftMessage.total_price` | `GiftMessage.total_coin` |
| --- | --- | --- |
| 盲盒（原始盲盒 ID 或名称非空） | `price × num` | 保留原始 `total_coin` |
| 普通多数量礼物（`num != 1` 且原始 `total_coin > 0`） | `price × num` | 与归一化 `total_price` 相同 |
| 其他普通礼物（含单个折扣礼物、原始金额为 0） | 原始 `total_coin` | 原始 `total_coin` |

例如普通礼物 `num=10, price=100, total_coin=100` 归一化为 1000 金瓜子；单个礼物 `price=100, total_coin=80` 保留 80；多数量但 `total_coin=0` 的普通礼物保持 0。`discount_price` 目前不参与该归一化规则，不根据推断字段增加收费逻辑。

业务 `_on_gift` 继续按 `message.total_price` 入账，1000 金瓜子对应 1 元。
解析器显式传递 `is_blind_box`，盲盒数量不再依赖 `total_price != total_coin`，盈亏为零也累计数量。
新增 `blind_cost_coin` 独立表示盲盒总成本：V2 优先取 `blind.blind_price * gift.num`；
缺少有效单价时告警并回退该条原始 `total_coin`（沿用旧整笔成本口径，不再次乘数量）。
成本均不可用时为 `None`，业务告警、仅累计数量，不增加盈亏，也不改变原流水。
`discount_price` 不用作成本回退。仅未提供显式标记的旧调用方保留金额差异判断兼容。

例如心动盲盒开出棉花糖：`num=1, price=9000, total_coin=9000, blind_price=15000`，
流水仍为 9 元，盲盒数量增加 1，盈亏增加 -6 元。盈亏继续以角存库（本例 -60），
复用月度、场次、15 分钟统计桶，不改表、不重放或回补历史数据。

真实盲盒包回归保存在 `tests/fixtures/send_gift_v2_blind_package.json`：
来源为 2026-10-03 录播 XML，UID `4941873`、相对时间 `851.436`。
一条国庆盲盒消息含故邑呈笺 ×2（总价值 20 元、成本 18 元、盈亏 +2 元）、
渡口衔风 ×3（总价值 15 元、成本 27 元、盈亏 -12 元）。每个盲盒成本 9000 瓜子，
整包沿用现有流水口径累计 35 元、成本 45 元，数量合计 5，盈亏 -10 元。
录播姬将同一 raw 展示为两行礼物，回归只回放一个原始消息，避免按 XML 展示行重复入账。
波仔盲盒是活动礼物，不用作实际盈亏的回归样本；本次样本替换不增加活动礼物分类规则。
`test_send_gift_v2_blind_package.py` 独立验证解析及月度、场次、15 分钟落库调用。

旧 `SEND_GIFT` 保持独立规则：普通礼物使用原始 `total_coin`；盲盒优先 `gift_tip_price`，其次 `price` 乘数量，无法取得有效单价/数量时回退 `total_coin`。明确传递盲盒标记，成本沿用正数 `total_coin`；成本未知时仅累计数量。V2 批量规则不应用到旧 JSON。

`tid/rnd` 会被用于鲸鱼收入事件键，付费人数也有集合去重；这不表示所有月汇总、场次金额都经过同一个全局去重事务。多协议重复、重发与多礼物包的回归应覆盖各类指标，不能用去重集合存在代替验证。

## 上舰、SC 与通知

- 当前上舰入账入口为 `USER_TOAST_MSG_V2`；`_record_guard` 保留红包价 1900 → 198000 金瓜子及指定舰种多月套餐修正。未经验证不要同时接入 `GUARD_BUY`，需先评估同一上舰行为的多 CMD 重复。
- SC 的 `price` 是人民币金额，不按礼物金瓜子再次除以 1000。事件键优先 SC ID；缺 ID 时使用业务字段组合。
- 通知礼物只使用 `COMMON_NOTICE_GIFT_COIN_MAP` 中已明确配置的名称与价格，未知礼物不猜测金额。
- 本文不扩大旧路径对免费/银瓜子礼物的处理范围；金额行为以对应模型与回归测试为准。

## 互动事件与访客进房

老协议读取 `uid/uname/timestamp/msg_type`；V2 protobuf 为 `uid=1`、`uname=2`、`msg_type=5`、`timestamp=7`、`uinfo=22`。V2 解码失败时记录异常类型并尝试外层同名字段回退；回退字段缺失不会凭空产生有效访客。

仅记录名单 UID、`msg_type=1`、已知房主且非主播本人。队列容量 10000，批量大小 200；消息回调只入队，后台写库并按三字段主键去重。名单每小时校验替换；非法名单保留上一版本。

时间戳允许秒/毫秒，有效年份不早于 2000，不能超过接收时间 5 分钟；异常值回退接收时间，并保留毫秒精度。保存字段只有 `room_id/uid/event_time`，名单中的名称不写入记录或响应。无上游时间、断线或进程突然退出的边界见 [runtime.md](./runtime.md)。

## 违规与开下播状态

`WARNING` 使用 `roomid`，`CUT_OFF/ROOM_LOCK` 使用 `room_id`，`send_time` 为毫秒时间戳，均位于命令顶层。`ROOM_LOCK.expire` 按北京时间解释。

违规仅作废当前开播片段的有效时长，不撤回较早正常片段的贡献，也不删除金额或弹幕。强制下播按事件时间停止片段，锁定期间忽略滞后在播状态；解除后旧 `live_time` 不允许恢复有效计时。正好 180 秒仍可合并场次，超过 180 秒关闭原场次；重开会创建新的正常片段。

`LIVE/PREPARING` 当前不能取代状态 API。启动、断线恢复、强制下播与宽限期到期还依赖 `monitoring_jobs.py`、`room_lifecycle.py` 的定时流程。以后接入生命周期 WS 事件时，应共用现有状态机，并保留 API 校正和独立到期调度。

## 扩展解析时的核对顺序

1. 先取得脱敏消息样本，确认包头/压缩、真实字段路径、类型和单位；区分实测与推断。
2. 修改对应模型和回调表，复用现有业务入口，明确未知字段、非法数据和多条记录的行为。
3. 覆盖旧/V2 共存、批量、免费/折扣、盲盒、缺字段、损坏包、重复和晚到事件的相关边界。
4. 同步本页、[协议参考](./API.live_websocket.md)、必要的 [API 合同](./api.md) 与[历史记录](./history.md)；库版本与后续分支维护见 [blivedm.md](./blivedm.md)。
