"""Bilibili websocket event ingestion backed by canonical runtime state."""

import datetime
import logging
from dataclasses import dataclass
from typing import Callable

from . import blivedm

from . import runtime_state
from .metrics_runtime import current_bucket_index, record_payment, start_session
from .redis_metrics import register_payer
from .models import LiveSession, RoomBlindBoxMonthly, RoomLiveStats, RoomStatsMonthly, SuperChatLog


COMMON_NOTICE_GIFT_COIN_MAP = {
    "干杯之旅": 10000,
    "启航之旅": 100000,
    "友谊的小船": 4900,
    "冲浪": 89900,
    "海湾之旅": 799900,
    "鸿运小电视": 1000000,
}


@dataclass(frozen=True)
class EventDependencies:
    month_str: Callable[[], str]
    profit_to_tenths: Callable[[int, int], int]
    send_cookie_invalid_email: Callable[[str], None]


_dependencies: EventDependencies | None = None


def configure(dependencies: EventDependencies) -> None:
    global _dependencies
    _dependencies = dependencies


def _configured_dependencies() -> EventDependencies:
    if _dependencies is None:
        raise RuntimeError("event ingestion dependencies are not configured")
    return _dependencies


def _timestamp_to_datetime(value: int | float | str | None) -> datetime.datetime | None:
    """Convert a Bilibili seconds/milliseconds timestamp when it is usable."""
    if value is None:
        return None
    try:
        seconds = int(value)
        if seconds > 1_000_000_000_000:
            seconds //= 1000
        if seconds >= 946684800:
            return datetime.datetime.fromtimestamp(seconds)
    except (TypeError, ValueError, OSError, OverflowError):
        return None
    return None


class MyHandler(blivedm.BaseHandler):
    def _resolve_session(self, client, session_id: int | None, event_time: datetime.datetime) -> int | None:
        if session_id is not None:
            return session_id
        room_id = client.room_id
        if room_id is None:
            return None
        open_session = LiveSession.find_open_session(room_id)
        if open_session is None:
            return None
        resolved_id, start_time = open_session
        runtime_state.CURRENT_SESSIONS[room_id] = resolved_id
        start_session(resolved_id, room_id, start_time or event_time)
        return resolved_id

    def _record_payment_metrics(
        self,
        client,
        session_id: int | None,
        uid: int,
        event_time: datetime.datetime,
        gift: float = 0.0,
        guard: float = 0.0,
        super_chat: float = 0.0,
        steel_coin: bool = False,
    ) -> bool:
        session_id = self._resolve_session(client, session_id, event_time)
        registration = None
        if int(uid or 0) > 0:
            registration = register_payer(
                client.room_id,
                session_id,
                current_bucket_index(session_id, event_time),
                int(uid),
                event_time.date(),
                event_time.strftime("%Y%m"),
                steel_coin=steel_coin,
            )
        if registration is not None and registration.session is not None and registration.session.size is not None:
            LiveSession.set_payer_count(session_id, registration.session.size)
        if registration is not None and registration.monthly.size is not None:
            RoomStatsMonthly.set_payer_count(
                client.room_id,
                event_time.strftime("%Y%m"),
                registration.monthly.size,
            )
        steel_delta = int(
            registration is not None
            and registration.steel_coin is not None
            and registration.steel_coin.added
        )
        RoomLiveStats.add_metrics(
            client.room_id,
            event_time.date(),
            gift=gift,
            guard=guard,
            super_chat=super_chat,
            payer_count=registration.daily.size if registration is not None else None,
            steel_coin_delta=steel_delta,
        )
        return bool(registration is not None and registration.bucket is not None and registration.bucket.added)

    def _record_blind_box(
        self,
        client,
        num: int,
        total_price: int,
        total_coin: int,
        event_time: datetime.datetime,
    ) -> None:
        if num <= 0:
            return
        dependencies = _configured_dependencies()
        profit = dependencies.profit_to_tenths(total_price, total_coin)
        RoomBlindBoxMonthly.add_amounts(client.room_id, event_time.strftime("%Y%m"), count=num, profit=profit)
        session_id = runtime_state.CURRENT_SESSIONS.get(client.room_id)
        if session_id:
            LiveSession.add_values_by_id(session_id, blind_box_count=num, blind_box_profit=profit)
        else:
            LiveSession.add_values_by_room_open(client.room_id, blind_box_count=num, blind_box_profit=profit)
        active_session_id = runtime_state.CURRENT_SESSIONS.get(client.room_id)
        if active_session_id:
            record_payment(
                active_session_id,
                event_time,
                blind_box_count=num,
                blind_box_profit=profit,
            )

    def _record_gift(
        self,
        client,
        gift_name: str,
        num: int,
        total_coin: int,
        uname: str = "",
        uid: int = 0,
        trigger_cookie_alert: bool = False,
        event_time: datetime.datetime | None = None,
    ) -> None:
        dependencies = _configured_dependencies()
        value = total_coin / 1000
        event_time = event_time or datetime.datetime.now()
        RoomStatsMonthly.add_amounts(client.room_id, event_time.strftime("%Y%m"), gift=value)
        session_id = runtime_state.CURRENT_SESSIONS.get(client.room_id)
        if session_id:
            LiveSession.add_values_by_id(session_id, gift=value)
        else:
            LiveSession.add_values_by_room_open(client.room_id, gift=value)
        bucket_payer_added = self._record_payment_metrics(
            client,
            session_id,
            uid,
            event_time,
            gift=value,
        )
        active_session_id = runtime_state.CURRENT_SESSIONS.get(client.room_id)
        if active_session_id:
            record_payment(active_session_id, event_time, gift=value, payer_added=bucket_payer_added)
        log_message = f"[{client.room_id}] {uname} uid{uid} 赠送 {gift_name}×{num} ({value:.2f})"
        logging.info(log_message)
        if trigger_cookie_alert and uid == 0:
            dependencies.send_cookie_invalid_email(log_message)

    @staticmethod
    def _parse_common_notice_gift(message) -> tuple[str, str]:
        segments = getattr(message, "content_segments", [])
        texts = [segment.text for segment in segments if getattr(segment, "text", "")] if segments else []
        if not texts:
            return "", ""
        return texts[0].strip(), texts[-1].strip()

    def _on_heartbeat(self, client, message) -> None:  # noqa: N802
        return None

    def _on_danmaku(self, client, message) -> None:  # noqa: N802
        try:
            room_id = client.room_id
            if room_id is None:
                return
            if getattr(message, "is_mirror", False) or runtime_state.LAST_STATUS.get(room_id, 0) != 1:
                return
            runtime_state.DANMAKU_PENDING[room_id] = runtime_state.DANMAKU_PENDING.get(room_id, 0) + 1
        except Exception as exc:  # noqa: BROAD_EXCEPT_OK
            logging.error("[Danmaku] 统计弹幕时出错: %s", exc)

    def _on_gift(self, client, message) -> None:  # noqa: N802
        try:
            total_coin = message.total_coin
            event_time = _timestamp_to_datetime(getattr(message, "timestamp", None))
            self._record_gift(
                client,
                message.gift_name,
                message.num,
                message.total_price,
                message.uname,
                message.uid,
                True,
                event_time,
            )
            if message.total_price != total_coin:
                self._record_blind_box(
                    client,
                    int(message.num or 0),
                    int(message.total_price or 0),
                    int(total_coin or 0),
                    event_time or datetime.datetime.now(),
                )
        except Exception as exc:  # noqa: BROAD_EXCEPT_OK
            logging.error("处理礼物记录时出错: %s", exc)

    def _on_common_notice_danmaku(self, client, message) -> None:  # noqa: N802
        try:
            sender, gift_name = self._parse_common_notice_gift(message)
            if not gift_name:
                logging.info("[%s] COMMON_NOTICE_DANMAKU 未解析到礼物名: %s", client.room_id, message.content_text)
                return
            coin_value = COMMON_NOTICE_GIFT_COIN_MAP.get(gift_name)
            if coin_value is None:
                logging.info("[%s] COMMON_NOTICE_DANMAKU 未匹配礼物价格: %s", client.room_id, gift_name)
                return
            self._record_gift(client, gift_name, 1, coin_value, sender, event_time=datetime.datetime.now())
        except Exception as exc:  # noqa: BROAD_EXCEPT_OK
            logging.error("处理 COMMON_NOTICE_DANMAKU 礼物记录时出错: %s", exc)

    def _record_guard(
        self,
        client,
        username: str,
        uid: int,
        guard_level: int,
        num: int,
        price: int,
        start_time: int | float | str | None,
    ) -> None:
        room_id = client.room_id
        if room_id is None:
            return
        total_coins = int(price) * int(num)
        is_red_pack = int(price) == 1900
        if is_red_pack:
            total_coins = 198000
        if int(num) != 1:
            mappings = {
                3: {3: 534000, 6: 1038000, 12: 2046000},
                2: {3: 4794000, 6: 9588000, 12: 19176000},
                1: {3: 51994000},
            }
            total_coins = mappings.get(int(guard_level), {}).get(int(num), total_coins)
        value = total_coins / 1000
        event_time = _timestamp_to_datetime(start_time) or datetime.datetime.now()
        RoomStatsMonthly.add_amounts(room_id, event_time.strftime("%Y%m"), guard=value)
        session_id = runtime_state.CURRENT_SESSIONS.get(room_id)
        if session_id:
            LiveSession.add_values_by_id(session_id, guard=value)
        else:
            LiveSession.add_values_by_room_open(room_id, guard=value)
        bucket_payer_added = self._record_payment_metrics(
            client,
            session_id,
            uid,
            event_time,
            guard=value,
        )
        active_session_id = runtime_state.CURRENT_SESSIONS.get(room_id)
        if active_session_id:
            record_payment(active_session_id, event_time, guard=value, payer_added=bucket_payer_added)
        logging.info(
            "[%s] %s %s 上舰 lvl=%s num=%s 修正后=%.1f RMB %s",
            room_id,
            username,
            uid,
            guard_level,
            num,
            value,
            "(红包上舰)" if is_red_pack else "",
        )

    def _on_user_toast_v2(self, client, message) -> None:  # noqa: N802
        try:
            self._record_guard(
                client,
                getattr(message, "username", ""),
                getattr(message, "uid", 0),
                getattr(message, "guard_level", 0),
                getattr(message, "num", 0),
                getattr(message, "price", 0),
                getattr(message, "start_time", None),
            )
        except Exception as exc:  # noqa: BROAD_EXCEPT_OK
            logging.error("处理舰长记录时出错: %s", exc)

    def _on_super_chat(self, client, message) -> None:  # noqa: N802
        try:
            room_id = client.room_id
            if room_id is None:
                return
            value = message.price
            event_time = _timestamp_to_datetime(getattr(message, "start_time", None))
            if event_time is None:
                event_time = _timestamp_to_datetime(getattr(message, "timestamp", None))
            if event_time is None:
                event_time = _timestamp_to_datetime(getattr(message, "ts", None))
            event_time = event_time or datetime.datetime.now()
            RoomStatsMonthly.add_amounts(room_id, event_time.strftime("%Y%m"), super_chat=value)
            session_id = runtime_state.CURRENT_SESSIONS.get(room_id)
            if session_id:
                LiveSession.add_values_by_id(session_id, super_chat=value)
            else:
                LiveSession.add_values_by_room_open(room_id, super_chat=value)
            bucket_payer_added = self._record_payment_metrics(
                client,
                session_id,
                message.uid,
                event_time,
                super_chat=value,
                steel_coin=value < 30,
            )
            active_session_id = runtime_state.CURRENT_SESSIONS.get(room_id)
            if active_session_id:
                record_payment(
                    active_session_id,
                    event_time,
                    super_chat=value,
                    payer_added=bucket_payer_added,
                )
            user_info = getattr(message, "user_info", None)
            uname = getattr(message, "uname", "") or (user_info.get("uname", "") if isinstance(user_info, dict) else "")
            uid = getattr(message, "uid", 0) or (user_info.get("uid", 0) if isinstance(user_info, dict) else 0)
            SuperChatLog.log_sc(room_id, uname, uid, value, getattr(message, "message", "") or "", event_time)
            logging.info("[%s] SC ¥%.2f %s %s: %s", room_id, value, uname, uid, getattr(message, "message", "") or "")
        except Exception as exc:  # noqa: BROAD_EXCEPT_OK
            logging.error("处理醒目留言记录时出错: %s", exc)
