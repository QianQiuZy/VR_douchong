"""Bilibili websocket event ingestion backed by canonical runtime state."""

import datetime
import logging
from dataclasses import dataclass
from typing import Callable

from . import blivedm

from . import runtime_state
from .models import LiveSession, RoomBlindBoxMonthly, RoomStatsMonthly, SuperChatLog


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


class MyHandler(blivedm.BaseHandler):
    def _record_blind_box(self, client, num: int, total_price: int, total_coin: int) -> None:
        if num <= 0:
            return
        dependencies = _configured_dependencies()
        profit = dependencies.profit_to_tenths(total_price, total_coin)
        RoomBlindBoxMonthly.add_amounts(client.room_id, dependencies.month_str(), count=num, profit=profit)
        session_id = runtime_state.CURRENT_SESSIONS.get(client.room_id)
        if session_id:
            LiveSession.add_values_by_id(session_id, blind_box_count=num, blind_box_profit=profit)
            return
        LiveSession.add_values_by_room_open(client.room_id, blind_box_count=num, blind_box_profit=profit)

    def _record_gift(
        self,
        client,
        gift_name: str,
        num: int,
        total_coin: int,
        uname: str = "",
        uid: int = 0,
        trigger_cookie_alert: bool = False,
    ) -> None:
        dependencies = _configured_dependencies()
        value = total_coin / 1000
        RoomStatsMonthly.add_amounts(client.room_id, dependencies.month_str(), gift=value)
        session_id = runtime_state.CURRENT_SESSIONS.get(client.room_id)
        if session_id:
            LiveSession.add_values_by_id(session_id, gift=value)
        else:
            LiveSession.add_values_by_room_open(client.room_id, gift=value)
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
            self._record_gift(client, message.gift_name, message.num, message.total_price, message.uname, message.uid, True)
            if message.total_price != total_coin:
                self._record_blind_box(client, int(message.num or 0), int(message.total_price or 0), int(total_coin or 0))
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
            self._record_gift(client, gift_name, 1, coin_value, sender)
        except Exception as exc:  # noqa: BROAD_EXCEPT_OK
            logging.error("处理 COMMON_NOTICE_DANMAKU 礼物记录时出错: %s", exc)

    def _on_user_toast_v2(self, client, message) -> None:  # noqa: N802
        try:
            room_id = client.room_id
            if room_id is None:
                return
            total_coins = message.price * message.num
            is_red_pack = message.price == 1900
            if is_red_pack:
                total_coins = 198000
            if message.num != 1:
                mappings = {
                    3: {3: 534000, 6: 1038000, 12: 2046000},
                    2: {3: 4794000, 6: 9588000, 12: 19176000},
                    1: {3: 51994000},
                }
                total_coins = mappings.get(message.guard_level, {}).get(message.num, total_coins)
            value = total_coins / 1000
            dependencies = _configured_dependencies()
            RoomStatsMonthly.add_amounts(room_id, dependencies.month_str(), guard=value)
            session_id = runtime_state.CURRENT_SESSIONS.get(room_id)
            if session_id:
                LiveSession.add_values_by_id(session_id, guard=value)
            else:
                LiveSession.add_values_by_room_open(room_id, guard=value)
            logging.info("[%s] %s %s 上舰 lvl=%s num=%s 修正后=%.1f RMB %s", room_id, message.username, message.uid, message.guard_level, message.num, value, "(红包上舰)" if is_red_pack else "")
        except Exception as exc:  # noqa: BROAD_EXCEPT_OK
            logging.error("处理舰长记录时出错: %s", exc)

    def _on_super_chat(self, client, message) -> None:  # noqa: N802
        try:
            room_id = client.room_id
            if room_id is None:
                return
            dependencies = _configured_dependencies()
            value = message.price
            RoomStatsMonthly.add_amounts(room_id, dependencies.month_str(), super_chat=value)
            session_id = runtime_state.CURRENT_SESSIONS.get(room_id)
            if session_id:
                LiveSession.add_values_by_id(session_id, super_chat=value)
            else:
                LiveSession.add_values_by_room_open(room_id, super_chat=value)
            user_info = getattr(message, "user_info", None)
            uname = getattr(message, "uname", "") or (user_info.get("uname", "") if isinstance(user_info, dict) else "")
            uid = getattr(message, "uid", 0) or (user_info.get("uid", 0) if isinstance(user_info, dict) else 0)
            timestamp = getattr(message, "time", None)
            if timestamp is None:
                timestamp = getattr(message, "ts", None)
            send_time = None
            if timestamp is not None:
                try:
                    seconds = int(timestamp)
                    if seconds > 1_000_000_000_000:
                        seconds //= 1000
                    if seconds >= 946684800:
                        send_time = datetime.datetime.fromtimestamp(seconds)
                except (TypeError, ValueError, OSError, OverflowError):
                    send_time = None
            SuperChatLog.log_sc(room_id, uname, uid, value, getattr(message, "message", "") or "", send_time)
            logging.info("[%s] SC ¥%.2f %s %s: %s", room_id, value, uname, uid, getattr(message, "message", "") or "")
        except Exception as exc:  # noqa: BROAD_EXCEPT_OK
            logging.error("处理醒目留言记录时出错: %s", exc)
