# douchong.py (monthly-only, no room_stats)
"""Compatibility facade / launcher for the VR douchong runtime.

Todo 5 removed the archive and FastAPI transport bodies from this file.
Every route lives in :mod:`api_app`, every archive helper lives in
:mod:`archive_service`, and every startup step lives in :mod:`bootstrap`.
This module now only:

* holds import-time side effects (load rooms config, wire event
  ingestion, seed per-room runtime state, hand cookie-alert email into
  the ingestion layer),
* re-exports the pre-extraction public surface so tests,
  ``migrate_sc_archive.py``, and any external callers keep working,
* dispatches ``__main__`` execution to :func:`bootstrap.run`.
"""

from __future__ import annotations

import asyncio  # noqa: F401 - re-exported for external callers of gift.asyncio
import datetime
import logging
import smtplib
import threading
from decimal import Decimal, ROUND_HALF_UP
from email.mime.text import MIMEText
from typing import Dict, Optional, Tuple

# Test / CLI compat surface: attributes accessed as ``gift.X`` before
# Todo 5 remain accessible via re-export.
from .config import (  # noqa: F401 - preserved compat re-exports
    API_SECRET,
    APP_HOST,
    APP_PORT,
    ATTENTION_DAILY_ROOM_SLEEP_SECONDS,
    DB_CONFIG,
    EMAIL_FROM,
    EMAIL_TO,
    SMTP_HOST,
    SMTP_PASS,
    SMTP_PORT,
    SMTP_USER,
    get_env_int,
)
from .database import (  # noqa: F401 - preserved compat re-exports
    Base,
    Session,
    create_schema,
    engine,
    ensure_runtime_schema,
)
from .models import (  # noqa: F401 - preserved compat re-exports
    Attention,
    LiveSession,
    LiveSession15mStats,
    RoomBlindBoxMonthly,
    RoomInfo,
    RoomLiveStats,
    RoomStatsMonthly,
    SuperChatLog,
)
from .repositories.tables import (  # noqa: F401 - preserved compat re-exports
    attention_table_name,
    ensure_attention_archive_table,
    ensure_live_session_archive_table,
    ensure_live_session_15m_stats_archive_table,
    ensure_room_live_stats_archive_table,
    ensure_sc_archive_table,
    is_current_month,
    live_session_table_name,
    live_session_15m_stats_table_name,
    month_range,
    month_str,
    normalize_month_code,
    room_live_stats_table_name,
    sc_log_table_exists,
    sc_log_table_name,
)

from . import bilibili_gateway, event_ingestion, monitoring_jobs, room_config, room_lifecycle, runtime_state
from .runtime_state import (  # noqa: F401 - preserved compat re-exports
    ATTENTION_QUEUE,
    CONCURRENCY_CACHE,
    CURRENT_SESSIONS,
    DAILY_ATTENTION_QUEUE,
    DAILY_FANS_QUEUE,
    DAILY_GUARD_QUEUE,
    DANMAKU_PENDING,
    FANS_COUNT,
    GUARD_COUNTS,
    GUARD_FANS_QUEUE,
    LAST_RECONNECT,
    LAST_STATUS,
    LIVE_INFO,
    LOCKED_ROOM_UNTIL,
    MAIN_LOOP,
    PENDING_SESSION_ENDS,
    RECONNECT_DAILY_STATE,
    ROOM_ANCHORS,
    ROOM_CLIENTS,
    ROOM_CONFIG_LOCK,
    ROOM_IDS,
    ROOM_UIDS,
    STREAM_STARTS,
    aiohttp_session,
)

# ------------------ Bilibili gateway compat re-exports ------------------ #
BILI_COOKIES_BASE = bilibili_gateway.BILI_COOKIES_BASE
BILI_TICKET_KEY = bilibili_gateway.BILI_TICKET_KEY
BILI_TICKET_URL = bilibili_gateway.BILI_TICKET_URL
BILI_TICKET_KEY_ID = bilibili_gateway.BILI_TICKET_KEY_ID
BILI_TICKET = bilibili_gateway.BILI_TICKET
BILI_TICKET_EXPIRES = bilibili_gateway.BILI_TICKET_EXPIRES
USER_AGENT = bilibili_gateway.USER_AGENT
LIVE_STATUS_API = bilibili_gateway.LIVE_STATUS_API
ROOM_INFO_API = bilibili_gateway.ROOM_INFO_API
ROOM_INIT_API = bilibili_gateway.ROOM_INIT_API
FANS_API = bilibili_gateway.FANS_API
GUARD_API = bilibili_gateway.GUARD_API
CONTRIBUTION_RANK_API = bilibili_gateway.CONTRIBUTION_RANK_API

# ------------------ Logging bootstrap (preserved) ------------------ #
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

_cookie_alert_sent = False

COMMON_NOTICE_GIFT_COIN_MAP = event_ingestion.COMMON_NOTICE_GIFT_COIN_MAP


# ------------------ Archive service compat re-exports ------------------ #
from . import archive_service as _archive_service

archive_super_chat_log = _archive_service.archive_super_chat_log
archive_live_session_15m_stats = _archive_service.archive_live_session_15m_stats
archive_live_session = _archive_service.archive_live_session
archive_room_live_stats = _archive_service.archive_room_live_stats
archive_attention = _archive_service.archive_attention


# ------------------ Cookie invalidation notification ------------------ #
def send_cookie_invalid_email_async(log_line: str) -> None:
    """
    检测到 uid=0 时发送一次告警邮件（进程生命周期内只发一次）。
    """
    global _cookie_alert_sent
    if _cookie_alert_sent:
        return
    _cookie_alert_sent = True

    def _worker() -> None:
        try:
            subject = "B站直播礼物监听 Cookies 失效告警"
            body = (
                "检测到 B 站直播礼物消息 uid=0，疑似 SESSDATA Cookies 已失效。\n\n"
                f"原始日志：{log_line}\n"
                "请尽快检查并更新 douchong.py 使用的 SESSDATA。"
            )
            msg = MIMEText(body, "plain", "utf-8")
            msg["Subject"] = subject
            msg["From"] = EMAIL_FROM
            msg["To"] = EMAIL_TO

            with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
                server.starttls()
                server.login(SMTP_USER, SMTP_PASS)
                server.sendmail(EMAIL_FROM, [EMAIL_TO], msg.as_string())

            logging.info("[SMTP] Cookies 失效告警邮件已发送")
        except Exception as e:
            logging.error(f"[SMTP] 发送 Cookies 失效告警失败: {e}")

    threading.Thread(target=_worker, daemon=True).start()


def __getattr__(name: str) -> bool:
    """Expose the legacy cookie-alert flag without a reassignable constant."""
    if name == "COOKIE_ALERT_SENT":
        return _cookie_alert_sent
    raise AttributeError(name)


# ------------------ Rooms config compat wrappers ------------------ #
ROOMS_JSON_PATH = runtime_state.ROOMS_JSON_PATH


def _sync_rooms_json_path() -> None:
    runtime_state.ROOMS_JSON_PATH = ROOMS_JSON_PATH


def load_rooms_config() -> None:
    _sync_rooms_json_path()
    room_config.load_rooms_config()


def save_rooms_config() -> None:
    _sync_rooms_json_path()
    room_config.save_rooms_config()


def get_room_ids() -> list[int]:
    return room_config.get_room_ids()


def get_room_anchors() -> Dict[int, str]:
    return room_config.get_room_anchors()


def get_room_anchor_name(room_id: int) -> str:
    return room_config.get_room_anchor_name(room_id)


load_rooms_config()


# ------------------ Small helpers preserved for compat ------------------ #
def _now() -> datetime.datetime:
    return datetime.datetime.now()


def _next_daily_target(now: datetime.datetime, hour: int, minute: int) -> datetime.datetime:
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if now >= target:
        target += datetime.timedelta(days=1)
    return target


async def _sleep_until(target: datetime.datetime) -> None:
    while True:
        remaining = (target - _now()).total_seconds()
        if remaining <= 0:
            return
        await asyncio.sleep(remaining)


def _profit_to_tenths(total_price: int, total_coin: int) -> int:
    profit_coin = int(total_price) - int(total_coin)
    profit_rmb = (Decimal(profit_coin) / Decimal(1000)).quantize(
        Decimal("0.1"), rounding=ROUND_HALF_UP
    )
    return int(profit_rmb * 10)


# ------------------ Event ingestion wiring ------------------ #
event_ingestion.configure(
    event_ingestion.EventDependencies(
        month_str=month_str,
        profit_to_tenths=_profit_to_tenths,
        send_cookie_invalid_email=send_cookie_invalid_email_async,
    )
)

MyHandler = event_ingestion.MyHandler


# ------------------ Room lifecycle compat wrappers ------------------ #
LIVE_SESSION_GRACE_SECONDS = room_lifecycle.LIVE_SESSION_GRACE_SECONDS


def _lifecycle_dependencies() -> room_lifecycle.LifecycleDependencies:
    return monitoring_jobs.lifecycle_dependencies()


def _finish_live_session(room_id: int, end_dt: datetime.datetime) -> Optional[str]:
    return room_lifecycle.finish_live_session(room_id, end_dt, _lifecycle_dependencies())


def _defer_live_session_finish(room_id: int, end_dt: datetime.datetime) -> Optional[str]:
    return room_lifecycle.defer_live_session_finish(
        room_id, end_dt, _lifecycle_dependencies()
    )


def _resume_interrupted_session(
    room_id: int,
    start_dt: datetime.datetime,
    now: datetime.datetime,
) -> Optional[int]:
    return room_lifecycle.resume_interrupted_session(room_id, start_dt, now)


def _finish_expired_live_sessions(now: datetime.datetime) -> None:
    room_lifecycle.finish_expired_live_sessions(now, _lifecycle_dependencies())


ensure_room_state = room_lifecycle.ensure_room_state

for _room_id in get_room_ids():
    ensure_room_state(_room_id)


async def add_room_async(room_id: int, anchor_name: str) -> Tuple[bool, str]:
    return await room_lifecycle.add_room_async(room_id, anchor_name, _lifecycle_dependencies())


async def delete_room_async(room_id: int) -> Tuple[bool, str]:
    return await room_lifecycle.delete_room_async(room_id, _lifecycle_dependencies())


# ------------------ Bilibili gateway wrappers ------------------ #
def init_session() -> None:
    bilibili_gateway.init_session()
    globals()["aiohttp_session"] = runtime_state.aiohttp_session


async def ensure_bili_ticket(force: bool = False) -> str:
    ticket = await bilibili_gateway.ensure_bili_ticket(force)
    globals()["BILI_TICKET"] = bilibili_gateway.BILI_TICKET
    globals()["BILI_TICKET_EXPIRES"] = bilibili_gateway.BILI_TICKET_EXPIRES
    return ticket


_fetch_room_info_and_update = bilibili_gateway.fetch_room_info_and_update
_fetch_room_init = bilibili_gateway.fetch_room_init
_fetch_guard_counts = bilibili_gateway.fetch_guard_counts
_fetch_fans_count = bilibili_gateway.fetch_fans_count
_fetch_contribution_count = bilibili_gateway.fetch_contribution_count


# ------------------ Monitoring jobs compat re-exports ------------------ #
_start_client = monitoring_jobs.start_client
_reconnect_one = monitoring_jobs.reconnect_one
run_clients_loop = monitoring_jobs.run_clients_loop
init_uids_and_attention_once = monitoring_jobs.init_uids_and_attention_once
init_uid_and_attention_for_room = monitoring_jobs.init_uid_and_attention_for_room
attention_worker = monitoring_jobs.attention_worker
daily_attention_worker = monitoring_jobs.daily_attention_worker
guard_fans_worker = monitoring_jobs.guard_fans_worker
daily_guard_worker = monitoring_jobs.daily_guard_worker
daily_fans_worker = monitoring_jobs.daily_fans_worker
danmaku_flush_scheduler = monitoring_jobs.danmaku_flush_scheduler
refresh_attention_scheduler = monitoring_jobs.refresh_attention_scheduler
attention_daily_scheduler = monitoring_jobs.attention_daily_scheduler
guard_daily_scheduler = monitoring_jobs.guard_daily_scheduler
fans_daily_scheduler = monitoring_jobs.fans_daily_scheduler
guard_fans_refresh_scheduler = monitoring_jobs.guard_fans_refresh_scheduler
concurrency_poll_scheduler = monitoring_jobs.concurrency_poll_scheduler
monitor_all_rooms_status = monitoring_jobs.monitor_all_rooms_status
reconnect_scheduler = monitoring_jobs.reconnect_scheduler
bili_ticket_scheduler = monitoring_jobs.bili_ticket_scheduler
_init_concurrency_cache = monitoring_jobs.init_concurrency_cache
_update_concurrency_cache = monitoring_jobs.update_concurrency_cache
_finalize_concurrency_cache = monitoring_jobs.finalize_concurrency_cache
_flush_pending_danmaku_for_room = monitoring_jobs.flush_pending_danmaku_for_room


# ------------------ API app / route compat re-exports ------------------ #
from . import api_app as _api_app

app = _api_app.app
_run_in_main_loop = _api_app._run_in_main_loop
_parse_room_payload = _api_app._parse_room_payload
_check_api_secret = _api_app._check_api_secret
_room_ids_for_month = _api_app._room_ids_for_month
_seconds_to_hms = _api_app._seconds_to_hms
_tenths_to_decimal = _api_app._tenths_to_decimal
_profit_display = _api_app._profit_display

# The FastAPI route callables (kept accessible for tests / external
# callers who imported them from ``gift`` before Todo 5).
add_room_api = _api_app.add_room_api
delete_room_api = _api_app.delete_room_api
get_stats_current_month = _api_app.get_stats_current_month
get_stats_by_month = _api_app.get_stats_by_month
get_live_sessions_by_room_month = _api_app.get_live_sessions_by_room_month
get_attention_logs = _api_app.get_attention_logs
get_sc_logs = _api_app.get_sc_logs


# ------------------ Bootstrap compat re-exports ------------------ #
from . import bootstrap as _bootstrap

main = _bootstrap.main
run = _bootstrap.run
_archive_month = _bootstrap._archive_month
monthly_reset_scheduler = _bootstrap.monthly_reset_scheduler
_run_api_server = _bootstrap._run_api_server
init_room_info = _bootstrap.init_room_info


# ------------------ Launcher ------------------ #
if __name__ == "__main__":
    _bootstrap.run()
