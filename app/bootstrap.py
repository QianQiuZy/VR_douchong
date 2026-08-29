"""Runtime bootstrap and startup orchestration.

Todo 5 makes ownership of ``MAIN_LOOP``, ``_run_in_main_loop``,
``_run_api_server``, and the launcher startup ordering explicit:

* ``MAIN_LOOP`` lives on :mod:`runtime_state` and is assigned to the
  running event loop by :func:`main` before any workers spin up.
* ``_run_in_main_loop`` is owned by :mod:`api_app` (used by the FastAPI
  routes to hop from the uvicorn thread to the main loop).
* ``_run_api_server`` (this module) constructs the uvicorn server bound to
  :attr:`api_app.app`.
* :func:`run` reproduces the pre-extraction ``__main__`` block: call
  :func:`create_schema`, :func:`ensure_runtime_schema`, spawn the API
  thread, and then ``asyncio.run(main())``.

The archive scheduler (:func:`monthly_reset_scheduler`) lives here because
it belongs to the runtime orchestration lane, invoking
:mod:`archive_service` at the appropriate times.
"""

from __future__ import annotations

import asyncio
import datetime
import logging
import threading
from typing import Optional

from . import api_app, archive_service, monitoring_jobs, runtime_state
from .config import APP_HOST, APP_PORT
from .database import create_schema, ensure_runtime_schema
from .metrics_runtime import flush_session
from .models import RoomInfo


# ------------------ time helpers ------------------ #
def _now() -> datetime.datetime:
    return datetime.datetime.now()


async def _sleep_until(target: datetime.datetime) -> None:
    while True:
        remaining = (target - _now()).total_seconds()
        if remaining <= 0:
            return
        await asyncio.sleep(remaining)


def _month_str_now() -> str:
    from .repositories.tables import month_str

    return month_str()


# ------------------ archive scheduler ------------------ #
async def _archive_month(target_month: Optional[str] = None) -> None:
    await asyncio.gather(
        asyncio.to_thread(archive_service.archive_super_chat_log, target_month),
        asyncio.to_thread(archive_service.archive_room_live_stats, target_month),
        asyncio.to_thread(archive_service.archive_attention, target_month),
    )
    await asyncio.to_thread(archive_service.archive_live_session, target_month)


async def monthly_reset_scheduler() -> None:
    startup_now = _now()
    startup_month = _month_str_now()
    if startup_now.month == 12:
        first_target = startup_now.replace(
            year=startup_now.year + 1,
            month=1,
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
    else:
        first_target = startup_now.replace(
            month=startup_now.month + 1,
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )

    if (first_target - startup_now).total_seconds() <= 60:
        await _sleep_until(first_target)
        await _archive_month(startup_month)
        await _archive_month()
    else:
        await _archive_month()
        if _month_str_now() != startup_month:
            await _archive_month(startup_month)

    while True:
        now = _now()
        if now.month == 12:
            target = now.replace(
                year=now.year + 1,
                month=1,
                day=1,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
            )
        else:
            target = now.replace(
                month=now.month + 1,
                day=1,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
            )

        await _sleep_until(target)
        previous_month = _month_str_now_at(target - datetime.timedelta(days=1))
        drift_seconds = max(0.0, (_now() - target).total_seconds())
        logging.info(
            f"[archive] 月切触发，month={previous_month} drift={drift_seconds:.3f}s"
        )
        await _archive_month(previous_month)


def _month_str_now_at(dt: datetime.datetime) -> str:
    from .repositories.tables import month_str

    return month_str(dt)


# ------------------ startup wiring ------------------ #
def init_room_info() -> None:
    """Seed RoomInfo anchor names from the configured rooms."""
    from . import room_config

    for room_id, name in room_config.get_room_anchors().items():
        RoomInfo.upsert(room_id, anchor_name=name)


def init_session() -> None:
    """Initialise the shared aiohttp session used by all Bilibili calls."""
    from . import bilibili_gateway

    bilibili_gateway.init_session()


def _flush_active_metrics(end_time: datetime.datetime) -> None:
    for session_id in set(runtime_state.CURRENT_SESSIONS.values()):
        flush_session(session_id, end_time)


# ------------------ main coroutine ------------------ #
async def main() -> None:
    """Top-level runtime coroutine.

    Assigns ``runtime_state.MAIN_LOOP`` (source of truth for the FastAPI
    thread bridge) and starts every worker gather.  The gather block is
    preserved verbatim from the pre-extraction launcher.
    """
    runtime_state.MAIN_LOOP = asyncio.get_running_loop()
    init_room_info()
    init_session()
    # 先初始化 UID + 粉丝数，完成后再开启状态轮询
    await monitoring_jobs.init_uids_and_attention_once()

    try:
        await asyncio.gather(
            monitoring_jobs.run_clients_loop(),
            monitoring_jobs.monitor_all_rooms_status(),  # 按 UID 批量轮询直播状态
            monthly_reset_scheduler(),
            monitoring_jobs.reconnect_scheduler(),  # 每日 6:00 全量重连
            monitoring_jobs.refresh_attention_scheduler(),  # 每 3 小时刷新关注数（attention）
            monitoring_jobs.attention_worker(),  # 粉丝数任务 worker（开播/下播+每日快照）
            monitoring_jobs.daily_attention_worker(),
            monitoring_jobs.attention_daily_scheduler(),
            monitoring_jobs.guard_daily_scheduler(),
            monitoring_jobs.fans_daily_scheduler(),
            monitoring_jobs.guard_fans_worker(),  # 守护 + 粉丝团队列 worker
            monitoring_jobs.daily_guard_worker(),
            monitoring_jobs.daily_fans_worker(),
            monitoring_jobs.guard_fans_refresh_scheduler(),  # 未开播房间每小时刷新守护 + 粉丝团
            monitoring_jobs.bili_ticket_scheduler(),  # 每日 5:00 刷新 bili_ticket
            monitoring_jobs.danmaku_flush_scheduler(),
            monitoring_jobs.concurrency_poll_scheduler(),  # 开播房间每 15 秒轮询同接
        )
    finally:
        _flush_active_metrics(_now())
        if runtime_state.aiohttp_session:
            await runtime_state.aiohttp_session.close()


# ------------------ API server thread ------------------ #
def _run_api_server() -> None:
    import uvicorn

    config = uvicorn.Config(api_app.app, host=APP_HOST, port=APP_PORT, log_level="info")
    server = uvicorn.Server(config)
    server.run()


# ------------------ launcher entry ------------------ #
def run() -> None:
    """Reproduce the pre-extraction ``__main__`` block launcher order."""
    create_schema()
    ensure_runtime_schema()
    threading.Thread(target=_run_api_server, daemon=True).start()
    asyncio.run(main())
