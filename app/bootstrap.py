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

from sqlalchemy.exc import SQLAlchemyError

from . import api_app, archive_service, monitoring_jobs, runtime_state
from .config import APP_HOST, APP_PORT
from .database import create_schema, ensure_runtime_schema, log_pool_status
from .metrics_runtime import flush_session
from .models import RoomInfo
from .whale_archive import archive_whale_month

POOL_STATUS_INTERVAL_SECONDS = 300
SESSION_ARCHIVE_INTERVAL_SECONDS = 300
SESSION_ARCHIVE_GRACE_SECONDS = 600
SESSION_ARCHIVE_DRAIN_TIMEOUT_SECONDS = 120


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
    """Run archive jobs serially, reporting failures before continuing."""
    for archive_job in (
        archive_service.archive_super_chat_log,
        archive_service.archive_room_live_stats,
        archive_service.archive_attention,
        archive_whale_month,
    ):
        try:
            await asyncio.to_thread(archive_job, target_month)
        except Exception as exc:
            logging.error(
                "[archive] job failed; continuing job=%s error_type=%s",
                archive_job.__name__,
                type(exc).__name__,
            )
    try:
        await _archive_closed_sessions(target_month)
    except Exception as exc:
        logging.error(
            "[archive] job failed; continuing job=%s error_type=%s",
            archive_service.archive_live_session.__name__,
            type(exc).__name__,
        )
    log_pool_status("monthly_archive")


async def _archive_closed_sessions(target_month: str | None = None) -> None:
    cutoff = _now() - datetime.timedelta(seconds=SESSION_ARCHIVE_GRACE_SECONDS)
    try:
        session_ids = archive_service.closed_session_ids(cutoff)
    except SQLAlchemyError as exc:
        logging.error("[archive] 读取候选场次失败 error_type=%s", type(exc).__name__)
        return
    if not session_ids:
        return
    try:
        await asyncio.wait_for(
            asyncio.gather(runtime_state.GUARD_FANS_QUEUE.join(), runtime_state.ATTENTION_QUEUE.join()),
            timeout=SESSION_ARCHIVE_DRAIN_TIMEOUT_SECONDS,
        )
    except TimeoutError:
        logging.warning("[archive] 结束快照队列未排空，暂缓场次归档")
        return
    for room_id in tuple(runtime_state.DANMAKU_PENDING):
        monitoring_jobs.flush_pending_danmaku_for_room(room_id)
    if any(pending.sessions or pending.buckets for pending in runtime_state.DANMAKU_PENDING.values()):
        logging.warning("[archive] 场次弹幕仍待写入，暂缓场次归档")
        return
    try:
        await asyncio.to_thread(archive_service.archive_live_session, target_month, end_time_before=cutoff, session_ids=session_ids)
    except Exception as exc:
        logging.error("[archive] 场次补归档失败 error_type=%s", type(exc).__name__)


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

        wake_at = min(target, now + datetime.timedelta(seconds=SESSION_ARCHIVE_INTERVAL_SECONDS))
        await _sleep_until(wake_at)
        if _now() < target:
            await _archive_closed_sessions()
            continue
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
    for room_id, session_id in tuple(runtime_state.CURRENT_SESSIONS.items()):
        monitoring_jobs.flush_pending_danmaku_for_room(room_id, session_id, end_time)
        flush_session(session_id, end_time)


async def pool_status_scheduler() -> None:
    while True:
        log_pool_status("periodic", level=logging.INFO)
        await asyncio.sleep(POOL_STATUS_INTERVAL_SECONDS)


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
            pool_status_scheduler(),
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
