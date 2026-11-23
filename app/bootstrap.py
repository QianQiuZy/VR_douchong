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
  thread, and then run the collector with graceful signal handling.

The archive scheduler (:func:`monthly_reset_scheduler`) lives here because
it belongs to the runtime orchestration lane, invoking
:mod:`archive_service` at the appropriate times.
"""

from __future__ import annotations

import asyncio
import datetime
import logging
import signal
import threading

from sqlalchemy.exc import SQLAlchemyError

from . import api_app, archive_service, event_ingestion, monitoring_jobs, runtime_state
from . import danmaku_metrics
from .danmaku_archive import archive_danmaku_metrics
from .config import APP_HOST, APP_PORT
from .database import create_schema, ensure_runtime_schema, log_pool_status
from .logging_setup import configure_logging
from .metrics_runtime import checkpoint_session, retry_failed_buckets
from .models import RoomInfo
from .whale_archive import archive_whale_month

logger = logging.getLogger(__name__)

POOL_STATUS_INTERVAL_SECONDS = 300
SESSION_ARCHIVE_INTERVAL_SECONDS = 300
SESSION_ARCHIVE_GRACE_SECONDS = 600
SESSION_ARCHIVE_DRAIN_TIMEOUT_SECONDS = 120


# ------------------ time helpers ------------------ #
def _now() -> datetime.datetime:
    return datetime.datetime.now().astimezone().replace(tzinfo=None)


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
async def _archive_month(target_month: str | None = None) -> None:
    """Run archive jobs serially, reporting failures before continuing."""
    for archive_job in (
        archive_service.archive_super_chat_log,
        archive_service.archive_room_live_stats,
        archive_service.archive_attention,
        archive_service.archive_room_entry_log,
        archive_whale_month,
        archive_danmaku_metrics,
    ):
        try:
            await asyncio.to_thread(archive_job, target_month)
        except Exception as exc:
            logger.exception(
                "[archive] job failed; continuing job=%s error_type=%s",
                archive_job.__name__,
                type(exc).__name__,
                exc_info=False,
            )
    try:
        await _archive_closed_sessions(target_month)
    except Exception as exc:
        logger.exception(
            "[archive] job failed; continuing job=%s error_type=%s",
            archive_service.archive_live_session.__name__,
            type(exc).__name__,
            exc_info=False,
        )
    log_pool_status("monthly_archive")


async def _archive_closed_sessions(target_month: str | None = None) -> None:
    cutoff = _now() - datetime.timedelta(seconds=SESSION_ARCHIVE_GRACE_SECONDS)
    try:
        session_ids = archive_service.closed_session_ids(cutoff)
    except SQLAlchemyError as exc:
        logger.error("[archive] 读取候选场次失败 error_type=%s", type(exc).__name__)
        return
    if not session_ids:
        return
    try:
        await asyncio.wait_for(
            asyncio.gather(
                runtime_state.GUARD_FANS_QUEUE.join(), runtime_state.ATTENTION_QUEUE.join(),
                *(asyncio.shield(snapshot.task)
                  for session_id in session_ids
                  if (snapshot := runtime_state.SESSION_END_SNAPSHOTS.get(session_id)) is not None
                  and snapshot.task is not None),
            ),
            timeout=SESSION_ARCHIVE_DRAIN_TIMEOUT_SECONDS,
        )
    except TimeoutError:
        logger.warning("[archive] 结束快照队列未排空，暂缓场次归档")
        return
    for room_id in tuple(runtime_state.DANMAKU_PENDING):
        monitoring_jobs.flush_pending_danmaku_for_room(room_id)
    if any(pending.sessions or pending.buckets for pending in runtime_state.DANMAKU_PENDING.values()):
        logger.warning("[archive] 场次弹幕仍待写入，暂缓场次归档")
        return
    try:
        await asyncio.to_thread(archive_service.archive_live_session, target_month, end_time_before=cutoff, session_ids=session_ids)
    except Exception as exc:
        logger.exception("[archive] 场次补归档失败 error_type=%s", type(exc).__name__, exc_info=False)


async def _retry_danmaku_settlement() -> None:
    try:
        await asyncio.to_thread(archive_danmaku_metrics, None)
    except Exception as exc:
        logger.warning("[archive] 弹幕结算待重试 error_type=%s", type(exc).__name__)


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
            await asyncio.to_thread(archive_service.archive_room_entry_log)
            await _retry_danmaku_settlement()
            await _archive_closed_sessions()
            continue
        previous_month = _month_str_now_at(target - datetime.timedelta(days=1))
        drift_seconds = max(0.0, (_now() - target).total_seconds())
        logger.info(
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
    for room_id in runtime_state.DANMAKU_PENDING.keys() | runtime_state.CURRENT_SESSIONS.keys():
        monitoring_jobs.flush_pending_danmaku_for_room(room_id, runtime_state.CURRENT_SESSIONS.get(room_id), end_time)
    for room_id, session_id in tuple(runtime_state.CURRENT_SESSIONS.items()):
        checkpoint_session(session_id, runtime_state.PENDING_SESSION_ENDS.get(room_id, end_time))
    retry_failed_buckets()


async def pool_status_scheduler() -> None:
    while True:
        log_pool_status("periodic", level=logging.INFO)
        await asyncio.sleep(POOL_STATUS_INTERVAL_SECONDS)


# ------------------ main coroutine ------------------ #
async def _run_clients_and_attention(ready: asyncio.Event) -> None:
    async with asyncio.TaskGroup() as tasks:
        tasks.create_task(monitoring_jobs.run_clients_loop())
        await ready.wait()
        tasks.create_task(monitoring_jobs.attention_worker())
        tasks.create_task(monitoring_jobs.daily_attention_worker())
        await monitoring_jobs.refresh_attention_scheduler()


async def main() -> None:
    """Top-level runtime coroutine.

    Assigns ``runtime_state.MAIN_LOOP`` for the FastAPI thread bridge and
    starts the collector workers.
    """
    runtime_state.MAIN_LOOP = asyncio.get_running_loop()
    attention_ready = asyncio.Event()
    runtime_state.STARTUP_ATTENTION_READY = attention_ready
    danmaku_metrics.start_tracking()
    init_room_info()
    init_session()
    monitoring_jobs.load_cached_room_uids()
    await asyncio.to_thread(event_ingestion.entry_monitor.reload)
    entry_worker = asyncio.create_task(event_ingestion.entry_monitor.worker(), name="entry-writer")
    connections = asyncio.create_task(_run_clients_and_attention(attention_ready), name="startup-connections")

    try:
        await asyncio.gather(
            connections,
            monthly_reset_scheduler(),
            monitoring_jobs.reconnect_scheduler(),  # 每日 6:00 全量重连
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
            event_ingestion.entry_monitor.reload_scheduler(),
            entry_worker,
        )
    finally:
        # gather does not cancel siblings when one job raises. Stop the writer
        # before the final drain so the same mutable batch cannot be saved twice.
        entry_worker.cancel()
        connections.cancel()
        await asyncio.gather(entry_worker, connections, return_exceptions=True)
        await asyncio.gather(*(client.stop_and_close() for client in tuple(runtime_state.ROOM_CLIENTS.values())),
                             return_exceptions=True)
        await event_ingestion.entry_monitor.flush()
        snapshot_tasks = [snapshot.task for snapshot in runtime_state.SESSION_END_SNAPSHOTS.values()
                          if snapshot.task is not None]
        for task in snapshot_tasks:
            task.cancel()
        await asyncio.gather(*snapshot_tasks, return_exceptions=True)
        runtime_state.SESSION_END_SNAPSHOTS.clear()
        _flush_active_metrics(_now())
        if runtime_state.aiohttp_session:
            await runtime_state.aiohttp_session.close()
        runtime_state.STARTUP_ATTENTION_READY = None


# ------------------ API server thread ------------------ #
def _run_api_server() -> None:
    import uvicorn

    from .api_request_logging import RequestLoggingMiddleware

    config = uvicorn.Config(RequestLoggingMiddleware(api_app.app), host=APP_HOST, port=APP_PORT,
                            log_level="info", log_config=None, access_log=False,
                            forwarded_allow_ips="127.0.0.1,::1")
    server = uvicorn.Server(config)
    server.run()


# ------------------ launcher entry ------------------ #
async def _run_collector() -> None:
    # systemd and container restarts send SIGTERM. Route it through the same
    # cancellation/drain path as SIGINT on Unix.
    loop = asyncio.get_running_loop()
    task = asyncio.create_task(main())
    stopping = False

    def terminate() -> None:
        nonlocal stopping
        if not stopping:
            stopping = True
            task.cancel()

    try:
        loop.add_signal_handler(signal.SIGTERM, terminate)
    except NotImplementedError:  # Windows event loops do not support Unix signals.
        await task
        return
    try:
        await task
    except asyncio.CancelledError:
        if not stopping or asyncio.current_task().cancelling():
            raise
    finally:
        loop.remove_signal_handler(signal.SIGTERM)


def run() -> None:
    """Reproduce the pre-extraction ``__main__`` block launcher order."""
    configure_logging()
    create_schema()
    ensure_runtime_schema()
    threading.Thread(target=_run_api_server, daemon=True).start()
    asyncio.run(_run_collector())
