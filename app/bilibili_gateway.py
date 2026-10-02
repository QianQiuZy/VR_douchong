"""Bilibili HTTP session, ticket, and API gateway operations."""

import datetime
import hashlib
import hmac
import http.cookies
import logging
import os
import time

import aiohttp
from aiohttp import ContentTypeError

from . import runtime_state
from .models import RoomInfo

logger = logging.getLogger(__name__)

SESSDATA_VALUE = os.getenv("SESSDATA_VALUE", "")
BILI_JCT_VALUE = os.getenv("BILI_JCT_VALUE", "")
DEDEUSERID_VALUE = os.getenv("DEDEUSERID_VALUE", "")
DEDEUSERID_CKMD5_VALUE = os.getenv("DEDEUSERID_CKMD5_VALUE", "")
SID_VALUE = os.getenv("SID_VALUE", "")
BUVID3_VALUE = os.getenv("BUVID3_VALUE", "")
DEVICE_FP_VALUE = os.getenv("DEVICE_FP_VALUE", "")

BILI_COOKIES_BASE = {
    "SESSDATA": SESSDATA_VALUE,
    "bili_jct": BILI_JCT_VALUE,
    "DedeUserID": DEDEUSERID_VALUE,
    "DedeUserID__ckMd5": DEDEUSERID_CKMD5_VALUE,
    "sid": SID_VALUE,
    "buvid3": BUVID3_VALUE,
    "deviceFingerprint": DEVICE_FP_VALUE,
}
BILI_TICKET: str | None = None
BILI_TICKET_EXPIRES: int | None = None
BILI_TICKET_KEY = "XgwSnGZ1p"
BILI_TICKET_URL = "https://api.bilibili.com/bapis/bilibili.api.ticket.v1.Ticket/GenWebTicket"
BILI_TICKET_KEY_ID = "ec02"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)
LIVE_STATUS_API = "https://api.live.bilibili.com/room/v1/Room/get_status_info_by_uids"
ROOM_INFO_API = "https://api.live.bilibili.com/room/v1/Room/get_info"
FANS_API = "https://api.live.bilibili.com/xlive/general-interface/v1/rank/getFansMembersRank"
GUARD_API = "https://api.live.bilibili.com/xlive/general-interface/v1/guard/GuardActive"
CONTRIBUTION_RANK_API = "https://api.live.bilibili.com/xlive/general-interface/v1/rank/queryContributionRank"


def init_session() -> None:
    """Create the shared Bilibili HTTP session with the configured cookies."""
    cookies = http.cookies.SimpleCookie()
    for key, value in BILI_COOKIES_BASE.items():
        if not value:
            continue
        cookies[key] = value
        cookies[key]["domain"] = "bilibili.com"
    connector = aiohttp.TCPConnector(ssl=False)
    runtime_state.aiohttp_session = aiohttp.ClientSession(connector=connector)
    runtime_state.aiohttp_session.cookie_jar.update_cookies(cookies)
    logger.info("[session] 已初始化基础 Cookies：%s", ",".join(cookies.keys()))


async def ensure_bili_ticket(force: bool = False) -> str:
    """Return a usable Bilibili ticket, renewing and installing it when needed."""
    session = runtime_state.aiohttp_session
    if session is None:
        raise RuntimeError("aiohttp_session 未初始化，无法获取 bili_ticket")
    now_ts = int(time.time())
    if not force and BILI_TICKET and BILI_TICKET_EXPIRES and BILI_TICKET_EXPIRES - now_ts > 60:
        return BILI_TICKET
    csrf = BILI_COOKIES_BASE.get("bili_jct", "") or ""
    if not csrf:
        logger.warning("[bili_ticket] bili_jct 为空，可能导致 GenWebTicket 调用失败")
    hexsign = hmac.new(BILI_TICKET_KEY.encode("utf-8"), f"ts{now_ts}".encode(), hashlib.sha256).hexdigest()
    try:
        async with session.post(
            BILI_TICKET_URL,
            params={"key_id": BILI_TICKET_KEY_ID, "hexsign": hexsign, "context[ts]": str(now_ts), "csrf": csrf},
            headers={"User-Agent": USER_AGENT},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as response:
            try:
                payload = await response.json(content_type=None)
            except ContentTypeError:
                text = (await response.text())[:200]
                raise RuntimeError(f"获取 bili_ticket 返回非 JSON，前 200 字：{text}")
    except Exception as exc:
        raise RuntimeError(f"请求 bili_ticket 接口异常: {exc}") from exc
    if payload.get("code") != 0 or "data" not in payload:
        raise RuntimeError(f"获取 bili_ticket 失败: {payload}")
    data = payload["data"] or {}
    ticket = data.get("ticket")
    if not ticket:
        raise RuntimeError(f"获取 bili_ticket 失败，未包含 ticket 字段: {payload}")
    expires_ts = int(data.get("created_at", now_ts)) + int(data.get("ttl", 0))
    globals()["BILI_TICKET"] = ticket
    globals()["BILI_TICKET_EXPIRES"] = expires_ts
    cookies = http.cookies.SimpleCookie()
    cookies["bili_ticket"] = ticket
    cookies["bili_ticket"]["domain"] = "bilibili.com"
    session.cookie_jar.update_cookies(cookies)
    logger.info("[bili_ticket] 刷新成功，过期时间=%s (%d)", datetime.datetime.fromtimestamp(expires_ts).astimezone().replace(tzinfo=None).strftime("%Y-%m-%d %H:%M:%S"), expires_ts)
    return ticket


async def fetch_room_info_and_update(room_id: int, update_uid: bool) -> bool:
    """Fetch room info and persist attention, optionally refreshing its UID."""
    session = runtime_state.aiohttp_session
    if session is None:
        logger.error("[RoomInfo] aiohttp_session 未初始化")
        return False
    try:
        async with session.get(f"{ROOM_INFO_API}?room_id={room_id}", timeout=aiohttp.ClientTimeout(total=5), headers={"User-Agent": USER_AGENT, "Referer": "https://live.bilibili.com"}) as response:
            if response.status != 200:
                logger.warning("[RoomInfo] 房间 %s get_info HTTP %s", room_id, response.status)
                return False
            try:
                payload = await response.json(content_type=None)
            except ContentTypeError:
                logger.warning("[RoomInfo] 房间 %s get_info 返回非 JSON，前 200 字：%s", room_id, (await response.text())[:200])
                return False
    except Exception as exc:
        logger.exception("[RoomInfo] 房间 %s 请求异常: %s", room_id, type(exc).__name__, exc_info=False)
        return False
    data = payload.get("data") or {}
    try:
        attention = int(data.get("attention", 0))
    except (TypeError, ValueError):
        attention = 0
    RoomInfo.upsert(room_id, attention=attention)
    if update_uid:
        uid_raw = data.get("uid")
        try:
            uid = int(str(uid_raw))
        except (TypeError, ValueError):
            uid = 0
        if uid:
            runtime_state.ROOM_UIDS[room_id] = uid
            logger.info("[RoomInfo] room_id=%s uid=%s attention=%s", room_id, uid, attention)
        else:
            logger.warning("[RoomInfo] room_id=%s uid 获取失败，原始值=%r", room_id, uid_raw)
    else:
        logger.debug("[RoomInfo] room_id=%s 刷新 attention=%s（不更新 uid）", room_id, attention)
    return True


async def fetch_guard_counts(uid: int, room_id: int) -> tuple[int, int, int] | None:
    """Fetch captain, admiral, and governor totals in the existing API order."""
    session = runtime_state.aiohttp_session
    if session is None:
        logger.error("[Guard] aiohttp_session 未初始化")
        return None
    try:
        async with session.get(GUARD_API, params={"ruid": str(uid), "platform": "pc"}, timeout=aiohttp.ClientTimeout(total=10), headers={"User-Agent": USER_AGENT, "Referer": "https://live.bilibili.com"}) as response:
            if response.status != 200:
                logger.warning("[Guard] room_id=%s HTTP %s", room_id, response.status)
                return None
            try:
                payload = await response.json(content_type=None)
            except ContentTypeError:
                logger.warning("[Guard] room_id=%s 返回非 JSON，前 200 字：%s", room_id, (await response.text())[:200])
                return None
    except Exception as exc:
        logger.exception("[Guard] room_id=%s 请求异常: %s", room_id, type(exc).__name__, exc_info=False)
        return None
    data = payload.get("data") or {}
    def as_int(value):
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0
    counts = (as_int(data.get("guard_num_3", 0)), as_int(data.get("guard_num_2", 0)), as_int(data.get("guard_num_1", 0)))
    logger.info("[Guard] room_id=%s guard_1(舰长)=%s guard_2(提督)=%s guard_3(总督)=%s", room_id, *counts)
    return counts


async def fetch_fans_count(uid: int, room_id: int) -> int | None:
    """Fetch the fan-club count."""
    session = runtime_state.aiohttp_session
    if session is None:
        logger.error("[Fans] aiohttp_session 未初始化")
        return None
    try:
        async with session.get(FANS_API, params={"ruid": str(uid), "page_size": "1", "page": "1"}, timeout=aiohttp.ClientTimeout(total=10), headers={"User-Agent": USER_AGENT, "Referer": "https://live.bilibili.com"}) as response:
            if response.status != 200:
                logger.warning("[Fans] room_id=%s HTTP %s", room_id, response.status)
                return None
            try:
                payload = await response.json(content_type=None)
            except ContentTypeError:
                logger.warning("[Fans] room_id=%s 返回非 JSON，前 200 字：%s", room_id, (await response.text())[:200])
                return None
    except Exception as exc:
        logger.exception("[Fans] room_id=%s 请求异常: %s", room_id, type(exc).__name__, exc_info=False)
        return None
    try:
        count = int((payload.get("data") or {}).get("num", 0))
    except (TypeError, ValueError):
        count = 0
    logger.info("[Fans] room_id=%s 粉丝团数量=%s", room_id, count)
    return count


async def fetch_contribution_count(uid: int, room_id: int) -> int | None:
    """Fetch the contribution-ranking count used as the concurrency sample."""
    session = runtime_state.aiohttp_session
    if session is None:
        logger.error("[Concurrency] aiohttp_session 未初始化")
        return None
    try:
        async with session.get(CONTRIBUTION_RANK_API, params={"ruid": str(uid), "room_id": str(room_id), "page": "1", "page_size": "1"}, timeout=aiohttp.ClientTimeout(total=10), headers={"User-Agent": USER_AGENT, "Referer": "https://live.bilibili.com"}) as response:
            if response.status != 200:
                logger.warning("[Concurrency] room_id=%s HTTP %s", room_id, response.status)
                return None
            try:
                payload = await response.json(content_type=None)
            except ContentTypeError:
                logger.warning("[Concurrency] room_id=%s 返回非 JSON，前 200 字：%s", room_id, (await response.text())[:200])
                return None
    except Exception as exc:
        logger.exception("[Concurrency] room_id=%s 请求异常: %s", room_id, type(exc).__name__, exc_info=False)
        return None
    if payload.get("code") != 0:
        logger.warning("[Concurrency] room_id=%s 接口返回异常: %s", room_id, payload)
        return None
    try:
        return int((payload.get("data") or {}).get("count", 0))
    except (TypeError, ValueError):
        return 0


_fetch_room_info_and_update = fetch_room_info_and_update
_fetch_guard_counts = fetch_guard_counts
_fetch_fans_count = fetch_fans_count
_fetch_contribution_count = fetch_contribution_count
