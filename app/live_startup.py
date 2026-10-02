import logging

import aiohttp
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class LiveStatusUnavailable(Exception):
    pass


class _RoomStatus(BaseModel):
    live_status: int


class _StatusResponse(BaseModel):
    code: int = 0
    data: dict[str, _RoomStatus] | None = None


async def fetch_live_room_ids() -> set[int]:
    """Read an ordering-only snapshot without consuming the 0-to-1 lifecycle transition."""
    from . import bilibili_gateway, room_config, runtime_state

    room_ids = room_config.get_room_ids()
    if not room_ids:
        return set()
    session = runtime_state.aiohttp_session
    if session is None or any(room_id not in runtime_state.ROOM_UIDS for room_id in room_ids):
        raise LiveStatusUnavailable
    uids = {room_id: runtime_state.ROOM_UIDS[room_id] for room_id in room_ids}
    uid_values = list(dict.fromkeys(uids.values()))
    live_uids: set[int] = set()
    for offset in range(0, len(uid_values), 100):
        batch = uid_values[offset:offset + 100]
        async with session.get(
            bilibili_gateway.LIVE_STATUS_API,
            params=[("uids[]", str(uid)) for uid in batch],
            timeout=aiohttp.ClientTimeout(total=10),
            headers={"User-Agent": bilibili_gateway.USER_AGENT, "Referer": "https://live.bilibili.com"},
        ) as response:
            response.raise_for_status()
            snapshot = _StatusResponse.model_validate(await response.json(content_type=None))
        if snapshot.code != 0 or snapshot.data is None:
            raise LiveStatusUnavailable
        live_uids.update(
            uid for uid in batch
            if str(uid) in snapshot.data and snapshot.data[str(uid)].live_status == 1
        )
    live_rooms = {room_id for room_id, uid in uids.items() if uid in live_uids}
    logger.info("[connect] 启动直播状态快照 live=%s other=%s", len(live_rooms), len(room_ids) - len(live_rooms))
    return live_rooms
