"""Local UID cache beside the configured room list."""

import json
import logging
from pathlib import Path
from tempfile import NamedTemporaryFile

from . import runtime_state

logger = logging.getLogger(__name__)


def _path() -> Path:
    rooms_path = Path(runtime_state.ROOMS_JSON_PATH)
    return rooms_path.with_name(f"{rooms_path.stem}.uids.json")


def _read(path: Path) -> dict[int, int]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}
    except ValueError:
        logger.warning("[RoomUID] 缓存无效，将重新查询: %s", path)
        return {}
    if not isinstance(payload, dict):
        logger.warning("[RoomUID] 缓存必须是 JSON 对象: %s", path)
        return {}

    uids = {}
    for key, uid in payload.items():
        try:
            room_id = int(key)
        except ValueError:
            continue
        if room_id > 0 and str(room_id) == key and type(uid) is int and uid > 0:
            uids[room_id] = uid
    return uids


def load() -> dict[int, int]:
    path = _path()
    try:
        return _read(path)
    except OSError as exc:
        logger.warning("[RoomUID] 无法读取缓存 %s: %s", path, type(exc).__name__)
        return {}


def remember(room_id: int, uid: int) -> None:
    """Merge a confirmed UID and replace the cache in one filesystem operation."""
    path = _path()
    temporary_path = None
    try:
        uids = _read(path)
        if uids.get(room_id) == uid:
            return
        uids[room_id] = uid
        with NamedTemporaryFile(
            mode="w", encoding="utf-8", dir=path.parent,
            prefix=f".{path.name}.", suffix=".tmp", delete=False,
        ) as handle:
            temporary_path = Path(handle.name)
            json.dump(dict(sorted(uids.items())), handle, indent=2)
            handle.write("\n")
        temporary_path.replace(path)
    except OSError as exc:
        logger.warning("[RoomUID] 无法保存缓存 %s: %s", path, type(exc).__name__)
    finally:
        if temporary_path is not None:
            try:
                temporary_path.unlink(missing_ok=True)
            except OSError as exc:
                logger.warning("[RoomUID] 无法清理临时文件 %s: %s", temporary_path, type(exc).__name__)
