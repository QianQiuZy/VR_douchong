"""Room configuration persistence backed by canonical runtime state."""

import json
import logging
import os

from . import runtime_state


def load_rooms_config() -> None:
    if not os.path.exists(runtime_state.ROOMS_JSON_PATH):
        logging.warning("[rooms] 未找到房间配置文件: %s", runtime_state.ROOMS_JSON_PATH)
        return
    try:
        with open(runtime_state.ROOMS_JSON_PATH, "r", encoding="utf-8") as handle:
            payload = json.load(handle) or {}
    except (json.JSONDecodeError, OSError, UnicodeDecodeError) as exc:
        logging.error("[rooms] 读取配置失败: %s", exc)
        return

    room_ids = payload.get("room_ids", [])
    room_anchors = payload.get("room_anchors", {})
    if not isinstance(room_ids, list):
        logging.error("[rooms] room_ids 必须是数组")
        return
    if not isinstance(room_anchors, dict):
        logging.error("[rooms] room_anchors 必须是对象")
        return

    normalized_ids: list[int] = []
    normalized_anchors: dict[int, str] = {}
    for room_id in room_ids:
        try:
            normalized_ids.append(int(room_id))
        except (TypeError, ValueError):
            continue
    for room_id, anchor_name in room_anchors.items():
        try:
            normalized_anchors[int(room_id)] = "" if anchor_name is None else str(anchor_name)
        except (TypeError, ValueError):
            continue
    for room_id in normalized_ids:
        normalized_anchors.setdefault(room_id, "")

    with runtime_state.ROOM_CONFIG_LOCK:
        runtime_state.ROOM_IDS.clear()
        runtime_state.ROOM_IDS.extend(sorted(set(normalized_ids)))
        runtime_state.ROOM_ANCHORS.clear()
        runtime_state.ROOM_ANCHORS.update(normalized_anchors)


def save_rooms_config() -> None:
    with runtime_state.ROOM_CONFIG_LOCK:
        payload = {
            "room_ids": sorted(set(runtime_state.ROOM_IDS)),
            "room_anchors": {str(room_id): name for room_id, name in runtime_state.ROOM_ANCHORS.items()},
        }
    try:
        with open(runtime_state.ROOMS_JSON_PATH, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
    except (OSError, TypeError, ValueError) as exc:
        logging.error("[rooms] 保存配置失败: %s", exc)


def get_room_ids() -> list[int]:
    with runtime_state.ROOM_CONFIG_LOCK:
        return list(runtime_state.ROOM_IDS)


def get_room_anchors() -> dict[int, str]:
    with runtime_state.ROOM_CONFIG_LOCK:
        return dict(runtime_state.ROOM_ANCHORS)


def get_room_anchor_name(room_id: int) -> str:
    with runtime_state.ROOM_CONFIG_LOCK:
        return runtime_state.ROOM_ANCHORS.get(room_id, "")


def add_room(room_id: int, anchor_name: str) -> bool:
    with runtime_state.ROOM_CONFIG_LOCK:
        if room_id in runtime_state.ROOM_IDS:
            return False
        runtime_state.ROOM_IDS.append(room_id)
        runtime_state.ROOM_IDS.sort()
        runtime_state.ROOM_ANCHORS[room_id] = anchor_name or ""
    save_rooms_config()
    return True


def delete_room(room_id: int) -> bool:
    with runtime_state.ROOM_CONFIG_LOCK:
        if room_id not in runtime_state.ROOM_IDS:
            return False
        runtime_state.ROOM_IDS.remove(room_id)
        runtime_state.ROOM_ANCHORS.pop(room_id, None)
    save_rooms_config()
    return True
