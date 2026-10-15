"""Offline checks for repository metadata, Python syntax and JSON configuration."""

import ast
import json
import platform
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def check_project() -> None:
    metadata = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    main_module = ast.parse((ROOT / "main.py").read_bytes())
    versions = [
        ast.literal_eval(node.value)
        for node in main_module.body
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "__version__" for target in node.targets)
    ]
    if len(versions) != 1 or not isinstance(versions[0], str) or not versions[0]:
        raise ValueError("main.py must define one literal __version__ string")
    if metadata["project"]["version"] != versions[0]:
        raise ValueError("pyproject.toml version must match main.py __version__")
    expected_python = (ROOT / ".python-version").read_text(encoding="utf-8").strip()
    if platform.python_version() != expected_python:
        raise ValueError(f"Expected Python {expected_python}, got {platform.python_version()}")
    if sys.prefix == sys.base_prefix:
        raise ValueError("Checks must run inside a project virtual environment")
    config = Path(sys.prefix) / "pyvenv.cfg"
    if "include-system-site-packages = true" in config.read_text(encoding="utf-8").lower():
        raise ValueError("The project environment must not inherit global packages")
    if not metadata["project"]["dependencies"] or not metadata["dependency-groups"]["dev"]:
        raise ValueError("Runtime and development dependencies must be declared")

    paths = [ROOT / "main.py"]
    for directory in ("app", "tests", "scripts"):
        paths.extend((ROOT / directory).rglob("*.py"))
    for path in paths:
        compile(path.read_bytes(), str(path.relative_to(ROOT)), "exec")

    for name in ("rooms.json", "psp_rooms.json"):
        path = ROOT / "config" / name
        data = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict) or set(data) != {"room_ids", "room_anchors"}:
            raise ValueError(f"{path.name}: expected room_ids and room_anchors")
        if not isinstance(data["room_ids"], list) or not isinstance(data["room_anchors"], dict):
            raise ValueError(f"{path.name}: invalid room collection types")
        if any(type(room_id) is not int or room_id <= 0 for room_id in data["room_ids"]):
            raise ValueError(f"{path.name}: invalid room ID")
    users = json.loads((ROOT / "config" / "entry_users.json").read_text(encoding="utf-8"))
    if not isinstance(users, list):
        raise ValueError("entry_users.json: expected a list")
    seen = set()
    for user in users:
        if not isinstance(user, dict):
            raise ValueError("entry_users.json: expected user objects")
        uid, name = user.get("uid"), user.get("name")
        if type(uid) is not int or uid <= 0 or uid in seen:
            raise ValueError("entry_users.json: invalid or duplicate UID")
        if not isinstance(name, str) or not name.strip():
            raise ValueError("entry_users.json: empty or invalid name")
        seen.add(uid)
    if list(ROOT.glob("*.json")):
        raise ValueError("Business JSON files must live in config/")
    if not (ROOT / "app" / "blivedm" / "LICENSE").is_file():
        raise ValueError("The embedded blivedm MIT license must be retained")
    print(f"Project metadata, {len(paths)} Python files and JSON configuration passed")


if __name__ == "__main__":
    check_project()
