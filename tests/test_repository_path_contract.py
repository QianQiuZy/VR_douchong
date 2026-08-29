from pathlib import Path

from app import config, runtime_state


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_default_config_paths_are_anchored_to_repository_root() -> None:
    assert config.DEFAULT_ENV_FILE == REPO_ROOT / ".env"
    assert runtime_state.DEFAULT_ROOMS_JSON_PATH == REPO_ROOT / "rooms.json"
