import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from app import config, runtime_state

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_default_config_paths_are_anchored_to_repository_root() -> None:
    assert config.DEFAULT_ENV_FILE == REPO_ROOT / ".env"
    assert config.DEFAULT_CONFIG_DIR == REPO_ROOT / "config"
    assert runtime_state.DEFAULT_ROOMS_JSON_PATH == REPO_ROOT / "config" / "rooms.json"


@pytest.mark.parametrize(
    ("variable", "filename"),
    [("ROOMS_JSON_PATH", "rooms.json"), ("ENTRY_USERS_JSON_PATH", "entry_users.json")],
)
@pytest.mark.parametrize("setting", [None, "", "relative", "absolute"])
def test_deployed_json_paths_follow_env_file_instead_of_cwd(tmp_path, variable, filename, setting):
    deployment = tmp_path / "deployment"
    deployment.mkdir()
    env_file = deployment / "service.env"
    env_file.write_text("", encoding="utf-8")
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    env = {**os.environ, "ENV_FILE": str(env_file), "PYTHONPATH": str(REPO_ROOT)}
    env.pop("ROOMS_JSON_PATH", None)
    env.pop("ENTRY_USERS_JSON_PATH", None)
    if setting == "relative":
        env[variable] = f"custom/{filename}"
        expected = deployment / "custom" / filename
    elif setting == "absolute":
        expected = tmp_path / "absolute" / filename
        env[variable] = str(expected)
    else:
        expected = deployment / "config" / filename
        if setting == "":
            env[variable] = ""
    script = (
        "import json; from app import config; "
        f"print(json.dumps(str(config.{variable})))"
    )
    result = subprocess.run(
        [sys.executable, "-c", script], cwd=elsewhere, env=env,
        capture_output=True, text=True, check=True,
    )
    assert json.loads(result.stdout) == str(expected)
