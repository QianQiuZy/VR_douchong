"""Environment-backed settings shared by the application bootstrap."""

import logging
import os
from pathlib import Path


DEFAULT_ENV_FILE = Path(__file__).resolve().parents[1] / ".env"


def load_env_file(env_path: str | Path = DEFAULT_ENV_FILE) -> None:
    """Load missing environment variables from the selected env file."""
    if not os.path.exists(env_path):
        return
    try:
        with open(env_path, "r", encoding="utf-8") as handle:
            for raw_line in handle:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip("'").strip('"')
                if key and key not in os.environ:
                    os.environ[key] = value
    except Exception as exc:
        logging.error(f"[env] 加载 .env 失败: {exc}")


def get_env_int(name: str, default: int) -> int:
    """Read an integer setting, retaining the launcher's fallback behavior."""
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        logging.warning(f"[env] {name} 不是有效整数，使用默认值 {default}")
        return default


_env_file_override = os.getenv("ENV_FILE")
load_env_file(_env_file_override if _env_file_override is not None else DEFAULT_ENV_FILE)

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "111"),
    "password": os.getenv("DB_PASSWORD", "111"),
    "db": os.getenv("DB_NAME", "111"),
    "port": get_env_int("DB_PORT", 3306),
}

SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
EMAIL_FROM = os.getenv("EMAIL_FROM", "")
EMAIL_TO = os.getenv("EMAIL_TO", "")
APP_HOST = os.getenv("APP_HOST", "0.0.0.0")
APP_PORT = get_env_int("APP_PORT", 4666)
API_SECRET = os.getenv("API_SECRET", "").strip()
ATTENTION_DAILY_ROOM_SLEEP_SECONDS = float(
    os.getenv("ATTENTION_DAILY_ROOM_SLEEP_SECONDS", "1")
)
