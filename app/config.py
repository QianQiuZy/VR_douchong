"""Environment-backed settings shared by the application bootstrap."""

import logging
import math
import os
from pathlib import Path

logger = logging.getLogger(__name__)

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
        logger.exception(f"[env] 加载 .env 失败: {type(exc).__name__}", exc_info=False)


def get_env_int(name: str, default: int, minimum: int = 1) -> int:
    """Read an integer setting, retaining the launcher's fallback behavior."""
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        logger.warning(f"[env] {name} 不是有效整数，使用默认值 {default}")
        return default
    if value < minimum:
        logger.warning(f"[env] {name} 超出允许范围，使用默认值 {default}")
        return default
    return value


_env_file_override = os.getenv("ENV_FILE")
load_env_file(_env_file_override if _env_file_override is not None else DEFAULT_ENV_FILE)

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "111"),
    "password": os.getenv("DB_PASSWORD", "111"),
    "db": os.getenv("DB_NAME", "111"),
    "port": get_env_int("DB_PORT", 3306),
}
DB_POOL_SIZE = get_env_int("DB_POOL_SIZE", 5)
DB_POOL_MAX_OVERFLOW = get_env_int("DB_POOL_MAX_OVERFLOW", 10, minimum=0)
DB_POOL_TIMEOUT = get_env_int("DB_POOL_TIMEOUT", 30)
DB_POOL_RECYCLE = get_env_int("DB_POOL_RECYCLE", 1800)
REPORT_MAX_CONCURRENCY = get_env_int("REPORT_MAX_CONCURRENCY", 2)


def get_env_float(name: str, default: float, minimum: float = 0.0) -> float:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        value = float(raw)
    except ValueError:
        logger.warning(f"[env] {name} 不是有效数值，使用默认值 {default}")
        return default
    if not math.isfinite(value) or value <= minimum:
        logger.warning(f"[env] {name} 超出允许范围，使用默认值 {default}")
        return default
    return value


REPORT_ACQUIRE_TIMEOUT_SECONDS = get_env_float("REPORT_ACQUIRE_TIMEOUT_SECONDS", 0.1)

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
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
REDIS_KEY_TTL_SECONDS = get_env_int("REDIS_KEY_TTL_SECONDS", 90 * 24 * 60 * 60)

# API reads fail closed by default; disabling this is an offline diagnostic mode.
API_CACHE_ENABLED = os.getenv("API_CACHE_ENABLED", "1").lower() not in {"0", "false", "no"}
API_CACHE_PREFIX = os.getenv("API_CACHE_PREFIX", "vr:api:v1").strip() or "vr:api:v1"
API_CACHE_REFRESH_SECONDS = get_env_float("API_CACHE_REFRESH_SECONDS", 10.0)
API_CACHE_MAX_AGE_SECONDS = min(get_env_float("API_CACHE_MAX_AGE_SECONDS", 30.0), 30.0)
API_CACHE_HISTORY_SECONDS = get_env_float("API_CACHE_HISTORY_SECONDS", 300.0)
API_CACHE_MAX_BYTES = get_env_int("API_CACHE_MAX_BYTES", 96 * 1024 * 1024)
API_CACHE_SQL_PER_SECOND = min(get_env_int("API_CACHE_SQL_PER_SECOND", 9), 9)
API_RATE_LIMIT_PER_IP = min(get_env_int("API_RATE_LIMIT_PER_IP", 20), 20)
API_CACHE_MAX_INFLIGHT = get_env_int("API_CACHE_MAX_INFLIGHT", 32)
