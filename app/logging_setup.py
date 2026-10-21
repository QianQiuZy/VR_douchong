"""Route API logs to a size-rotated file; leave collector output to BaoTa."""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
from logging.handlers import BaseRotatingHandler
from pathlib import Path

SHANGHAI = timezone(timedelta(hours=8), name="Asia/Shanghai")
API_LOG_MAX_BYTES = 50 * 1024 * 1024
_api_context: ContextVar[str | None] = ContextVar("api_log_context", default=None)


@contextmanager
def api_log_context(request_id: str | None = "-"):
    """Follow request work; None clears the context for new collector tasks."""
    token = _api_context.set(request_id)
    try:
        yield
    finally:
        _api_context.reset(token)


def _is_api(record: logging.LogRecord) -> bool:
    return (
        _api_context.get() is not None
        or record.name == "uvicorn"
        or record.name.startswith(("uvicorn.", "app.api_"))
    )


class ApiLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.request_id = getattr(record, "request_id", _api_context.get() or "-")
        return _is_api(record)


class CollectorLogFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return not _is_api(record)


class ApiLogFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        return datetime.fromtimestamp(record.created, SHANGHAI).isoformat(timespec="milliseconds")


class ApiLogHandler(BaseRotatingHandler):
    """One process/handler owns api.log; archives are retained without deletion."""

    def __init__(self, path: Path, project_name: str, max_bytes: int = API_LOG_MAX_BYTES):
        self.project_name = project_name
        self.max_bytes = max_bytes
        path.parent.mkdir(parents=True, exist_ok=True)
        (path.parent / "api-history_logs").mkdir(exist_ok=True)
        # Open at explicit startup so permission/path errors fail visibly then.
        super().__init__(path, "a", encoding="utf-8", errors="backslashreplace")

    def shouldRollover(self, record):
        if self.stream is None:
            self.stream = self._open()
        size = os.fstat(self.stream.fileno()).st_size
        pending = len((self.format(record) + self.terminator).encode("utf-8", "backslashreplace"))
        # Do not split individual records, including an exceptionally oversized one.
        return size > 0 and size + pending >= self.max_bytes

    def doRollover(self):
        if self.stream is not None:
            self.stream.close()
            self.stream = None
        source = Path(self.baseFilename)
        history = source.parent / "api-history_logs"
        history.mkdir(exist_ok=True)
        stamp = datetime.now(SHANGHAI).strftime("%Y-%m-%d_%H%M%S")
        prefix = f"{self.project_name}_{stamp}"
        target = history / f"{prefix}_log.log"
        collision = 0
        while target.exists():
            collision += 1
            target = history / f"{prefix}_{collision:03d}_log.log"
        source.rename(target)
        self.stream = self._open()


def configure_logging() -> ApiLogHandler:
    """Called once by the launcher, never as an import-time file side effect."""
    from . import config

    root = logging.getLogger()
    path = config.API_LOG_PATH.resolve()
    for handler in root.handlers:
        if isinstance(handler, ApiLogHandler) and Path(handler.baseFilename) == path:
            return handler

    api_handler = ApiLogHandler(path, config.API_LOG_PROJECT_NAME)
    api_handler.addFilter(ApiLogFilter())
    api_handler.setFormatter(ApiLogFormatter(
        "%(asctime)s - %(levelname)s - %(name)s - request_id=%(request_id)s - %(message)s"
    ))
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    root.setLevel(logging.INFO)
    for handler in root.handlers[:]:
        if isinstance(handler, ApiLogHandler):
            root.removeHandler(handler)
            handler.close()
        elif not any(isinstance(f, CollectorLogFilter) for f in handler.filters):
            handler.addFilter(CollectorLogFilter())
    root.addHandler(api_handler)

    # Uvicorn must use this routing instead of installing its default streams.
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "uvicorn.asgi"):
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.propagate = True
        logger.setLevel(logging.INFO)
    return api_handler
