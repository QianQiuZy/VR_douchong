"""Database engine, session factory, and explicit schema bootstrap."""

import logging

import pymysql
from pymysql.constants import COMMAND
from sqlalchemy import create_engine, event, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.pool import QueuePool

from .api_cache_store import api_sql_scope
from .config import (
    DB_CONFIG,
    DB_POOL_MAX_OVERFLOW,
    DB_POOL_RECYCLE,
    DB_POOL_SIZE,
    DB_POOL_TIMEOUT,
)

logger = logging.getLogger(__name__)

engine = create_engine(
    f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
    f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['db']}",
    echo=False,
    pool_size=DB_POOL_SIZE,
    max_overflow=DB_POOL_MAX_OVERFLOW,
    pool_timeout=DB_POOL_TIMEOUT,
    pool_recycle=DB_POOL_RECYCLE,
    pool_pre_ping=True,
)
Session = sessionmaker(bind=engine)
Base = declarative_base()


class ApiBudgetConnection(pymysql.connections.Connection):
    """Only HTTP management work shares the cache SQL budget; collectors do not."""

    def _execute_command(self, command, sql):
        scope = api_sql_scope.get()
        if command == COMMAND.COM_QUERY and scope is not None and scope.active:
            scope.store.take_sql(scope.stop, require_lease=False)
        return super()._execute_command(command, sql)


@event.listens_for(engine, "do_connect")
def _budget_capable_connection(_dialect, _record, args, params):
    return ApiBudgetConnection(*args, **params)


def log_pool_status(label: str, level: int = logging.WARNING) -> None:
    """Log safe QueuePool counters at an internal report or job boundary."""
    pool = engine.pool
    if not isinstance(pool, QueuePool):
        return
    logger.log(
        level,
        "[pool] label=%s size=%d checked_out=%d overflow=%d timeout=%d recycle=%d",
        label,
        pool.size(),
        pool.checkedout(),
        pool.overflow(),
        DB_POOL_TIMEOUT,
        DB_POOL_RECYCLE,
    )


def create_schema() -> None:
    """Create declared tables when an explicit launcher/bootstrap requests it."""
    Base.metadata.create_all(engine)


def ensure_runtime_schema() -> None:
    """Compatibly add current metrics columns to hot and old archive tables."""
    required_columns = {
        "live_session": {
            "duration_valid": "INT NOT NULL DEFAULT 1",
            "duration_ledger": "TEXT NULL",
            "start_attention": "INT NULL",
            "end_attention": "INT NULL",
            "payer_count": "INT NOT NULL DEFAULT 0",
            "captain_danmaku_count": "INT NULL DEFAULT NULL",
            "admiral_danmaku_count": "INT NULL DEFAULT NULL",
            "governor_danmaku_count": "INT NULL DEFAULT NULL",
            "normal_danmaku_count": "INT NULL DEFAULT NULL",
        },
        "live_session_15m_stats": {
            "room_id": "INT NOT NULL DEFAULT 0",
            "month": "VARCHAR(6) NOT NULL DEFAULT ''",
            "start_time": "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP",
            "end_time": "DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP",
            "gift": "FLOAT NOT NULL DEFAULT 0",
            "guard": "FLOAT NOT NULL DEFAULT 0",
            "super_chat": "FLOAT NOT NULL DEFAULT 0",
            "blind_box_count": "INT NOT NULL DEFAULT 0",
            "blind_box_profit": "INT NOT NULL DEFAULT 0",
            "danmaku_count": "INT NOT NULL DEFAULT 0",
            "captain_danmaku_count": "INT NULL DEFAULT NULL",
            "admiral_danmaku_count": "INT NULL DEFAULT NULL",
            "governor_danmaku_count": "INT NULL DEFAULT NULL",
            "normal_danmaku_count": "INT NULL DEFAULT NULL",
            "avg_concurrency": "FLOAT NULL",
            "max_concurrency": "INT NULL",
            "sample_count": "INT NOT NULL DEFAULT 0",
            "payer_count": "INT NOT NULL DEFAULT 0",
        },
        "room_stats_monthly": {
            "payer_count": "INT NOT NULL DEFAULT 0",
            "danmaku_count": "INT NULL DEFAULT NULL",
            "captain_danmaku_count": "INT NULL DEFAULT NULL",
            "admiral_danmaku_count": "INT NULL DEFAULT NULL",
            "governor_danmaku_count": "INT NULL DEFAULT NULL",
            "normal_danmaku_count": "INT NULL DEFAULT NULL",
            "whale_top1_amount": "BIGINT NOT NULL DEFAULT 0",
            "whale_top1_ratio": "DECIMAL(12,8) NULL",
            "whale_top5_amount": "BIGINT NOT NULL DEFAULT 0",
            "whale_top5_ratio": "DECIMAL(12,8) NULL",
            "whale_top10_amount": "BIGINT NOT NULL DEFAULT 0",
            "whale_top10_ratio": "DECIMAL(12,8) NULL",
            "whale_top1pct_amount": "BIGINT NOT NULL DEFAULT 0",
            "whale_top1pct_ratio": "DECIMAL(12,8) NULL",
            "whale_total_revenue": "BIGINT NOT NULL DEFAULT 0",
            "whale_attributed_revenue": "BIGINT NOT NULL DEFAULT 0",
            "whale_unattributed_revenue": "BIGINT NOT NULL DEFAULT 0",
            "whale_payer_count": "INT NOT NULL DEFAULT 0",
            "whale_status": "VARCHAR(16) NULL",
            "whale_metric_version": "INT NOT NULL DEFAULT 1",
            "whale_calculated_at": "DATETIME NULL",
        },
        "room_live_stats": {
            "gift": "DECIMAL(20,1) NOT NULL DEFAULT 0",
            "guard": "DECIMAL(20,0) NOT NULL DEFAULT 0",
            "super_chat": "DECIMAL(20,0) NOT NULL DEFAULT 0",
            "payer_count": "INT NOT NULL DEFAULT 0",
            "steel_coin_count": "INT NOT NULL DEFAULT 0",
        },
    }
    try:
        inspector = inspect(engine)
        table_names = set(inspector.get_table_names())
    except SQLAlchemyError as exc:
        logger.error(f"[schema] 读取表结构失败: {exc}")
        return
    targets = dict(required_columns)
    for table_name in table_names:
        if table_name.startswith("live_session_") and table_name[len("live_session_"):].isdigit():
            targets.setdefault(table_name, {})["payer_count"] = "INT NOT NULL DEFAULT 0"
            targets[table_name].update({
                "duration_valid": "INT NOT NULL DEFAULT 1",
                "duration_ledger": "TEXT NULL",
            })
        if table_name.startswith("live_session_15m_stats_") and table_name[len("live_session_15m_stats_"):].isdigit():
            targets.setdefault(table_name, {}).update(
                {
                    name: ddl
                    for name, ddl in required_columns["live_session_15m_stats"].items()
                    if name not in {
                        "captain_danmaku_count",
                        "admiral_danmaku_count",
                        "governor_danmaku_count",
                        "normal_danmaku_count",
                    }
                }
            )
        if table_name.startswith("room_live_stats_") and table_name[len("room_live_stats_"):].isdigit():
            targets.setdefault(table_name, {}).update(required_columns["room_live_stats"])
    for table_name, columns in targets.items():
        if table_name not in table_names:
            continue
        try:
            existing_cols = {col.get("name") for col in inspector.get_columns(table_name)}
        except SQLAlchemyError as exc:
            logger.error(f"[schema] 读取 {table_name} 列失败: {exc}")
            continue
        for col_name, ddl in columns.items():
            if col_name in existing_cols:
                continue
            try:
                with engine.begin() as conn:
                    conn.execute(text(f"ALTER TABLE `{table_name}` ADD COLUMN `{col_name}` {ddl}"))
                logger.info(f"[schema] 已补齐 {table_name}.{col_name}")
            except SQLAlchemyError as exc:
                logger.error(f"[schema] 新增 {table_name}.{col_name} 失败: {exc}")
