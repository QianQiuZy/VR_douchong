"""Database engine, session factory, and explicit schema bootstrap."""

import logging

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import declarative_base, sessionmaker

from .config import DB_CONFIG


engine = create_engine(
    f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
    f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['db']}",
    echo=False,
    pool_recycle=1800,
    pool_pre_ping=True,
)
Session = sessionmaker(bind=engine)
Base = declarative_base()


def create_schema() -> None:
    """Create declared tables when an explicit launcher/bootstrap requests it."""
    Base.metadata.create_all(engine)


def ensure_runtime_schema() -> None:
    """Compatibly add current metrics columns to hot and old archive tables."""
    required_columns = {
        "live_session": {
            "start_attention": "INT NULL",
            "end_attention": "INT NULL",
            "payer_count": "INT NOT NULL DEFAULT 0",
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
            "avg_concurrency": "FLOAT NULL",
            "max_concurrency": "INT NULL",
            "sample_count": "INT NOT NULL DEFAULT 0",
            "payer_count": "INT NOT NULL DEFAULT 0",
        },
        "room_stats_monthly": {
            "payer_count": "INT NOT NULL DEFAULT 0",
        },
        "room_live_stats": {
            "gift": "FLOAT NOT NULL DEFAULT 0",
            "guard": "FLOAT NOT NULL DEFAULT 0",
            "super_chat": "FLOAT NOT NULL DEFAULT 0",
            "payer_count": "INT NOT NULL DEFAULT 0",
            "steel_coin_count": "INT NOT NULL DEFAULT 0",
        },
    }
    try:
        inspector = inspect(engine)
        table_names = set(inspector.get_table_names())
    except SQLAlchemyError as exc:
        logging.error(f"[schema] 读取表结构失败: {exc}")
        return
    targets = dict(required_columns)
    for table_name in table_names:
        if table_name.startswith("live_session_") and table_name[len("live_session_"):].isdigit():
            targets.setdefault(table_name, {})["payer_count"] = "INT NOT NULL DEFAULT 0"
        if table_name.startswith("live_session_15m_stats_") and table_name[len("live_session_15m_stats_"):].isdigit():
            targets.setdefault(table_name, {}).update(required_columns["live_session_15m_stats"])
        if table_name.startswith("room_live_stats_") and table_name[len("room_live_stats_"):].isdigit():
            targets.setdefault(table_name, {}).update(required_columns["room_live_stats"])
    for table_name, columns in targets.items():
        if table_name not in table_names:
            continue
        try:
            existing_cols = {col.get("name") for col in inspector.get_columns(table_name)}
        except SQLAlchemyError as exc:
            logging.error(f"[schema] 读取 {table_name} 列失败: {exc}")
            continue
        for col_name, ddl in columns.items():
            if col_name in existing_cols:
                continue
            try:
                with engine.begin() as conn:
                    conn.execute(text(f"ALTER TABLE `{table_name}` ADD COLUMN `{col_name}` {ddl}"))
                logging.info(f"[schema] 已补齐 {table_name}.{col_name}")
            except SQLAlchemyError as exc:
                logging.error(f"[schema] 新增 {table_name}.{col_name} 失败: {exc}")
