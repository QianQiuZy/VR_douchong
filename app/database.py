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
    """Compatibly add live_session columns required by current runtime code."""
    required_columns = {
        "start_attention": "INT NULL",
        "end_attention": "INT NULL",
    }
    try:
        existing_cols = {col.get("name") for col in inspect(engine).get_columns("live_session")}
    except SQLAlchemyError as exc:
        logging.error(f"[schema] 读取 live_session 列失败: {exc}")
        return
    for col_name, ddl in required_columns.items():
        if col_name in existing_cols:
            continue
        try:
            with engine.begin() as conn:
                conn.execute(text(f"ALTER TABLE `live_session` ADD COLUMN `{col_name}` {ddl}"))
            logging.info(f"[schema] 已补齐 live_session.{col_name}")
        except SQLAlchemyError as exc:
            logging.error(f"[schema] 新增 live_session.{col_name} 失败: {exc}")
