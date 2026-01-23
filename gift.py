# douchong.py (monthly-only, no room_stats)

import asyncio
import http.cookies
import logging
import datetime
import json
import re
from typing import Optional, Tuple, Dict
import threading
from aiohttp import ContentTypeError
import random, math, time, os, hmac, hashlib
import smtplib
from email.mime.text import MIMEText

import aiohttp
import blivedm
import blivedm.models.web as web_models  # 保留以兼容 blivedm
from sqlalchemy.dialects.mysql import insert
from flask import Flask, jsonify, request
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    Float,
    String,
    Date,
    DateTime,
    PrimaryKeyConstraint,
    and_,
    Index,
    text,
    BigInteger,
)
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import SQLAlchemyError

# ------------------ 日志 ------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

def load_env_file(env_path: str = ".env") -> None:
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

load_env_file(os.getenv("ENV_FILE", ".env"))

def _get_env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        logging.warning(f"[env] {name} 不是有效整数，使用默认值 {default}")
        return default

# ------------------ 数据库 ------------------
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "111"),
    "password": os.getenv("DB_PASSWORD", "111"),
    "db": os.getenv("DB_NAME", "111"),
    "port": _get_env_int("DB_PORT", 3306),
}
engine = create_engine(
    f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
    f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['db']}",
    echo=False,
    pool_recycle=1800,
    pool_pre_ping=True,
)
Session = sessionmaker(bind=engine)
Base = declarative_base()

SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
EMAIL_FROM = os.getenv("EMAIL_FROM", "")
EMAIL_TO   = os.getenv("EMAIL_TO", "")
APP_HOST = os.getenv("APP_HOST", "0.0.0.0")
APP_PORT = _get_env_int("APP_PORT", 4666)

COOKIE_ALERT_SENT = False  # 防止同一次失效被疯狂刷邮件

COMMON_NOTICE_GIFT_COIN_MAP = {
    "干杯之旅": 10000,
    "启航之旅": 100000,
    "友谊的小船": 4900,
    "冲浪": 89900,
    "海湾之旅": 799900,
    "鸿运小电视": 1000000,
}

# ------------------ 工具 ------------------
def month_str(dt: Optional[datetime.datetime] = None) -> str:
    dt = dt or datetime.datetime.now()
    return dt.strftime("%Y%m")

def month_range(month: str) -> Tuple[datetime.date, datetime.date]:
    """返回 [start_date, next_month_date)"""
    year = int(month[:4])
    mon = int(month[4:])
    start = datetime.date(year, mon, 1)
    if mon == 12:
        end = datetime.date(year + 1, 1, 1)
    else:
        end = datetime.date(year, mon + 1, 1)
    return start, end
    
def normalize_month_code(raw: Optional[str]) -> Optional[str]:
    """
    接受 'YYYYMM' 或 'YYYY-MM'，返回标准 'YYYYMM'；非法返回 None。
    """
    if not raw:
        return None
    s = raw.strip()
    m1 = re.fullmatch(r"(\d{4})(\d{2})", s)
    m2 = re.fullmatch(r"(\d{4})-(\d{2})", s)
    if m1:
        yyyy, mm = m1.group(1), m1.group(2)
    elif m2:
        yyyy, mm = m2.group(1), m2.group(2)
    else:
        return None
    try:
        mi = int(mm)
        if 1 <= mi <= 12:
            return f"{yyyy}{mm}"
    except Exception:
        return None
    return None

def send_cookie_invalid_email_async(log_line: str):
    """
    检测到 uid=0 时发送一次告警邮件（进程生命周期内只发一次）。
    """
    global COOKIE_ALERT_SENT
    if COOKIE_ALERT_SENT:
        return
    COOKIE_ALERT_SENT = True

    def _worker():
        try:
            subject = "B站直播礼物监听 Cookies 失效告警"
            body = (
                "检测到 B 站直播礼物消息 uid=0，疑似 SESSDATA Cookies 已失效。\n\n"
                f"原始日志：{log_line}\n"
                "请尽快检查并更新 douchong.py 使用的 SESSDATA。"
            )
            msg = MIMEText(body, "plain", "utf-8")
            msg["Subject"] = subject
            msg["From"] = EMAIL_FROM
            msg["To"] = EMAIL_TO

            with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
                server.starttls()
                server.login(SMTP_USER, SMTP_PASS)
                server.sendmail(EMAIL_FROM, [EMAIL_TO], msg.as_string())

            logging.info("[SMTP] Cookies 失效告警邮件已发送")
        except Exception as e:
            logging.error(f"[SMTP] 发送 Cookies 失效告警失败: {e}")

    threading.Thread(target=_worker, daemon=True).start()

# ------------------ 表定义 ------------------
class RoomInfo(Base):
    """房间基础信息：主播名称 & 粉丝数"""
    __tablename__ = "room_info"
    room_id     = Column(Integer, primary_key=True)
    anchor_name = Column(String(100), nullable=False)
    attention   = Column(Integer, default=0, nullable=False)

    @classmethod
    def upsert(cls, room_id: int, anchor_name: Optional[str] = None, attention: Optional[int] = None):
        session = Session()
        try:
            info = session.query(cls).filter_by(room_id=room_id).first()
            if info:
                if anchor_name is not None:
                    info.anchor_name = anchor_name
                if attention is not None:
                    info.attention = attention
            else:
                info = cls(room_id=room_id, anchor_name=anchor_name or "", attention=attention or 0)
                session.add(info)
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            logging.error(f"[RoomInfo] upsert 失败: {e}")
        finally:
            session.close()

class RoomStatsMonthly(Base):
    """按月累计礼物/上舰/SC；主键：room_id + month"""
    __tablename__ = "room_stats_monthly"
    room_id    = Column(Integer, nullable=False)
    month      = Column(String(6), nullable=False)  # YYYYMM
    gift       = Column(Float, default=0.0, nullable=False)
    guard      = Column(Float, default=0.0, nullable=False)
    super_chat = Column(Float, default=0.0, nullable=False)
    __table_args__ = (
        PrimaryKeyConstraint("room_id", "month", name="pk_room_month"),
        Index("idx_rsm_month", "month"),
    )

    @classmethod
    def add_amounts(cls, room_id: int, month: str, gift: float = 0.0, guard: float = 0.0, super_chat: float = 0.0):
        # 可选：0 三项全为 0 时直接跳过，降低无效写入竞争
        if not gift and not guard and not super_chat:
            return

        max_tries = 6  # 增加到 6 次，带退避
        base_sleep = 0.05
        for attempt in range(1, max_tries + 1):
            session = Session()
            try:
                stmt = insert(cls).values(
                    room_id=room_id, month=month,
                    gift=gift, guard=guard, super_chat=super_chat
                ).on_duplicate_key_update(
                    gift=cls.gift + gift,
                    guard=cls.guard + guard,
                    super_chat=cls.super_chat + super_chat
                )
                session.execute(stmt)
                session.commit()
                return
            except SQLAlchemyError as e:
                session.rollback()
                err = getattr(e, "orig", None)
                code = getattr(err, "args", [None])[0] if err else None
                # MySQL 死锁 1213 / 锁等待超时 1205 -> 退避重试
                if code in (1205, 1213):
                    sleep = base_sleep * (2 ** (attempt - 1)) + random.uniform(0, 0.1)
                    logging.warning(f"[RoomStatsMonthly] 死锁/锁超时，第 {attempt} 次退避 {sleep:.3f}s；code={code}")
                    time.sleep(sleep)
                    continue
                logging.error(f"[RoomStatsMonthly] 写入失败（非可重试）: {repr(e)} orig={repr(err)}")
                return
            finally:
                try:
                    session.close()
                except Exception:
                    pass
        logging.error("[RoomStatsMonthly] 多次重试仍失败，数据可能不完整。")

class RoomLiveStats(Base):
    """按自然日累计当日直播秒数"""
    __tablename__ = "room_live_stats"
    room_id  = Column(Integer, nullable=False)
    date     = Column(Date, nullable=False)       # YYYY-MM-DD
    duration = Column(Integer, default=0, nullable=False)  # 秒
    __table_args__ = (
        PrimaryKeyConstraint("room_id", "date", name="pk_room_date"),
        Index("idx_rls_date", "date"),
    )

    @classmethod
    def add_duration(cls, room_id: int, date_: datetime.date, seconds: int):
        for i in range(3):
            session = Session()
            try:
                row = session.query(cls).filter_by(room_id=room_id, date=date_).first()
                if row:
                    row.duration += seconds
                else:
                    row = cls(room_id=room_id, date=date_, duration=seconds)
                    session.add(row)
                session.commit()
                return
            except SQLAlchemyError as e:
                session.rollback()
                logging.warning(f"[RoomLiveStats] 第 {i+1} 次尝试 add_duration 失败: {e}")
            finally:
                try:
                    session.close()
                except Exception:
                    pass
        logging.error("[RoomLiveStats] add_duration 最终失败，数据可能不完整。")

    @classmethod
    def month_aggregate_for_month(cls, room_id: int, month: str) -> Tuple[int, int]:
        session = Session()
        try:
            start, end = month_range(month)
            from sqlalchemy import func, case
            total_sec, eff_days = (session.query(
                    func.coalesce(func.sum(cls.duration), 0),
                    func.coalesce(func.sum(case((cls.duration >= 7200, 1), else_=0)), 0),
                )
                .filter(and_(cls.room_id == room_id, cls.date >= start, cls.date < end))
                .one())
            return int(total_sec), int(eff_days)
        except SQLAlchemyError as e:
            logging.error(f"[RoomLiveStats] month_aggregate_for_month 读取失败: {e}")
            return 0, 0
        finally:
            session.close()

class LiveSession(Base):
    """单场直播（以开播为准）"""
    __tablename__ = "live_session"
    id          = Column(Integer, primary_key=True, autoincrement=True)
    room_id     = Column(Integer, nullable=False, index=True)
    start_time  = Column(DateTime, nullable=False)
    end_time    = Column(DateTime, nullable=True)
    title       = Column(String(255), default="", nullable=False)
    gift        = Column(Float, default=0.0, nullable=False)
    guard       = Column(Float, default=0.0, nullable=False)
    super_chat  = Column(Float, default=0.0, nullable=False)
    month       = Column(String(6), nullable=False, index=True)  # 以开播月份为准
    
    danmaku_count = Column(Integer, default=0, nullable=False)

    # 新增：开播/下播时的守护快照 + 粉丝团快照
    # guard_1 = 舰长数量；guard_2 = 提督数量；guard_3 = 总督数量
    start_guard_1   = Column(Integer, nullable=True)
    start_guard_2   = Column(Integer, nullable=True)
    start_guard_3   = Column(Integer, nullable=True)
    start_fans_count = Column(Integer, nullable=True)

    end_guard_1     = Column(Integer, nullable=True)
    end_guard_2     = Column(Integer, nullable=True)
    end_guard_3     = Column(Integer, nullable=True)
    end_fans_count  = Column(Integer, nullable=True)

    # 新增：同接统计（开播期间轮询贡献榜 count）
    avg_concurrency = Column(Float, nullable=True)
    max_concurrency = Column(Integer, nullable=True)

    @classmethod
    def start_session(cls, room_id: int, start_dt: datetime.datetime, title: str) -> Optional[int]:
        session = Session()
        try:
            row = cls(
                room_id=room_id,
                start_time=start_dt,
                end_time=None,
                title=title or "",
                month=month_str(start_dt),
            )
            session.add(row)
            session.commit()
            return row.id
        except SQLAlchemyError as e:
            session.rollback()
            logging.error(f"[LiveSession] start_session 失败: {e}")
            return None
        finally:
            session.close()

    @classmethod
    def add_values_by_id(cls, session_id: int, gift: float = 0.0, guard: float = 0.0, super_chat: float = 0.0):
        if not session_id:
            return
        session = Session()
        try:
            row = session.query(cls).filter_by(id=session_id).first()
            if not row:
                return
            row.gift += gift
            row.guard += guard
            row.super_chat += super_chat
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            logging.error(f"[LiveSession] add_values_by_id 失败: {e}")
        finally:
            session.close()

    @classmethod
    def add_values_by_room_open(cls, room_id: int, gift: float = 0.0, guard: float = 0.0, super_chat: float = 0.0):
        """找该房当前未结束的会话并累加（兜底，用于进程重启后仍在播的场次）。"""
        session = Session()
        try:
            row = (
                session.query(cls)
                .filter(and_(cls.room_id == room_id, cls.end_time.is_(None)))
                .order_by(cls.start_time.desc())
                .first()
            )
            if not row:
                return
            row.gift += gift
            row.guard += guard
            row.super_chat += super_chat
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            logging.error(f"[LiveSession] add_values_by_room_open 失败: {e}")
        finally:
            session.close()

    @classmethod
    def close_session_by_id(cls, session_id: Optional[int], end_dt: datetime.datetime):
        if not session_id:
            return
        session = Session()
        try:
            row = session.query(cls).filter_by(id=session_id).first()
            if row and row.end_time is None:
                row.end_time = end_dt
                session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            logging.error(f"[LiveSession] close_session_by_id 失败: {e}")
        finally:
            session.close()

    @classmethod
    def update_concurrency_by_id(
        cls,
        session_id: Optional[int],
        avg_concurrency: Optional[float] = None,
        max_concurrency: Optional[int] = None,
    ):
        if not session_id:
            return
        session = Session()
        try:
            row = session.query(cls).filter_by(id=session_id).first()
            if not row:
                return
            if avg_concurrency is not None:
                row.avg_concurrency = float(avg_concurrency)
            if max_concurrency is not None:
                row.max_concurrency = int(max_concurrency)
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            logging.error(f"[LiveSession] update_concurrency_by_id 失败: {e}")
        finally:
            session.close()

    @classmethod
    def update_start_counts(
        cls,
        session_id: int,
        guard_1: Optional[int] = None,
        guard_2: Optional[int] = None,
        guard_3: Optional[int] = None,
        fans_count: Optional[int] = None,
    ):
        """更新开播瞬间的守护/粉丝团快照。"""
        if not session_id:
            return
        session = Session()
        try:
            row = session.query(cls).filter_by(id=session_id).first()
            if not row:
                return
            if guard_1 is not None:
                row.start_guard_1 = guard_1
            if guard_2 is not None:
                row.start_guard_2 = guard_2
            if guard_3 is not None:
                row.start_guard_3 = guard_3
            if fans_count is not None:
                row.start_fans_count = fans_count
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            logging.error(f"[LiveSession] update_start_counts 失败: {e}")
        finally:
            session.close()

    @classmethod
    def update_end_counts(
        cls,
        session_id: int,
        guard_1: Optional[int] = None,
        guard_2: Optional[int] = None,
        guard_3: Optional[int] = None,
        fans_count: Optional[int] = None,
    ):
        """更新下播瞬间的守护/粉丝团快照。"""
        if not session_id:
            return
        session = Session()
        try:
            row = session.query(cls).filter_by(id=session_id).first()
            if not row:
                return
            if guard_1 is not None:
                row.end_guard_1 = guard_1
            if guard_2 is not None:
                row.end_guard_2 = guard_2
            if guard_3 is not None:
                row.end_guard_3 = guard_3
            if fans_count is not None:
                row.end_fans_count = fans_count
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            logging.error(f"[LiveSession] update_end_counts 失败: {e}")
        finally:
            session.close()
            
    @classmethod
    def add_danmaku_by_id(cls, session_id: int, count: int):
        """按 session_id 追加弹幕数量"""
        if not session_id or count <= 0:
            return
        session = Session()
        try:
            row = session.query(cls).filter_by(id=session_id).first()
            if not row:
                return
            row.danmaku_count = (row.danmaku_count or 0) + int(count)
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            logging.error(f"[LiveSession] add_danmaku_by_id 失败: {e}")
        finally:
            try:
                session.close()
            except Exception:
                pass

    @classmethod
    def add_danmaku_by_room_open(cls, room_id: int, count: int):
        """兜底：按 room_id 找当前未结束场次追加弹幕数量"""
        if count <= 0:
            return
        session = Session()
        try:
            row = (
                session.query(cls)
                .filter(and_(cls.room_id == room_id, cls.end_time.is_(None)))
                .order_by(cls.start_time.desc())
                .first()
            )
            if not row:
                return
            row.danmaku_count = (row.danmaku_count or 0) + int(count)
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            logging.error(f"[LiveSession] add_danmaku_by_room_open 失败: {e}")
        finally:
            try:
                session.close()
            except Exception:
                pass
            
class SuperChatLog(Base):
    """单条 SC 消息日志"""
    __tablename__ = "super_chat_log"

    id        = Column(Integer, primary_key=True, autoincrement=True)
    room_id   = Column(Integer, nullable=False, index=True)
    uname     = Column(String(100), nullable=False)
    uid       = Column(BigInteger, nullable=False)
    send_time = Column(DateTime, nullable=False, index=True)
    price     = Column(Float, nullable=False)
    message   = Column(String(500), nullable=False)

    __table_args__ = (
        Index("idx_scl_room_time", "room_id", "send_time"),
        Index("idx_scl_uid_time", "uid", "send_time"),
    )

    @classmethod
    def log_sc(
        cls,
        room_id: int,
        uname: str,
        uid: int,
        price: float,
        content: str,
        send_time: Optional[datetime.datetime] = None,
    ):
        session = Session()
        try:
            final_time = send_time or datetime.datetime.now()
            # 避免 1970 年之类的异常时间被写入，统一兜底为当前时间
            if final_time.year < 2000:
                final_time = datetime.datetime.now()

            row = cls(
                room_id=room_id,
                uname=uname or "",
                uid=int(uid or 0),
                price=float(price or 0.0),
                message=content or "",
                send_time=final_time,
            )
            session.add(row)
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            logging.error(f"[SuperChatLog] 写入失败: {e}")
        finally:
            try:
                session.close()
            except Exception:
                pass

ROOMS_JSON_PATH = os.getenv("ROOMS_JSON_PATH", "rooms.json")
ROOM_CONFIG_LOCK = threading.Lock()
ROOM_IDS: list[int] = []
ROOM_ANCHORS: Dict[int, str] = {}

def load_rooms_config() -> None:
    if not os.path.exists(ROOMS_JSON_PATH):
        logging.warning(f"[rooms] 未找到房间配置文件: {ROOMS_JSON_PATH}")
        return
    try:
        with open(ROOMS_JSON_PATH, "r", encoding="utf-8") as handle:
            payload = json.load(handle) or {}
    except json.JSONDecodeError as exc:
        logging.error(f"[rooms] JSON 解析失败: {exc}")
        return
    except Exception as exc:
        logging.error(f"[rooms] 读取配置失败: {exc}")
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
    normalized_anchors: Dict[int, str] = {}
    for rid in room_ids:
        try:
            rid_int = int(rid)
        except (TypeError, ValueError):
            continue
        normalized_ids.append(rid_int)

    for raw_id, name in room_anchors.items():
        try:
            rid_int = int(raw_id)
        except (TypeError, ValueError):
            continue
        normalized_anchors[rid_int] = str(name) if name is not None else ""

    for rid in normalized_ids:
        normalized_anchors.setdefault(rid, "")

    with ROOM_CONFIG_LOCK:
        ROOM_IDS.clear()
        ROOM_IDS.extend(sorted(set(normalized_ids)))
        ROOM_ANCHORS.clear()
        ROOM_ANCHORS.update(normalized_anchors)

def save_rooms_config() -> None:
    with ROOM_CONFIG_LOCK:
        payload = {
            "room_ids": sorted(set(ROOM_IDS)),
            "room_anchors": {str(rid): name for rid, name in ROOM_ANCHORS.items()},
        }
    try:
        with open(ROOMS_JSON_PATH, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2)
    except Exception as exc:
        logging.error(f"[rooms] 保存配置失败: {exc}")

def get_room_ids() -> list[int]:
    with ROOM_CONFIG_LOCK:
        return list(ROOM_IDS)

def get_room_anchors() -> Dict[int, str]:
    with ROOM_CONFIG_LOCK:
        return dict(ROOM_ANCHORS)

def get_room_anchor_name(room_id: int) -> str:
    with ROOM_CONFIG_LOCK:
        return ROOM_ANCHORS.get(room_id, "")

load_rooms_config()

def _room_ids_for_month(m: str, include_config: bool = True) -> list[int]:
    """返回指定月份应展示的房间集合：DB出现过的房间 ∪ (可选) ROOM_IDS"""
    session = Session()
    try:
        start, end = month_range(m)
        ids = set(get_room_ids()) if include_config else set()

        # room_stats_monthly 有记录的房间
        q1 = session.query(RoomStatsMonthly.room_id).filter_by(month=m).all()
        ids.update(rid for (rid,) in q1)

        # live_session 以开播月归档的房间
        q2 = session.query(LiveSession.room_id).filter_by(month=m).all()
        ids.update(rid for (rid,) in q2)

        # room_live_stats 在该月有天级时长记录的房间
        q3 = (session.query(RoomLiveStats.room_id)
              .filter(RoomLiveStats.date >= start, RoomLiveStats.date < end)
              .distinct()
              .all())
        ids.update(rid for (rid,) in q3)

        return sorted(ids)
    finally:
        session.close()

# 确保所有表存在（不会删除既有数据）
Base.metadata.create_all(engine)

# ------------------ 直播间配置 ------------------
SESSDATA_VALUE = os.getenv("SESSDATA_VALUE", "")
BILI_JCT_VALUE           = os.getenv("BILI_JCT_VALUE", "")
DEDEUSERID_VALUE         = os.getenv("DEDEUSERID_VALUE", "")
DEDEUSERID_CKMD5_VALUE   = os.getenv("DEDEUSERID_CKMD5_VALUE", "")
SID_VALUE                = os.getenv("SID_VALUE", "")
BUVID3_VALUE             = os.getenv("BUVID3_VALUE", "")
DEVICE_FP_VALUE          = os.getenv("DEVICE_FP_VALUE", "")

BILI_COOKIES_BASE = {
    "SESSDATA": SESSDATA_VALUE,
    "bili_jct": BILI_JCT_VALUE,
    "DedeUserID": DEDEUSERID_VALUE,
    "DedeUserID__ckMd5": DEDEUSERID_CKMD5_VALUE,
    "sid": SID_VALUE,
    "buvid3": BUVID3_VALUE,
    "deviceFingerprint": DEVICE_FP_VALUE,
}

BILI_TICKET: Optional[str] = None
BILI_TICKET_EXPIRES: Optional[int] = None  # Unix 时间戳

# bili_ticket 相关常量（按你示例）
BILI_TICKET_KEY    = "XgwSnGZ1p"
BILI_TICKET_URL    = "https://api.bilibili.com/bapis/bilibili.api.ticket.v1.Ticket/GenWebTicket"
BILI_TICKET_KEY_ID = "ec02"

aiohttp_session: Optional[aiohttp.ClientSession] = None

# room_id -> uid（仅初始化时获取一次，后续不刷新）
ROOM_UIDS: Dict[int, int] = {}

def init_room_info():
    for rid, name in get_room_anchors().items():
        RoomInfo.upsert(rid, anchor_name=name)

def init_session():
    cookies = http.cookies.SimpleCookie()
    # 基础 Cookie 统一写入
    for k, v in BILI_COOKIES_BASE.items():
        if not v:
            continue
        cookies[k] = v
        cookies[k]["domain"] = "bilibili.com"

    global aiohttp_session
    connector = aiohttp.TCPConnector(ssl=False)
    aiohttp_session = aiohttp.ClientSession(connector=connector)
    aiohttp_session.cookie_jar.update_cookies(cookies)
    logging.info("[session] 已初始化基础 Cookies：%s", ",".join(cookies.keys()))

# ------------------ blivedm 事件处理 ------------------
CURRENT_SESSIONS: Dict[int, int] = {}  # room_id -> live_session.id
ROOM_CLIENTS: Dict[int, blivedm.BLiveClient] = {}
LAST_RECONNECT: Dict[int, datetime.datetime] = {}
RECONNECT_DAILY_STATE = {"date": None, "done": set()}  # 保留占位，不再使用配额逻辑

def _now():
    return datetime.datetime.now()

def _seconds_to_hms(sec: int) -> str:
    h, r = divmod(sec, 3600)
    m, s = divmod(r, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

async def _start_client(room_id: int):
    """启动单个房间连接，并登记到全局表"""
    client = blivedm.BLiveClient(room_id, session=aiohttp_session)
    client.set_handler(MyHandler())
    client.start()
    ROOM_CLIENTS[room_id] = client
    # 初始化 last_reconnect，使初始分布更均匀（回溯 0~3 天随机偏移）
    LAST_RECONNECT.setdefault(
        room_id,
        _now() - datetime.timedelta(days=random.random() * 3.0)
    )
    logging.info(f"[connect] 已连接房间 {room_id}")

async def _reconnect_one(room_id: int):
    if LAST_STATUS.get(room_id, 0) == 1:
        logging.info(f"[reconnect] 房间 {room_id} 已在播，跳过重连")
        return

    client = ROOM_CLIENTS.get(room_id)
    if client is not None:
        try:
            # blivedm 新版的优雅关闭
            await client.stop_and_close()
        except asyncio.CancelledError:
            # Python 3.12: CancelledError 是 BaseException，属预期的关闭流程
            logging.debug(f"[reconnect] room={room_id} stop_and_close 触发取消（预期），忽略")
        except Exception as e:
            logging.warning(f"[reconnect] stop_and_close 异常 room={room_id}: {e}")

    await asyncio.sleep(3)
    await _start_client(room_id)
    LAST_RECONNECT[room_id] = _now()
    logging.info(f"[reconnect] 房间 {room_id} 重连完成")

class MyHandler(blivedm.BaseHandler):
    def _record_gift(
        self,
        client,
        gift_name: str,
        num: int,
        total_coin: int,
        uname: str = "",
        uid: int = 0,
        trigger_cookie_alert: bool = False,
    ):
        value = total_coin / 1000  # RMB
        RoomStatsMonthly.add_amounts(client.room_id, month_str(), gift=value)
        sid = CURRENT_SESSIONS.get(client.room_id)
        if sid:
            LiveSession.add_values_by_id(sid, gift=value)
        else:
            LiveSession.add_values_by_room_open(client.room_id, gift=value)
        log_msg = (
            f"[{client.room_id}] {uname} uid{uid} "
            f"赠送 {gift_name}×{num} ({value:.2f})"
        )
        logging.info(log_msg)

        if trigger_cookie_alert:
            try:
                if uid == 0:
                    send_cookie_invalid_email_async(log_msg)
            except Exception as e:
                logging.error(f"[Gift] 检测 uid0 告警时出错: {e}")

    @staticmethod
    def _parse_common_notice_gift(message) -> Tuple[str, str]:
        segments = getattr(message, "content_segments", [])
        texts = [seg.text for seg in segments if getattr(seg, "text", "")] if segments else []
        if not texts:
            return "", ""
        sender = texts[0].strip()
        gift_name = texts[-1].strip()
        return sender, gift_name

    def _on_heartbeat(self, client, message):  # noqa: N802
        pass

    def _on_danmaku(self, client, message):  # noqa: N802
        """
        弹幕统计：仅做计数，不直接写库。
        每条弹幕按房间累加，后由 danmaku_flush_scheduler 每分钟统一入库。
        """
        try:
            room_id = client.room_id
            # 只在当前判定为“在播”时统计，避免误计
            if LAST_STATUS.get(room_id, 0) != 1:
                return
            DANMAKU_PENDING[room_id] = DANMAKU_PENDING.get(room_id, 0) + 1
        except Exception as e:
            logging.error(f"[Danmaku] 统计弹幕时出错: {e}")

    def _on_gift(self, client, message):  # noqa: N802
        try:
            self._record_gift(
                client=client,
                gift_name=message.gift_name,
                num=message.num,
                total_coin=message.total_coin,
                uname=message.uname,
                uid=message.uid,
                trigger_cookie_alert=True,
            )
        except Exception as e:
            logging.error(f"处理礼物记录时出错: {e}")

    def _on_common_notice_danmaku(self, client, message):  # noqa: N802
        try:
            sender, gift_name = self._parse_common_notice_gift(message)
            if not gift_name:
                logging.info(
                    f"[{client.room_id}] COMMON_NOTICE_DANMAKU 未解析到礼物名: {message.content_text}"
                )
                return
            coin_value = COMMON_NOTICE_GIFT_COIN_MAP.get(gift_name)
            if coin_value is None:
                logging.info(
                    f"[{client.room_id}] COMMON_NOTICE_DANMAKU 未匹配礼物价格: {gift_name}"
                )
                return
            self._record_gift(
                client=client,
                gift_name=gift_name,
                num=1,
                total_coin=coin_value,
                uname=sender,
                uid=0,
                trigger_cookie_alert=False,
            )
        except Exception as e:
            logging.error(f"处理 COMMON_NOTICE_DANMAKU 礼物记录时出错: {e}")

    def _on_user_toast_v2(self, client, message):  # noqa: N802
        try:
            total_coins = message.price * message.num
            if message.price == 1900:  # 修正
                total_coins = 198000
            if message.num != 1:
                if message.guard_level == 3:
                    mapping = {3: 534000, 6: 1038000, 12: 2046000}
                    total_coins = mapping.get(message.num, total_coins)
                elif message.guard_level == 2:
                    mapping = {3: 4794000, 6: 9588000, 12: 19176000}
                    total_coins = mapping.get(message.num, total_coins)
                elif message.guard_level == 1:
                    mapping = {3: 51994000}
                    total_coins = mapping.get(message.num, total_coins)

            value = total_coins / 1000  # RMB
            RoomStatsMonthly.add_amounts(client.room_id, month_str(), guard=value)
            sid = CURRENT_SESSIONS.get(client.room_id)
            if sid:
                LiveSession.add_values_by_id(sid, guard=value)
            else:
                LiveSession.add_values_by_room_open(client.room_id, guard=value)
            logging.info(
                f"[{client.room_id}] {message.username} {message.uid} 上舰 lvl={message.guard_level} "
                f"num={message.num} 修正后={value:.1f} RMB"
            )
        except Exception as e:
            logging.error(f"处理舰长记录时出错: {e}")

    def _on_super_chat(self, client, message):  # noqa: N802
        try:
            value = message.price
            # 月累计 + 单场累计
            RoomStatsMonthly.add_amounts(client.room_id, month_str(), super_chat=value)

            sid = CURRENT_SESSIONS.get(client.room_id)
            if sid:
                LiveSession.add_values_by_id(sid, super_chat=value)
            else:
                LiveSession.add_values_by_room_open(client.room_id, super_chat=value)

            # === SC 日志记录 ===
            # 发送人名称
            uname = getattr(message, "uname", "") or ""
            if not uname:
                user_info = getattr(message, "user_info", None)
                if isinstance(user_info, dict):
                    uname = user_info.get("uname", "") or uname

            # 发送人 UID
            uid = getattr(message, "uid", 0) or 0
            if not uid:
                user_info = getattr(message, "user_info", None)
                if isinstance(user_info, dict):
                    uid = user_info.get("uid", 0) or uid

            content = getattr(message, "message", "") or ""

            # 时间戳：优先用 message.time，其次 ts
            ts_raw = getattr(message, "time", None)
            if ts_raw is None:
                ts_raw = getattr(message, "ts", None)

            send_dt = None
            if ts_raw is not None:
                try:
                    ts_int = int(ts_raw)
                    # 处理毫秒级时间戳：大于 1e12 视为毫秒
                    if ts_int > 1_000_000_000_000:
                        ts_int = ts_int // 1000

                    # 过滤明显异常时间戳（2000 年之前视为无效）
                    if ts_int >= 946684800:  # 2000-01-01 00:00:00
                        send_dt = datetime.datetime.fromtimestamp(ts_int)
                except Exception:
                    send_dt = None

            SuperChatLog.log_sc(
                room_id=client.room_id,
                uname=uname,
                uid=uid,
                price=value,
                content=content,
                send_time=send_dt,
            )

            logging.info(
                f"[{client.room_id}] SC ¥{value:.2f} {uname} {uid}: {content}"
            )
        except Exception as e:
            logging.error(f"处理醒目留言记录时出错: {e}")

async def run_clients_loop():
    # 批量按 3 秒间隔启动
    for room_id in get_room_ids():
        await _start_client(room_id)
        await asyncio.sleep(3)

# ------------------ 直播状态 & 粉丝数监视 ------------------
LAST_STATUS: Dict[int, int] = {}
STREAM_STARTS: Dict[int, datetime.datetime] = {}
LIVE_INFO: Dict[int, Dict[str, str]] = {}

DANMAKU_PENDING: Dict[int, int] = {}
# 新增：当前粉丝团数量 / 当前守护数量（非按场次，是“最新状态”）
FANS_COUNT: Dict[int, int] = {}  # room_id -> 当前粉丝团数量
GUARD_COUNTS: Dict[int, Dict[str, int]] = {}  # room_id -> {"guard_1": 舰长, "guard_2": 提督, "guard_3": 总督}
# 新增：同接轮询缓存（按房间缓存当前场次统计）
# 结构：room_id -> {"session_id": int, "total": int, "samples": int, "max": int, "last": int}
CONCURRENCY_CACHE: Dict[int, Dict[str, int]] = {}

# 粉丝团 & 守护 信息获取任务队列：元素为 (room_id, session_id)，session_id 为 None 表示只更新当前状态
GUARD_FANS_QUEUE: "asyncio.Queue[tuple[int, Optional[int], Optional[str]]]" = asyncio.Queue()

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)

LIVE_STATUS_API = "https://api.live.bilibili.com/room/v1/Room/get_status_info_by_uids"
ROOM_INFO_API   = "https://api.live.bilibili.com/room/v1/Room/get_info"

# 新增：粉丝团/舰长 API
FANS_API  = "https://api.live.bilibili.com/xlive/general-interface/v1/rank/getFansMembersRank"
GUARD_API = "https://api.live.bilibili.com/xlive/general-interface/v1/guard/GuardActive"
CONTRIBUTION_RANK_API = "https://api.live.bilibili.com/xlive/general-interface/v1/rank/queryContributionRank"

def _init_concurrency_cache(room_id: int, session_id: int) -> None:
    CONCURRENCY_CACHE[room_id] = {
        "session_id": int(session_id),
        "total": 0,
        "samples": 0,
        "max": 0,
        "last": 0,
    }

def _update_concurrency_cache(room_id: int, session_id: int, count: int) -> None:
    cache = CONCURRENCY_CACHE.get(room_id)
    if not cache or cache.get("session_id") != session_id:
        _init_concurrency_cache(room_id, session_id)
        cache = CONCURRENCY_CACHE[room_id]
    cache["total"] = int(cache.get("total", 0)) + int(count)
    cache["samples"] = int(cache.get("samples", 0)) + 1
    cache["max"] = max(int(cache.get("max", 0)), int(count))
    cache["last"] = int(count)

def _finalize_concurrency_cache(room_id: int, session_id: Optional[int]) -> tuple[Optional[float], Optional[int]]:
    cache = CONCURRENCY_CACHE.get(room_id)
    if not cache or session_id is None or cache.get("session_id") != session_id:
        return None, None
    samples = int(cache.get("samples", 0))
    total = int(cache.get("total", 0))
    max_val = int(cache.get("max", 0)) if samples > 0 else None
    avg_val = (total / samples) if samples > 0 else None
    return avg_val, max_val

def ensure_room_state(room_id: int) -> None:
    LAST_STATUS.setdefault(room_id, 0)
    LIVE_INFO.setdefault(room_id, {"live_time": "0000-00-00 00:00:00", "title": ""})
    FANS_COUNT.setdefault(room_id, 0)
    GUARD_COUNTS.setdefault(room_id, {"guard_1": 0, "guard_2": 0, "guard_3": 0})

for _room_id in get_room_ids():
    ensure_room_state(_room_id)

async def init_uid_and_attention_for_room(room_id: int, max_rounds: int = 3) -> None:
    for round_idx in range(1, max_rounds + 1):
        if room_id in ROOM_UIDS:
            return
        logging.info(f"[init] room_id={room_id} 获取 UID 第 {round_idx}/{max_rounds} 轮")
        await _fetch_room_info_and_update(room_id, update_uid=True)
        await asyncio.sleep(0.3)
    if room_id not in ROOM_UIDS:
        logging.error(f"[init] room_id={room_id} UID 获取失败，后续状态轮询将跳过")

async def add_room_async(room_id: int, anchor_name: str) -> Tuple[bool, str]:
    with ROOM_CONFIG_LOCK:
        if room_id in ROOM_IDS:
            return False, "房间已存在"
        ROOM_IDS.append(room_id)
        ROOM_IDS.sort()
        ROOM_ANCHORS[room_id] = anchor_name or ""
    save_rooms_config()
    ensure_room_state(room_id)
    RoomInfo.upsert(room_id, anchor_name=anchor_name)
    await init_uid_and_attention_for_room(room_id)
    if aiohttp_session is None:
        return False, "aiohttp_session 未初始化"
    await _start_client(room_id)
    if LAST_STATUS.get(room_id, 0) != 1:
        try:
            GUARD_FANS_QUEUE.put_nowait((room_id, None, None))
        except Exception as e:
            logging.error(f"[Guard/Fans] 新增房间投递任务 room_id={room_id} 失败: {e}")
    return True, "房间已添加并启动任务"

async def delete_room_async(room_id: int) -> Tuple[bool, str]:
    with ROOM_CONFIG_LOCK:
        if room_id not in ROOM_IDS:
            return False, "房间不存在"
        ROOM_IDS.remove(room_id)
        ROOM_ANCHORS.pop(room_id, None)
    save_rooms_config()

    client = ROOM_CLIENTS.pop(room_id, None)
    if client is not None:
        try:
            await client.stop_and_close()
        except asyncio.CancelledError:
            logging.debug(f"[delete] room={room_id} stop_and_close 触发取消（预期）")
        except Exception as e:
            logging.warning(f"[delete] room={room_id} stop_and_close 异常: {e}")

    if room_id in STREAM_STARTS:
        st = STREAM_STARTS.pop(room_id)
        end_dt = _now()
        _split_and_record(room_id, st, end_dt)
        sid = CURRENT_SESSIONS.pop(room_id, None)
        LiveSession.close_session_by_id(sid, end_dt)
        avg_concurrency, max_concurrency = _finalize_concurrency_cache(room_id, sid)
        LiveSession.update_concurrency_by_id(
            sid,
            avg_concurrency=avg_concurrency,
            max_concurrency=max_concurrency,
        )
        CONCURRENCY_CACHE.pop(room_id, None)

    ROOM_UIDS.pop(room_id, None)
    LAST_STATUS.pop(room_id, None)
    LIVE_INFO.pop(room_id, None)
    LAST_RECONNECT.pop(room_id, None)
    DANMAKU_PENDING.pop(room_id, None)
    FANS_COUNT.pop(room_id, None)
    GUARD_COUNTS.pop(room_id, None)
    CONCURRENCY_CACHE.pop(room_id, None)
    return True, "房间已删除并停止任务"

def _split_and_record(room_id: int, start: datetime.datetime, end: datetime.datetime):
    """将 [start, end] 按自然日切片，并累加到 RoomLiveStats 表中。"""
    cur = start
    while cur < end:
        if cur.date() == end.date():
            next_boundary = end
        else:
            next_boundary = datetime.datetime.combine(cur.date() + datetime.timedelta(days=1), datetime.time.min)
        slice_end = min(end, next_boundary)
        seconds = int((slice_end - cur).total_seconds())
        if seconds > 0:
            RoomLiveStats.add_duration(room_id, cur.date(), seconds)
        cur = slice_end

async def ensure_bili_ticket(force: bool = False) -> str:
    """
    确保全局 bili_ticket 可用：
      - 若已有未过期 ticket 且 force=False，则直接复用；
      - 否则调用 GenWebTicket 接口获取新 ticket，并写入 Cookie。
    """
    global BILI_TICKET, BILI_TICKET_EXPIRES

    if aiohttp_session is None:
        raise RuntimeError("aiohttp_session 未初始化，无法获取 bili_ticket")

    now_ts = int(time.time())
    # 若已有 ticket 且尚未过期，且本次不是强制刷新，则直接返回
    if (
        not force
        and BILI_TICKET
        and BILI_TICKET_EXPIRES
        and BILI_TICKET_EXPIRES - now_ts > 60
    ):
        return BILI_TICKET

    csrf = BILI_COOKIES_BASE.get("bili_jct", "") or ""
    if not csrf:
        logging.warning("[bili_ticket] bili_jct 为空，可能导致 GenWebTicket 调用失败")

    hexsign = hmac.new(
        BILI_TICKET_KEY.encode("utf-8"),
        f"ts{now_ts}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()

    params = {
        "key_id": BILI_TICKET_KEY_ID,
        "hexsign": hexsign,
        "context[ts]": str(now_ts),
        "csrf": csrf,
    }
    headers = {"User-Agent": USER_AGENT}

    try:
        async with aiohttp_session.post(
            BILI_TICKET_URL,
            params=params,
            headers=headers,
            timeout=10,
        ) as resp:
            try:
                obj = await resp.json(content_type=None)
            except ContentTypeError:
                text = (await resp.text())[:200]
                raise RuntimeError(f"获取 bili_ticket 返回非 JSON，前 200 字：{text}")
    except Exception as e:
        raise RuntimeError(f"请求 bili_ticket 接口异常: {e}") from e

    if obj.get("code") != 0 or "data" not in obj:
        raise RuntimeError(f"获取 bili_ticket 失败: {obj}")

    data = obj["data"] or {}
    ticket = data.get("ticket")
    if not ticket:
        raise RuntimeError(f"获取 bili_ticket 失败，未包含 ticket 字段: {obj}")

    created_at = int(data.get("created_at", now_ts))
    ttl        = int(data.get("ttl", 0))
    expires_ts = created_at + ttl

    BILI_TICKET = ticket
    BILI_TICKET_EXPIRES = expires_ts

    # 写入 Cookie
    c = http.cookies.SimpleCookie()
    c["bili_ticket"] = ticket
    c["bili_ticket"]["domain"] = "bilibili.com"
    aiohttp_session.cookie_jar.update_cookies(c)

    logging.info(
        "[bili_ticket] 刷新成功，过期时间=%s (%d)",
        datetime.datetime.fromtimestamp(expires_ts).strftime("%Y-%m-%d %H:%M:%S"),
        expires_ts,
    )
    return ticket

async def _fetch_room_info_and_update(room_id: int, update_uid: bool) -> bool:
    """
    旧 API：get_info?room_id=xxx
    - 初始化阶段：update_uid=True -> 写入 UID + attention
    - 定时刷新阶段：update_uid=False -> 仅刷新 attention
    """
    if aiohttp_session is None:
        logging.error("[RoomInfo] aiohttp_session 未初始化")
        return False

    url = f"{ROOM_INFO_API}?room_id={room_id}"
    try:
        async with aiohttp_session.get(
            url,
            timeout=5,
            headers={"User-Agent": USER_AGENT, "Referer": "https://live.bilibili.com"},
        ) as resp:
            if resp.status != 200:
                logging.warning(f"[RoomInfo] 房间 {room_id} get_info HTTP {resp.status}")
                return False
            try:
                payload = await resp.json(content_type=None)
            except ContentTypeError:
                text = (await resp.text())[:200]
                logging.warning(f"[RoomInfo] 房间 {room_id} get_info 返回非 JSON，前 200 字：{text}")
                return False
    except Exception as e:
        logging.error(f"[RoomInfo] 房间 {room_id} 请求异常: {e}")
        return False

    data = payload.get("data") or {}
    attention_raw = data.get("attention", 0)
    try:
        attention = int(attention_raw)
    except (TypeError, ValueError):
        attention = 0

    RoomInfo.upsert(room_id, attention=attention)

    if update_uid:
        uid_raw = data.get("uid")
        try:
            uid = int(uid_raw)
        except (TypeError, ValueError):
            uid = 0
        if uid:
            ROOM_UIDS[room_id] = uid
            logging.info(f"[RoomInfo] room_id={room_id} uid={uid} attention={attention}")
        else:
            logging.warning(f"[RoomInfo] room_id={room_id} uid 获取失败，原始值={uid_raw!r}")
    else:
        logging.debug(f"[RoomInfo] room_id={room_id} 刷新 attention={attention}（不更新 uid）")

    return True

async def init_uids_and_attention_once(max_rounds: int = 5):
    """
    首次启动时：通过 get_info 拉取所有房间的 uid + 粉丝数。
    要求：在开始使用 get_status_info_by_uids 轮询前，尽量让所有房间都有 UID。
    """
    logging.info("[init] 开始初始化 UID 和粉丝数（get_info）")
    for round_idx in range(1, max_rounds + 1):
        missing = [rid for rid in get_room_ids() if rid not in ROOM_UIDS]
        if not missing:
            logging.info("[init] 所有 UID 已成功获取")
            return
        logging.info(f"[init] 第 {round_idx}/{max_rounds} 轮获取 UID，待获取房间数={len(missing)}")
        for room_id in missing:
            await _fetch_room_info_and_update(room_id, update_uid=True)
            await asyncio.sleep(0.3)  # 稍微限速，避免过快
    missing = [rid for rid in get_room_ids() if rid not in ROOM_UIDS]
    if missing:
        logging.error(f"[init] 经过 {max_rounds} 轮仍有 UID 获取失败，将在状态轮询中跳过这些房间: {missing}")
    else:
        logging.info("[init] 所有 UID 已成功获取")

async def _fetch_guard_counts(uid: int, room_id: int) -> Optional[tuple[int, int, int]]:
    """
    调用 GuardActive 接口，返回 (guard_1, guard_2, guard_3)
    guard_1 = 舰长数(来自 guard_num_3)
    guard_2 = 提督数(来自 guard_num_2)
    guard_3 = 总督数(来自 guard_num_1)
    """
    if aiohttp_session is None:
        logging.error("[Guard] aiohttp_session 未初始化")
        return None

    try:
        async with aiohttp_session.get(
            GUARD_API,
            params={"ruid": str(uid), "platform": "pc"},
            timeout=10,
            headers={"User-Agent": USER_AGENT, "Referer": "https://live.bilibili.com"},
        ) as resp:
            if resp.status != 200:
                logging.warning(f"[Guard] room_id={room_id} HTTP {resp.status}")
                return None
            try:
                payload = await resp.json(content_type=None)
            except ContentTypeError:
                text = (await resp.text())[:200]
                logging.warning(f"[Guard] room_id={room_id} 返回非 JSON，前 200 字：{text}")
                return None
    except Exception as e:
        logging.error(f"[Guard] room_id={room_id} 请求异常: {e}")
        return None

    data = payload.get("data") or {}

    def _to_int(x):
        try:
            return int(x)
        except (TypeError, ValueError):
            return 0

    # bilibili 原始：guard_num_3 = 舰长, guard_num_2 = 提督, guard_num_1 = 总督
    num_captain  = _to_int(data.get("guard_num_3", 0))
    num_admiral  = _to_int(data.get("guard_num_2", 0))
    num_governor = _to_int(data.get("guard_num_1", 0))

    # 我们 API 约定：guard_1 = 舰长；guard_2 = 提督；guard_3 = 总督
    guard_1 = num_captain
    guard_2 = num_admiral
    guard_3 = num_governor

    logging.info(
        f"[Guard] room_id={room_id} guard_1(舰长)={guard_1} guard_2(提督)={guard_2} guard_3(总督)={guard_3}"
    )

    return guard_1, guard_2, guard_3


async def _fetch_fans_count(uid: int, room_id: int) -> Optional[int]:
    """
    调用 fans members rank 接口，返回粉丝团数量 num。
    """
    if aiohttp_session is None:
        logging.error("[Fans] aiohttp_session 未初始化")
        return None

    try:
        async with aiohttp_session.get(
            FANS_API,
            params={"ruid": str(uid), "page_size": "1", "page": "1"},
            timeout=10,
            headers={"User-Agent": USER_AGENT, "Referer": "https://live.bilibili.com"},
        ) as resp:
            if resp.status != 200:
                logging.warning(f"[Fans] room_id={room_id} HTTP {resp.status}")
                return None
            try:
                payload = await resp.json(content_type=None)
            except ContentTypeError:
                text = (await resp.text())[:200]
                logging.warning(f"[Fans] room_id={room_id} 返回非 JSON，前 200 字：{text}")
                return None
    except Exception as e:
        logging.error(f"[Fans] room_id={room_id} 请求异常: {e}")
        return None

    data = payload.get("data") or {}
    num_raw = data.get("num", 0)
    try:
        num = int(num_raw)
    except (TypeError, ValueError):
        num = 0

    logging.info(f"[Fans] room_id={room_id} 粉丝团数量={num}")
    return num

async def _fetch_contribution_count(uid: int, room_id: int) -> Optional[int]:
    """
    调用贡献榜接口，返回 data.count 作为同接统计基数。
    """
    if aiohttp_session is None:
        logging.error("[Concurrency] aiohttp_session 未初始化")
        return None

    params = {
        "ruid": str(uid),
        "room_id": str(room_id),
        "page": "1",
        "page_size": "1",
    }
    try:
        async with aiohttp_session.get(
            CONTRIBUTION_RANK_API,
            params=params,
            timeout=10,
            headers={"User-Agent": USER_AGENT, "Referer": "https://live.bilibili.com"},
        ) as resp:
            if resp.status != 200:
                logging.warning(f"[Concurrency] room_id={room_id} HTTP {resp.status}")
                return None
            try:
                payload = await resp.json(content_type=None)
            except ContentTypeError:
                text = (await resp.text())[:200]
                logging.warning(f"[Concurrency] room_id={room_id} 返回非 JSON，前 200 字：{text}")
                return None
    except Exception as e:
        logging.error(f"[Concurrency] room_id={room_id} 请求异常: {e}")
        return None

    if payload.get("code") != 0:
        logging.warning(f"[Concurrency] room_id={room_id} 接口返回异常: {payload}")
        return None

    data = payload.get("data") or {}
    count_raw = data.get("count", 0)
    try:
        count = int(count_raw)
    except (TypeError, ValueError):
        count = 0
    return count


async def guard_fans_worker():
    """
    全局唯一 worker：
    - 从 GUARD_FANS_QUEUE 取出 (room_id, session_id, phase)
    - 先调用 GuardActive，再等待 ≥1s，再调用 Fans API，再等待 ≥1s
    - phase:
        * None: 仅更新当前状态缓存（FANS_COUNT/GUARD_COUNTS）
        * "start": 更新开播快照（live_session.start_*）
        * "end":   更新下播快照（live_session.end_*）
    """
    while True:
        room_id, session_id, phase = await GUARD_FANS_QUEUE.get()
        uid = ROOM_UIDS.get(room_id)
        if uid is None:
            logging.warning(f"[Guard/Fans] room_id={room_id} 未找到 uid，跳过")
            GUARD_FANS_QUEUE.task_done()
            continue

        try:
            # 1) 守护数量
            guard_vals = None
            try:
                guard_vals = await _fetch_guard_counts(uid, room_id)
            except Exception as e:
                logging.error(f"[Guard/Fans] room_id={room_id} _fetch_guard_counts 异常: {e}")

            if guard_vals is not None:
                guard_1, guard_2, guard_3 = guard_vals
                GUARD_COUNTS[room_id] = {
                    "guard_1": guard_1,
                    "guard_2": guard_2,
                    "guard_3": guard_3,
                }
                if session_id:
                    if phase == "start":
                        LiveSession.update_start_counts(
                            session_id,
                            guard_1=guard_1,
                            guard_2=guard_2,
                            guard_3=guard_3,
                        )
                    elif phase == "end":
                        LiveSession.update_end_counts(
                            session_id,
                            guard_1=guard_1,
                            guard_2=guard_2,
                            guard_3=guard_3,
                        )

            await asyncio.sleep(1.0)

            # 2) 粉丝团数量
            fans = None
            try:
                fans = await _fetch_fans_count(uid, room_id)
            except Exception as e:
                logging.error(f"[Guard/Fans] room_id={room_id} _fetch_fans_count 异常: {e}")

            if fans is not None:
                FANS_COUNT[room_id] = fans
                if session_id:
                    if phase == "start":
                        LiveSession.update_start_counts(
                            session_id,
                            fans_count=fans,
                        )
                    elif phase == "end":
                        LiveSession.update_end_counts(
                            session_id,
                            fans_count=fans,
                        )

            await asyncio.sleep(1.0)
        finally:
            GUARD_FANS_QUEUE.task_done()

async def danmaku_flush_scheduler():
    """
    弹幕计数定时入库：
      - 每 60 秒执行一次；
      - 将 DANMAKU_PENDING 中的增量写入当前未结束的 live_session.danmaku_count。
    """
    while True:
        await asyncio.sleep(60)
        if not DANMAKU_PENDING:
            continue

        # 拍快照并清零缓冲，避免长时间持有锁
        snapshot = dict(DANMAKU_PENDING)
        DANMAKU_PENDING.clear()

        for room_id, inc in snapshot.items():
            if inc <= 0:
                continue
            sid = CURRENT_SESSIONS.get(room_id)
            if sid:
                LiveSession.add_danmaku_by_id(sid, inc)
            else:
                LiveSession.add_danmaku_by_room_open(room_id, inc)
        logging.debug(f"[Danmaku] 本轮入库完成，房间数={len(snapshot)}")

async def refresh_attention_scheduler():
    """
    每 3 小时通过旧 API 刷新一次所有房间的粉丝数（attention），uid 不再更新。
    """
    # 首次刷新前等待 3 小时；初始化阶段已经拉过一次
    while True:
        await asyncio.sleep(3 * 3600)
        logging.info("[RoomInfo] 开始 3 小时粉丝数刷新任务")
        for room_id in get_room_ids():
            await _fetch_room_info_and_update(room_id, update_uid=False)
            await asyncio.sleep(0.3)
        logging.info("[RoomInfo] 本轮粉丝数刷新任务完成")

async def guard_fans_refresh_scheduler():
    """
    未开播房间每小时刷新一次守护数量 + 粉丝团数量。
    启动后会先立即执行一轮刷新，之后每小时执行一次。
    - 使用 GUARD_FANS_QUEUE，确保两个接口统一排队执行。
    """
    # 等待 UID 初始化完成
    while not ROOM_UIDS:
        logging.info("[Guard/Fans] 等待 UID 初始化...")
        await asyncio.sleep(1)

    # 启动后先刷一遍未开播房间
    logging.info("[Guard/Fans] 启动后立刻执行一轮未开播房间守护+粉丝团刷新")
    for room_id in get_room_ids():
        if LAST_STATUS.get(room_id, 0) != 1:
            try:
                await GUARD_FANS_QUEUE.put((room_id, None, None))
            except Exception as e:
                logging.error(f"[Guard/Fans] 启动刷新 投递任务 room_id={room_id} 失败: {e}")
            await asyncio.sleep(0.1)
    logging.info("[Guard/Fans] 启动初次未开播房间刷新任务结束")

    # 之后每小时执行
    while True:
        await asyncio.sleep(3600)
        logging.info("[Guard/Fans] 开始每小时未开播房间刷新任务")
        for room_id in get_room_ids():
            # 仅未开播房间
            if LAST_STATUS.get(room_id, 0) != 1:
                try:
                    await GUARD_FANS_QUEUE.put((room_id, None, None))
                except Exception as e:
                    logging.error(f"[Guard/Fans] 投递任务 room_id={room_id} 失败: {e}")
                await asyncio.sleep(0.1)
        logging.info("[Guard/Fans] 本轮未开播房间刷新任务结束")

async def concurrency_poll_scheduler():
    """
    开播房间每 15 秒轮询贡献榜 count，用于同接统计。
    """
    # 等待 UID 初始化完成
    while not ROOM_UIDS:
        logging.info("[Concurrency] 等待 UID 初始化...")
        await asyncio.sleep(1)

    while True:
        for room_id in get_room_ids():
            if LAST_STATUS.get(room_id, 0) != 1:
                continue
            uid = ROOM_UIDS.get(room_id)
            session_id = CURRENT_SESSIONS.get(room_id)
            if uid is None or session_id is None:
                continue
            count = await _fetch_contribution_count(uid, room_id)
            if count is None:
                continue
            _update_concurrency_cache(room_id, session_id, count)
            logging.debug(
                f"[Concurrency] room_id={room_id} session_id={session_id} count={count}"
            )
            await asyncio.sleep(0.1)
        await asyncio.sleep(15)

async def monitor_all_rooms_status():
    """
    使用新 API：get_status_info_by_uids?uids[]=... 批量查询所有房间的直播状态。
    查询周期：每 3 秒。

    改进点：
      - 当本轮结果中缺少某个 UID（或结构异常）时，不再立刻判定下播，
        而是沿用上一轮状态，等待下一轮。
    """
    # 等待 UID 初始化
    while not ROOM_UIDS:
        logging.info("[LiveStatus] 等待 UID 初始化...")
        await asyncio.sleep(1)

    logging.info(f"[LiveStatus] UID 初始化完成，当前可用房间数={len(ROOM_UIDS)}，启动状态轮询")

    while True:
        try:
            if aiohttp_session is None:
                logging.error("[LiveStatus] aiohttp_session 未初始化")
                await asyncio.sleep(3)
                continue
            if not ROOM_UIDS:
                logging.info("[LiveStatus] 暂无可用 UID，等待房间加入")
                await asyncio.sleep(3)
                continue

            # 组装批量查询参数
            params = [("uids[]", str(uid)) for uid in ROOM_UIDS.values()]
            async with aiohttp_session.get(
                LIVE_STATUS_API,
                params=params,
                timeout=10,
                headers={"User-Agent": USER_AGENT, "Referer": "https://live.bilibili.com"},
            ) as resp:
                if resp.status != 200:
                    logging.warning(f"[LiveStatus] get_status_info_by_uids HTTP {resp.status}")
                    await asyncio.sleep(3)
                    continue

                try:
                    payload = await resp.json(content_type=None)
                except ContentTypeError:
                    text = (await resp.text())[:200]
                    logging.warning(f"[LiveStatus] get_status_info_by_uids 返回非 JSON，前 200 字：{text}")
                    await asyncio.sleep(3)
                    continue

            data = payload.get("data") or {}
            now = _now()

            # 逐房间处理
            for room_id in get_room_ids():
                uid = ROOM_UIDS.get(room_id)
                info = data.get(str(uid)) if uid is not None else None
                prev = LAST_STATUS.get(room_id, 0)

                # === 关键改动：当本轮没有该 UID 或结构异常时，不判定下播，沿用上一状态 ===
                if not info or "live_status" not in info:
                    if prev == 1:
                        logging.warning(
                            f"[LiveStatus] room_id={room_id} 本轮未返回有效数据，沿用上一轮“在播”状态"
                        )
                    else:
                        logging.debug(
                            f"[LiveStatus] room_id={room_id} 本轮未返回有效数据，保持状态={prev}"
                        )
                    # 不改 LAST_STATUS / STREAM_STARTS / LIVE_INFO，直接下一房间
                    continue

                # === 正常有数据的分支 ===
                raw_status = int(info.get("live_status", 0))
                # B 站约定：2 为轮播，视作未开播
                status = 0 if raw_status == 2 else raw_status
                LAST_STATUS[room_id] = status

                if status == 1:
                    # live_time 为时间戳（秒），未开播为 0
                    live_time_raw = info.get("live_time", 0)
                    try:
                        if isinstance(live_time_raw, (int, float)) or str(live_time_raw).isdigit():
                            start_dt = datetime.datetime.fromtimestamp(int(live_time_raw))
                        else:
                            start_dt = now
                    except (ValueError, OSError, OverflowError):
                        start_dt = now

                    raw_title = info.get("title") or ""

                    # 从未播 -> 开播：认为是上播
                    if prev == 0:
                        STREAM_STARTS[room_id] = start_dt
                        sid = LiveSession.start_session(room_id, start_dt, raw_title)
                        if sid:
                            CURRENT_SESSIONS[room_id] = sid
                            _init_concurrency_cache(room_id, sid)
                            # 开播瞬间：立刻刷新守护数量 + 粉丝团数量，并记录到本场 live_session 中
                            try:
                                GUARD_FANS_QUEUE.put_nowait((room_id, sid, "start"))
                            except Exception as e:
                                logging.error(f"[Guard/Fans] 开播投递任务 room_id={room_id} 失败: {e}")
                        logging.info(f"[{room_id}] 上播，开始时间 {start_dt:%F %T}")

                    LIVE_INFO.setdefault(room_id, {})
                    LIVE_INFO[room_id]["live_time"] = start_dt.strftime("%Y-%m-%d %H:%M:%S")
                    LIVE_INFO[room_id]["title"] = raw_title

                else:
                    # 已下播/未开播
                    LAST_STATUS[room_id] = 0
                    if prev == 1 and room_id in STREAM_STARTS:
                        st = STREAM_STARTS.pop(room_id)
                        end_dt = now
                        _split_and_record(room_id, st, end_dt)
                        # 关闭该场会话
                        sid = CURRENT_SESSIONS.pop(room_id, None)
                        avg_concurrency, max_concurrency = _finalize_concurrency_cache(room_id, sid)
                        LiveSession.close_session_by_id(sid, end_dt)
                        LiveSession.update_concurrency_by_id(
                            sid,
                            avg_concurrency=avg_concurrency,
                            max_concurrency=max_concurrency,
                        )
                        CONCURRENCY_CACHE.pop(room_id, None)
                        duration_str = _seconds_to_hms(int((end_dt - st).total_seconds()))
                        logging.info(f"[{room_id}] 下播，时长 {duration_str}")

                        # 下播瞬间：立刻刷新守护 + 粉丝团并写入本场
                        if sid:
                            try:
                                GUARD_FANS_QUEUE.put_nowait((room_id, sid, "end"))
                            except Exception as e:
                                logging.error(f"[Guard/Fans] 下播投递任务 room_id={room_id} 失败: {e}")

                    LIVE_INFO.setdefault(room_id, {})
                    LIVE_INFO[room_id]["live_time"] = "0000-00-00 00:00:00"
                    LIVE_INFO[room_id]["title"] = ""

        except Exception as e:
            logging.error(f"[LiveStatus] get_status_info_by_uids 调用异常: {e}")

        # 两次调用间隔 3 秒
        await asyncio.sleep(3)

# ------------------ 每日 6:00 全量重连调度 ------------------
async def reconnect_scheduler():
    """
    每日重连：
    - 每天约 06:00 对所有房间执行一次重连；
    - 若房间当前在播（LAST_STATUS == 1），则跳过本日重连；
    - 两次重连之间间隔 >= 3 秒，并带一点随机抖动。
    """
    while True:
        now = _now()
        target = now.replace(hour=6, minute=0, second=0, microsecond=0)
        if now >= target:
            target += datetime.timedelta(days=1)
        sleep_sec = max(1.0, (target - now).total_seconds())
        logging.info(f"[reconnect] 距离下一次全量重连还有约 {sleep_sec/60:.1f} 分钟")
        await asyncio.sleep(sleep_sec)

        logging.info("[reconnect] 开始执行每日全量重连任务")
        for room_id in get_room_ids():
            if LAST_STATUS.get(room_id, 0) == 1:
                logging.info(f"[reconnect] 房间 {room_id} 当前在播，跳过今日重连")
                continue
            try:
                await _reconnect_one(room_id)
            except asyncio.CancelledError:
                logging.debug(f"[reconnect] room={room_id} 被取消（预期），略过")
            except Exception as e:
                logging.error(f"[reconnect] 房间 {room_id} 重连失败: {e}")
            await asyncio.sleep(3 + random.uniform(0.5, 2.0))
        logging.info("[reconnect] 本日全量重连任务完成")

async def bili_ticket_scheduler():
    """
    bili_ticket 刷新调度：
      - 启动后先尝试获取一次；
      - 之后每天 05:00 强制刷新一次 bili_ticket。
    """
    # 等待 aiohttp_session 初始化
    while aiohttp_session is None:
        logging.info("[bili_ticket] 等待 aiohttp_session 初始化...")
        await asyncio.sleep(1)

    # 启动时先强制获取一次
    try:
        await ensure_bili_ticket(force=True)
    except Exception as e:
        logging.error(f"[bili_ticket] 首次获取失败: {e}")

    while True:
        now = _now()
        target = now.replace(hour=5, minute=0, second=0, microsecond=0)
        if now >= target:
            target += datetime.timedelta(days=1)
        sleep_sec = max(60.0, (target - now).total_seconds())
        logging.info(
            "[bili_ticket] 距离下一次刷新还有约 %.2f 小时",
            sleep_sec / 3600.0,
        )
        await asyncio.sleep(sleep_sec)

        try:
            await ensure_bili_ticket(force=True)
        except Exception as e:
            logging.error(f"[bili_ticket] 定时刷新失败: {e}")

# ------------------ 月底清零（无操作以保留全部历史） ------------------
async def monthly_reset_scheduler():
    while True:
        await asyncio.sleep(60)

# 主入口
async def main():
    global MAIN_LOOP
    MAIN_LOOP = asyncio.get_running_loop()
    init_room_info()
    init_session()
    # 先初始化 UID + 粉丝数，完成后再开启状态轮询
    await init_uids_and_attention_once()

    try:
        await asyncio.gather(
            run_clients_loop(),
            monitor_all_rooms_status(),      # 按 UID 批量轮询直播状态
            monthly_reset_scheduler(),
            reconnect_scheduler(),           # 每日 6:00 全量重连
            refresh_attention_scheduler(),   # 每 3 小时刷新关注数（attention）
            guard_fans_worker(),             # 守护 + 粉丝团队列 worker
            guard_fans_refresh_scheduler(),  # 未开播房间每小时刷新守护 + 粉丝团
            bili_ticket_scheduler(),         # 每日 5:00 刷新 bili_ticket
            danmaku_flush_scheduler(),
            concurrency_poll_scheduler(),    # 开播房间每 15 秒轮询同接
        )
    finally:
        if aiohttp_session:
            await aiohttp_session.close()

# ------------------ Flask API ------------------
app = Flask(__name__)
MAIN_LOOP: Optional[asyncio.AbstractEventLoop] = None

def _run_in_main_loop(coro: asyncio.Future, timeout: int = 30):
    if MAIN_LOOP is None:
        raise RuntimeError("MAIN_LOOP 未初始化")
    future = asyncio.run_coroutine_threadsafe(coro, MAIN_LOOP)
    return future.result(timeout=timeout)

def _parse_room_payload(payload: dict) -> Tuple[Optional[int], Optional[str], str]:
    room_id_raw = payload.get("room_id")
    anchor_raw = payload.get("room_anchors")
    if room_id_raw is None:
        return None, None, "room_id 必填"
    try:
        room_id = int(room_id_raw)
    except (TypeError, ValueError):
        return None, None, "room_id 必须为整数"
    if room_id <= 0:
        return None, None, "room_id 必须为正整数"
    if anchor_raw is None:
        return room_id, None, "room_anchors 必填"
    if isinstance(anchor_raw, dict):
        name = anchor_raw.get(str(room_id)) or anchor_raw.get(room_id)
    else:
        name = anchor_raw
    if not isinstance(name, str) or not name.strip():
        return room_id, None, "room_anchors 必须为非空字符串"
    return room_id, name.strip(), ""

@app.route("/add/room", methods=["POST"])
def add_room_api():
    payload = request.get_json(silent=True) or {}
    room_id, anchor_name, error = _parse_room_payload(payload)
    if error:
        return jsonify({"error": error}), 400
    try:
        ok, message = _run_in_main_loop(add_room_async(room_id, anchor_name))
    except Exception as exc:
        logging.error(f"[API] /add/room 执行失败: {exc}")
        return jsonify({"error": "添加房间失败"}), 500
    status = 200 if ok else 409
    return jsonify({"ok": ok, "room_id": room_id, "message": message}), status

@app.route("/delete/room", methods=["POST"])
def delete_room_api():
    payload = request.get_json(silent=True) or {}
    room_id, anchor_name, error = _parse_room_payload(payload)
    if error:
        return jsonify({"error": error}), 400
    try:
        ok, message = _run_in_main_loop(delete_room_async(room_id))
    except Exception as exc:
        logging.error(f"[API] /delete/room 执行失败: {exc}")
        return jsonify({"error": "删除房间失败"}), 500
    status = 200 if ok else 404
    return jsonify({"ok": ok, "room_id": room_id, "message": message}), status

@app.route("/gift", methods=["GET"])
def get_stats_current_month():
    """
    当月汇总：
      - room_stats_monthly 当月 gift/guard/super_chat
      - room_live_stats 聚合当月直播时长与有效天数
      - 当前月返回实时 live_time/title/status；历史月置空
      - 当前月开播时返回即时同接（current_concurrency），未开播返回 null
    """
    results = []
    session = Session()
    m = month_str()  # 当前月
    try:
        for room_id in _room_ids_for_month(m, include_config=True):
            # 读当月累计
            rsm = session.query(RoomStatsMonthly).filter_by(room_id=room_id, month=m).first()
            g  = rsm.gift if rsm else 0.0
            gd = rsm.guard if rsm else 0.0
            sc = rsm.super_chat if rsm else 0.0

            anchor_name = (session.query(RoomInfo.anchor_name).filter_by(room_id=room_id).scalar()) or get_room_anchor_name(room_id)
            attention   = (session.query(RoomInfo.attention).filter_by(room_id=room_id).scalar()) or 0

            total_sec, eff_days = RoomLiveStats.month_aggregate_for_month(room_id, m)
            live_dur_str = _seconds_to_hms(total_sec)

            info = LIVE_INFO.get(room_id, {})
            live_time_val = info.get("live_time", "0000-00-00 00:00:00")
            title_val     = info.get("title", "")
            status_val    = LAST_STATUS.get(room_id, 0)
            current_concurrency = None
            if status_val == 1:
                session_id = CURRENT_SESSIONS.get(room_id)
                cache = CONCURRENCY_CACHE.get(room_id)
                if cache and cache.get("session_id") == session_id:
                    samples = int(cache.get("samples", 0))
                    if samples > 0:
                        current_concurrency = int(cache.get("last", 0))

            # 当前守护数量 / 粉丝团数量（最新状态）
            guard_info  = GUARD_COUNTS.get(room_id, {}) or {}
            guard_1 = guard_info.get("guard_1", 0)  # 舰长
            guard_2 = guard_info.get("guard_2", 0)  # 提督
            guard_3 = guard_info.get("guard_3", 0)  # 总督
            fans_count = FANS_COUNT.get(room_id, 0)

            results.append({
                "room_id": room_id,
                "anchor_name": anchor_name,
                "attention": attention,
                "status": status_val,
                "gift": g,
                "guard": gd,
                "super_chat": sc,
                "live_duration": live_dur_str,
                "effective_days": eff_days,
                "live_time": live_time_val,
                "title": title_val,
                "month": m,
                "guard_1": guard_1,       # 舰长
                "guard_2": guard_2,       # 提督
                "guard_3": guard_3,       # 总督
                "fans_count": fans_count, # 粉丝团数量
                "current_concurrency": current_concurrency,
            })
        return jsonify(results)
    except SQLAlchemyError as e:
        session.rollback()
        logging.error(f"[get_stats_current_month] 数据库查询出错: {e}")
        return jsonify({"error": "数据库查询失败"}), 500
    finally:
        session.close()

@app.route("/gift/by_month", methods=["GET"])
def get_stats_by_month():
    """
    指定月份汇总：
    GET ?month=YYYYMM
    历史月不返回实时 live_time/title/status（置空/0）
    """
    m = request.args.get("month") or month_str()
    results = []
    session = Session()
    try:
        is_current = (m == month_str())
        for room_id in _room_ids_for_month(m, include_config=True):
            rsm = session.query(RoomStatsMonthly).filter_by(room_id=room_id, month=m).first()
            g  = rsm.gift if rsm else 0.0
            gd = rsm.guard if rsm else 0.0
            sc = rsm.super_chat if rsm else 0.0

            anchor_name = (session.query(RoomInfo.anchor_name).filter_by(room_id=room_id).scalar()) or get_room_anchor_name(room_id)
            attention   = (session.query(RoomInfo.attention).filter_by(room_id=room_id).scalar()) or 0

            total_sec, eff_days = RoomLiveStats.month_aggregate_for_month(room_id, m)
            live_dur_str = _seconds_to_hms(total_sec)

            if is_current:
                info = LIVE_INFO.get(room_id, {})
                live_time_val = info.get("live_time", "0000-00-00 00:00:00")
                title_val     = info.get("title", "")
                status_val    = LAST_STATUS.get(room_id, 0)

                guard_info  = GUARD_COUNTS.get(room_id, {}) or {}
                guard_1 = guard_info.get("guard_1", 0)
                guard_2 = guard_info.get("guard_2", 0)
                guard_3 = guard_info.get("guard_3", 0)
                fans_count = FANS_COUNT.get(room_id, 0)
            else:
                live_time_val = "0000-00-00 00:00:00"
                title_val     = ""
                status_val    = 0
                # 历史月份不保留守护 / 粉丝团历史，直接返回 null
                guard_1 = None
                guard_2 = None
                guard_3 = None
                fans_count = None

            results.append({
                "room_id": room_id,
                "anchor_name": anchor_name,
                "attention": attention,
                "status": status_val,
                "gift": g,
                "guard": gd,
                "super_chat": sc,
                "live_duration": live_dur_str,
                "effective_days": eff_days,
                "live_time": live_time_val,
                "title": title_val,
                "month": m,
                "guard_1": guard_1,
                "guard_2": guard_2,
                "guard_3": guard_3,
                "fans_count": fans_count,
            })
        return jsonify(results)
    except SQLAlchemyError as e:
        session.rollback()
        logging.error(f"[get_stats_by_month] 数据库查询出错: {e}")
        return jsonify({"error": "数据库查询失败"}), 500
    finally:
        session.close()

@app.route("/gift/live_sessions", methods=["GET"])
def get_live_sessions_by_room_month():
    """
    指定房间 + 月份的单场直播清单：
    GET ?room_id=xxx&month=YYYYMM
    """
    try:
        room_id = int(request.args.get("room_id", "0"))
    except ValueError:
        return jsonify({"error": "room_id 参数无效"}), 400
    if room_id <= 0:
        return jsonify({"error": "room_id 必填且需为正整数"}), 400

    m = request.args.get("month") or month_str()
    session = Session()
    try:
        rows = (
            session.query(LiveSession)
            .filter(and_(LiveSession.room_id == room_id, LiveSession.month == m))
            .order_by(LiveSession.start_time.asc())
            .all()
        )
        out = []
        for r in rows:
            avg_concurrency = r.avg_concurrency
            max_concurrency = r.max_concurrency
            current_concurrency = None
            if r.end_time is None:
                cache = CONCURRENCY_CACHE.get(room_id)
                if cache and cache.get("session_id") == r.id:
                    samples = int(cache.get("samples", 0))
                    total = int(cache.get("total", 0))
                    avg_concurrency = (total / samples) if samples > 0 else None
                    max_concurrency = int(cache.get("max", 0)) if samples > 0 else None
                    current_concurrency = int(cache.get("last", 0)) if samples > 0 else None
            out.append({
                "start_time": r.start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "end_time":   (r.end_time.strftime("%Y-%m-%d %H:%M:%S") if r.end_time else None),
                "title":      r.title,
                "gift":       r.gift,
                "guard":      r.guard,
                "super_chat": r.super_chat,
                "danmaku_count": r.danmaku_count or 0,

                # 开播时快照（旧数据为 None -> JSON null）
                "start_guard_1": r.start_guard_1,     # 舰长
                "start_guard_2": r.start_guard_2,     # 提督
                "start_guard_3": r.start_guard_3,     # 总督
                "start_fans_count": r.start_fans_count,

                # 下播时快照（旧数据为 None -> JSON null）
                "end_guard_1": r.end_guard_1,
                "end_guard_2": r.end_guard_2,
                "end_guard_3": r.end_guard_3,
                "end_fans_count": r.end_fans_count,

                "avg_concurrency": avg_concurrency,
                "max_concurrency": max_concurrency,
                "current_concurrency": current_concurrency,
            })
        return jsonify({"room_id": room_id, "month": m, "sessions": out})
    except SQLAlchemyError as e:
        session.rollback()
        logging.error(f"[get_live_sessions_by_room_month] 查询失败: {e}")
        return jsonify({"error": "数据库查询失败"}), 500
    finally:
        session.close()
        
@app.route("/gift/sc", methods=["GET"])
def get_sc_logs():
    """
    SC 日志查询：
      GET /gift/sc?room_id=1111&month=202511
      - room_id 必填
      - month 可选，默认当前月；支持 YYYYMM 或 YYYY-MM
      返回：发送时间、发送人名称、UID、价格、内容
    """
    room_id_str = request.args.get("room_id")
    if not room_id_str:
        return jsonify({"error": "room_id 参数必填"}), 400
    try:
        room_id = int(room_id_str)
    except ValueError:
        return jsonify({"error": "room_id 参数无效"}), 400
    if room_id <= 0:
        return jsonify({"error": "room_id 必须为正整数"}), 400

    month_raw = request.args.get("month")
    if month_raw:
        month_code = normalize_month_code(month_raw)
        if not month_code:
            return jsonify({"error": "month 格式不正确，应为 YYYYMM 或 YYYY-MM"}), 400
    else:
        month_code = month_str()

    # 利用已有 month_range，算出该月起止 date，再转为 datetime
    start_date, end_date = month_range(month_code)
    start_dt = datetime.datetime.combine(start_date, datetime.time.min)
    end_dt = datetime.datetime.combine(end_date, datetime.time.min)

    session = Session()
    try:
        rows = (
            session.query(SuperChatLog)
            .filter(
                SuperChatLog.room_id == room_id,
                SuperChatLog.send_time >= start_dt,
                SuperChatLog.send_time < end_dt,
            )
            .order_by(SuperChatLog.send_time.asc())
            .all()
        )

        out = []
        for r in rows:
            out.append({
                "send_time": r.send_time.strftime("%Y-%m-%d %H:%M:%S"),
                "uname": r.uname,
                "uid": r.uid,
                "price": r.price,
                "message": r.message,
            })

        return jsonify({
            "room_id": room_id,
            "month": month_code,
            "list": out,
        })
    except SQLAlchemyError as e:
        session.rollback()
        logging.error(f"[get_sc_logs] 查询失败: {e}")
        return jsonify({"error": "数据库查询失败"}), 500
    finally:
        session.close()

if __name__ == "__main__":
    threading.Thread(target=lambda: app.run(host=APP_HOST, port=APP_PORT), daemon=True).start()
    asyncio.run(main())
