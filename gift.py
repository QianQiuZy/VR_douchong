# douchong.py (monthly-only, no room_stats)
import asyncio
import http.cookies
import logging
import datetime
from typing import Optional, Tuple, Dict
import threading
from aiohttp import ContentTypeError
import random, math, time

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
)
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import SQLAlchemyError

# ------------------ 日志 ------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# ------------------ 数据库 ------------------
DB_CONFIG = {
    "host": "localhost",
    "user": "111",
    "password": "111",
    "db": "111",
    "port": 3306,
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
        for _ in range(3):
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
                session.close()
                logging.warning(f"[RoomLiveStats] 第 {_+1} 次尝试 add_duration 失败: {e}")
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
    def close_session_by_id(cls, session_id: int, end_dt: datetime.datetime):
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

def _room_ids_for_month(m: str, include_config: bool = True) -> list[int]:
    """返回指定月份应展示的房间集合：DB出现过的房间 ∪ (可选) ROOM_IDS"""
    session = Session()
    try:
        start, end = month_range(m)
        ids = set(ROOM_IDS) if include_config else set()

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
ROOM_IDS = [1820703922,21696950,1947277414,27628019,21756924,23771189,1967216004,23805029,23260993,23771092,23771139,25788785,21452505,21224291,21484828,25788858,23550749,22470216,22605464,1967215387,1967212929,31368705,80397,282208,30655190,22778610,27628030,31368697,3032130,21403601,23017349,21457197,30655198,31368686,23770996,1861373970,21672023,784734,23017343,30655213,21613356,30655179,27627985,1766907940,27628009,22389319,26966466,30655172,1766909591,21696957,21763344,23550773,23805059,25788830,26966452,30655203,32638817,32638818,6068126,22359795,6374209,938957,1791260756,1791264553,1791260716]
SESSDATA = "自己填"
ROOM_ANCHORS = {1820703922:"花礼Harei",21696950:"阿萨AzA",1947277414:"泽音Melody",27628019:"雨纪_Ameki",21756924:"雪绘Yukie",23771189:"恬豆发芽了",1967216004:"三理Mit3uri",23805029:"雾深Girimi",23260993:"瑞娅_Rhea",23771092:"又一充电中",23771139:"沐霂是MUMU呀",25788785:"岁己SUI",21452505:"七海Nana7mi",21224291:"安堂いなり_official",21484828:"轴伊Joi_Channel",25788858:"莱恩Leo",23550749:"尤格Yog",22470216:"悠亚Yua",22605464:"千幽Chiyuu",1967215387:"命依Mei",1967212929:"沐毛Meme",31368705:"米汀Nagisa",80397:"阿梓从小就很可爱",282208:"诺莺Nox",30655190:"弥月Mizuki",22778610:"勾檀Mayumi",27628030:"未知夜Michiya",31368697:"雪烛Yukisyo",3032130:"舒三妈Susam",21403601:"艾因Eine",23017349:"桃星Tocci",21457197:"中单光一",30655198:"蜜言Mikoto",31368686:"帕可Pako",23770996:"梨安不迷路",1861373970:"妮慕Nimue",21672023:"弥希Miki",784734:"宫园凛RinMiyazono",23017343:"吉吉Kiti",30655213:"漆羽Urushiha",21613356:"惑姬Waku",30655179:"入福步Ayumi",27627985:"哎小呜Awu",1766907940:"点酥Susu",27628009:"初濑Hatsuse",22389319:"千春_Chiharu",26966466:"栞栞Shiori",30655172:"离枝Richi",1766909591:"桃代Momoka",21696957:"度人Tabibito",21763344:"沙夜_Saya",23550773:"暴食Hunger",23805059:"希维Sybil",25788830:"江乱Era",26966452:"伊舞Eve",30655203:"晴一Hajime",32638817:"鬼间Kima",32638818:"阿命Inochi",6068126:"糯依Noi",22359795:"菜菜子Nanako",6374209:"小可学妹",938957:"祖娅纳惜",1791260756:"柚雨Kioi",1791264553:"能能Nori",1791260716:"犬绒Mofu"}

aiohttp_session: Optional[aiohttp.ClientSession] = None

def init_room_info():
    for rid, name in ROOM_ANCHORS.items():
        RoomInfo.upsert(rid, anchor_name=name)

def init_session():
    cookies = http.cookies.SimpleCookie()
    cookies["SESSDATA"] = SESSDATA
    cookies["SESSDATA"]["domain"] = "bilibili.com"
    global aiohttp_session
    connector = aiohttp.TCPConnector(ssl=False)
    aiohttp_session = aiohttp.ClientSession(connector=connector)
    aiohttp_session.cookie_jar.update_cookies(cookies)

# ------------------ blivedm 事件处理 ------------------
CURRENT_SESSIONS: Dict[int, int] = {}  # room_id -> live_session.id
ROOM_CLIENTS: Dict[int, blivedm.BLiveClient] = {}
LAST_RECONNECT: Dict[int, datetime.datetime] = {}
RECONNECT_DAILY_STATE = {"date": None, "done": set()}  # 每日重连进度

async def _start_client(room_id: int):
    """启动单个房间连接，并登记到全局表"""
    client = blivedm.BLiveClient(room_id, session=aiohttp_session)
    client.set_handler(MyHandler())
    client.start()
    ROOM_CLIENTS[room_id] = client
    # 初始化 last_reconnect，使初始分布更均匀（回溯 0~3 天随机偏移）
    LAST_RECONNECT.setdefault(
        room_id,
        datetime.datetime.now() - datetime.timedelta(days=random.random() * 3.0)
    )
    logging.info(f"[connect] 已连接房间 {room_id}")

async def _reconnect_one(room_id: int):
    if LAST_STATUS.get(room_id, 0) == 1:
        logging.info(f"[reconnect] 房间 {room_id} 已在播，跳过重连")
        return

    client = ROOM_CLIENTS.get(room_id)
    try:
        if client is not None:
            if hasattr(client, "stop_and_close"):
                await client.stop_and_close()
            else:
                client.stop()
            if hasattr(client, "join"):
                await client.join()
    except Exception as e:
        logging.warning(f"[reconnect] 停止/回收旧连接异常 room={room_id}: {e}")

    await asyncio.sleep(3)
    await _start_client(room_id)
    LAST_RECONNECT[room_id] = datetime.datetime.now()
    logging.info(f"[reconnect] 房间 {room_id} 重连完成")

def _seconds_to_hms(sec: int) -> str:
    h, r = divmod(sec, 3600)
    m, s = divmod(r, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

class MyHandler(blivedm.BaseHandler):
    def _on_heartbeat(self, client, message):  # noqa: N802
        pass

    def _on_danmaku(self, client, message):  # noqa: N802
        pass

    def _on_gift(self, client, message):  # noqa: N802
        try:
            value = message.total_coin / 1000  # RMB
            # 写入按月累计
            RoomStatsMonthly.add_amounts(client.room_id, month_str(), gift=value)
            # 写入单场（仅在开播的会话内）
            sid = CURRENT_SESSIONS.get(client.room_id)
            if sid:
                LiveSession.add_values_by_id(sid, gift=value)
            else:
                LiveSession.add_values_by_room_open(client.room_id, gift=value)
            logging.info(f"[{client.room_id}] {message.uname} uid{message.uid} 赠送 {message.gift_name}×{message.num} ({value:.2f})")
        except Exception as e:
            logging.error(f"处理礼物记录时出错: {e}")

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
            RoomStatsMonthly.add_amounts(client.room_id, month_str(), super_chat=value)
            sid = CURRENT_SESSIONS.get(client.room_id)
            if sid:
                LiveSession.add_values_by_id(sid, super_chat=value)
            else:
                LiveSession.add_values_by_room_open(client.room_id, super_chat=value)
            logging.info(f"[{client.room_id}] SC ¥{message.price:.2f} {message.uname} {message.uid}: {message.message}")
        except Exception as e:
            logging.error(f"处理醒目留言记录时出错: {e}")

async def run_clients_loop():
    # 批量按 3 秒间隔启动
    for room_id in ROOM_IDS:
        await _start_client(room_id)
        await asyncio.sleep(3)

async def reconnect_scheduler():
    """
    目标：约 3 天全量重连一次。
    - 仅在 03:00~07:00 内执行；
    - 00:00~00:30 禁止重连；
    - 总开播数 > 10 时跳过；
    - 直播中的房间跳过；
    - 每天配额：ceil(N/3)，分散在窗口内逐个执行；
    """
    global RECONNECT_DAILY_STATE
    N = len(ROOM_IDS)
    daily_quota = max(1, math.ceil(N / 3))  # 约 3 天覆盖全部

    while True:
        now = _now()

        # 00:00~00:30 禁止重连
        if now.hour == 0 and now.minute < 30:
            await asyncio.sleep(60)
            continue

        # 跨天重置当日进度
        if RECONNECT_DAILY_STATE["date"] != now.date():
            RECONNECT_DAILY_STATE = {"date": now.date(), "done": set()}
            logging.info(f"[reconnect] 新的一天，重置配额，今日计划重连 ≈ {daily_quota} 个房间")

        # 仅在 03:00~07:00 窗口执行
        if not (4 <= now.hour < 6):
            await asyncio.sleep(300)  # 5 分钟后再看
            continue

        # 总开播数 > 10 -> 跳过本轮
        live_count = sum(1 for v in LAST_STATUS.values() if v == 1)
        if live_count > 10:
            logging.debug(f"[reconnect] 开播数={live_count} > 10，暂不重连")
            await asyncio.sleep(120)
            continue

        done_today = RECONNECT_DAILY_STATE["done"]
        if len(done_today) >= daily_quota:
            # 今日额度已满，降低频率
            await asyncio.sleep(600)
            continue

        # 候选：未达成今日额度、当前不在播、确有客户端的房间
        candidates = [
            rid for rid in ROOM_IDS
            if rid not in done_today
            and LAST_STATUS.get(rid, 0) != 1
            and rid in ROOM_CLIENTS
        ]
        if not candidates:
            await asyncio.sleep(180)
            continue

        # 以“上次重连时间”升序优先（更久未重连者优先），再在前若干个里做少量随机打散
        candidates.sort(key=lambda rid: LAST_RECONNECT.get(rid, datetime.datetime.fromtimestamp(0)))
        top_k = candidates[:min(10, len(candidates))]
        target = random.choice(top_k)

        # 执行重连（单个）
        try:
            await _reconnect_one(target)
            done_today.add(target)
        except Exception as e:
            logging.error(f"[reconnect] 房间 {target} 重连失败: {e}")

        # 两次重连之间：至少 3s，再额外加一点抖动，避免集中
        await asyncio.sleep(3 + random.uniform(0.5, 2.0))

def _now():
    return datetime.datetime.now()

# ------------------ 直播状态 & 粉丝数监视 ------------------
LAST_STATUS: Dict[int, int] = {rid: 0 for rid in ROOM_IDS}
STREAM_STARTS: Dict[int, datetime.datetime] = {}
LIVE_INFO: Dict[int, Dict[str, str]] = {rid: {"live_time": "0000-00-00 00:00:00", "title": ""} for rid in ROOM_IDS}

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)

def _split_and_record(room_id: int, start: datetime.datetime, end: datetime.datetime):
    """将 [start, end] 按自然日切片，并累加到 RoomLiveStats 表中。"""
    cur = start
    while cur < end:
        next_boundary = (
            datetime.datetime.combine(cur.date(), datetime.time.max)
            if cur.date() == end.date()
            else datetime.datetime.combine(cur.date() + datetime.timedelta(days=1), datetime.time.min)
        )
        slice_end = min(end, next_boundary)
        seconds = int((slice_end - cur).total_seconds())
        RoomLiveStats.add_duration(room_id, cur.date(), seconds)
        cur = slice_end

async def monitor_all_rooms_status():
    while True:
        for idx, room_id in enumerate(ROOM_IDS):
            url = f"https://api.live.bilibili.com/room/v1/Room/get_info?room_id={room_id}"
            try:
                async with aiohttp_session.get(
                    url, timeout=5,
                    headers={"User-Agent": USER_AGENT, "Referer": "https://live.bilibili.com"}
                ) as resp:
                    if resp.status != 200:
                        logging.warning(f"[LiveStatus] 房间 {room_id} 接口返回状态 {resp.status}")
                        if resp.status == 412:
                            await asyncio.sleep(2)
                        continue
                    try:
                        payload = await resp.json(content_type=None)
                    except ContentTypeError:
                        text = (await resp.text())[:200]
                        logging.warning(f"[LiveStatus] 房间 {room_id} 返回非 JSON，前 200 字：{text}")
                        continue

                data = payload.get("data") or {}
                raw_status    = int(data.get("live_status", 0))
                status        = 0 if raw_status == 2 else raw_status
                live_time_val = data.get("live_time") or "0000-00-00 00:00:00"
                raw_title     = data.get("title") or ""
                attention     = int(data.get("attention", 0))

                RoomInfo.upsert(room_id, attention=attention)

                prev = LAST_STATUS.get(room_id, 0)
                LAST_STATUS[room_id] = status

                if status == 1:
                    # 解析开播时间
                    try:
                        start_dt = datetime.datetime.strptime(live_time_val, "%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        if str(live_time_val).isdigit():
                            start_dt = datetime.datetime.fromtimestamp(int(live_time_val))
                        else:
                            start_dt = datetime.datetime.now()

                    # 首次上播：建立会话
                    if prev == 0:
                        STREAM_STARTS[room_id] = start_dt
                        sid = LiveSession.start_session(room_id, start_dt, raw_title)
                        if sid:
                            CURRENT_SESSIONS[room_id] = sid
                        logging.info(f"[{room_id}] 上播，开始时间 {start_dt:%F %T}")

                    # 缓存 live_time 和 title
                    LIVE_INFO[room_id]["live_time"] = live_time_val
                    LIVE_INFO[room_id]["title"]     = raw_title

                else:
                    # 真正下播：分片、关闭会话
                    if prev == 1 and room_id in STREAM_STARTS:
                        st = STREAM_STARTS.pop(room_id)
                        end_dt = datetime.datetime.now()
                        _split_and_record(room_id, st, end_dt)
                        # 关闭该场会话
                        sid = CURRENT_SESSIONS.pop(room_id, None)
                        LiveSession.close_session_by_id(sid, end_dt)
                        duration_str = _seconds_to_hms(int((end_dt - st).total_seconds()))
                        logging.info(f"[{room_id}] 下播，时长 {duration_str}")

                    # 清空缓存
                    LIVE_INFO[room_id]["live_time"] = "0000-00-00 00:00:00"
                    LIVE_INFO[room_id]["title"]     = ""

            except Exception as e:
                logging.error(f"[LiveStatus] 房间 {room_id} 状态获取异常: {e}")

            if idx < len(ROOM_IDS) - 1:
                await asyncio.sleep(0.8)

# ------------------ 月底清零（无操作以保留全部历史） ------------------
async def monthly_reset_scheduler():
    while True:
        await asyncio.sleep(60)

# 主入口
async def main():
    init_room_info()
    init_session()
    try:
        await asyncio.gather(
            run_clients_loop(),
            monitor_all_rooms_status(),
            monthly_reset_scheduler(),
            reconnect_scheduler(),
        )
    finally:
        if aiohttp_session:
            await aiohttp_session.close()

# ------------------ Flask API ------------------
app = Flask(__name__)

@app.route("/gift", methods=["GET"])
def get_stats_current_month():
    """
    当月汇总：
      - room_stats_monthly 当月 gift/guard/super_chat
      - room_live_stats 聚合当月直播时长与有效天数
      - 当前月返回实时 live_time/title/status；历史月置空
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

            anchor_name = (session.query(RoomInfo.anchor_name).filter_by(room_id=room_id).scalar()) or ROOM_ANCHORS.get(room_id, "")
            attention   = (session.query(RoomInfo.attention).filter_by(room_id=room_id).scalar()) or 0

            total_sec, eff_days = RoomLiveStats.month_aggregate_for_month(room_id, m)
            live_dur_str = _seconds_to_hms(total_sec)

            info = LIVE_INFO.get(room_id, {})
            live_time_val = info.get("live_time", "0000-00-00 00:00:00")
            title_val     = info.get("title", "")
            status_val    = LAST_STATUS.get(room_id, 0)

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

            anchor_name = (session.query(RoomInfo.anchor_name).filter_by(room_id=room_id).scalar()) or ROOM_ANCHORS.get(room_id, "")
            attention   = (session.query(RoomInfo.attention).filter_by(room_id=room_id).scalar()) or 0

            total_sec, eff_days = RoomLiveStats.month_aggregate_for_month(room_id, m)
            live_dur_str = _seconds_to_hms(total_sec)

            if is_current:
                info = LIVE_INFO.get(room_id, {})
                live_time_val = info.get("live_time", "0000-00-00 00:00:00")
                title_val     = info.get("title", "")
                status_val    = LAST_STATUS.get(room_id, 0)
            else:
                live_time_val = "0000-00-00 00:00:00"
                title_val     = ""
                status_val    = 0

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
            out.append({
                "start_time": r.start_time.strftime("%Y-%m-%d %H:%M:%S"),
                "end_time":   (r.end_time.strftime("%Y-%m-%d %H:%M:%S") if r.end_time else None),
                "title":      r.title,
                "gift":       r.gift,
                "guard":      r.guard,
                "super_chat": r.super_chat,
            })
        return jsonify({"room_id": room_id, "month": m, "sessions": out})
    except SQLAlchemyError as e:
        session.rollback()
        logging.error(f"[get_live_sessions_by_room_month] 查询失败: {e}")
        return jsonify({"error": "数据库查询失败"}), 500
    finally:
        session.close()

if __name__ == "__main__":
    threading.Thread(target=lambda: app.run(host="0.0.0.0", port=4666), daemon=True).start()
    asyncio.run(main())
