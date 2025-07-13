# douchong.py

import asyncio
import http.cookies
import logging
import datetime
from typing import Optional, Tuple, Dict
import threading
from aiohttp import ContentTypeError

import aiohttp
# blivedm需要使用特殊版本，不能直接pip install blivedm，需要使用https://github.com/xyself/blivedm版本
import blivedm
import blivedm.models.web as web_models
from flask import Flask, jsonify
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    Float,
    String,
    Date,
    PrimaryKeyConstraint,
    and_,
)
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import SQLAlchemyError

# ------------------ 日志 ------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# ------------------ 数据库 ------------------
# 这里要自己填自己数据库对应的账户密码
DB_CONFIG = {
    "host": "localhost",
    "user": "user",
    "password": "password",
    "db": "db",
    "port": 3306,
}
engine = create_engine(
    f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
    f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['db']}",
    echo=False,
    pool_recycle=1800,
    pool_pre_ping=True,
)
# 只保留 Session 工厂，不再在模块层面创建一个全局 db_session
Session = sessionmaker(bind=engine)
Base = declarative_base()

class RoomInfo(Base):
    """房间基础信息：主播名称 & 粉丝数"""
    __tablename__ = "room_info"
    room_id     = Column(Integer, primary_key=True)
    anchor_name = Column(String(100), nullable=False)
    attention   = Column(Integer, default=0, nullable=False)  # 粉丝数

    @classmethod
    def upsert(cls, room_id: int, anchor_name: Optional[str] = None, attention: Optional[int] = None):
        """
        如果记录存在，则更新 anchor_name 和/或 attention；
        如果不存在，则插入新记录。anchor_name 和 attention 均可单独更新。
        """
        session = Session()
        try:
            info = session.query(cls).filter_by(room_id=room_id).first()
            if info:
                if anchor_name is not None:
                    info.anchor_name = anchor_name
                if attention is not None:
                    info.attention = attention
            else:
                # 插入新记录时，如果 anchor_name 缺失则填空串
                new_anchor = anchor_name if anchor_name is not None else ""
                new_attention = attention if attention is not None else 0
                info = cls(room_id=room_id, anchor_name=new_anchor, attention=new_attention)
                session.add(info)
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            logging.error(f"[RoomInfo] upsert 失败: {e}")
        finally:
            session.close()

class RoomStats(Base):
    """月度累计：礼物 / 上舰 / 醒目留言金额"""
    __tablename__ = "room_stats"
    room_id     = Column(Integer, primary_key=True)
    gift        = Column(Float, default=0.0, nullable=False)
    guard       = Column(Float, default=0.0, nullable=False)
    super_chat  = Column(Float, default=0.0, nullable=False)

    def __repr__(self):
        return (
            f"<RoomStats {self.room_id}: "
            f"gift={self.gift}, guard={self.guard}, super_chat={self.super_chat}>"
        )

    @classmethod
    def update_or_create(
        cls,
        room_id: int,
        gift: float = 0.0,
        guard: float = 0.0,
        super_chat: float = 0.0,
        retries: int = 3,
    ):
        """
        每次都新建一个 Session，插入或累加指定房间的礼物、舰长、醒目留言金额。
        """
        for attempt in range(retries):
            session = Session()
            try:
                stats = session.query(cls).filter_by(room_id=room_id).first()
                if stats:
                    stats.gift += gift
                    stats.guard += guard
                    stats.super_chat += super_chat
                else:
                    stats = cls(
                        room_id=room_id,
                        gift=gift,
                        guard=guard,
                        super_chat=super_chat,
                    )
                    session.add(stats)
                session.commit()
                return stats
            except SQLAlchemyError as e:
                session.rollback()
                session.close()
                if attempt < retries - 1:
                    logging.warning(f"[RoomStats] 第 {attempt+1} 次尝试失败，重试：{e}")
                    continue
                else:
                    logging.error(f"[RoomStats] 最终失败: {e}")
                    raise
            finally:
                if session.is_active:
                    session.close()

    @classmethod
    def reset_stats(cls):
        """
        月末将所有房间的礼物、舰长、SC 数据归零，并在日志中输出归零前的累计值。
        """
        session = Session()
        try:
            stats_list = session.query(cls).all()
            for s in stats_list:
                total = s.gift + s.guard + s.super_chat
                logging.info(
                    f"[清零] 房间 {s.room_id} 本月总流水：礼物={s.gift:.2f} 舰长={s.guard:.2f} SC={s.super_chat:.2f} 总计={total:.2f}"
                )
                s.gift = 0.0
                s.guard = 0.0
                s.super_chat = 0.0
            session.commit()
            logging.info("已归零所有房间的礼物统计。")
        except SQLAlchemyError as e:
            session.rollback()
            logging.error(f"[RoomStats] 重置统计数据时出错: {e}")
        finally:
            session.close()

class RoomLiveStats(Base):
    """按自然日累计当日直播秒数"""
    __tablename__ = "room_live_stats"
    room_id  = Column(Integer, nullable=False)
    date     = Column(Date, nullable=False)       # YYYY-MM-DD
    duration = Column(Integer, default=0, nullable=False)  # 秒
    __table_args__ = (
        PrimaryKeyConstraint("room_id", "date", name="pk_room_date"),
    )

    @classmethod
    def add_duration(cls, room_id: int, date_: datetime.date, seconds: int):
        """
        将给定房间的 date_ 当日时长累加 seconds 秒。若当天不存在记录则插入新行。
        """
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
                if session.is_active:
                    session.close()
        logging.error("[RoomLiveStats] add_duration 最终失败，数据可能不完整。")

    @classmethod
    def month_aggregate(cls, room_id: int) -> Tuple[int, int]:
        """
        返回当月(从当月1日到今天)该房间累计直播秒数和有效天数（>=7200秒）。
        """
        session = Session()
        try:
            today = datetime.date.today()
            month_begin = today.replace(day=1)
            rows = (
                session.query(cls)
                .filter(
                    and_(
                        cls.room_id == room_id,
                        cls.date >= month_begin,
                    )
                )
                .all()
            )
            total_sec = sum(r.duration for r in rows)
            effective_days = sum(1 for r in rows if r.duration >= 7200)
            return total_sec, effective_days
        except SQLAlchemyError as e:
            logging.error(f"[RoomLiveStats] month_aggregate 读取失败: {e}")
            return 0, 0
        finally:
            session.close()

    @classmethod
    def reset_month(cls):
        """
        月底清空所有房间的本月直播时长数据，并在日志中输出归零前的累计时长。
        """
        session = Session()
        try:
            rows = session.query(cls).all()
            for row in rows:
                h, m, s = row.duration // 3600, (row.duration % 3600) // 60, row.duration % 60
                logging.info(
                    f"[清零] 房间 {row.room_id} 本月直播总时长：{h:02d}:{m:02d}:{s:02d}（{row.duration} 秒）"
                )
            deleted = session.query(cls).delete()
            session.commit()
            logging.info(f"已清空本月直播时长记录（共 {deleted} 条）。")
        except SQLAlchemyError as e:
            session.rollback()
            logging.error(f"[RoomLiveStats] reset_month 清空失败: {e}")
        finally:
            session.close()

# 确保新旧表存在
Base.metadata.create_all(engine)

# ------------------ 直播间配置 ------------------
# 这里是VR所有主播的直播间，自己根据自己需要监控的自己增减
ROOM_IDS = [1820703922,21696950,1947277414,21756924,23771189,1967216004,23805029,23260993,23771092,23771139,25788785,21452505,21224291,21484828,25788858,23550749,22470216,22605464,1967215387,1967212929,31368705,80397,282208,30655190,22778610,27628030,31368697,3032130,21403601,23017349,21457197,30655198,31368686,23770996,1861373970,21672023,784734,23017343,30655213,21613356,30655179,27627985,1766907940,27628009,22389319,26966466,30655172,1766909591,21696957,21763344,23550773,23805059,25788830,26966452,30655203,32638817,32638818,6068126,22359795,6374209]
SESSDATA = "这里自己填，cookies获取方法自己查询"
ROOM_ANCHORS = {1820703922:"花礼Harei",21696950:"阿萨AzA",1947277414:"泽音Melody",21756924:"雪绘Yukie",23771189:"恬豆发芽了",1967216004:"三理Mit3uri",23805029:"雾深Girimi",23260993:"瑞娅_Rhea",23771092:"又一充电中",23771139:"沐霂是MUMU呀",25788785:"岁己SUI",21452505:"七海Nana7mi",21224291:"安堂いなり_official",21484828:"轴伊Joi_Channel",25788858:"莱恩Leo",23550749:"尤格Yog",22470216:"悠亚Yua",22605464:"千幽Chiyuu",1967215387:"命依Mei",1967212929:"沐毛Meme",31368705:"米汀Nagisa",80397:"阿梓从小就很可爱",282208:"诺莺Nox",30655190:"弥月Mizuki",22778610:"勾檀Mayumi",27628030:"未知夜Michiya",31368697:"雪烛Yukisyo",3032130:"舒三妈Susam",21403601:"艾因Eine",23017349:"桃星Tocci",21457197:"中单光一",30655198:"蜜言Mikoto",31368686:"帕可Pako",23770996:"梨安不迷路",1861373970:"妮慕Nimue",21672023:"弥希Miki",784734:"宫园凛RinMiyazono",23017343:"吉吉Kiti",30655213:"漆羽Urushiha",21613356:"惑姬Waku",30655179:"入福步Ayumi",27627985:"哎小呜Awu",1766907940:"点酥Susu",27628009:"初濑Hatsuse",22389319:"千春_Chiharu",26966466:"栞栞Shiori",30655172:"离枝Richi",1766909591:"桃代Momoka",21696957:"度人Tabibito",21763344:"沙夜_Saya",23550773:"暴食Hunger",23805059:"希维Sybil",25788830:"江乱Era",26966452:"伊舞Eve",30655203:"晴一Hajime",32638817:"鬼间Kima",32638818:"阿命Inochi",6068126:"糯依Noi",22359795:"菜菜子Nanako",6374209:"小可学妹"}

aiohttp_session: Optional[aiohttp.ClientSession] = None

def init_room_info():
    """
    预先将 ROOM_ANCHORS 中的(房间ID, 主播名)写入 room_info 表，粉丝数字段为默认 0。
    """
    for rid, name in ROOM_ANCHORS.items():
        RoomInfo.upsert(rid, anchor_name=name)

def init_session():
    """
    初始化 aiohttp Session，并附带 SESSDATA Cookie。
    """
    cookies = http.cookies.SimpleCookie()
    cookies["SESSDATA"] = SESSDATA
    cookies["SESSDATA"]["domain"] = "bilibili.com"
    global aiohttp_session
    connector = aiohttp.TCPConnector(ssl=False)
    aiohttp_session = aiohttp.ClientSession(connector=connector)
    aiohttp_session.cookie_jar.update_cookies(cookies)

# ------------------ blivedm 事件处理 ------------------
class MyHandler(blivedm.BaseHandler):
    def _on_heartbeat(self, client, message):
        pass

    def _on_danmaku(self, client, message):
        pass

    def _on_gift(self, client, message):
        try:
            value = message.total_coin / 1000
            RoomStats.update_or_create(room_id=client.room_id, gift=value)
            logging.info(
                f"[{client.room_id}] {message.uname} 赠送 {message.gift_name}×{message.num} ({value:.2f})"
            )
        except Exception as e:
            logging.error(f"处理礼物记录时出错: {e}")

    def _on_user_toast_v2(self, client, message):
        try:
            value = (message.price * message.num) / 1000
            RoomStats.update_or_create(room_id=client.room_id, guard=value)
            logging.info(
                f"[{client.room_id}] {message.username} 上舰 lvl={message.guard_level} ({value:.2f})"
            )
        except Exception as e:
            logging.error(f"处理舰长记录时出错: {e}")

    def _on_super_chat(self, client, message):
        try:
            value = message.price
            RoomStats.update_or_create(room_id=client.room_id, super_chat=value)
            logging.info(
                f"[{client.room_id}] SC ¥{message.price:.2f} {message.uname}: {message.message}"
            )
        except Exception as e:
            logging.error(f"处理醒目留言记录时出错: {e}")

async def run_clients_loop():
    """
    依次连接所有 ROOM_IDS 中的房间，首次连接间隔 3 秒。后续只保留所有连接并发监听。
    """
    clients = []
    for room_id in ROOM_IDS:
        client = blivedm.BLiveClient(room_id, session=aiohttp_session)
        client.set_handler(MyHandler())

        client.start()
        logging.info(f"已连接房间 {room_id}")
        clients.append(client)
        await asyncio.sleep(3)  # 首次连接间隔 3 秒

    # 等待所有连接协程结束
    await asyncio.gather(*(client.join() for client in clients))

# ------------------ 直播状态 & 粉丝数监视 ------------------
LAST_STATUS: Dict[int, int] = {rid: 0 for rid in ROOM_IDS}
STREAM_STARTS: Dict[int, datetime.datetime] = {}
LIVE_INFO: Dict[int, Dict[str, str]] = {rid: {"live_time": "0000-00-00 00:00:00", "title": ""}for rid in ROOM_IDS}

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0.0.0 Safari/537.36"
)

def _split_and_record(room_id: int, start: datetime.datetime, end: datetime.datetime):
    """
    将 [start, end] 按自然日切片，并累加到 RoomLiveStats 表中。
    """
    cur = start
    while cur < end:
        next_boundary = (
            datetime.datetime.combine(cur.date(), datetime.time.max)
            if cur.date() == end.date()
            else datetime.datetime.combine(
                cur.date() + datetime.timedelta(days=1), datetime.time.min
            )
        )
        slice_end = min(end, next_boundary)
        seconds = int((slice_end - cur).total_seconds())
        RoomLiveStats.add_duration(room_id, cur.date(), seconds)
        cur = slice_end

def _seconds_to_hms(sec: int) -> str:
    """
    将秒数转换为 HH:MM:SS 格式字符串。
    """
    h, r = divmod(sec, 3600)
    m, s = divmod(r, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

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
                        if live_time_val.isdigit():
                            start_dt = datetime.datetime.fromtimestamp(int(live_time_val))
                        else:
                            start_dt = datetime.datetime.now()

                    # 仅在首次上播时记录日志和分片起点
                    if prev == 0:
                        STREAM_STARTS[room_id] = start_dt
                        logging.info(f"[{room_id}] 上播，开始时间 {start_dt:%F %T}")

                    # 缓存 live_time 和 title
                    LIVE_INFO[room_id]["live_time"] = live_time_val
                    LIVE_INFO[room_id]["title"]     = raw_title

                else:
                    # 真正下播时做分片并记录日志
                    if prev == 1 and room_id in STREAM_STARTS:
                        st = STREAM_STARTS.pop(room_id)
                        end_dt = datetime.datetime.now()
                        _split_and_record(room_id, st, end_dt)
                        duration_str = _seconds_to_hms(int((end_dt - st).total_seconds()))
                        logging.info(f"[{room_id}] 下播，时长 {duration_str}")

                    # 清空缓存
                    LIVE_INFO[room_id]["live_time"] = "0000-00-00 00:00:00"
                    LIVE_INFO[room_id]["title"]     = ""

            except Exception as e:
                logging.error(f"[LiveStatus] 房间 {room_id} 状态获取异常: {e}")

            # 间隔 0.8 秒
            if idx < len(ROOM_IDS) - 1:
                await asyncio.sleep(0.8)

# ------------------ 月底清零 ------------------
async def monthly_reset_scheduler():
    """
    每天检查一次，如果已到当月最后一天的 23:59，则触发月末清零逻辑：
      1. 清零 RoomStats
      2. 清零 RoomLiveStats
      3. 如果某房间仍在直播，则将其 start_time 推到次月 0:00
    """
    global STREAM_STARTS
    while True:
        now = datetime.datetime.now()
        next_day = now + datetime.timedelta(days=1)
        is_last_day = (next_day.day == 1)
        if is_last_day and now.hour == 23 and now.minute == 59:
            # 清零金额
            RoomStats.reset_stats()
            # 清空直播时长
            RoomLiveStats.reset_month()
            # 若正在直播，将起点挪到次月 0:00
            for room_id, start_dt in list(STREAM_STARTS.items()):
                STREAM_STARTS[room_id] = datetime.datetime.combine(next_day.date(), datetime.time.min)
                logging.info(f"[LiveMonitor] 房间 {room_id} 跨月直播，起点已重置为次月 0:00")
            # 防止短时间重复触发
            await asyncio.sleep(60)
        else:
            await asyncio.sleep(30)

# 主入口，此部分不要动
async def main():
    init_room_info()
    init_session()
    try:
        await asyncio.gather(
            run_clients_loop(),
            monitor_all_rooms_status(),
            monthly_reset_scheduler(),
        )
    finally:
        if aiohttp_session:
            await aiohttp_session.close()

app = Flask(__name__)

@app.route("/gift", methods=["GET"])
def get_stats():
    results = []
    session = Session()
    try:
        for room_id in ROOM_IDS:
            # 查询 RoomStats
            stats = session.query(RoomStats).filter_by(room_id=room_id).first()
            gift_val = stats.gift if stats else 0.0
            guard_val = stats.guard if stats else 0.0
            sc_val = stats.super_chat if stats else 0.0

            # 查询 RoomInfo 中的 anchor_name 与 attention
            anchor_name = session.query(RoomInfo.anchor_name).filter_by(room_id=room_id).scalar() or ""
            attention = session.query(RoomInfo.attention).filter_by(room_id=room_id).scalar() or 0

            # 查询当月累计直播时长与有效天数
            total_sec, eff_days = RoomLiveStats.month_aggregate(room_id)
            live_dur_str = _seconds_to_hms(total_sec)
            info = LIVE_INFO.get(room_id, {})
            live_time_val = info.get("live_time", "0000-00-00 00:00:00")
            title_val     = info.get("title", "")

            results.append({
                "room_id": room_id,
                "anchor_name": anchor_name,
                "attention": attention,
                "status": LAST_STATUS.get(room_id, 0),
                "gift": gift_val,
                "guard": guard_val,
                "super_chat": sc_val,
                "live_duration": live_dur_str,
                "effective_days": eff_days,
                "live_time": live_time_val,
                "title": title_val,
            })
        return jsonify(results)
    except SQLAlchemyError as e:
        session.rollback()
        logging.error(f"[get_stats] 数据库查询出错: {e}")
        return jsonify({"error": "数据库查询失败"}), 500
    finally:
        session.close()

if __name__ == "__main__":
    threading.Thread(target=lambda: app.run(host="0.0.0.0", port=4666),daemon=True,).start()
    asyncio.run(main())