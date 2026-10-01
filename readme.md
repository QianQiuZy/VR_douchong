# VR斗虫自搭版

VR斗虫排行榜，每月最后一天24点归档

> host：`https://vr.qianqiuzy.cn` `https://psp.qianqiuzy.cn`

## 准备工作

1. **下载代码:**
   ```bash
   git clone https://github.com/QianQiuZy/VR_douchong
   ```

2. **创建虚拟环境 (不强制但是建议):**
   ```bash
   python3 -m venv venv
   source venv/bin/activate  # Windows端使用 `venv\Scripts\activate`
   ```

3. **安装依赖:**
   ```bash
   pip install -r requirements.txt -i http://mirrors.cloud.tencent.com/pypi/simple
   ```

4.**安装数据库**

   [windows](https://downloads.mysql.com/archives/get/p/25/file/mysql-installer-community-8.0.40.0.msi)
   
   ```bash
   CREATE DATABASE db;
   CREATE USER 'user'@'localhost' IDENTIFIED BY 'password'; # 替换账户密码
   GRANT ALL PRIVILEGES ON db.* TO 'user'@'localhost'; #替换数据库名和账户名
   FLUSH PRIVILEGES;
   EXIT;
   ```

   linux
   
   ```bash
   sudo apt install mysql-server # 安装数据库
   sudo mysql -u root -p # 登入数据库
   CREATE DATABASE db;
   CREATE USER 'user'@'localhost' IDENTIFIED BY 'password'; # 替换账户密码
   GRANT ALL PRIVILEGES ON db.* TO 'user'@'localhost'; #替换数据库名和账户名
   FLUSH PRIVILEGES;
   EXIT;
   ```

5.**配置环境与房间**

   将 `.env.example` 复制为 `.env`，并填写数据库与 B 站 Cookies 等关键配置。
   
   房间列表与主播映射统一存放在 `rooms.json`（字段：`room_ids`、`room_anchors`）。
   
   也可以通过 API 动态新增或删除房间（FastAPI + Uvicorn）：
   - `POST /add/room`：payload 需包含 `room_id` 与 `room_anchors`
   - `POST /delete/room`：payload 需包含 `room_id` 与 `room_anchors`

6.**运行**
   ```bash
   python main.py
   ```
   运行后 FastAPI 会通过 Uvicorn 在 `APP_HOST:APP_PORT` 对外提供接口。
    主进程每 5 分钟以 INFO 级别记录一次连接池状态；服务退出时该观察任务随主运行协程取消。

    重启后先查询配置房间的直播状态（每批最多 100 个 UID），按原房间顺序启动所有在播房间；等待这些房间收到成功的弹幕服务器鉴权响应后，再启动未开播或轮播房间。成功响应中合法缺失的“无直播间” UID 归入后组，不会导致无限重试；状态查询失败会重试。在播房间初始化或鉴权失败时，不会提前放行未开播房间。两组内部仍保留原来的 3 秒启动间隔。排序快照不提前修改 `LAST_STATUS`，开播场次仍由状态监听器创建。每日重连只处理已启动的客户端，避免在启动阶段绕过顺序或重复创建连接。

    月末归档以场次开始月份为准：跨月仍在直播的场次及其 15 分钟数据保留在主表，确认下播后再整体迁移。月归档调度器每 5 分钟补扫历史场次，无需等到下个月或重启。自动归档保留 10 分钟结束缓冲，每轮先固定已经结束的候选场次，再等待结束快照队列排空、重试待写场次弹幕；等待期间才确认下播的场次留到下轮，避免旧结束时间绕过保护。队列等待超过 120 秒或场次弹幕仍未写入时，本轮暂缓并记录告警，下轮继续。因此正常情况下跨月场次在结束后约 10～15 分钟归档；队列积压或数据库故障时可能延后。父表、15 分钟子表使用相同的候选场次和结束截止时间，先归档子表，再归档父表；手工归档 CLI 不使用自动结束缓冲。

    `room_live_stats` 的礼物、上舰和 SC 金额列在 ORM 中分别声明为 `DECIMAL(20,1)`、`DECIMAL(20,0)`、`DECIMAL(20,0)`。启动时只补不存在的列，**不会修改现有表的列类型，也不会截断已有金额**。升级旧库须在数据库备份后手动迁移主表及已有 `room_live_stats_YYYYMM` 归档表：先用 `TRUNCATE(gift, 1)` 舍去礼物百分位及以后的小数，再修改列类型。MySQL 类型转换会重建表并阻塞该表写入，请在停止相关写入的维护窗口执行；只改 ORM 或重启服务不能修复旧列。

## 2026 年 9 月一次性场次修复

`repair_live_sessions_202609.sql` 针对已有生产快照中的 9 月残留数据，不能用日常补归档代替其中的异常场次修复。该快照中，24 条正常跨月下播场次及 396 条区间未及时归档；另有场次 `17436` 在房间锁定后仍未关闭，残留 80 条区间。生产日志在 `2026-09-30 18:00:20` 将 `ROOM_LOCK` 记为未知命令；当前代码已有该事件处理，部署时须包含它。

执行顺序：**完整备份生产数据库，停止采集程序，先运行 SQL 修复，再启动更新后的程序**。在 MySQL 客户端选择正确数据库，先做 dry run：

```sql
USE hareigift;
SOURCE /absolute/path/to/VR_douchong/repair_live_sessions_202609.sql;
```

原始快照预期输出为 `committed=0, archived_sessions=25, archived_buckets=400, removed_zombie_buckets=76`。核对无误后执行：

```sql
SET @vr_apply_changes=TRUE;
SOURCE /absolute/path/to/VR_douchong/repair_live_sessions_202609.sql;
```

提交前，SQL 将原始 25 条场次、476 条区间写入 `vr_repair_20261001_sessions` 和 `vr_repair_20261001_buckets` 备份表；删除 76 条锁定后仅有同接采样的异常区间，裁剪最后一个跨越锁定时刻的区间，并关闭异常场次。无法重建的同接数据保留为 `NULL`，不会伪造为 0。礼物、上舰、SC 总额及 10 月场次不变。重复执行不再迁移或删除数据。若生产库已变化导致身份、数据活动或键冲突检查失败，应停止并检查，不要删除保护条件。
