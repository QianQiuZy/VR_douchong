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

## 违规直播片段的有效时长

所有场次保留原有 180 秒下播宽限期，宽限期内重开沿用同一个 `session_id`。有效时长按每次实际开播的片段独立计算：开播期间收到 `WARNING`、`CUT_OFF` 或 `ROOM_LOCK` 时，该片段从开播到下播都不计入 `room_live_stats.duration`；同一个合并场次中其他正常片段的时长保留。有效天仍按同一自然日内正常片段累计满 7200 秒计算。礼物、上舰、SC、弹幕、合并场次起止时间和其他场次统计继续按原规则记录。

- `WARNING` 使用事件中的 `roomid` 字段，只作废当前开播片段，不主动下播。状态轮询确认下播后进入宽限期，重新开播的片段恢复正常计时。例如 21:00 下播、21:01 重开，保持原场次 ID；21:00 前的违规片段无效，21:00～21:01 的下播间隔不计时，21:01 后的新片段有效。
- `CUT_OFF`、`ROOM_LOCK` 使用 `room_id` 和毫秒级 `send_time`，在事件时间停止当前片段并进入宽限期。`ROOM_LOCK` 仍遵守平台给出的锁定到期时间；若解除锁定时宽限期尚未结束，可沿用场次；否则在宽限期结束后关闭原场次，之后重开创建新场次。强制下播后，状态接口返回的旧开播时间不会恢复有效计时。
- 正好 180 秒仍可合并，超过 180 秒关闭原场次。重复违规事件只撤回一次当前片段的贡献；跨日、跨月已累计的该片段贡献也一并撤回。较早正常片段的时长不会因后续片段违规被撤回，晚到的旧片段事件也不会作废新片段。

启动时自动为 `live_session` 及已有月归档表补充 `duration_valid`（默认 1）和 `duration_ledger`（可空 TEXT）两列。`duration_valid` 表示当前/最后一个开播片段是否有效；收到违规事件设为 0，宽限期内重新开播设回 1，不能用这个字段直接过滤整个合并场次来汇总有效时长。`duration_ledger` 的 JSON 中，`days` 保存整个合并场次已累计的有效秒数，`segment_days` 保存当前片段可撤回的贡献，`segment_start`/`segment_end` 保存当前片段边界，`end` 防止重复计时。恢复未结束场次时读取这些状态。部署需重启采集程序，并确保数据库账户具备补列权限。

此处理接收实时 WebSocket 事件，**不会自动回放 `downloads/error.log` 或修正部署前的历史违规场次**。部署前已写入的片段没有场次贡献台账，无法自动精确撤回；如需修正这些旧数据，应另行核对日志和数据库快照。

## API Redis 缓存与限流

正式运行默认开启 `API_CACHE_ENABLED=1`。五类 GET 查询只读取 Redis DB2 的完整响应缓存；两个房间管理 POST 保持原有鉴权与写入语义。DB2 从 `REDIS_URL` 自动派生，即使 URL 路径或 `?db=` 指向其他库，也不会与 DB0 付费去重、DB1 鲸鱼指标混用。DB2 仍共享 Redis 进程的内存与持久化，并不是独立内存配额。

- 启动后后台预热全部已知月份及已知/配置房间的查询结果，不阻塞直播采集启动。完整预热完成前 GET 返回 `503`；未知但合法的月份/房间使用缓存空模板，不制造无限缓存键。
- 当前月默认每 10 秒刷新，按读取事务开始前的时间判断快照年龄；超过 30 秒、缓存缺失、数据损坏或 Redis 不可用均返回 `503` 和 `Retry-After: 1`，绝不直接回源 MySQL。时间约定与已有 MySQL `DATETIME` 一致，服务器应使用 `Asia/Shanghai` 本地时间。
- 基础数据、日统计、场次和区间批量读取。SC 按房间计数、最大 ID 及内容校验摘要判断变化，只重读变化房间；每 60 秒重新发现表并校准 SC 数据。未变化的 SC、场次、关注响应复用压缩结果。读取闭播区间是为了及时接收晚到的弹幕重试数据。
- 历史数据默认每月每 300 秒轮转修复；当前刷新优先，归档通知和月累计变化会提前安排历史刷新。历史汇总的最新房间信息直接复用当前基础快照，不重复扫描历史 SC。
- API/缓存 SQL 共用 Redis 滚动预算，任意滚动秒最多 9 条；连接设置、元数据、事务控制和重试 SQL 也计入。直播采集、归档、启动 schema 检查不在这个预算中。缓存构建使用额外一个专用读连接，原 `QueuePool` 的 `5 + 10`、30 秒等待、1800 秒回收保持不变。
- 同一 IP 对七个 API 合计每滚动秒最多放行 20 次，跨进程共享，超额返回 `429` 和 `Retry-After: 1`。这不是服务全局 20 次；另有每进程 32 个在途请求容量保护，过载返回 `503`，避免慢客户端持有过多大响应。
- 沿用 EO → 本机 Nginx → `127.0.0.1:4666`。Uvicorn 只信任回环代理；直接读取代理头时优先 `EO-Connecting-IP`，其次 `X-Real-IP`，不盲取 XFF 首项。源站必须限制 EO 回源，4666 端口不应对公网开放。Python 只能限流回源请求，EO 缓存命中不计入 Python 额度。
- 缓存按月分代原子发布，旧代短暂保留后回收，压缩 JSON 支持 gzip 和普通响应。`API_CACHE_MAX_BYTES` 默认 96 MiB，是应用缓存键预算；建议保留已有 Redis 持久化和 `noeviction` 策略，不允许全局淘汰 DB0/DB1 数据。不要长期关闭 API 缓存：`API_CACHE_ENABLED=0` 仅用于离线诊断，旧 GET 会直接访问数据库且不受缓存预算保护。
- 不记录正常预热/刷新/跳过日志；仅记录异常，按故障类别每分钟最多一次，日志不输出 SQL 参数、响应内容或凭据。内部指标保存在 `app.state.api_cache.metrics`。原业务、访问和连接池日志保持独立。

配置项及默认值见 `.env.example`。数据库 SQL、IP 请求和当前缓存年龄上限分别硬限制为不超过 9、20、30；填更大值不会放宽。当前正常响应及 429/503 使用 `Cache-Control: no-store`，避免 CDN 缓存错误响应或突破当前快照的新鲜度边界。

### 开发验证

```bash
python -m venv .venv
.venv/bin/python -m pip install -r requirements-dev.txt
.venv/bin/python -m ruff check .
.venv/bin/python -m pytest -q
```

默认测试使用隔离环境和带 Lua 支持的 fakeredis，不读取真实 `.env`、不连接 B 站；原报表契约测试明确运行离线旧路径，新测试单独启用缓存路径。真实版本集成测试仅在提供专用容器端口后运行，数据库名称固定为 `vr_cache_test`，不要指向生产服务：

```bash
VR_CACHE_TEST_MYSQL_PORT=<本机专用 MySQL 5.7.44 测试端口> \
VR_CACHE_TEST_REDIS_PORT=<本机专用 Redis 8.0.5 测试端口> \
.venv/bin/python -m pytest -q
```

2026-10-03 本地验证：`ruff check .` 通过；启用 MySQL 5.7.44 / Redis 8.0.5 隔离容器的完整 pytest 为 **324 passed**（9 条既有弃用警告）。使用 `hareigift.sql` 快照的另一次只读模型测试覆盖 15 个月、74 个房间：全量预热 19.0 秒，Redis 分代缓存键约 5.1 MiB，两次当前月刷新分别 1.75 / 1.82 秒；14 组新旧响应一致，缓存 HTTP 查询 SQL 为 0，预算化 SQL 滚动秒峰值为 9，同一 IP 25 次快速请求返回 20 个 200 和 5 个 429。这些是本地快照测量，不是生产 2c2g 整机并发及实时采集负载保证；部署后仍需观察刷新年龄、Redis 总内存和异常日志。

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
