# 历史问题与修复记录

原 README 中的历史问题、一次性修复和历史测量集中保存在这里。下列结论针对记录时的代码、日志或数据库快照；部署前应重新核对实际源码和数据，不能据此自动修改生产数据库。

## 已有问题与当前处理

| 问题 | 当前处理与边界 | 相关实现 |
| --- | --- | --- |
| 重启时未开播房间抢先建立连接 | 先按批次查询状态，等待在播房间真正鉴权成功，再启动其余房间；两组保留 3 秒间隔 | `app/live_startup.py`、`app/live_client.py` |
| 跨月未结束场次过早归档或结束后留在主表 | 按开始月份归档，结束缓冲后补扫；区间先于场次归档，排空结束快照并重试待写弹幕 | `app/bootstrap.py`、`app/archive_service.py` |
| 旧数据库金额列精度与 ORM 不一致 | 启动只补缺失列，不修改旧列类型；需备份并在维护窗口手工迁移主表和归档表 | `app/models/aggregates.py`、[运行约定](./runtime.md) |
| `ROOM_LOCK` 未处理，状态 API 仍显示在播 | 当前处理强制下播、锁定到期及旧开播时间保护；仍保留 180 秒宽限期 | `app/room_lock_events.py`、`app/monitoring_jobs.py` |
| 违规片段污染有效时长 | 台账只撤回当前片段贡献；宽限期内重开沿用场次并开启正常片段；部署前无台账数据不能自动精确修正 | `app/repositories/session_duration.py` |
| V2 礼物重复字段被当作单个礼物 | field 10 按列表解码、整包验证后逐条回调；普通多数量礼物按当前归一化规则入账 | `app/blivedm/models/web.py`、[当前解析](./parsing.md) |
| HTTP 查询直接回源放大数据库负载 | 默认 GET 仅读 Redis DB2，缺失、超龄或故障返回 503；与 IP 429 限流区分 | `app/api_cache*.py`、[API 文档](./api.md) |
| 访客进房只需名单 UID 且应排除主播本人 | 老/V2 事件共用名单过滤与有界写入队列；房主未知也跳过；不回补断线或部署前事件 | `app/event_ingestion.py`、`app/repositories/entries.py` |

## 下播结束快照提前采集（2026-10-31，1.0.5）

原实现等待 180 秒下播宽限期结束才将结束守护、粉丝团和关注数采集任务放入队列，导致结束快照混入宽限期内的变化。现改为首次发现下播时并发请求并缓存，确认场次结束后写入当次快照；重开取消并丢弃旧快照，再次下播重新采集，重复通知不重采。各字段采集失败留空，不延后补采；自动归档等待未完成的结束快照。普通下播仍由 HTTP 状态轮询发现，强制下播仍按 WS 事件处理，180 秒合并规则不变。内存快照的重启边界见[运行约定](./runtime.md)。

版本同步至 1.0.5，随用户调整的 `config/rooms.json` 一并交付；依赖版本不变。

本地 Python 3.13.11 独立环境验证：全量离线测试 **462 passed、7 skipped**；Ruff、项目元数据/语法/JSON、依赖兼容和固定 uv 0.11.7 锁文件检查通过。新增回归覆盖即时缓存与延迟写入、180 秒边界、重开与取消、重复下播、字段失败与零值、重启快照缺失和归档等待。七项专用 MySQL/Redis 集成测试未配置服务而跳过，未连接真实 B 站或部署线上服务。

## HTTP Brotli 断连与 PSP 一次性补录（2026-10-24，版本保持 1.0.4）

房间初始化在 `_init_host_server()` 读取 HTTP `Content-Encoding: br` 响应时失败，异常为 `TypeError: process() takes exactly 1 argument (2 given)`，随后包装成 `ContentEncodingError` / `ClientPayloadError`。新版 aiohttp 调用带输出长度限制的解压接口，旧 Brotli 不支持该参数；本地用真实 aiohttp 3.14.5 / Brotli 1.1.0 复现相同异常，Brotli 1.2.0 通过相同调用。线上实际包版本未取得，不能由锁文件推定。

本次依赖修复：Brotli 下限由 1.1 收紧为 1.2，锁定版本仍为 1.2.1，不改业务逻辑或内置 blivedm；项目版本保持 1.0.4。标准检查通过 **449 passed、7 skipped**，其中七项专用 MySQL/Redis 集成测试未配置服务而跳过。宝塔需在实际启动解释器中同步依赖并重启，若安装了优先加载的 brotlicffi，也需核对其兼容性。

用户提供 `Downloads/hareigift.sql`（2026-10-24 11:07 导出）用于核对 PSP 缺失数据。北京时间 **2026-10-23 06:00 至 2026-10-24 06:00** 涉及 **19 个 PSP 房间、23 场，场次 ID 6600–6622**：已有起止时间、有效时长、同接与开下播快照保留，但事件金额、弹幕、付费人数和对应区间数据均为零。VR 侧无此故障，不在补录范围。

对照 `config/psp_rooms.json`、`config/entry_users.json` 的名称/UID，查询 danmakus 频道和逐条记录；23 个来源均选择**非合并、已结束且标记完整**的版本，校验 UID、房间和时间，每场仅选一个版本。例如这是亦枝YY使用[用户指定的非合并版本](https://danmakus.com/live/cb74e906-5cd4-4b4e-9d9c-0e28dcc996ba)。场次统计按原起止时间截取，排除下播后 338 条弹幕，不把重复文本或同秒连送误作重复记录删除。

| 可恢复指标 | 本次来源核对量 |
| --- | ---: |
| 弹幕总数 | 95,569 |
| 礼物流水 | 23,756.8 元 |
| 上舰流水 | 10,266 元 |
| SC 流水 / 明细 | 5,418 元 / 483 条 |
| 总流水 | 39,440.8 元 |
| 盲盒数量 / 盈亏 | 1,097 / +1,696.4 元（16,964 角） |
| 名单访客入场 | 41 条 |

一次性工具和已下载数据位于本机 `Downloads/psp_backfill_20261023/`，服务端上传包为 `Downloads/psp_backfill_20261023.zip`，均在仓库外，不纳入常规采集或发布。`repair_psp.py` 默认只读预览；使用实际 PSP `.env`、停止 PSP 采集进程并备份后加 `--apply` 写入。修复现有场次、15 分钟区间、日/月金额与盲盒汇总、SC/名单入场；原时长、同接、快照及时间窗外数据保留。付费人数与原 Redis UID 集合求并集，SC 钢镚去重沿用按日集合；Redis DB1 追加缺失收入/UID 归属和鲸鱼统计原始值，保留 DB2 隐藏状态，重启后由启动预热刷新 API 缓存。

脚本检查目标场次仍为零、身份/边界吻合，以及日/月付费集合与 SQL 人数一致，发现冲突则中止。执行前保存行快照和必要 Redis 原值；MySQL 业务更新与 `psp_backfill_20261023` 台账同事务提交，Redis DB0/DB1 分别使用原子标记并记录进度，支持中断重跑，SQL 和 Redis 均不重复累加。台账 DDL 只在实际写入时创建辅助表，不改业务表结构；不提供跳过核对的强制补录。

**不可恢复边界：** danmakus 导出不含每条弹幕当时的舰长/提督/总督身份。只补总数，受影响场次/有记录区间的四类身份计数为 `NULL`，月分类保留原已采集部分，不能当作完整分类；不将未知身份全部算普通用户。来源自身漏记或开始录制前的消息也无法恢复。

本地对 SQL 副本使用运行时核验的 **MySQL 5.7.44 / Redis 8.0.5** 临时容器，通过 **8 类验证**：只读预览；已有统计/UID 集合冲突拒绝；SQL 中断回滚；Redis 中断续跑；逐条来源与场次/区间/日月/明细/鲸鱼一致；保护原字段、无关表、VR键及 DB2；重复执行幂等；Redis 已执行但 SQL 进度未保存时安全重试。旧 Redis UID 使用合成交集夹具，未读取线上 Redis。**未在生产执行补录**；实际服务器原集合是否完整仍须预览检查，不能将本地结果描述成线上数据已恢复。本次补充历史文档，版本不递增。

## 临时隐藏与依赖更新验证（2026-10-23，1.0.4）

新增两个只接收 JSON payload 房间号的管理 POST，隐藏状态存入独立 Redis 集合，所有公开 GET 在输出前统一过滤；当前月、历史月、未知月份空模板、访客 UID 和最近窗口均覆盖。完整缓存、房间配置和采集写入继续保留，恢复不重建连接或清除历史数据。

本次更新 FastAPI 0.161.0、Starlette 1.9.0 及兼容依赖；详细清单与剩余弃用警告见 [开发规范](./development.md)。在项目隔离环境中，标准 `scripts/check.sh` 通过依赖一致性、Ruff、元数据/语法与离线回归：**448 passed、7 skipped**。随后使用经过版本核验的临时 **MySQL 5.7.44 / Redis 8.0.5** 服务单独运行集成套件：**7 passed**，包括真实 MySQL 持续写入、Redis 完整缓存刷新与恢复后的数据展示。未启动真实 B 站采集连接或部署到线上主机。

全量运行发现旧离线测试没有默认隔离指标 Redis 客户端，新增礼物场景会影响后续查询测试。默认夹具现为每例隔离付费人数与鲸鱼指标客户端，显式集成夹具仍连接专用服务；已核对并清除本次写入本机 Redis 的三个合成测试键，未批量清库。新版 FastAPI 插入内部中间件后，旧容量测试改为按实际类型寻找限流中间件，原有容量与状态码断言保留。

## 文档整理时发现的限制（2026-10-13）

- 原协议参考曾将 `ROOM_LOCK` 写成“立即结束当前场次”，并遗漏 V2 普通多数量礼物的金额修正规则；现已按源码校正。协议列表中的 CMD 不代表全部已接入业务，实际范围见[当前解析说明](./parsing.md)。
- 原文引用的 `API.live_upgrade_events.md` 未包含在本仓库，协议文档已移除该失效链接；尚未实现的协议字段不能据此宣称已支持。
- 下方一次性修复引用的 `repair_live_sessions_202609.sql` 已于提交 `c6b3f2b` 删除，完整 Git 历史保留原文件。礼物补齐文档和工具也在任务结束后的多个提交中移除，来源见下表。本次没有恢复或执行这些一次性文件。
- 当前 WSL `.venv` 的 `include-system-site-packages = true`，全局包可能掩盖缺失的项目依赖。原有验证不能证明干净虚拟环境可复现；本次文档整理保留现有环境。

## 已删除文件的 Git 历史索引

本次执行 `git fetch --all --tags` 并确认当前仓库不是浅克隆。以下链接固定到保留相关文件的提交 `5bbfdc0a7cafd8d5a5b884a2c7e9bccaecd2fed6`，不会随主分支更新而失效。

| 历史文件 | 用途 | 删除提交 |
| --- | --- | --- |
| [repair_live_sessions_202609.sql](https://github.com/QianQiuZy/VR_douchong/blob/5bbfdc0a7cafd8d5a5b884a2c7e9bccaecd2fed6/repair_live_sessions_202609.sql) | 2026-09 场次、区间归档与锁房异常修复 | `c6b3f2b` |
| [GIFT_V2_REPAIR.md](https://github.com/QianQiuZy/VR_douchong/blob/5bbfdc0a7cafd8d5a5b884a2c7e9bccaecd2fed6/GIFT_V2_REPAIR.md) | V2 批量礼物金额一次性历史补齐说明 | `d9545af` |
| [QUEUE_POOL_RECOVERY_RUNBOOK.md](https://github.com/QianQiuZy/VR_douchong/blob/5bbfdc0a7cafd8d5a5b884a2c7e9bccaecd2fed6/QUEUE_POOL_RECOVERY_RUNBOOK.md) | 旧连接池恢复、部署核对与 EdgeOne 可选规则 | `c6b3f2b` |
| [app/repair_gifts.py](https://github.com/QianQiuZy/VR_douchong/blob/5bbfdc0a7cafd8d5a5b884a2c7e9bccaecd2fed6/app/repair_gifts.py) | 礼物修复 CLI，需配套同版本模块 | `2139e9b` |
| [app/gift_repair_database.py](https://github.com/QianQiuZy/VR_douchong/blob/5bbfdc0a7cafd8d5a5b884a2c7e9bccaecd2fed6/app/gift_repair_database.py)、[gift_repair_targets.py](https://github.com/QianQiuZy/VR_douchong/blob/5bbfdc0a7cafd8d5a5b884a2c7e9bccaecd2fed6/app/gift_repair_targets.py) | SQL 修复目标与事务台账 | `7ef550c` |
| [app/gift_repair_redis.py](https://github.com/QianQiuZy/VR_douchong/blob/5bbfdc0a7cafd8d5a5b884a2c7e9bccaecd2fed6/app/gift_repair_redis.py) | 鲸鱼收入差额、修复标记与历史指标重算 | `5e41061` |
| [app/gift_repair_audit.py](https://github.com/QianQiuZy/VR_douchong/blob/5bbfdc0a7cafd8d5a5b884a2c7e9bccaecd2fed6/app/gift_repair_audit.py)、[gift_repair_log.py](https://github.com/QianQiuZy/VR_douchong/blob/5bbfdc0a7cafd8d5a5b884a2c7e9bccaecd2fed6/app/gift_repair_log.py)、[gift_repair_types.py](https://github.com/QianQiuZy/VR_douchong/blob/5bbfdc0a7cafd8d5a5b884a2c7e9bccaecd2fed6/app/gift_repair_types.py) | 离线日志审计、价格基准与修复明细 | `f8db6ce` |

无需恢复文件即可在仓库根目录只读查看：

```bash
git show 5bbfdc0a7cafd8d5a5b884a2c7e9bccaecd2fed6:repair_live_sessions_202609.sql
git show 5bbfdc0a7cafd8d5a5b884a2c7e9bccaecd2fed6:GIFT_V2_REPAIR.md
git log --all -- repair_live_sessions_202609.sql GIFT_V2_REPAIR.md
```

确需重新运行历史工具时，在独立检出环境使用同一提交的完整配套模块，先对隔离副本核对；不要仅恢复 CLI 到当前主分支。历史 SQL 即使默认 dry run 也有事务外 DDL，会创建备份表及调整存储过程，不能视为完全只读检查。

旧连接池手册中的 EdgeOne Force Cache/TTL 5 秒是当时的可选部署方案，不证明线上已部署，且早于当前 Redis DB2 与 `Cache-Control: no-store` 约定。开发与部署应以 [runtime.md](./runtime.md)、[api.md](./api.md) 的当前规则为准。

## SEND_GIFT_V2 批量金额一次性补齐（2026-10-02 历史资料）

当时部分非零多数量礼物的记录金额仍等于单个价格，当前解析器已修正后续事件；历史缺额曾由 `app.repair_gifts` 单独处理，不通过重放 `_on_gift` 修复。

- 以同房间同名礼物最近的非零 ×1 记录建立基准，必要时再查全日志同名礼物。免费 `0.00` 不补齐，也不作为单价基准；缺基准、基准冲突或其他金额不猜测。
- 只补 `(num - 1) × 单价` 的明确差额，已经记录整笔金额的不再乘数量。重复文件和轮转重叠跳过，同毫秒同内容的真实多笔保留次数。
- 离线审计写入私有 SQLite 与汇总/缺口报告；`--verify`/`--apply` 才连接实际目标。SQL 台账与业务差额同事务，Redis DB1 使用独立修复标记；同一审计文件续跑不得重复补齐。
- 日统计、15 分钟桶、鲸鱼指标按当时各功能的生效日期和实际存在记录处理，不制造缺失历史行，不改变事件数、付费人数、舰长或 SC 金额。Redis 原始收入缺失/过期时不凭空重算。

原文记录的四份日志包含 1,211,102 条礼物记录，明确需修复 8,357 笔、差额 164,364.90 元，122 条免费批量已排除；另有 1 笔缺非零单价基准、94 笔其他价格未自动处理。隔离 MySQL 5.7.44 副本与合成 Redis 验证了重复执行和中断续跑不会新增金额，并核验 5,745 个 SQL 金额目标及 99 个 Redis 快照。这些是历史文档中的离线/隔离验证记录，不是本次重新执行或生产完成证明。

原修复文档在 `d9545af` 以“移除已完成礼物补齐”的说明删除，后续依次删除 CLI、SQL、Redis、审计模块。保留审计台账与备份的要求仍适用于当时已经执行的修复，但当前运行程序不再包含这套命令。

## 历史本地验证（2026-10-03）

2026-10-03 本地验证：`ruff check .` 通过；启用 MySQL 5.7.44 / Redis 8.0.5 隔离容器的完整 pytest 为 **324 passed**（9 条既有弃用警告）。使用 `hareigift.sql` 快照的另一次只读模型测试覆盖 15 个月、74 个房间：全量预热 19.0 秒，Redis 分代缓存键约 5.1 MiB，两次当前月刷新分别 1.75 / 1.82 秒；14 组新旧响应一致，缓存 HTTP 查询 SQL 为 0，预算化 SQL 滚动秒峰值为 9，同一 IP 25 次快速请求返回 20 个 200 和 5 个 429。这些是本地快照测量，不是生产 2c2g 整机并发及实时采集负载保证；部署后仍需观察刷新年龄、Redis 总内存和异常日志。

## 2026 年 9 月一次性场次修复

以下保留原 README 的快照修复说明。文件已删除，来源见上方 Git 索引；命令不是当前部署步骤，只有在取得原脚本并重新核对目标快照后才适用。

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

## 测试精简与漏检修正（2026-10-20，1.0.1 补充）

本次只调整测试和文档，不修改核心业务代码、依赖、CI 或版本号，作为 `1.0.1` 的补充维护。

- **一次性重构检查积累：** 旧 Todo 4/5 测试固定函数的物理归属、精确导入名称和模块存在性，并重复验证路由注册。删除 `test_todo4_module_contract.py`、`test_todo5_extraction_contract.py` 和归档 CLI 的静态导入检查；CLI 改为验证实际调用顺序与禁止执行关注归档，接口行为继续由路由测试覆盖。
- **防导入检查漏检：** 旧 `_module_imports()` 将 `app.gift` 截为 `app`，却判断 `gift` 是否存在，因此会漏掉 `import app.gift`、`from app import gift` 和 `from app.gift import Session`。旧“独立导入”检查复用了 pytest 中已加载的模块，也不能证明独立加载。替换为全新子进程中的实际导入，检查 API、归档、启动模块不会加载兼容入口，并阻止网络连接、数据库连接和建表；兼容入口本身也必须能离线导入。
- **共享状态身份漏检：** 旧检查对房间字典和列表使用 `==`，内容相同的副本也可能通过。改用 `is` 验证它们与队列确实引用同一对象。
- **测试顺序依赖：** 旧事件和礼物测试直接修改全局场次、弹幕及队列状态，文件重命名后污染了后续关闭刷新测试。事件测试改为使用每例独立的状态容器与队列，礼物测试通过夹具自动恢复场次状态，不依赖文件排序或手工清理。
- **离线夹具静默吞掉建表：** 旧 `conftest.py` 将建表调用记录后直接忽略，且注释仍声称导入应触发建表。当前建表已由显式启动流程负责；夹具改为拒绝意外建表，专用集成测试仍显式使用保存的原始 SQLAlchemy 方法。
- **保留业务回归：** 原 Todo 测试中的礼物人民币换算、舰长去重、SC 场次恢复、后台任务取消、连接池、归档顺序和旧表兼容检查按功能归入 `test_gift_accounting.py`、`test_event_pipeline.py`、`test_database_pool.py`、`test_archive_reporting_compatibility.py`。删除重复的默认连接池快照；环境覆盖、非法值回退及连接释放回归继续保留。

已删除 `tests/_artifacts/` 下六份一次性验证记录：`todo3-verification.txt`、`todo4-verification.txt`、`todo5-verification.txt`、`todo6-verification.txt`、`todo6-layout-verification.txt`、`todo6-live-gateway.txt`。完整原文仍可从本次维护前的提交 `d61a439` 中查看，例如：

```bash
git show d61a439:tests/_artifacts/todo5-verification.txt
```

这些记录针对 2026-08 模块化期间的代码快照：离线结果先后为 139、146、172、173 项通过，部分流程连续运行了两遍相同全量测试。它们不是当前测试数量或当前部署证明，不再作为日常验证步骤。

当时真实服务启动因数据库认证失败而未建立本地 API 监听；独立 B 站网关探测收到 HTTP 412、无法解析有效业务 JSON。因此真实房间 API、UID/状态链路和运行中退出验证仍属当时未完成的验证，不能将离线通过等同于这些集成检查通过。旧 LSP 曾超时或仅返回历史警告，也不能证明当前诊断状态。

## 后续记录规范

新增问题应写明日期、复现条件、根因、对应代码或测试、修复后的行为及未验证的边界。涉及一次性数据修复时，单独记录快照身份、备份与 dry run、执行前提、预期影响和恢复办法；不要把历史脚本当作启动步骤，也不要删除保护条件。
