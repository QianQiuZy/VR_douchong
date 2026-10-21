# 开发规范

核对日期：2026-10-20。以当前源码为准；运行边界见 [runtime.md](./runtime.md)，实际事件范围见 [parsing.md](./parsing.md)，业务接口见 [api.md](./api.md)。

## 入口与模块归属

启动链路为 `main.py → app.gift.run → app.bootstrap.run`。`app/gift.py` 保留兼容导出和启动接线，已有外部调用、测试和归档 CLI 依赖它的公开名称；新业务应落在负责该功能的模块。

| 职责 | 现有模块 | 修改约定 |
| --- | --- | --- |
| 配置与启动 | `app/config.py`、`app/bootstrap.py` | 配置集中读取，启动和退出任务集中接线 |
| 日志路由与访问记录 | `app/logging_setup.py`、`app/api_request_logging.py` | 仅显式启动时创建文件；API 上下文不能传入长期采集任务 |
| 进程内共享状态 | `app/runtime_state.py` | 保留唯一状态来源；不要复制房间、场次、队列字典 |
| 房间配置、增删与场次生命周期 | `app/room_config.py`、`app/room_lifecycle.py` | 沿用房间锁、主循环调度、场次合并与结束规则 |
| 启动排序与鉴权 | `app/live_startup.py`、`app/live_client.py` | 判断成功鉴权，不能用任务创建或连接打开替代 |
| B 站 HTTP 适配 | `app/bilibili_gateway.py` | 在已有网关与调用方检查超时、返回码、重试和频率 |
| 定时监控与采集调度 | `app/monitoring_jobs.py` | 状态轮询、指标采样、重连和队列 worker 复用既有流程 |
| WebSocket 协议层 | `app/blivedm/clients/`、`handlers.py`、`models/` | 解包、分发、JSON/protobuf 转换；不加入本项目数据库业务 |
| 事件业务层 | `app/event_ingestion.py`、`app/room_lock_events.py` | 消息过滤、统计入账、进房队列与违规生命周期 |
| ORM 与持久化 | `app/models/`、`app/repositories/`、`app/database.py` | 模型定义、事务与查询放在对应层，说明主表/归档迁移 |
| 15 分钟指标与 Redis 指标 | `app/metrics_runtime.py`、`app/redis_metrics.py`、`app/whale_metrics.py` | 区分统计桶、人数集合和鲸鱼收入去重的作用域 |
| 月归档 | `app/archive_service.py`、`app/whale_archive.py` | 使用统一候选场次、截止时间和子表先行规则 |
| FastAPI 路由与查询 | `app/api_app.py` | 公共接口统一放在这里；不要新增平行的 `entry_api.py` |
| API 缓存协调 | `app/api_cache.py` | 启动、刷新、历史失效通知与指标 |
| API 缓存读写链路 | `app/api_cache_http.py`、`api_cache_source.py`、`api_cache_payloads.py`、`api_cache_store.py` | 分别负责 HTTP 策略、预算化快照、响应构建、Redis 存储 |

访客名单重载和消息队列放在 `event_ingestion.py`，访客存储放在 `repositories/entries.py`；扩展时先检查已有归属，避免重新拆出重复入口。确需新模块时，应明确其职责与调用关系。

## 并发和外部调用

- B 站采集、连接和接口调用只能由一个采集进程管理。当前 API 在同一进程的 Uvicorn 线程中运行；不通过多个进程、多 worker 或自动重载扩大采集实例数。
- API 的房间管理操作需要切回采集主事件循环，继续使用现有 `MAIN_LOOP` 与线程安全调度。不要在 API 线程中新建另一套采集循环或客户端。
- 不跨事件循环复用 aiohttp session、锁或队列；连接关闭和后台任务取消遵循已有退出流程。
- 在异步 worker 中执行阻塞数据库或归档工作时，沿用已有线程调度和 session 生命周期，避免阻塞采集循环。
- 错误定位先区分 B 站 HTTP/WS、业务入账、MySQL、Redis、反向代理与 HTTP 请求保护。IP 超限 429、缓存不可用 503、旧报表容量保护 503 是不同原因。

## 解析与数据合同

1. 协议字段转换放在内置 blivedm；业务过滤、金额统计和数据库写入放在事件业务层。老/V2 事件兼容、整包多条礼物和未知字段的处理见 [parsing.md](./parsing.md)。
2. 保留秒/毫秒时间戳的归一化和异常时间回退；MySQL DATETIME 与月界限使用上海本地时间。房间 ID 与主播 UID 不可混用。
3. 不能把单个开播片段的有效性等同于整个合并场次的有效性。生命周期调整必须覆盖重复/晚到事件、旧开播时间、锁定到期、正好 180 秒与超过 180 秒、重启恢复以及跨日/月撤回。
4. 默认 GET 仅读 Redis DB2，失败返回 503，不增加 MySQL 回源兜底。新增 API 除路由外，还需同步路径识别、参数校验、快照查询、响应构建、空结果模板、缓存读取与限流策略。
5. 保持 DB0 付费人数集合、DB1 鲸鱼指标、DB2 API 缓存的边界。既有去重只作用于对应指标，不能据此声称所有业务写入都已全局去重。
6. 自动建表/补列不等于旧库类型迁移。涉及表结构变动，应列出主表、月归档表、恢复旧场次与兼容默认值，写明维护窗口和备份要求。
7. 不用重放历史日志替代精确数据修复；一次性修复与常规采集分开，历史脚本及适用快照见 [history.md](./history.md)。

## 环境与配置

部署基准为 OpenCloudOS 9.2 x86-64、Python 3.13.11；本机 WSL 为 Ubuntu 24.04 x86-64。安装 uv 0.11.7，使用 `.python-version`、`pyproject.toml` 和提交的 `uv.lock`，本地和 CI 均执行 `bash scripts/check.sh`。项目按应用管理，不发布 Python 包；运行依赖在 `project.dependencies`，开发依赖在 `dependency-groups.dev`。

新建环境不继承全局包，Windows 环境与 WSL 分开；不复用其他仓库的依赖目录。已有 `.venv` 不随意重建：原本地环境 `include-system-site-packages = true`，因此使用新建 `.venv-ci` 验证。命令前设置 `UV_PROJECT_ENVIRONMENT=.venv-ci` 可保留旧环境；`scripts/check_project.py` 检查 Python 补丁版本和环境隔离，拒绝继承全局包的环境。

修改依赖后执行 `uv lock` 并审查 `pyproject.toml` / `uv.lock` 差异。只有明确升级依赖时才使用 `uv lock --upgrade`；部署使用 `uv sync --locked --no-dev`，检查使用 `uv sync --locked --group dev`，防止运行时静默改变锁文件。旧 requirements 文件已移除，不再维护第二份依赖清单。

项目当前正式版本为 `1.0.3`，在 `main.py` 的 `__version__` 中维护。发布时同步 `pyproject.toml` 的项目版本并执行 `uv lock` 更新锁文件；`scripts/check_project.py` 静态读取版本并检查一致性，不为读取版本而导入或启动采集程序。内置 blivedm 继续独立维护库版本，不随项目发布版本变化。

安装依赖前确认实际解释器。安装、Git 等外网开发命令沿用会话代理配置；未配置时按工作区约定检查 `http://127.0.0.1:10808` 是否可达后，仅为该命令/会话配置。回环与专用本地测试服务直连，不把本机代理写入业务配置。

不提交真实 `.env`、Cookie、token、SQL 快照或日志明细；配置排障只输出变量名、存在性与脱敏结构。日志异常输出错误类型和必要上下文，避免新增原始认证包或完整凭据输出。当前源码部分未知 CMD 日志会包含消息内容，导出抓包/日志用于测试或文档时须先脱敏。

## 最小验证与回归

安装和基本检查（已有环境直接使用，不覆盖重建）：

```bash
uv python install
bash scripts/check.sh
git diff --check
```

按变更范围先选相关测试，跨模块或发布前再执行完整测试：

新增测试应对应具体业务错误、历史故障或必要的运行边界，优先复用已有夹具和相关用例。不要将一次性重构的函数位置、精确导入语法或模块存在性永久冻结；相同行为保留一份有效检查即可。没有新改动、失败或明确的偶发性疑点时，不重复执行相同的全量验证。

导入隔离使用全新 Python 子进程验证，不能用当前 pytest 进程里已加载的模块证明独立导入；共享可变状态应比较对象身份（`is`），不能仅比较内容。历史验证结果与外部服务受阻记录统一保存在 [history.md](./history.md)，不作为当前通过证明。

| 变更范围 | 相关测试 |
| --- | --- |
| 老/V2 礼物与金额 | `test_send_gift_v2.py`、`test_send_gift_v2_batch.py` |
| 访客事件与查询 | `test_entry_events.py`、`test_entry_api.py` |
| 违规、锁定与有效时长 | `test_room_lock.py`、`test_room_cut_off.py`、`test_session_duration_validity.py` |
| 启动优先级与鉴权 | `test_live_first_startup.py`、`test_live_client_authentication.py` |
| 路由、月份、响应兼容 | `test_routes_contract.py`、`test_month_parameter_contract.py` |
| API 缓存与预算 | `test_api_cache.py`、`test_api_cache_integration.py` |
| 日志分流、请求计时与大小轮转 | `test_api_logging.py`、`test_import_contract.py` |
| 归档与表结构 | `test_archive_catchup.py`、`test_schema_contract.py`、`test_room_live_stats_decimal.py` |

这些测试位于 `tests/`。例如：

```bash
uv run --locked pytest -q tests/test_send_gift_v2.py tests/test_send_gift_v2_batch.py
```

默认 `tests/conftest.py` 指向空配置、测试房间文件并阻止实际数据库初始化，不读取真实 `.env` 或连接 B 站。旧报表测试走离线路径，缓存测试分别启用隔离缓存。

真实 MySQL 5.7.44 / Redis 8.0.5 集成验证只连接专用测试端口，数据库名固定为 `vr_cache_test`：

```bash
VR_CACHE_TEST_MYSQL_PORT=<本机专用 MySQL 测试端口> \
VR_CACHE_TEST_REDIS_PORT=<本机专用 Redis 测试端口> \
uv run --locked pytest -q tests/test_api_cache_integration.py
```

未提供端口时相应集成测试跳过，不能将跳过称为通过。离线测试、隔离数据库验证、真实 B 站与线上主机验证应分别报告；文档整理只需检查内容、相对链接和差异，不必启动真实服务。

## GitHub Actions

`.github/workflows/ci.yml` 默认只有一个 Ubuntu 24.04 x86-64 任务，与当前 WSL 系统保持一致，统一运行 `scripts/check.sh`：锁文件安装、`uv pip check`、Ruff、Python 语法及业务 JSON 结构检查、完整离线 pytest。Python 固定为部署版本 3.13.11，本机与云端使用同一锁文件。CI 不额外启动 OpenCloudOS 容器，不声称 Ubuntu 检查等同于云端系统验证。

为节省额度，集成任务仅在手动触发并勾选 `integration` 且默认检查通过后运行；它使用 GitHub 的临时 MySQL 5.7.44 / Redis 8.0.5 服务，运行已有六项集成测试。测试数据库和凭据均为测试专用，不配置生产 Secrets。工作流只有仓库只读权限，第三方 Actions 固定提交 SHA，启用依赖缓存并自动取消同一分支的旧运行。没有配置自动部署。

Ruff 明确启用 E4/E7/E9/F；内置 blivedm 仅为既有星号导入保留 F403/F405 的文件级兼容，其他规则照常检查。`gift.py` 的延后导入用于原有兼容接线，按行说明 E402 原因；不借配置迁移更改 V2 协议处理。

## 提交规范

参照 liveshow 的版本化提交方式，标题格式为：

```text
v<版本> <YYYY-MM-DD> <维护者> [<类型>](<范围>) <摘要>
```

例如：`v1.0.0 2026-10-16 QianQiuZy [improvement](project) 规范化项目配置与检查流程`。版本对应 `main.py` 的 `__version__`，维护者填写实际账号；Git 作者沿用本仓库真实身份，不复制参考项目的作者。

正文按 `[类型](范围)` 说明最终改动，列出相关文件和验证结果，使用真实换行与仓库相对路径。常用类型包括 `feature`、`fix`、`improvement`、`performance`、`docs`。

只有用户主动明确要求时才创建 commit；push 需要单独授权。授权范围限定在当前任务和指定仓库，不自动提交相邻独立 blivedm 仓库或工作区文档。

## 文档同步与交付

新增或调整 API 更新 [api.md](./api.md)；调整 WS 字段更新 [API.live_websocket.md](./API.live_websocket.md) 与 [parsing.md](./parsing.md)；调整统计、归档或配置更新 [runtime.md](./runtime.md)。内置库改动记录在 [blivedm.md](./blivedm.md)，历史问题记录在 [history.md](./history.md)，新增文档加入根 [README](../readme.md) 索引。

保留用户未提交修改，不自动提交、推送或清理生成物。交付写明改动目的、检查结果和未验证边界；不弱化测试断言来绕过失败。
