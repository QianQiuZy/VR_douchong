# 准备工作与开发检查

本文说明环境准备、配置、启动与检查步骤。下载代码后的命令均在仓库根目录执行；代码修改约定见[开发规范](./development.md)，返回[项目首页](../readme.md)。

## 准备工作

1. 下载代码：

   ```bash
   git clone https://github.com/QianQiuZy/VR_douchong
   cd VR_douchong
   ```

2. 安装 [uv](https://docs.astral.sh/uv/getting-started/installation/) `0.11.7`，按锁文件创建独立环境：

   ```bash
   uv python install
   uv sync --locked --no-dev
   ```

   部署基准为 **OpenCloudOS 9.2 x86-64、Python 3.13.11、MySQL 9.7.4 LTS**。`.python-version` 固定 Python 补丁版本，`pyproject.toml` 集中管理运行依赖、开发依赖、pytest 和 Ruff 配置，`uv.lock` 固定依赖版本。WSL 使用 Ubuntu 24.04 x86-64，并保持相同 Python 和锁文件；Windows 原生环境单独安装，不复用 WSL 虚拟环境。

   **Brotli 兼容要求：** 新版 aiohttp 的 HTTP 解压要求 `Brotli>=1.2`，锁文件固定为 1.2.1。若房间初始化出现 `process() takes exactly 1 argument (2 given)` / `Can not decode content-encoding: br`，应在实际运行环境同步依赖并重启采集进程。宝塔使用自有解释器时，确认安装与启动使用同一环境；不要只更新系统 Python 的包。具体排查见[开发规范](./development.md#http-brotli-依赖兼容修复2026-10-24)。

3. 安装 MySQL 9.7.4 和 Redis，为项目创建独立数据库与用户。

   ```sql
   CREATE DATABASE db;
   CREATE USER 'user'@'localhost' IDENTIFIED WITH caching_sha2_password BY '<替换为自己的密码>';
   GRANT ALL PRIVILEGES ON db.* TO 'user'@'localhost';
   ```

   将示例库名、用户名和密码替换为实际部署配置。启动时需要创建表和补缺失列，已有金额列的类型迁移见[数据库升级说明](./runtime.md#启动归档与数据库升级)。部署前在实际 Python 环境中用应用账号验证首次 TLS 连接，详见[数据库部署基准](./runtime.md#数据库部署基准)。

4. 将仓库中的 `.env.example` 复制为 `.env`，配置数据库、Redis、B 站 Cookies、管理 API 密钥等。变量名与默认值可查模板、`app/config.py`、`app/bilibili_gateway.py`，不要复制其他项目的配置。

   业务 JSON 集中到 `config/`：`rooms.json` 为 VR 房间配置，`psp_rooms.json` 为 PSP 房间配置，`vup_rooms.json` 为 VUP 房间配置，`entry_users.json` 为独立访客名单。通过 `ROOMS_JSON_PATH` 选择房间文件；两类 JSON 相对路径都以实际使用的 `.env` 所在目录为基准，默认是 `config/rooms.json` 和 `config/entry_users.json`。也可通过带鉴权的 `POST /add/room`、`POST /delete/room` 管理房间。

   `POST /delete/room/temporary` 和 `POST /restore/room/temporary` 仅改变房间对外可见性，JSON payload 只需 `room_id`，密钥通过请求头传递。临时隐藏覆盖当前月、历史月和所有访客查询，MySQL 写入、blivedm 连接及归档照常运行；隐藏状态保存在 Redis DB2，不设过期时间。共用 Redis 的 VR/PSP/VUP 部署必须配置不同的 `API_CACHE_PREFIX`。

   **升级现有部署时**，先将原 JSON 文件原样移入 `config/`，保留线上房间和名单内容，并更新 `.env` 中的路径；原来的显式绝对路径仍可使用。若旧 `.venv` 继承了全局包，先新建独立环境验证，不覆盖正在使用的环境。

5. 从仓库根目录启动，使用上海时区：

   ```bash
   TZ=Asia/Shanghai uv run --locked --no-dev main.py
   ```

   启动路径是 `main.py → app.gift → app.bootstrap`，同一进程的后台线程提供 `APP_HOST:APP_PORT` 上的 Uvicorn API（默认端口 4666）。B 站采集和接口调用保持单实例，禁止通过多个进程、多 Uvicorn worker 或开发自动重载重复启动采集。

   宝塔继续收集采集控制台日志到原 `error.log`。API 日志独立写入项目根目录 `logs/api.log`，按 50 MiB 轮转到 `logs/api-history_logs/VR_douchong_日期_时间_log.log`；路径可通过 `API_LOG_PATH` 覆盖。目录权限、归档和请求耗时口径见[运行约定](./runtime.md#日志分离与归档)。

## 开发检查

本机与 GitHub Actions 使用同一检查入口：

```bash
uv python install
bash scripts/check.sh
git diff --check
```

默认测试隔离真实配置、MySQL 与 B 站连接；真实 MySQL/Redis 集成测试需要专用测试服务。环境隔离、按模块选取检查及集成测试配置见[开发规范](./development.md)。

[GitHub Actions](../.github/workflows/ci.yml) 在 main 分支 push、pull request 和手动触发时，默认只运行一个 Ubuntu 24.04 x86-64 任务：锁文件安装、依赖一致性、Ruff、Python 语法/JSON 校验、完整离线 pytest。手动触发并勾选 `integration` 后，会创建一次性 MySQL 9.7.4 / Redis 8.0.5 服务并执行集成测试。检查任务不启动真实采集器，也不需要线上凭据。
