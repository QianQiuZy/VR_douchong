# VR斗虫自搭版

当前正式版本：**1.0.3**。版本号在 `main.py` 的 `__version__` 中维护；项目元数据和锁文件同步更新，CI 检查版本一致性。

VR/PSP 直播礼物、上舰、SC、场次、弹幕及访客进房统计服务。使用内置 blivedm 采集，FastAPI 提供查询接口，MySQL 保存业务数据，Redis 保存指标与 API 缓存。月度归档在月切时执行，跨月场次确认结束后补归档。

> host：`https://vr.qianqiuzy.cn` `https://psp.qianqiuzy.cn`

## 文档索引

| 文档 | 内容 |
| --- | --- |
| [API 文档](./docs/api.md) | 八个业务接口、鉴权、参数、响应、缓存与限流 |
| [直播 WebSocket 协议参考](./docs/API.live_websocket.md) | 包头、认证、CMD、JSON/protobuf 字段；包含历史抓包资料 |
| [当前解析说明](./docs/parsing.md) | 当前真正处理的事件、金额归一化、互动与违规事件规则 |
| [运行与数据约定](./docs/runtime.md) | 连接启动、跨月归档、有效时长、Redis 缓存、访客进房 |
| [开发规范](./docs/development.md) | 模块归属、单进程边界、修改流程、环境与测试 |
| [blivedm 维护规范](./docs/blivedm.md) | 内置版本、自定义改动边界、后续独立分支维护流程 |
| [数据许可说明](./LICENSE-DATA.md) | 两个官方 host 的数据集范围、CC BY-NC-SA 4.0 与署名方式 |
| [历史问题与修复记录](./docs/history.md) | 已有问题、一次性修复、历史验证数据与适用限制 |

文档以本仓库源码为准。新增接口或调整解析、统计、归档行为时，同步更新对应文档；历史测量不能作为当前线上性能保证。

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

   部署基准为 **OpenCloudOS 9.2 x86-64、Python 3.13.11**。`.python-version` 固定 Python 补丁版本，`pyproject.toml` 集中管理运行依赖、开发依赖、pytest 和 Ruff 配置，`uv.lock` 固定依赖版本。WSL 使用 Ubuntu 24.04 x86-64，并保持相同 Python 和锁文件；Windows 原生环境单独安装，不复用 WSL 虚拟环境。

3. 安装 MySQL 和 Redis，为 MySQL 创建独立数据库与用户：

   ```sql
   CREATE DATABASE db;
   CREATE USER 'user'@'localhost' IDENTIFIED BY '<替换为自己的密码>';
   GRANT ALL PRIVILEGES ON db.* TO 'user'@'localhost';
   FLUSH PRIVILEGES;
   ```

   将示例数据库名、用户名替换为实际部署配置。启动会创建表并补缺失列，数据库用户需要相应权限；已有金额列的类型迁移见[运行与数据约定](./docs/runtime.md)。

4. 将仓库中的 `.env.example` 复制为 `.env`，配置数据库、Redis、B 站 Cookies、管理 API 密钥等。变量名与默认值可查模板、`app/config.py`、`app/bilibili_gateway.py`，不要复制其他项目的配置。

   业务 JSON 集中到 `config/`：`rooms.json` 为 VR 房间配置，`psp_rooms.json` 为 PSP 房间配置，`entry_users.json` 为独立访客名单。通过 `ROOMS_JSON_PATH` 选择房间文件；两类 JSON 相对路径都以实际使用的 `.env` 所在目录为基准，默认是 `config/rooms.json` 和 `config/entry_users.json`。也可通过带鉴权的 `POST /add/room`、`POST /delete/room` 管理房间。

   **升级现有部署时**，先将原 JSON 文件原样移入 `config/`，保留线上房间和名单内容，并更新 `.env` 中的路径；原来的显式绝对路径仍可使用。若旧 `.venv` 继承了全局包，先新建独立环境验证，不覆盖正在使用的环境。

5. 从仓库根目录启动，使用上海时区：

   ```bash
   TZ=Asia/Shanghai uv run --locked --no-dev main.py
   ```

   启动路径是 `main.py → app.gift → app.bootstrap`，同一进程的后台线程提供 `APP_HOST:APP_PORT` 上的 Uvicorn API（默认端口 4666）。B 站采集和接口调用保持单实例，禁止通过多个进程、多 Uvicorn worker 或开发自动重载重复启动采集。

   宝塔继续收集采集控制台日志到原 `error.log`。API 日志独立写入项目根目录 `logs/api.log`，按 50 MiB 轮转到 `logs/api-history_logs/VR_douchong_日期_时间_log.log`；路径可通过 `API_LOG_PATH` 覆盖。目录权限、归档和请求耗时口径见[运行约定](./docs/runtime.md#日志分离与归档)。

## 开发检查

本机与 GitHub Actions 使用同一检查入口：

```bash
uv python install
bash scripts/check.sh
git diff --check
```

默认测试隔离真实配置、MySQL 与 B 站连接；真实 MySQL/Redis 集成测试需要专用测试服务。环境隔离、按模块选取检查及集成测试配置见[开发规范](./docs/development.md)。

[GitHub Actions](./.github/workflows/ci.yml) 在 main 分支 push、pull request 和手动触发时，默认只运行一个 Ubuntu 24.04 x86-64 任务：锁文件安装、依赖一致性、Ruff、Python 语法/JSON 校验、完整离线 pytest。为节省时间和额度，只有手动触发并勾选 `integration`，才创建一次性 MySQL 5.7.44 / Redis 8.0.5 服务并执行缓存集成测试。检查任务不启动真实采集器，也不需要线上凭据。

## License

### 软件代码

本项目软件代码使用 **GNU General Public License v2（GPLv2）**，完整条款见 [LICENSE](./LICENSE) 和 [GNU 官方文本](https://www.gnu.org/licenses/old-licenses/gpl-2.0.html)。可依许可证使用、修改和再分发，包括商业用途；再分发时须保留版权和许可声明，修改版须注明修改及日期，并按 GPLv2 的适用条款提供对应源代码。软件按现状提供，不作担保。具体权利和义务以许可证原文为准。

内置的 `app/blivedm/` 源自 [xfgryujk/blivedm](https://github.com/xfgryujk/blivedm)，上游采用 MIT License；其原始版权和 MIT 许可保留在 [app/blivedm/LICENSE](./app/blivedm/LICENSE)。项目的 GPLv2 声明不替代第三方组件原有的许可声明；其他依赖亦保留各自许可证。

### 官方数据集

由 `https://vr.qianqiuzy.cn` 和 `https://psp.qianqiuzy.cn` 对外提供的直播统计数据集，在本项目有权许可的范围内，单独采用 **CC BY-NC-SA 4.0（署名—非商业性使用—相同方式共享）**。公开分享时须注明来源、许可及修改情况；许可范围内的使用仅限非商业目的，公开分享构成改编的数据成果时须遵循相同方式共享条款。具体范围、署名示例与权利边界见 [LICENSE-DATA.md](./LICENSE-DATA.md)，完整条款见 [CC 官方法律文本](https://creativecommons.org/licenses/by-nc-sa/4.0/legalcode.zh-hans)。

数据许可与软件代码许可分别适用，不因使用本软件就要求公开独立采集的数据，也不要求公开使用者的全部数据库或网站源码。
