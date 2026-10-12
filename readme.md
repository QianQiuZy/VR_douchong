# VR斗虫自搭版

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
| [历史问题与修复记录](./docs/history.md) | 已有问题、一次性修复、历史验证数据与适用限制 |

文档以本仓库源码为准。新增接口或调整解析、统计、归档行为时，同步更新对应文档；历史测量不能作为当前线上性能保证。

## 准备工作

1. 下载代码：

   ```bash
   git clone https://github.com/QianQiuZy/VR_douchong
   cd VR_douchong
   ```

2. 创建项目独立虚拟环境并安装运行依赖：

   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   python -m pip install -r requirements.txt
   ```

   Windows 使用独立的 Windows 虚拟环境和对应的 `Scripts\activate`，不要复用 WSL 环境。需要开发检查时，安装 `requirements-dev.txt`，详见[开发规范](./docs/development.md)。

3. 安装 MySQL 和 Redis，为 MySQL 创建独立数据库与用户：

   ```sql
   CREATE DATABASE db;
   CREATE USER 'user'@'localhost' IDENTIFIED BY '<替换为自己的密码>';
   GRANT ALL PRIVILEGES ON db.* TO 'user'@'localhost';
   FLUSH PRIVILEGES;
   ```

   将示例数据库名、用户名替换为实际部署配置。启动会创建表并补缺失列，数据库用户需要相应权限；已有金额列的类型迁移见[运行与数据约定](./docs/runtime.md)。

4. 将仓库中的 `.env.example` 复制为 `.env`，配置数据库、Redis、B 站 Cookies、管理 API 密钥等。变量名与默认值可查模板、`app/config.py`、`app/bilibili_gateway.py`，不要复制其他项目的配置。

   `rooms.json` 维护 WS 房间与主播映射（`room_ids`、`room_anchors`）。`entry_users.json` 独立维护访客 UID 和名称，默认放在实际使用的 `.env` 同一目录。也可通过带鉴权的 `POST /add/room`、`POST /delete/room` 管理房间。

5. 从仓库根目录启动，使用上海时区：

   ```bash
   TZ=Asia/Shanghai .venv/bin/python main.py
   ```

   启动路径是 `main.py → app.gift → app.bootstrap`，同一进程的后台线程提供 `APP_HOST:APP_PORT` 上的 Uvicorn API（默认端口 4666）。B 站采集和接口调用保持单实例，禁止通过多个进程、多 Uvicorn worker 或开发自动重载重复启动采集。

## 开发检查

使用项目 WSL 虚拟环境：

```bash
.venv/bin/python -m pip install -r requirements-dev.txt
.venv/bin/python -m ruff check .
.venv/bin/python -m pytest -q
git diff --check
```

默认测试隔离真实配置、MySQL 与 B 站连接；真实 MySQL/Redis 集成测试需要专用测试服务。环境隔离、按模块选取检查及集成测试配置见[开发规范](./docs/development.md)。
