# VR斗虫自搭版

VR斗虫排行耪，每月最后一天24点清零

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
   python gift.py
   ```
   运行后 FastAPI 会通过 Uvicorn 在 `APP_HOST:APP_PORT` 对外提供接口。
