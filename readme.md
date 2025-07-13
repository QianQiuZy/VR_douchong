# VirtuaReal War

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

5.**修改数据**

   修改app.py中的SESSDATA字段保证链接稳定
   
   修改app.py中的DB_CONFIG来连接数据库（保证账户名密码数据库名和上述命名时保持一致即可）
   
   增加或去除主播：修改uid_list中的UID，并且在对应列表中增加主播名称

6.**运行**
   ```bash
   python gift.py
   ```

## TODO
- [x] 一键跳转直播间打钱
- [x] 显示开播状态
- [x] VR斗虫——永无止境版开发完成（不依赖VR活动的cookie而是仅依靠B站官方的API）
