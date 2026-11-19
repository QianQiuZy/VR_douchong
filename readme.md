# VR斗虫自搭版

当前正式版本：**1.0.11**。版本号在 `main.py` 的 `__version__` 中维护；项目元数据和锁文件同步更新，CI 检查版本一致性。

VR/PSP/VUP 直播礼物、上舰、SC、场次、弹幕及访客进房统计服务。使用内置 blivedm 采集，FastAPI 提供查询接口，MySQL 保存业务数据，Redis 保存指标与 API 缓存。月度归档在月切时执行，跨月场次确认结束后补归档。

> host：`https://vr.qianqiuzy.cn` `https://psp.qianqiuzy.cn` `https://vup.qianqiuzy.cn`

## 文档索引

| 文档 | 内容 |
| --- | --- |
| [API 文档](./docs/api.md) | 十个业务接口、公开健康检查、鉴权、参数、响应、临时隐藏与恢复、缓存与限流 |
| [直播 WebSocket 协议参考](./docs/API.live_websocket.md) | 包头、认证、CMD、JSON/protobuf 字段；包含历史抓包资料 |
| [当前解析说明](./docs/parsing.md) | 当前真正处理的事件、金额归一化、互动与违规事件规则 |
| [运行与数据约定](./docs/runtime.md) | 连接启动、跨月归档、有效时长、Redis 缓存、访客进房 |
| [准备工作与开发检查](./docs/development-checks.md) | 环境安装、数据库与配置、启动命令、本地与 CI 检查 |
| [开发规范](./docs/development.md) | 模块归属、单进程边界、修改流程、环境与测试 |
| [blivedm 维护规范](./docs/blivedm.md) | 内置版本、自定义改动边界、后续独立分支维护流程 |
| [数据许可说明](./LICENSE-DATA.md) | 三个官方 host 的数据集范围、CC BY-NC-SA 4.0 与署名方式 |
| [历史问题与修复记录](./docs/history.md) | 已有问题、一次性修复、历史验证数据与适用限制 |

文档以本仓库源码为准。新增接口或调整解析、统计、归档行为时，同步更新对应文档；历史测量不能作为当前线上性能保证。

## License

### 软件代码

除另有明确许可声明的第三方组件与数据集外，本项目软件代码按 **GNU General Public License version 3 或（由使用者选择）任何后续版本（SPDX：`GPL-3.0-or-later`）** 授权。完整条款见 [LICENSE](./LICENSE) 和 [GNU 官方文本](https://www.gnu.org/licenses/gpl-3.0.html)。可依许可证使用、修改和再分发，包括商业用途；再分发时须保留版权和许可声明，修改版须注明修改及日期，并按适用条款提供对应源代码。软件按现状提供，不作担保。具体权利和义务以许可证原文为准。

内置的 `app/blivedm/` 源自 [xfgryujk/blivedm](https://github.com/xfgryujk/blivedm)，上游采用 MIT License；其原始版权和 MIT 许可保留在 [app/blivedm/LICENSE](./app/blivedm/LICENSE)。项目的 GPL-3.0-or-later 声明不替代第三方组件原有的许可声明；其他依赖亦保留各自许可证。

### 官方数据集

由 `https://vr.qianqiuzy.cn`、`https://psp.qianqiuzy.cn` 和 `https://vup.qianqiuzy.cn` 对外提供的直播统计数据集，在本项目有权许可的范围内，单独采用 **CC BY-NC-SA 4.0（署名—非商业性使用—相同方式共享）**。公开分享时须注明来源、许可及修改情况；许可范围内的使用仅限非商业目的，公开分享构成改编的数据成果时须遵循相同方式共享条款。具体范围、署名示例与权利边界见 [LICENSE-DATA.md](./LICENSE-DATA.md)，完整条款见 [CC 官方法律文本](https://creativecommons.org/licenses/by-nc-sa/4.0/legalcode.zh-hans)。

数据许可与软件代码许可分别适用，不因使用本软件就要求公开独立采集的数据，也不要求公开使用者的全部数据库或网站源码。
