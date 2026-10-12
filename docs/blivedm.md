# blivedm 维护规范

## 当前维护范围

VR_douchong 实际导入的是本仓库 `app/blivedm/`，例如 `app/live_client.py` 的相对导入。该目录是普通受版本控制的源码目录，不是 Git 子模块，也不依赖相邻独立 blivedm 仓库的当前分支。

当前用户保留的版本标识为：

```python
__version__ = '1.0.0-VR_douchong'
```

该值标识本项目内置维护版本，不应在同步上游时覆盖回 `1.1.5-dev`。它不是发布到 PyPI 或打包可用性的验证结论。

本次只整理文档并保留该修改；独立维护分支以后处理，不创建或切换相邻 blivedm 仓库分支，不改成 pip 包或子模块，不迁移源码、不提交或推送。

## 源码职责与现有定制

| 位置 | 职责与需要保留的接口 |
| --- | --- |
| `app/blivedm/clients/ws_base.py` | 压缩包、心跳、鉴权及消息传递；异常与连接生命周期 |
| `app/blivedm/clients/web.py` | Web 房间初始化、WBI、弹幕服务器认证参数 |
| `app/blivedm/handlers.py` | CMD 后缀处理、老/V2 事件分发、V2 礼物多条回调及解码失败跳过 |
| `app/blivedm/models/web.py` | 老 JSON、SEND_GIFT_V2 repeated field 10、金额归一化、老/V2 互动模型 |
| `app/blivedm/models/pb.py` | INTERACT_WORD_V2 字段与枚举兼容 |
| `app/blivedm/models/open_live.py`、`clients/open_live.py` | 库已有开放平台接口；本项目未接入开放平台业务统计 |
| `app/live_client.py` | 本项目首次成功鉴权等待器，保留在业务侧 |
| `app/event_ingestion.py`、`app/room_lock_events.py` | 本项目统计、进房名单、违规处理，保留在业务侧 |

这是当前维护面清单，不是对上游全部差异的完整审计。具体行为见 [parsing.md](./parsing.md)；提交上游或迁移分支前应比较确定的上游基线。

## 后续独立分支维护流程

1. 确认目标独立 blivedm 仓库、现有分支和脏工作树，记录上游基线提交与 VR_douchong 来源提交；分支名可采用 `vr-douchong`，实际创建时确定。
2. 将通用协议模型、兼容处理和回调修正迁入维护分支，保留本版本标识；不迁入 MySQL、Redis、FastAPI、访客名单或本项目场次业务。
3. 对比维护分支与上游的真实 diff，给每项本地改动写明原因、字段样本和回归测试。不要用覆盖整个目录的方式同步上游。
4. 同步上游时先合并/挑选明确提交，再核对旧 JSON、V2 批量、盲盒、互动与成功鉴权的兼容合同。保留上游许可证与来源说明。
5. 选择回同步 VR_douchong 的方式：继续内置源码、子模块或包依赖需另行确定。继续内置时记录独立库提交，避免两个目录分别修改后无法追溯。
6. 在两个仓库各自环境中验证，并记录“独立库验证”与“VR_douchong 集成验证”；跨仓库同步、提交和推送按实际授权执行。

原版本字符串是否符合未来打包要求应在发布任务中验证，不在本次文档整理中擅自更改。

## 同步后的相关回归

从 VR_douchong 仓库根目录，使用本项目解释器：

```bash
.venv/bin/python -m pytest -q tests/test_send_gift_v2.py tests/test_send_gift_v2_batch.py tests/test_entry_events.py tests/test_live_client_authentication.py tests/test_live_first_startup.py
```

若调整 CMD 分发或生命周期，再运行 `test_room_lock.py`、`test_room_cut_off.py`、`test_session_duration_validity.py`。不得启动第二个真实采集器验证分支；隔离测试方法见 [development.md](./development.md)。
