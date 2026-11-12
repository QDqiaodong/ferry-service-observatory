# Ferry Service Observatory

围绕虚构港湾轮渡，把岸端事件导出和独立班次计划串成采集、SQLite 存储、状态归并和服务绩效分析。
已实现事件重放去重、冲突整批拒绝、乱序文件归并、明确 as-of 截止口径、逐班次 CSV 和路线 JSON。
所有数据为自制合成样例，没有真实运营历史或外部账号。

## 独立安装

Python 3.11+，标准库运行依赖；macOS/Linux 无桌面要求。首次安装构建工具需要网络。

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements-build.lock
.venv/bin/python -m pip install -r requirements.lock
.venv/bin/python -m pip install --no-build-isolation --no-deps .
```

## 离线演示

在根目录运行。首次数据库与输出目录不存在即为干净数据状态，重复命令验证幂等事件入库。

```sh
.venv/bin/python -m ferry_observer --as-of 2026-01-01T12:00:00Z --db run/ferry.sqlite --output run/report
.venv/bin/python -m ferry_observer --as-of 2026-01-01T12:00:00Z --db run/ferry.sqlite --output run/report
.venv/bin/python -m unittest discover -s tests -v
.venv/bin/python -c 'import json; print(json.load(open("run/report/performance.json")))'
```

6 行输入含 1 行同 ID 重复，首次插入 5 事件，重放插入 0。12:00 UTC 已到计划开航时间 4 班，
已发班 2、取消 1、未决 1；13:00 的未来班次不纳入。已发班中准点 1，准点率 1/2；
取消率 1/4；两班已到达的平均航行时间 2220 秒，平均开航延误 330 秒。
取消、缺失、未来事件均有独立语义，不混入准点率分母。

## 目录与边界

- `ferry_observer/contracts.py`：计划和事件格式、时区契约。
- `ferry_observer/storage.py`：不可变计划、事件事务、去重与来源哈希。
- `ferry_observer/reconcile.py`：事件生命周期。
- `ferry_observer/analysis.py`：可观察的逐班次与路线口径。
- `data/`：刻意乱序、含合法重复的合成输入；`tests/`：正常、重放、边界及失败测试。
- `docs/event-contract.md`：全部状态、分母、输入完整性约束及输出行为。

当前只支持一份固定计划和完整有效事件前缀；不包含在线消息队列、跨日 service-day、班次改签或事件撤回。
数据库与报告均在忽略的 `run/` 中；虚拟环境与源码不共享。代码与自制数据 MIT，无第三方复制。
