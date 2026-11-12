# Ferry Service Observatory

围绕虚构港湾轮渡，把岸端事件导出和独立班次计划串成采集、SQLite 存储、状态归并和服务绩效分析。
在重放去重、整批冲突拒绝、乱序归并和明确 as-of 截止的基础上，本版加入不可变修订账本、事件更正
（upsert 新版本）与撤回（void）、单调知识版本（knowledge_seq）、双重截止的历史回放，以及旧库显式升级。
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
# v1 导出（默认）：6 行含 1 行同 ID 重复，首次插入 5 个修订，重放插入 0
.venv/bin/python -m ferry_observer --as-of 2026-01-01T12:00:00Z --db run/ferry.sqlite --output run/report
.venv/bin/python -m ferry_observer --as-of 2026-01-01T12:00:00Z --db run/ferry.sqlite --output run/report

# v2 修订导出：把 F2 的 e3 开航从 08:39 更正为 08:34，产生知识版本 2
.venv/bin/python -m ferry_observer --event-format v2 --events data/events-v2-correction.jsonl \
  --as-of 2026-01-01T12:00:00Z --db run/ferry.sqlite --output run/report

# 只读回放知识版本 1，重建复核当时可得的结论（不采集、不建库、不升级）
.venv/bin/python -m ferry_observer --report-only --db run/ferry.sqlite \
  --output run/historical --as-of 2026-01-01T12:00:00Z --known-at 1

.venv/bin/python -m unittest discover -s tests -v
```

12:00 UTC 已到计划开航时间 4 班：首版知识下已发班 2、取消 1、未决 1；13:00 的未来班次不纳入。
已发班中准点 1，准点率 1/2；取消率 1/4；两班已到达的平均航行时间 2220 秒，平均开航延误 330 秒。
e3 更正后最新知识下准点率为 1、平均延误 180 秒、平均航行 2370 秒；而 `--known-at 1` 仍稳定返回
复核前的原结果。取消、缺失、未来事件均有独立语义，不混入准点率分母。

## 修订、知识版本与双重截止

- `event_id` 是逻辑事件身份，`(event_id, revision)` 是不可变修订身份；修订从 revision=1 的 upsert
  开始且连续递增，void 撤回整个逻辑事件，之后可用下一版 upsert 恢复。
- 修订只追加、不覆盖、不删除。每批至少新增一条修订就在同一事务获得一个从 1 开始的 knowledge_seq；
  空文件或全重复文件只登记来源（sources），不消耗序号。失败批次整批回滚、不占序号。
- 报告顺序固定：先按 `knowledge_seq <= K` 限制可见修订，再为每个事件取最高 revision 并应用 void，
  最后才按 `occurred_at <= T` 过滤。先按事件时间过滤会在“事件被更正到 T 之后”时错误复活旧值。
- JSON 为 `schema_version=2`，逐班次列出截至 K 已知的每个修订、提交序号、规范载荷、首次来源，以及
  `used / superseded / voided / after_cutoff` 状态；trips.csv 保留原六列并追加 `knowledge_seq`。
- 相同 `(K, T)` 随时可重建字节一致的 JSON 与 CSV；报告不含当前时间、机器路径等易变内容。

## 旧库升级

旧结构数据库（含 `events` 表）不会被隐式迁移。采集或报告遇到旧库会提示先升级：

```sh
.venv/bin/python -m ferry_observer --upgrade-db --db run/legacy.sqlite
```

升级在单个显式事务内保留旧 `trips`、`events`、`sources` 全部事实；旧事件成为合成知识版本 1 的
revision=1，旧库无事件则停在版本 0。升级幂等，失败不留部分迁移。旧库没有事件到来源的映射和真实
入库次序，迁移修订的 `source_sha256` 为空并带 `legacy_unattributed` 说明；只有新代码接收的修订才有
首次来源关联。

## 目录与边界

- `ferry_observer/contracts.py`：计划、v1 事件与 v2 修订格式、时区与规范载荷契约。
- `ferry_observer/storage.py`：不可变修订账本、知识序号事务、来源哈希与旧库显式升级。
- `ferry_observer/reconcile.py`：事件生命周期（口径不变）。
- `ferry_observer/analysis.py`：双重截止、逐班次/路线口径与修订依据。
- `data/`：刻意乱序、含合法重复的合成 v1 输入与 v2 修订示例；`tests/`：原口径与修订、回放、迁移、并发及真实 CLI 子进程测试。
- `docs/event-contract.md`：修订身份、批次生效、双重截止、来源归属与升级边界的完整契约。

当前只支持一份固定计划和基于文件的采集；不包含在线消息队列、跨日 service-day 或班次改签。
数据库与报告均在忽略的 `run/` 中；虚拟环境与源码不共享。代码与自制数据 MIT，无第三方复制。
