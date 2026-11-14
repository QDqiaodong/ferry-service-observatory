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

## 离线换乘分析

在只读快照上回答“此刻从某港出发能怎样换乘到达”：`--connections` 读入严格 JSON 请求
（航线→起终港/非负整数分票价的唯一映射、各港换乘秒数、不同起终港与就绪时刻），按既有
双重截止规则（先 K 内最高修订/撤回，再截 T）只取已到达航次的实开实到，首航开航不早于
就绪时刻，后续航段满足同港换乘秒数。输出到达时刻、总票价、换乘次数三项指标下全部非支配
方案，同指标仅留 trip_id 字典序最小者；不可达是正常诊断而非错误。报告原子替换，坏输入或
写入失败保留旧报告。

```sh
# 多航线合成网络：Harbor/East/North 三港，直达贵、换乘便宜
.venv/bin/python -m ferry_observer --schedule data/connections/schedule.csv \
  --events data/connections/events.jsonl --db run/conn.sqlite --output run/conn-report \
  --as-of 2026-01-01T12:00:00Z
.venv/bin/python -m ferry_observer --connections data/connections/request.json \
  --db run/conn.sqlite --output run/conn --as-of 2026-01-01T12:00:00Z

# 更正 E1 开航到 08:45（换乘恰好临界可行）并撤回 D1 到达后，按知识版本复算
.venv/bin/python -m ferry_observer --event-format v2 --events data/connections/events-v2-e1-shift.jsonl \
  --schedule data/connections/schedule.csv --db run/conn.sqlite --output run/conn-report \
  --as-of 2026-01-01T12:00:00Z
.venv/bin/python -m ferry_observer --event-format v2 --events data/connections/events-v2-void-d1.jsonl \
  --schedule data/connections/schedule.csv --db run/conn.sqlite --output run/conn-report \
  --as-of 2026-01-01T12:00:00Z
.venv/bin/python -m ferry_observer --connections data/connections/request.json \
  --db run/conn.sqlite --output run/conn-k1 --as-of 2026-01-01T12:00:00Z --known-at 1
```

演示网络下前沿恰含两案：直达 D1（09:50 到、900 分、0 换乘）与 H1→E1（10:12 到、800 分、
1 换乘）——晚到但更便宜的方案不被支配。撤回 D1 到达事件后（知识版本 3）仅剩 H1→E1；
`--known-at 1` 仍字节一致地重现原前沿。完整契约见 `docs/connections-contract.md`。

## 客票清分入库与重导

客票路径复用既有不可变 `trips` 班次表，不触碰事件修订账本。`--ticket-ingest` 接收严格
JSONL：售票为 `type=sale`（`ticket_id, paid_cents, legs[{trip_id, weight}]`），退款为
`type=refund`（`refund_id, ticket_id, cents`）。ID 非空；金额为整数分（售票 ≥0、退款 >0，
布尔与浮点一律拒绝）；权重为正整数；航段非空、不重复且班次必须存在。售票/退款 ID 各自
独立唯一，航段顺序无关（规范化按 trip_id 排序）；同 ID 同内容幂等、异内容拒绝整批。

票款按权重以最大余数法（Hamilton）整数分摊，余数并列按 `trip_id` 字典序；累计退款以**原
分摊金额**为权重新做一次最大余数分摊，每航段净额为两者整数之差，全程无浮点。退款可先于
售票到达（列为 `pending_refunds`），同批先合并再校验，累计退款永远不超过票款。票据、航段、
退款、分摊事实与来源 SHA256 在同一个 `BEGIN IMMEDIATE` 事务提交，失败整批回滚、不占序号。

```sh
# 先有班次表（事件采集或空事件文件均可），再逐批入库客票
.venv/bin/python -m ferry_observer --schedule data/schedule.csv --events data/events.jsonl \
  --db run/ferry.sqlite --output run/report --as-of 2026-01-01T12:00:00Z
.venv/bin/python -m ferry_observer --ticket-ingest data/ticketing/batch1-sales.jsonl \
  --db run/ferry.sqlite --output run/settle
# 退款先行与同批合并、跨批部分退款、乱序幂等重放
.venv/bin/python -m ferry_observer --ticket-ingest data/ticketing/batch2-early-refunds.jsonl \
  --db run/ferry.sqlite --output run/settle
.venv/bin/python -m ferry_observer --ticket-ingest data/ticketing/batch3-partial-refunds.jsonl \
  --db run/ferry.sqlite --output run/settle
.venv/bin/python -m ferry_observer --ticket-ingest data/ticketing/batch1-replay-shuffled.jsonl \
  --db run/ferry.sqlite --output run/settle
# 提交后只读重导：字节一致；写入失败保留旧 settlement.json
.venv/bin/python -m ferry_observer --settlement --db run/ferry.sqlite --output run/settle-re
```

10 分等权三段票款分为 4/3/3（余数并列，多余 1 分归字典序最小的 F1）；累计退 3 分按原
4/3/3 权重各退 1 分，净额 3/2/2。`settlement.json` 逐票给出票款与退款两套分摊依据
（floor、余数字、`extra_unit`）及逐班次收入/退款/净额汇总。完整契约见
`docs/ticketing-contract.md`。

## 旧库升级

旧结构数据库（含 `events` 表）不会被隐式迁移。采集或报告遇到旧库会提示先升级：

```sh
.venv/bin/python -m ferry_observer --upgrade-db --db run/legacy.sqlite
```

升级在单个显式事务内保留旧 `trips`、`events`、`sources` 全部事实；旧事件成为合成知识版本 1 的
revision=1，旧库无事件则停在版本 0。升级幂等，失败不留部分迁移。旧库没有事件到来源的映射和真实
入库次序，迁移修订的 `source_sha256` 为空并带 `legacy_unattributed` 说明；只有新代码接收的修订才有
首次来源关联。

## 修订状态机质量门禁

`quality_gate/` 提供可复现的离线质量门禁：独立内存预言机（不导入生产解析、投影或
生命周期代码）按 `docs/event-contract.md` 重写账本与双重截止口径，对 40 条固定种子、
每条 15 批的交错历史（更正、撤回恢复、旧版重复、同批乱序/冲突、账本冲突、空与全重复
文件）逐批核对决策、知识序号、首次来源、SQLite 全表内容与报告字节稳定性，并带三个
AST 隔离变异的杀死检查。生产代码与业务契约不做任何改动。

```sh
python3 -m quality_gate.run_gate --seeds 40 --out run/quality-gate
python3 -m quality_gate.replay --bundle quality_gate/singleton-bundle.json
python3 -m unittest tests.test_quality_gate tests.test_quality_mutations -v
```

完整设计、失败工件与变异说明见 `docs/quality-gate.md`。

## 目录与边界

- `ferry_observer/contracts.py`：计划、v1 事件与 v2 修订格式、时区与规范载荷契约。
- `ferry_observer/storage.py`：不可变修订账本、知识序号事务、来源哈希与旧库显式升级。
- `ferry_observer/reconcile.py`：事件生命周期（口径不变）。
- `ferry_observer/analysis.py`：双重截止、逐班次/路线口径与修订依据。
- `ferry_observer/connections.py`：离线换乘分析（请求校验、前沿扫描、原子报告替换）。
- `ferry_observer/ticketing.py`：客票售票/退款严格 JSONL 入库、整数最大余数分摊、清分报告与原子重导。
- `data/`：刻意乱序、含合法重复的合成 v1 输入与 v2 修订示例；`data/connections/` 多航线换乘演示网络；
  `data/ticketing/` 客票批次、乱序重放、冲突与各类坏记录样例。
- `tests/`：原口径与修订、回放、迁移、并发、真实 CLI 子进程测试，换乘前沿对独立小图穷举的核对，
  以及客票清分的分摊算法、跨批退款、乱序冲突、回滚与 CLI 贯通测试。
- `docs/event-contract.md`：修订身份、批次生效、双重截止、来源归属与升级边界的完整契约。
- `docs/connections-contract.md`：换乘请求、候选口径、前沿与平局裁决、诊断与原子替换契约。
- `docs/ticketing-contract.md`：客票 JSONL、最大余数分摊、累计退款口径、事务存储与确定性清分导出契约。

当前只支持一份固定计划和基于文件的采集；不包含在线消息队列、跨日 service-day 或班次改签。
数据库与报告均在忽略的 `run/` 中；虚拟环境与源码不共享。代码与自制数据 MIT，无第三方复制。
