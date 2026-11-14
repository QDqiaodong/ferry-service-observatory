# 修订状态机质量门禁（quality_gate）

本目录为 `ferry_observer` 的不可变修订状态机建立一套**可复现、可离线重放**的
质量门禁。门禁的核心是一个与生产代码完全隔离的独立内存模型（预言机），它只依据
`docs/event-contract.md` 的文字契约重新实现解析、生命周期、账本与双重截止报告，
然后把同一批输入同时喂给生产实现与预言机，逐批、逐表、逐报告比对。

生产代码与业务契约**零改动**：门禁只新增 `quality_gate/` 与 `tests/` 下的新文件，
`ferry_observer/` 不被修改，变异测试也只在系统临时目录的副本上注入故障。

## 目录结构

| 文件 | 职责 |
| --- | --- |
| `canonical.py` | 独立的时区归一化、规范载荷、v2 JSONL 与计划 CSV 解析（不导入生产包） |
| `oracle.py` | 独立内存账本：批次接受/拒绝、知识序号、首次来源、全表快照、双重截止报告 |
| `generator.py` | 固定种子（`random.Random`，纯标准库）交错历史生成器，每条历史 15 个批次 |
| `harness.py` | 逐批比对执行器：决策、计数、全表、回滚无残留、双重截止网格、字节稳定性 |
| `mutator.py` | 基于 AST 的隔离故障注入，只改写临时副本，绝不匹配/修改工作树源码 |
| `replay.py` | 单例重放 CLI：直接消费 JSON 包，不经过随机生成器 |
| `run_gate.py` | 全量门禁运行器 CLI，并可导出单例重放包 |
| `singleton-bundle.json` | 随仓库交付的固定种子单例包，离线即可重放 |

## 生成的历史覆盖了什么

每条历史固定 15 个批次（≥12 的要求），40 条历史使用 40 个固定种子
（`20260101..20260140`），单条历史内包含：

- 更正：同一事件连续更高 revision 的 upsert（含出发与到达一起后移仍合法的场景）；
- 撤回恢复：整班事件 void 后，用下一连续 revision upsert 恢复；
- 旧版重复：已被取代的旧 revision 重放，包含等价 `+01:00` 偏移与乱序键序两种
  归一化等价形态；
- 同批乱序：同一批次内连续 revision 逆序呈现；
- 同批重复与同批冲突：同一修订身份的等价重复与不同载荷冲突；
- 账本冲突：对已接受修订身份重投不同规范载荷；
- 生命周期拒绝：void 出发却保留到达；未知事件 void；
- 空文件与全重复文件：登记来源但不消耗知识序号；
- 截止跨越：把 10:00 的出发更正到 12:30（越过 12:00 的 `T`），再更正回 09:58。

批次的 `expected` 标签只是文档；接受/拒绝的真正权威是独立预言机。

## 逐批核对的内容

对每个批次，执行器同时调用生产 `storage.ingest` 与 `RevisionOracle.ingest`，比对：

1. **接受/拒绝决策**必须一致；
2. 接受时比对全部计数字段：`input_rows / inserted / duplicates / total_events /
   knowledge_seq / new_revisions / active_events / source_sha256`；
3. 拒绝时比对数据库与批前快照逐字节相同（失败批无任何残留，来源登记也不留下）；
4. 每个批次后导出 SQLite **全部六张表**（`trips / revisions / event_projection /
   knowledge_batches / sources / meta`）的完整有序内容，与预言机快照逐行相等；
5. 单独核对每条修订的 `source_sha256` 首次来源归属，重复投递不得改写归属；
6. 在 5 个事件时间截止 `T` × 全部已知知识序号 `K` 的网格上，逐班状态与路线指标
   （准点率、取消率、平均延误、平均航行时间、证据状态）全结构相等；
7. 每个 `(K, T)` 历史报告首次生成后冻结 JSON/CSV 字节，后续入库完成后再重建，
   必须字节一致。

## 运行命令

```sh
# 全量门禁：40 条固定种子历史（约 8 秒），摘要写入 run/quality-gate/summary.json
python3 -m quality_gate.run_gate --seeds 40 --out run/quality-gate

# 导出一条历史为自包含单例包（JSON，无 RNG 依赖）
python3 -m quality_gate.run_gate --seeds 1 \
    --save-bundle quality_gate/singleton-bundle.json --seed 20260101

# 脱离随机生成器的单例重放（CI、离线复核、失败现场复现都用它）
python3 -m quality_gate.replay --bundle quality_gate/singleton-bundle.json

# 重放时指定失败工件目录
python3 -m quality_gate.replay --bundle quality_gate/singleton-bundle.json \
    --failures-dir run/qg-failures

# 单元测试（含门禁与变异测试），或整个项目的全套件
python3 -m unittest tests.test_quality_gate tests.test_quality_mutations -v
python3 -m unittest discover -s tests -v
```

## 失败时保存什么

任一批次出现预期/实际分歧时，`run/<gate>/failures/seed-<种子>-batch-<序号>/`
会落盘：

- `seed.txt`：固定种子；
- `schedule.csv` 与 `batch-01..15.jsonl`：**完整输入**（不是节选）；
- `diff.json`：失败定位（批次标签、上下文、`expected`、`actual`、细节）与
  重放提示；
- `history-bundle.json`：可直接交给 `quality_gate.replay` 复现的自包含包。

运行数据库与所有 `run/` 产物都被 `.gitignore` 忽略，不会进入版本控制。

## 隔离变异检查

`tests/test_quality_mutations.py` 通过 `ast` 重写临时副本来注入三个真实故障，
测试以**行为差异**（预言机预期 vs 变异体实际）作为杀死证据，不读取、不匹配源码
文本，且每个变异体位于系统临时目录的独立别名包，结束即删除：

| 变异 | 注入的错误 | 被什么核对杀死 |
| --- | --- | --- |
| `duplicate_batch_advances_seq` | 知识序号赋值变成无条件，空/全重复批也推进序号 | 批次计数与 `knowledge_seq` 比对 |
| `filter_time_before_revision` | 报告先按事件时间过滤再选最高修订，复活被更正的旧值 | 双重截止网格的报告结构比对 |
| `redelivery_overwrites_first_source` | 重复投递时 `UPDATE` 改写已接受修订的首次来源 | `revisions` 全表与首次来源归属比对 |

测试同时断言：未变异的原版通过门禁；三个变异全部失败（被杀死）；变异前后
`ferry_observer/` 工作树字节不变；临时目录无变异包残留。
