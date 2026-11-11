# 统一吞吐、确认延迟与跨片比例统计

本目录仅离线读取新日志，不启动节点、不运行实验，不修改日志或配置。使用 Python 3.9+ 标准库，无须安装第三方包。逐行读取 CSV，用临时磁盘 SQLite 去重，避免把数百万笔交易全部读入内存；处理期间应预留磁盘空间，完成或报错后自动删除临时索引。

## 用法

等一次实验正常结束，再在项目根目录运行：

```bash
python3 scripts/unified_metrics/summarize.py \
  --run-dir /你的实验输出目录/result/unified_metrics \
  --window-seconds 48 \
  --duration-seconds 960 \
  --output-dir /新的统计输出目录
```

`--run-dir` 必须包含同一次运行的 `run.json` 和 `events.csv`。`--output-dir` 必须是新目录或空目录，已有统计结果不会被覆盖。以上 E1 用法同时生成全程结果和前 960 秒结果，过程曲线每 48 秒一箱。原始日志必须覆盖完整观察时长；不能通过补零把短运行画成完整曲线。命令行兼容既有用法，`--window-seconds` 未指定时仍默认 60 秒。

输出：

- `windows.csv`：指定观察区间内各时间箱的四项指标、跨片比例、注入与积压、可合并的整数分子。
- `summary.json`：`primary` 为指定观察区间结果；`full_run` 为同一次完整运行结果。
- `validation.json`：完整性检查、重复回执数量、错误原因。

仅当 `summary.json` 的 `valid_for_publication` 为 `true`，此次统计才通过本脚本的指标日志完整性检查；它不认证数据集、重配置安排、建图可用信息是否公平，也不自动证明论文主张。

## 绘图口径

时间零点是 supervisor 首次实际派发交易的时刻。完成时间采用 supervisor 收到该交易完成回执的单调时钟，避免混用不同服务器的时钟。因此这是**统一接收完成回执口径**，包括回执传输耗时；不等同于 worker 本地提交时间。

单笔延迟 `L = 最早最终完成回执 elapsed_ns − 最早实际注入 elapsed_ns`，包括实际注入后的重配置暂停与执行等待，不包括实际注入前的等待。这里不使用计划到达时间，不改变原始日志的时间原点。重试派发和重复回执均取各自最早时间。

按 `[0,60)`、`[60,120)` 等固定窗口统计。恰好在 60 秒完成的交易计入第二个窗口，恰好在观察终点完成的不计入该观察区间。暂停、迁移期间的时间照常包含在分母中。最后一个不足 60 秒的窗口使用其实际宽度。

| 绘图指标 | 计算方式 |
|---|---|
| 完成 TPS | 本窗口唯一完成的原始交易数 / 本窗口秒数 |
| 基础 Gas 吞吐 | 本窗口已完成交易的基础 Gas 总量 / 本窗口秒数 |
| 平均确认延迟 | 本窗口已完成交易的 `sum(L) / count` |
| Gas 加权平均确认延迟 | 本窗口已完成交易的 `sum(base_gas × L) / sum(base_gas)` |
| 顶层跨片比例 | 本窗口完成交易中，以 `RELAY2` 完成的笔数 / 本窗口完成笔数 |
| 内部调用跨片比例 | 本窗口完成交易中，最终执行阶段内部跨片调用次数大于 0 的笔数 / 本窗口完成笔数 |
| 并集跨片比例 | 本窗口完成交易中，顶层跨片或内部调用跨片的笔数 / 本窗口完成笔数 |

原始交易按 `source_tx_hash` 识别，只在 `LOCAL` 或 `RELAY2` 完成时计一次；`RELAY1` 不算半笔或一笔完成。并集采用“或”：同一笔同时具有两类跨片只计一笔。内部跨片以最终执行阶段实际记录的调用数为准；不会因为跨片调用费设置为 0 而变成“没有内部跨片”。

四项指标使用**同一个最终完成集合**，不把窗口内注入但窗口结束后才完成的交易加入窗口延迟。基础 Gas 每笔只计一次，既不累计 RELAY1 的重复基础工作，也不将 `attached_gas` 加入有效吞吐或权重。窗口延迟只描述已完成交易，应与全程延迟一起解释。

`windows.csv` 的 `tps`、`base_gas_per_second`、`mean_latency_seconds`、`gas_weighted_mean_latency_seconds` 直接绘制；后两者单位均为秒。各 `*_ratio` 列是 **0～1 的比例**；百分数纵轴乘以 100。没有完成交易时两项吞吐为 0，延迟、平均基础 Gas 和比例为 `NA`（JSON 中为 `null`），相应曲线应留空。横轴可用 `end_seconds`，图注注明实际分箱宽度。

整体柱状图使用 `summary.json` 的 `primary`：

- 整体 TPS = 区间内完成总数 / 区间总秒数。
- 整体跨片比例 = 区间内跨片完成总数 / 区间内完成总数。
- 整体普通延迟 = 区间内 `latency_sum_ns / completed / 1e9`。
- 整体 Gas 加权延迟 = 区间内 `gas_latency_sum_ns / base_gas / 1e9`。
- 不要对各窗口比例或平均延迟做算术平均，也不要使用窗口 TPS 的简单平均来处理最后一个较短窗口。

例如一个窗口完成 10 笔、其中 5 笔跨片，另一个窗口完成 90 笔、其中 9 笔跨片，整体比例是 `(5+9)/(10+90)=14%`，不是 `(50%+10%)/2=30%`。

同一设置有多次重复时，每次先独立得到完整的窗口与整体统计，再按预先确定的规则画重复间中位数及范围；不要把重复当作一次更长的运行，也不要混合不同交易集合计算“平均跨片比例”。

## 检查与诊断

默认要求：logger 正常关闭、日志行数符合元数据、观察时长足够、配置要求的交易数全部派发且完成、块内记录完整、Relay 阶段齐全。无论主图只看前 600 秒还是 1800 秒，默认都检查**完整运行**，防止短日志或丢交易产生看似正常的主图。

还会检查：来源交易已派发、基础 Gas 一致、最终阶段 Gas 与内部跨片附加费一致、每块 Gas 不超过上限。重复到达的同块同阶段回执按最早接收时间去重；同一阶段在不同区块再次执行、同一回执内容冲突、同一来源同时出现 LOCAL 和 Relay 路径会报错，不会悄悄去重掩盖错误。

有问题时退出码为 2，错误详见 `validation.json`。`--allow-incomplete` 用于排查**日志正常关闭，但交易未完成或观察时长不足**的运行，会明确标为 `valid_for_publication=false`，并且只统计已有覆盖的时间；不放宽计费冲突、重复执行、原始行数不一致等完整性错误。突然崩溃的运行尚未写入最终时长和行数，无法确认日志覆盖范围，因此仍会拒绝；该选项不是崩溃日志恢复工具。

`--start-seconds 60 --duration-seconds 600` 可事后观察 `[60,660)`。任意新窗口都可以从保留的原始日志重新计算；若直接合并已有时间箱，窗口边界必须与分箱边界对齐。默认从 0 开始，不自动替各算法寻找最有利的区间。全程结果不受指定窗口起止影响。

`full_run.duration_seconds` 等于最后一笔最终完成回执相对首笔实际注入的时间；全程集合包括这一笔最终完成，分母不使用 logger 关闭时刻。`full_run.drain_seconds` 表示最后一次派发到最后一次接收完成回执的时间，只有完整运行检查通过才给值。若没有完成事件，全程时长为 `null`；若最后完成时刻恰为 0，全程时长为 0，两项吞吐为 `null`，避免除零。诊断模式下的完成子集指标不表示成功完成全部负载。

## 输出字段契约

既有 `summary.json.schema_version=1`、`primary` 中 TPS/跨片字段及 `full_run` 中完成检查字段保持兼容，新增字段如下；输入仍使用原有 schema 1，不依赖其他控制事件文件。

| 字段 | `primary`、`full_run` 与 `windows.csv` 的含义 |
|---|---|
| `completed` | 本完成集合的唯一交易数 |
| `base_gas` | 本完成集合的基础 Gas 总量，整数 |
| `base_gas_per_second` | 基础 Gas 总量 / 对应统计时长 |
| `mean_latency_seconds` | 实际注入到最终回执的普通平均延迟 |
| `gas_weighted_mean_latency_seconds` | 按基础 Gas 加权的同一延迟 |
| `mean_base_gas` | `base_gas / completed`，用于解释窗口完成集合的异构程度 |
| `latency_sum_ns` | 完成集合的延迟总和，整数纳秒 |
| `gas_latency_sum_ns` | `sum(base_gas × latency_ns)`，整数 Gas·纳秒 |
| `injected`、`injected_base_gas` | 指定区间内实际注入数/基础 Gas；全程为全部实际注入 |

整数总和与乘积由 Python 任意精度整数累计，不通过 SQLite 浮点 `total()` 计算。JSON/CSV 保存完整整数；外部 JavaScript 或表格工具如将它们转成浮点数，需自行避免大整数精度损失。

`primary` 和每个时间箱另有 `injected_before_end`、`completed_before_end`、`outstanding_at_start/end`、`outstanding_base_gas_at_start/end`。截止点采用严格小于：在 end 恰好完成的交易仍属于该截止点的未完成集合，在 end 恰好注入的交易不计入。积压包含更早区间注入的交易，因此对于 start>0 的窗口，不能简单用本窗口 `injected - completed` 代替末尾积压。

`full_run` 保留 `expected_transactions`、`dispatched_unique`、`completed_unique`、`outstanding`、`last_dispatch_seconds`、`last_completion_seconds`、`drain_seconds`，新增 `outstanding_base_gas`、`duration_seconds` 和 `complete`。`complete` 与此次完整性检查状态一致；所有诊断输出均为 false。

## 输入协议与测试

`run.json` 的 `schema_version=1`，时钟名为 `supervisor_receipt_monotonic`。`events.csv` 的固定列为：

```text
event,source_tx_hash,stage,elapsed_ns,observed_unix_ns,worker_commit_unix_ns,shard_id,block_height,block_hash,block_body_length,base_gas,stage_gas,cross_call_count,attached_gas,top_cross,internal_cross,union_cross
```

事件类型为 `inject`、`block`、`stage`；派发阶段为 `TOP`，执行阶段为 `LOCAL`、`RELAY1`、`RELAY2`。旧 TPS/跨片比例 CSV 不满足此协议，不能直接输入本脚本，也不能与新口径混画。

运行小型人工日志测试（不会启动实验）：

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
  -s scripts/unified_metrics -p 'test_*.py' -v
```

## E2 固定时间吞吐与同一批交易的延迟

同一份运行记录产生两张表。E2-a 对所有唯一最终确认回执做计数，在半开区间
`[0,600)`、`[0,960)` 分别除以 600、960。E2-b 对有效输入序列的前
30/50/70/90 万个源交易哈希分组，使用最早实际注入到最早 LOCAL/RELAY2 回执的延迟。
P95 取从小到大排列后的第 `ceil(0.95 × 样本数)` 个值。超过 960 秒的确认也计入；
四组前缀嵌套，注入日志中不同分片的写入顺序不能代替输入顺序。

独立离线入口（可用于原有完整运行）。`prefix.csv` 必须是与该次运行一致的、
共同读取器准入后的前 90 万个哈希，每行一个，列名 `source_tx_hash`，只用小写
`0x` 加 64 个十六进制字符。请保留生成它的有效数据前缀与文件指纹。

```bash
python3 scripts/unified_metrics/summarize_e2.py \
  --run-dir "$run_dir/result/unified_metrics" \
  --output-dir "$run_dir/analysis_e2" \
  --prefix-file "$prefix_csv" \
  --windows 600 960 --prefixes 300000 500000 700000 900000
```

`e2_throughput.csv` 记录每个窗口的确认数、完成 TPS、注入数与截止时积压；
`e2_latency.csv` 记录每个固定前缀的完成数、平均/P95 延迟、最大窗口结束前确认数、
最后派发与确认时刻。脚本复用统一账本的去重、关联、阶段和 Gas 校验。
如果是提前收尾的 E2 运行，还核对运行时登记的样本文件 SHA-256、最大窗口、
最大前缀和在线计数。只允许样本之外有尚未完成或尚未派发的交易；
时间覆盖不足、样本未齐、坏回执、方法失败或记录器未正常关闭都会拒绝。
旧的全量运行仍要求所有交易完成；这类存档不记录前缀指纹，调用者需用原始输入和相同准入规则生成名单。校验文件的 `prefix_bound_by_recorder` 区分这一情况。

请检查 `e2_validation.json` 的 `valid_for_e2` 再绘图。
`all_input_complete` 单独标明整个原定负载是否全部完成。输出不覆盖已有的 E2 文件，
亦不改动原始运行目录与既有全程统计。
