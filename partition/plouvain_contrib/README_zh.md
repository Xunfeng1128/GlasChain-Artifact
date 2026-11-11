# P-Louvain（ContribChain 原始交易计数账户算法）

本包接入 `ContribChain-INFOCOM2025/louvain` 的原始交易计数 P-Louvain 账户划分分支。它与现有 Gas 加权 `partition/plouvain`、TxAllo 变体 `partition/txallo` 分离，未修改这两个包。本包不包含 ContribChain 的节点贡献评价、节点重分配或完整协议。

## 源码流程与默认参数

原项目 `supervisor/committee/committee_PLouvain.go` 第 93、673 行实际使用 `secStageMoveNode=false, isDeterministic=true, isTxAllo=false, israndom=false`；不是注释中被停用的全 false 分支。

本包调用：

1. `NewLouvain(graph, equalPerformance, false, beta, K)`，各分片性能统一为 1。
2. `Compute(true, true, false)`：原 P-Louvain 的确定性社区发现分支。
3. `CommToShard(beta)` 与 `UpdateNodeShardIndex()`：按社区账户数排序，前 K 个社区分别分配到各片，剩余整社区进入当前建模处理时间最小的片。
4. `NodeMerge(placement, true, false)`：在邻居所在分片之间移动账户，选择使源、目标两片最大建模处理时间严格下降的移动。外层最多 70 次，返回无移动时提前停止。

默认 `beta=2`、`MaxIterations=70`，与原项目默认值一致。`MaxIterations=0` 明确表示跳过第三阶段。建模处理时间为 `(innerTx + beta * crossIncidentTx) / shardPerformance`，其中所有量均按交易次数计算，不使用 Gas 或内部调用。

`isDeterministic=true` 是原源码的分支名，不代表任意输入的输出逐字节确定：社区聚合内部仍使用 Go map 遍历。源代码的 float32 算术、`roundImprovedInc`、遍历标记、收益公式与停止条件全部保留，不在移植时重新解释或修正它们。

## 图与状态边界

每笔已完成顶层逻辑交易为两个账户之间的无向关系增加 1，重复关系聚合计数。调用方应仅加入片内最终完成和跨片 Relay2，不能加入 Relay1；这对应原项目的 `ExcutedTxs`。不读取待处理交易或未来交易。

`Optimize(graph, current, cfg)` 保留输入图和映射。`current` 用于划分前诊断与迁移账户数，不用于暖启动社区发现。成功提交新映射后，由委员会调用 `Graph.Reset()`，下一轮积累新完成的交易；账户映射由委员会保留。原项目同样在每轮划分后重置图并继承映射。

`Before/After`、`ShardLoads`、`CrossTransactionRate` 是该轮历史交易图上的规划诊断指标，不能替代运行期间 TPS、跨片最终交易比例或内部调用并集比例。

适配层保留账户首次出现顺序，并按编号稳定排列初始邻接关系。源 GraphReader 初始邻接顺序来自 Go map，因此平分收益时不承诺与原项目任意一次运行逐账户完全一致。各片性能固定为 1 是仅比较账户算法的共同实验设定。

空图返回空映射，单片直接全部分到 0。无效参数、缺失或越界当前映射、空端点返回错误。原共同输入过滤自转账，源图与迁移计数不支持原始自环，因此本包明确拒绝自转账；没有静默丢弃或自行改变源核心。源核心使用 float32，为保持图整数计数精确，每轮图最多接收 `2^23` 笔（总度最多 `2^24`）；超限在规划入口返回错误。

## 来源与测试

`internal/contribchain/{graph,louvain}.go` 复制自 ContribChain 原项目，仅增加来源注释并改包名。`adapter.go` 仅暴露原私有图结构的构造入口。源 MIT 许可位于 `LICENSE`。为了保留已经接入的 TxAllo 代码不变，本包持有独立源码副本；未来升级时应同时核对两份副本来源。

`testdata/source_oracle.json` 记录源文件 SHA-256、源控制流程文件 SHA-256、原 flags 和直接执行原源码得到的 7 组小图结果。原目录无 Git 元数据，来源 commit 记为 null。小图覆盖多片单关系、断开关系、桥接、禁用细调、细调、零跨片惩罚；第七组明确区分 P-Louvain 和 TxAllo，不能用 TxAllo 分支冒充本算法。每组 P-Louvain 源输出曾重复检查 100 次。

复核 oracle 时，在临时目录复制原 `graph.go`、`louvain.go`，只把包名改为 `main`；复制 `testdata/source_fixture_generator.go.txt` 为 `main.go`，把 fixture 的 `cases` 数组存为 `cases.json`，创建只含标准库的独立 Go module 后执行。这个程序仅计算小图，不启动模拟器、不发送网络交易、不运行系统实验。新包单测同时核对核心源码摘要、oracle、计数守恒、映射与图不被修改、默认参数与边界。
