# TxAllo（ContribChain 实现）账户基线

本包移植 `ContribChain-INFOCOM2025/louvain` 中的 `isTxAllo=true` 分支。它是 ContribChain 项目实现的 TxAllo 变体，不声称完整复现 TxAllo 论文。现有 `partition/plouvain` 的 `P_Louvain_Gas` 不受影响。

## 图与方法

- 每笔已完成顶层逻辑交易形成无向账户关系，重复关系累加交易次数；规划不使用 `GasUsed`、内部调用或待处理交易。
- `Graph.AddTransaction` 应只接收片内完成交易和跨片最终阶段 Relay2；不要加入 Relay1。
- 完整交易仍由执行层按原有 Gas 字段计费，本包不修改交易对象。
- 原有三个步骤保留：TxAllo 分支的 Louvain 社区发现；按社区账户数排序、将整社区放入分片；在邻居所在分片间逐账户移动。
- 规划负载为 `(innerTx + beta * crossIncidentTx) / shardPerformance`；各分片性能固定为 `1`，不接入 ContribChain 的节点贡献评价与节点重分配。
- 默认 `beta=2`，第三阶段最多 `70` 次。仅接收严格为正的源/目标两片最大处理时间下降收益。`MaxIterations=0` 表示禁用第三阶段。

## 接口与指标

使用 `NewGraph()`、`AddTransaction(from,to)` 累计图，调用 `Optimize(graph,current,DefaultConfig(K))` 获得映射。`current` 用于划分前诊断，不用于初始化原算法；原算法每轮重新发现并分配社区。成功发送新映射后，由调用方 `Reset()` 图。

结果的 `Before`、`After`、`ShardLoads` **均为交易计数规划指标**。`CrossTransactionRate` 是该轮历史图的跨片逻辑交易比例，不是运行期间完成 TPS 或通用的内部调用并集跨片比例。`MovedAccounts` 是相对输入 `current` 映射的不同账户数，`RefinementPasses` 是第三阶段实际调用次数，不等于迁移操作次数。

## 与原源码的适配边界

`internal/contribchain/graph.go` 和 `louvain.go` 仅添加来源说明和修改包名，保留源公式、float32 算术、遍历计数及停止条件。没有用现有 Gas P-Louvain 优化器代替，也没有修正源实现独特的 `unProcessed` 遍历。

适配层保留账户首次出现顺序，并按节点编号稳定排列第一层邻接关系；原 GraphReader 使用 Go map 遍历，遇到平分收益可能有顺序噪声。源核心在后续社区聚合层仍遍历 Go map，本移植保留这一行为，因此不保证任意输入的逐账户映射完全确定，也不承诺与原项目任意一次随机 map 遍历结果逐账户一致。分片性能等值化和共同执行层是账户算法对比的实验设定，并非完整 ContribChain 协议。

空图直接返回空映射；单片直接返回全 0；无效参数和缺失/越界当前映射返回错误。原共同 CSV reader 会过滤自转账，源计数图/迁移代码不支持原始自环；本包明确拒绝 `from==to`，不静默丢弃或双计。为保留源 float32 的精确整数计数，每轮图最多允许 `2^23` 笔（总度数最多 `2^24`），超限返回错误。

源许可保留在 `LICENSE`。`testdata/source_oracle.json` 记录源文件 SHA-256 及六组直接运行原源码生成的预期结果；原始目录不含 Git 元数据，故来源 commit 为 null。测试除固定结果对照外还核对移植核心文件与源文件的字节摘要、计数守恒、重复关系、自环拒绝、空图及参数边界。
