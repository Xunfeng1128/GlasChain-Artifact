# P_Louvain_Gas 基线

`P_Louvain_Gas` 是把 ContribChain P-Louvain 直接 Gas 化后的对照基线，模式编号为 `7`。

它保留的流程是：

1. 用上一轮已经完成的顶层交易建无向账户图；
2. 将一条关系的边权由“交易次数”改为累计 `GasUsed`；
3. 运行 Gas 加权 Louvain 社区发现；
4. 按 P-Louvain 的社区到分片启发式分配社区；
5. 在邻居所在分片之间逐账户迁移，目标负载为 `innerGas + beta * crossIncidentGas`；
6. 账户迁移后，下一轮仍按真实 `BlockGasLimit` 打包交易。

它故意不读取待处理交易、内部合约调用轨迹，也没有加入容量硬约束或全局装箱。因此它适合回答一个单独的问题：只把 P-Louvain 的权重和区块容量同时换成 Gas，算法是否仍然有效。

## 运行

示例配置是仓库根目录下的 `paramsConfig_plouvain_gas.json`。16 分片、每片 4 节点的运行命令为：

```bash
BLOCKEMULATOR_CONFIG=./paramsConfig_plouvain_gas.json ./darwin_exe_run_IpAddr=127_0_0_1_S16N4.sh
```

配置中的两个专用参数是：

- `PLouvainGasBeta`：跨片 incident Gas 的惩罚系数，默认 `2.0`；
- `PLouvainGasMaxIterations`：第三阶段逐账户迁移的最大遍数，默认 `70`。

该模式强制要求 `BlockCapacityMode` 为 `gas`，并要求 `BlockGasLimit > 0`。

## 诊断输出

除系统已有 TPS、确认延迟和跨片率结果外，`result/supervisor_measureOutput` 会新增：

- `PLouvainGas_Round.csv`：每轮划分前后跨片交易率、跨片 Gas 比例、最大分片 Gas、预计所需块数和 Gas 负载 CV；
- `PLouvainGas_Shard.csv`：每轮、每个分片在划分前后的 `InnerGas`、`CrossIncidentGas`、物理 Gas 和所需块数；
- `PLouvainGas_Community.csv`：Louvain 社区规模、内部/外部 Gas 以及第二阶段的初始分片。

其中运行时的跨片交易仍采用现有 Relay 两阶段语义：跨片交易的源片和目标片各执行一次完整 `GasUsed`。诊断里的 `CrossOverheadGas` 是相对顶层逻辑交易额外产生的第二份 Gas。
