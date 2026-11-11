# GARET 核心算法

本包实现 Woo 等人的 GARET（Cluster Computing 2020）§4.2 的线性历史预测与 §4.3 / Algorithm 1 的 Full Shuffling。目录和历史独立于交易图，不调用 CLPA、Louvain 或 GLAS 的图构建与目标函数。

## 固定账户组

`cmd/garet_catalog` 使用现有 `workload.Open` 的统一准入规则，只读取前 `--count` 条准入交易的 From、To 和附属 Calls 的 Caller、Callee 身份。它不根据交易频次、Gas、边关系或后续交易优化分组；但遵循公共读取器的 Gas 校验。提前 EOF 是错误。

地址接受可选的 `0x` / `0X` 前缀，规范化为40位小写十六进制，去重排序后划为连续等量组。不能整除时，前几个组多一个地址；每组至少一个地址。默认100组，可配置。这个具体分桶方法是复现适配约定：论文要求等量地址组，但未指定分桶函数。

目录仅保存 schema_version、address_count 及每组 upper_bound / count，不保存完整地址映射。运行时二分首个大于等于地址的上边界；低于最小样本仍归首组，高于最后上边界归末组。因此后来出现的地址也能确定组，目录不会动态改写。目录构建样本等量不保证后来新增地址仍等量。

`Digest()` 是规范紧凑 JSON 的 SHA256，不受缩进影响。输入文件身份、前缀交易数量和工具版本由 runner 的缓存 manifest 绑定，目录自身只表达账户组内容。初始分片按 group ID round-robin 分配；不能整除时各片组数相差至多1。初始映射和同负载并列顺序都是显式适配约定。

## 历史窗口与预测

`NewHistory(groupCount, slotNS, cycleSlots)` 默认配置由调用方传入：`DefaultSlotNS = 8s`，`DefaultCycleSlots = 6`。时钟从首次注入开始，使用相对纳秒。时槽为左闭右开区间，回执恰好落到边界时归新时槽。

调用方以收到执行回执的时间调用 `Record(relativeNS, group, gas)`，按真实执行阶段的归属组累加 Gas；不会把后续交易需求或图边权写入历史。具体阶段归属和公共平台的额外执行成本是平台适配，不是论文另加的预测项。

预测时间位于第 k 个重分配周期时，使用第 k-2 周期：跳过最近已闭合周期 k-1。例如每周期6个8秒槽，t=96秒首次就绪，使用[0,48)秒，跳过[48,96)秒。图6的论文例子则为5槽周期，使用 C[t-10] 到 C[t-6]。

组 i 的预测是 sum(gas[i,j] * j) / sum(j)，j 从1到 cycleSlots 按时间递增。API 将共同分母保存为 `Prediction.WeightDenominator`，分子保存为 `Prediction.WeightedGas`；预测值是单时槽 Gas，不是周期总量。FS只需比较分子，避免浮点和整数除法截断。预测同时给出 Cycle、SourceStartNS、SourceEndNS 供日志核查。

不足两个完整周期返回 ready=false，调用方维持初始映射。空槽为0。最多保留当前及前两个周期，清理边界只随最大已见时间向前移动。Record 允许保留窗口内、尚未用于预测的周期乱序到达，避免回执时间先捕获而回调后获锁时被误拒。已过期或已封存周期的回执返回可用 `errors.Is(err, garet.ErrLateSample)` 识别的错误；调用方单独计数，公共执行Gas指标仍保留，但不得把这些Gas转移到后续预测周期。

每个源周期首次成功 Predict 后封存结果；重复预测返回缓存的独立副本，迟到回执和调用方修改返回切片都不能改变已用预测。Predict 时间须相对前一次 Predict 单调不减，但可以小于先获锁的 Record 时间；已清除的源周期会显式报错，不冒充空周期。调用方负责加锁。Gas加法、权重乘法、累计预测和分片负载溢出都返回错误。

## Full Shuffling

`FullShuffle(weightedGas, shardCount)` 清零所有分片负载，按组预测Gas降序逐组分配到当前预测负载最小分片。同Gas组选较小group ID；同负载分片先选已分配组数较少者，再选较小shard ID，使全零观测时组数均匀分散。该并列规则是确定性适配约定，论文没有指定，最小预测Gas仍是第一排序条件。FS重新考虑所有组，但不强迫每组实际换片。

实现不增加容量拒绝分支、迁移惩罚、跨片率目标或保持原片的偏好。论文形式化问题包含 Gaslimit，但 Algorithm 1 没有容量检查；不能将其改成容量约束装箱。论文伪代码的遍历末项遗漏按正文“全部组”修正。

本模块仅实现默认 GARET-FS，不包含 §5.3 的 GARET-PS。已有平台负责映射生效、状态迁移、队列路由和统一指标。

## 验证

`go test ./partition/garet ./cmd/garet_catalog`

测试包含手算权重与FS、前前周期滞后、边界和空槽、并列规则、目录等量与区间归属、输入规范化、规范指纹、提前EOF、统一准入前缀、Call端点身份和溢出错误。测试仅使用临时小型合成CSV，不读取正式数据，不启动节点。
