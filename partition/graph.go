// 图的相关操作
package partition

// 图中的结点，即区块链网络中参与交易的账户
type Vertex struct {
	Addr string // 账户地址
	// 其他属性待补充
}

// 描述当前区块链交易集合的图
type Graph struct {
	VertexSet  map[Vertex]bool              // 节点集合，其实是 set
	EdgeSet    map[Vertex][]Vertex          // 记录节点与节点间是否存在交易，邻接表
	EdgeWeight map[Vertex]map[Vertex]uint64 // 记录账户间累计边权，tx 模式为交易次数，gas 模式为累计 GasUsed
	// lock      sync.RWMutex       //锁，但是每个储存节点各自存储一份图，不需要此
}

// 创建节点
func (v *Vertex) ConstructVertex(s string) {
	v.Addr = s
}

// 增加图中的点
func (g *Graph) AddVertex(v Vertex) {
	if g.VertexSet == nil {
		g.VertexSet = make(map[Vertex]bool)
	}
	g.VertexSet[v] = true
}

// 增加图中的边
func (g *Graph) AddEdge(u, v Vertex) {
	g.AddWeightedEdge(u, v, 1)
}

// 增加图中的加权边
func (g *Graph) AddWeightedEdge(u, v Vertex, weight uint64) {
	// 如果没有点，则增加边
	if _, ok := g.VertexSet[u]; !ok {
		g.AddVertex(u)
	}
	if _, ok := g.VertexSet[v]; !ok {
		g.AddVertex(v)
	}
	if g.EdgeSet == nil {
		g.EdgeSet = make(map[Vertex][]Vertex)
	}
	if g.EdgeWeight == nil {
		g.EdgeWeight = make(map[Vertex]map[Vertex]uint64)
	}
	if g.EdgeWeight[u] == nil {
		g.EdgeWeight[u] = make(map[Vertex]uint64)
	}
	if g.EdgeWeight[v] == nil {
		g.EdgeWeight[v] = make(map[Vertex]uint64)
	}
	// 无向图，使用双向边
	if _, ok := g.EdgeWeight[u][v]; !ok {
		g.EdgeSet[u] = append(g.EdgeSet[u], v)
		g.EdgeSet[v] = append(g.EdgeSet[v], u)
	}
	g.EdgeWeight[u][v] += weight
	g.EdgeWeight[v][u] += weight
}

// 复制图
func (dst *Graph) CopyGraph(src Graph) {
	dst.VertexSet = make(map[Vertex]bool)
	for v := range src.VertexSet {
		dst.VertexSet[v] = true
	}
	if src.EdgeSet != nil {
		dst.EdgeSet = make(map[Vertex][]Vertex)
		for v := range src.VertexSet {
			dst.EdgeSet[v] = make([]Vertex, len(src.EdgeSet[v]))
			copy(dst.EdgeSet[v], src.EdgeSet[v])
		}
	}
	if src.EdgeWeight != nil {
		dst.EdgeWeight = make(map[Vertex]map[Vertex]uint64)
		for v, edges := range src.EdgeWeight {
			dst.EdgeWeight[v] = make(map[Vertex]uint64)
			for u, weight := range edges {
				dst.EdgeWeight[v][u] = weight
			}
		}
	}
}

// 输出图
func (g Graph) PrintGraph() {
	for v := range g.VertexSet {
		print(v.Addr, " ")
		print("edge:")
		for _, u := range g.EdgeSet[v] {
			print(" ", u.Addr, "\t")
		}
		println()
	}
	println()
}
