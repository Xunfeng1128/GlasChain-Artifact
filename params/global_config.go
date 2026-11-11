package params

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
)

var (
	// The following parameters can be set in main.go.
	// default values:
	NodesInShard = 4 // \# of Nodes in a shard.
	ShardNum     = 4 // \# of shards.
)

// consensus layer & output file path
var (
	ConsensusMethod = 0 // Index into CommitteeMethod.

	PbftViewChangeTimeOut = 10000 // The view change threshold of pbft. If the process of PBFT is too slow, the view change mechanism will be triggered.

	Block_Interval = 5000 // The time interval for generating a new block

	MaxBlockSize_global = 2000  // The maximum number of transactions a block contains
	BlocksizeInBytes    = 20000 // The maximum size (in bytes) of block body
	UseBlocksizeInBytes = 0     // Use blocksizeInBytes as the blocksize measurement if '1'.
	BlockCapacityMode   = "tx"  // "tx", "gas", or "bytes"; empty config keeps compatibility with UseBlocksizeInBytes.
	BlockGasLimit       = uint64(42000000)

	InjectSpeed   = 2000   // The speed of transaction injection
	TotalDataSize = 160000 // The total number of txs to be injected
	TxBatchSize   = 16000  // The supervisor read a batch of txs then send them. The size of a batch is 'TxBatchSize'

	BrokerNum            = 10 // The # of Broker accounts used in Broker / CLPA_Broker.
	BrokerFilePath       = `./broker_300k_BlockTransaction/broker_300k_BlockTransaction`
	RelayWithMerkleProof = 0 // When using a consensus about "Relay", nodes will send Tx Relay with proof if "RelayWithMerkleProof" = 1
	EnableGasCostModel   = true

	// StopSignalMode controls the termination strategy.
	// "global"    – original behaviour: single shared counter, threshold = 3 × ShardNums.
	// "per_shard" – each shard keeps its own counter; stop only when every shard
	//               has reported 3 consecutive empty blocks independently.
	StopSignalMode = "global"
	Alpha          = 1.0 // deprecated for the gas model (kept for config compatibility)
	Beta           = 0.5
	Lambda1        = 1.0
	Lambda2        = 1.0

	// Linear gas-to-time model:
	//   ExecDelayMs = FixedPerTxMs + GasSlopeMsPerGas * max(0, gasUsed-21000)
	FixedPerTxMs     = 0.0    // fixed per-tx wall-clock overhead (ms), gas-independent
	GasSlopeMsPerGas = 1.7e-5 // ms per unit of execution gas (gasUsed above the 21000 intrinsic)

	ExpDataRootDir     = "expTest"                     // The root dir where the experimental data should locate.
	DataWrite_path     = ExpDataRootDir + "/result/"   // Measurement data result output path
	LogWrite_path      = ExpDataRootDir + "/log"       // Log output path
	DatabaseWrite_path = ExpDataRootDir + "/database/" // database write path

	SupervisorAddr = "127.0.0.1:18800"        // Supervisor ip address
	IpTableFile    = "./ipTable.json"         // The ip table path for nodes and supervisor
	DatasetFile    = `./selectedTxs_300K.csv` // The raw BlockTransaction data path

	// UnifiedExecution enables one canonical workload and execution accounting
	// for comparison modes. Legacy configurations keep the original paths.
	UnifiedExecution        = false
	UnifiedPackingOptimized = false // opt-in pool preview/commit simplification; legacy paths stay unchanged
	UnifiedMetrics          = false // opt-in logical completion event log; requires UnifiedExecution
	E2ObservationSeconds    = 0     // zero keeps the complete-workload guard
	FormalMapCheckpoint     = 0     // Optional E1 input position for an offline placement audit.
	E2PrefixCount           = 0
	E2PrefixFile            = ""
	UnifiedTransactionsFile = ""
	UnifiedCallsFile        = ""
	UnifiedCrossCallGas     = uint64(5000)

	ReconfigTimeGap   = 50   // The time gap between epochs. This variable is only used in CLPA / CLPA_Broker now.
	CLPAWeightMode    = "tx" // deprecated: use ConsensusMethod=1 for CLPA(tx) or ConsensusMethod=4 for CLPA_Gas.
	CLPAWeightPenalty = 0.5  // CLPA shard-load penalty strength.
	// P_Louvain_Gas is the direct-gasification baseline. It intentionally keeps
	// P-Louvain's original community ordering and neighbor-only refinement while
	// replacing transaction-count graph weights with cumulative GasUsed.
	PLouvainGasBeta          = 2.0
	PLouvainGasMaxIterations = 70

	// TxAllo_Contrib preserves ContribChain's transaction-count TxAllo branch.
	// These are independent of P_Louvain_Gas and the legacy execution Beta.
	TxAlloBeta          = 2.0
	TxAlloMaxIterations = 70

	// P_Louvain_Contrib is ContribChain's original transaction-count account
	// algorithm with equal shard performance; keep its knobs independent.
	PLouvainContribBeta          = 2.0
	PLouvainContribMaxIterations = 70

	// GARET keeps fixed account groups and predicts processed stage Gas from
	// the cycle before the latest closed cycle. Catalogs contain address ranges,
	// not pre-created account states or future workload weights.
	GARETGroupCount                    = 100
	GARETSampleSeconds                 = 8
	GARETGroupCatalogFile              = ""
	GARETReconfigurationTimeoutSeconds = 600 // runtime failure bound, not an optimization parameter
	GLASReconfigTimeGap                = 30  // Default idle-grace floor for the unified recorder.
)

// network layer
var (
	Delay       int // The delay of network (ms) when sending. 0 if delay < 0
	JitterRange int // The jitter range of delay (ms). Jitter follows a uniform distribution. 0 if JitterRange < 0.
	Bandwidth   int // The bandwidth limit (Bytes). +inf if bandwidth < 0
)

// read from file
type globalConfig struct {
	GLASPendingConfigFields
	CLPASchedulerConfigFields
	ConsensusMethod int `json:"ConsensusMethod"`

	PbftViewChangeTimeOut int `json:"PbftViewChangeTimeOut"`

	ExpDataRootDir string `json:"ExpDataRootDir"`

	BlockInterval int `json:"Block_Interval"`

	BlocksizeInBytes    int     `json:"BlocksizeInBytes"`
	MaxBlockSizeGlobal  int     `json:"BlockSize"`
	UseBlocksizeInBytes int     `json:"UseBlocksizeInBytes"`
	BlockCapacityMode   *string `json:"BlockCapacityMode"`
	BlockGasLimit       *uint64 `json:"BlockGasLimit"`

	InjectSpeed   int `json:"InjectSpeed"`
	TotalDataSize int `json:"TotalDataSize"`

	TxBatchSize                        int      `json:"TxBatchSize"`
	BrokerNum                          int      `json:"BrokerNum"`
	BrokerFilePath                     string   `json:"BrokerFilePath"`
	RelayWithMerkleProof               int      `json:"RelayWithMerkleProof"`
	IpTableFile                        string   `json:"IpTableFile"`
	DatasetFile                        string   `json:"DatasetFile"`
	UnifiedExecution                   bool     `json:"UnifiedExecution"`
	UnifiedPackingOptimized            bool     `json:"UnifiedPackingOptimized"`
	UnifiedMetrics                     bool     `json:"UnifiedMetrics"`
	E2ObservationSeconds               int      `json:"E2ObservationSeconds"`
	ExperimentEpochCount               int      `json:"ExperimentEpochCount"`
	ExperimentBlocksPerEpoch           int      `json:"ExperimentBlocksPerEpoch"`
	E2PrefixCount                      int      `json:"E2PrefixCount"`
	FormalMapCheckpoint                int      `json:"FormalMapCheckpoint"`
	E2PrefixFile                       string   `json:"E2PrefixFile"`
	UnifiedTransactionsFile            string   `json:"UnifiedTransactionsFile"`
	UnifiedCallsFile                   string   `json:"UnifiedCallsFile"`
	UnifiedCrossCallGas                *uint64  `json:"UnifiedCrossCallGas"`
	ReconfigTimeGap                    int      `json:"ReconfigTimeGap"`
	CLPAWeightMode                     string   `json:"CLPAWeightMode"`
	CLPAWeightPenalty                  *float64 `json:"CLPAWeightPenalty"`
	PLouvainGasBeta                    *float64 `json:"PLouvainGasBeta"`
	PLouvainGasMaxIterations           *int     `json:"PLouvainGasMaxIterations"`
	TxAlloBeta                         *float64 `json:"TxAlloBeta"`
	TxAlloMaxIterations                *int     `json:"TxAlloMaxIterations"`
	PLouvainContribBeta                *float64 `json:"PLouvainContribBeta"`
	PLouvainContribMaxIterations       *int     `json:"PLouvainContribMaxIterations"`
	GARETGroupCount                    *int     `json:"GARETGroupCount"`
	GARETSampleSeconds                 *int     `json:"GARETSampleSeconds"`
	GARETGroupCatalogFile              string   `json:"GARETGroupCatalogFile"`
	GARETReconfigurationTimeoutSeconds *int     `json:"GARETReconfigurationTimeoutSeconds"`

	StopSignalMode     string   `json:"StopSignalMode"`
	EnableGasCostModel *bool    `json:"EnableGasCostModel"`
	Alpha              *float64 `json:"Alpha"`
	Beta               *float64 `json:"Beta"`
	Lambda1            *float64 `json:"Lambda1"`
	Lambda2            *float64 `json:"Lambda2"`
	FixedPerTxMs       *float64 `json:"FixedPerTxMs"`
	GasSlopeMsPerGas   *float64 `json:"GasSlopeMsPerGas"`

	Delay       int `json:"Delay"`
	JitterRange int `json:"JitterRange"`
	Bandwidth   int `json:"Bandwidth"`
}

func ReadConfigFile() {
	configPath := strings.TrimSpace(os.Getenv("BLOCKEMULATOR_CONFIG"))
	if configPath == "" {
		configPath = "paramsConfig.json"
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		log.Fatalf("Error reading file: %v", err)
	}
	var config globalConfig
	err = json.Unmarshal(data, &config)
	if err != nil {
		log.Fatalf("Error unmarshalling JSON: %v", err)
	}

	// output configurations
	fmt.Printf("Config: %+v\n", config)

	// set configurations to params
	// consensus params
	ConsensusMethod = config.ConsensusMethod
	if ConsensusMethod < 0 || ConsensusMethod >= len(CommitteeMethod) || CommitteeMethod[ConsensusMethod] == "" {
		log.Fatalf("Invalid ConsensusMethod %d: expected a value in [0, %d), representing %v", ConsensusMethod, len(CommitteeMethod), CommitteeMethod)
	}
	if err := loadCLPASchedulerConfig(config.CLPASchedulerConfigFields); err != nil {
		log.Fatalf("invalid CLPA scheduler configuration: %v", err)
	}

	PbftViewChangeTimeOut = config.PbftViewChangeTimeOut

	// data file params
	ExpDataRootDir = config.ExpDataRootDir
	DataWrite_path = ExpDataRootDir + "/result/"
	LogWrite_path = ExpDataRootDir + "/log"
	DatabaseWrite_path = ExpDataRootDir + "/database/"

	Block_Interval = config.BlockInterval

	MaxBlockSize_global = config.MaxBlockSizeGlobal
	BlocksizeInBytes = config.BlocksizeInBytes
	UseBlocksizeInBytes = config.UseBlocksizeInBytes
	if config.BlockGasLimit != nil {
		BlockGasLimit = *config.BlockGasLimit
	}
	if config.BlockCapacityMode != nil {
		BlockCapacityMode = strings.ToLower(strings.TrimSpace(*config.BlockCapacityMode))
	} else if UseBlocksizeInBytes == 1 {
		BlockCapacityMode = "bytes"
	} else {
		BlockCapacityMode = "tx"
	}
	if BlockCapacityMode == "" {
		BlockCapacityMode = "tx"
	}
	if BlockCapacityMode != "tx" && BlockCapacityMode != "gas" && BlockCapacityMode != "bytes" {
		log.Fatalf("Invalid BlockCapacityMode %q: expected \"tx\", \"gas\", or \"bytes\"", BlockCapacityMode)
	}
	if BlockCapacityMode == "gas" && BlockGasLimit == 0 {
		log.Fatalf("Invalid BlockGasLimit %d: expected a positive value when BlockCapacityMode is \"gas\"", BlockGasLimit)
	}

	InjectSpeed = config.InjectSpeed
	TotalDataSize = config.TotalDataSize
	TxBatchSize = config.TxBatchSize

	BrokerNum = config.BrokerNum
	if config.BrokerFilePath != "" {
		BrokerFilePath = config.BrokerFilePath
	}
	RelayWithMerkleProof = config.RelayWithMerkleProof
	if config.IpTableFile != "" {
		IpTableFile = config.IpTableFile
	}
	DatasetFile = config.DatasetFile
	UnifiedExecution = config.UnifiedExecution
	UnifiedPackingOptimized = config.UnifiedPackingOptimized
	UnifiedMetrics = config.UnifiedMetrics
	FormalMapCheckpoint = config.FormalMapCheckpoint
	ExperimentEpochCount, ExperimentBlocksPerEpoch = config.ExperimentEpochCount, config.ExperimentBlocksPerEpoch
	E2ObservationSeconds, E2PrefixCount, E2PrefixFile = config.E2ObservationSeconds, config.E2PrefixCount, config.E2PrefixFile
	UnifiedTransactionsFile = strings.TrimSpace(config.UnifiedTransactionsFile)
	UnifiedCallsFile = strings.TrimSpace(config.UnifiedCallsFile)
	UnifiedCrossCallGas = 5000
	if config.UnifiedCrossCallGas != nil {
		UnifiedCrossCallGas = *config.UnifiedCrossCallGas
	}
	if config.StopSignalMode != "" {
		StopSignalMode = config.StopSignalMode
	}
	if config.EnableGasCostModel != nil {
		EnableGasCostModel = *config.EnableGasCostModel
	}
	if config.Alpha != nil {
		Alpha = *config.Alpha
	}
	if config.Beta != nil {
		Beta = *config.Beta
	}
	if config.Lambda1 != nil {
		Lambda1 = *config.Lambda1
	}
	if config.Lambda2 != nil {
		Lambda2 = *config.Lambda2
	}
	if config.FixedPerTxMs != nil {
		FixedPerTxMs = *config.FixedPerTxMs
	}
	if config.GasSlopeMsPerGas != nil {
		GasSlopeMsPerGas = *config.GasSlopeMsPerGas
	}

	ReconfigTimeGap = config.ReconfigTimeGap
	if config.CLPAWeightMode != "" {
		CLPAWeightMode = strings.ToLower(config.CLPAWeightMode)
	}
	if CLPAWeightMode != "tx" && CLPAWeightMode != "gas" {
		log.Fatalf("Invalid CLPAWeightMode %q: expected \"tx\" or \"gas\"", CLPAWeightMode)
	}
	if config.CLPAWeightPenalty != nil {
		CLPAWeightPenalty = *config.CLPAWeightPenalty
	}
	if config.PLouvainGasBeta != nil {
		PLouvainGasBeta = *config.PLouvainGasBeta
	}
	if config.PLouvainGasMaxIterations != nil {
		PLouvainGasMaxIterations = *config.PLouvainGasMaxIterations
	}
	if ConsensusMethod == 7 {
		if BlockCapacityMode != "gas" || BlockGasLimit == 0 {
			log.Fatal("P_Louvain_Gas requires BlockCapacityMode=\"gas\" and a positive BlockGasLimit")
		}
		if (!UnifiedExecution && DatasetFile == "") || ReconfigTimeGap <= 0 || PLouvainGasBeta < 0 || PLouvainGasMaxIterations < 0 {
			log.Fatal("invalid P_Louvain_Gas configuration")
		}
	}
	if config.TxAlloBeta != nil {
		TxAlloBeta = *config.TxAlloBeta
	}
	if config.TxAlloMaxIterations != nil {
		TxAlloMaxIterations = *config.TxAlloMaxIterations
	}
	if ConsensusMethod == 8 {
		if err := ValidateTxAlloConfig(); err != nil {
			log.Fatalf("invalid TxAllo_Contrib configuration: %v", err)
		}
	}
	if config.PLouvainContribBeta != nil {
		PLouvainContribBeta = *config.PLouvainContribBeta
	}
	if config.PLouvainContribMaxIterations != nil {
		PLouvainContribMaxIterations = *config.PLouvainContribMaxIterations
	}
	if ConsensusMethod == 9 {
		if err := ValidatePLouvainContribConfig(); err != nil {
			log.Fatalf("invalid P_Louvain_Contrib configuration: %v", err)
		}
	}
	GARETGroupCount, GARETSampleSeconds = 100, 8
	GARETReconfigurationTimeoutSeconds = 600
	GARETGroupCatalogFile = strings.TrimSpace(config.GARETGroupCatalogFile)
	if config.GARETGroupCount != nil {
		GARETGroupCount = *config.GARETGroupCount
	}
	if config.GARETSampleSeconds != nil {
		GARETSampleSeconds = *config.GARETSampleSeconds
	}
	if config.GARETReconfigurationTimeoutSeconds != nil {
		GARETReconfigurationTimeoutSeconds = *config.GARETReconfigurationTimeoutSeconds
	}
	if ConsensusMethod == 10 {
		if err := ValidateGARETConfig(); err != nil {
			log.Fatalf("invalid GARET configuration: %v", err)
		}
	}
	if ConsensusMethod != 12 && bool(config.GLASPendingConfigFields.GLASPendingFullWindowEarly) {
		log.Fatal("GLASPendingFullWindowEarly requires GLAS_Pending")
	}
	if ConsensusMethod != 12 && bool(config.GLASPendingConfigFields.GLASPendingEarlyReport) {
		log.Fatal("GLASPendingEarlyReport requires GLAS_Pending")
	}
	if ConsensusMethod == 12 {
		loadGLASPendingConfig(config.GLASPendingConfigFields)
		if err := ValidateGLASPendingConfig(); err != nil {
			log.Fatal(err)
		}
	}
	if err := ValidateExperimentEpochQuota(); err != nil {
		log.Fatal(err)
	}
	if err := ValidateUnifiedExecutionConfig(); err != nil {
		log.Fatal(err)
	}

	// network params
	Delay = config.Delay
	JitterRange = config.JitterRange
	Bandwidth = config.Bandwidth
}
