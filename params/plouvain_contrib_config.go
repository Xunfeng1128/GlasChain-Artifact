package params

import (
	"fmt"
	"math"
)

// ValidatePLouvainContribConfig checks the account-only ContribChain baseline. ShardNum
// is supplied later through -S; the optimizer validates it after CLI parsing.
func ValidatePLouvainContribConfig() error {
	if BlockCapacityMode != "gas" || BlockGasLimit == 0 {
		return fmt.Errorf("P_Louvain_Contrib requires BlockCapacityMode=gas and a positive BlockGasLimit")
	}
	if (!UnifiedExecution && DatasetFile == "") || ReconfigTimeGap <= 0 {
		return fmt.Errorf("DatasetFile and a positive ReconfigTimeGap are required")
	}
	if InjectSpeed <= 0 || TxBatchSize <= 0 || TotalDataSize <= 0 {
		return fmt.Errorf("InjectSpeed, TxBatchSize and TotalDataSize must be positive")
	}
	if math.IsNaN(PLouvainContribBeta) || math.IsInf(PLouvainContribBeta, 0) || PLouvainContribBeta < 0 || PLouvainContribBeta > math.MaxFloat32 {
		return fmt.Errorf("PLouvainContribBeta must be finite, nonnegative and representable as float32")
	}
	if PLouvainContribMaxIterations < 0 {
		return fmt.Errorf("PLouvainContribMaxIterations must be nonnegative")
	}
	return nil
}
