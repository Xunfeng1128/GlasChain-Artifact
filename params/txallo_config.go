package params

import (
	"fmt"
	"math"
)

// ValidateTxAlloConfig checks the account-only ContribChain baseline. ShardNum
// is supplied later through -S; the optimizer validates it after CLI parsing.
func ValidateTxAlloConfig() error {
	if BlockCapacityMode != "gas" || BlockGasLimit == 0 {
		return fmt.Errorf("TxAllo_Contrib requires BlockCapacityMode=gas and a positive BlockGasLimit")
	}
	if (!UnifiedExecution && DatasetFile == "") || ReconfigTimeGap <= 0 {
		return fmt.Errorf("DatasetFile and a positive ReconfigTimeGap are required")
	}
	if InjectSpeed <= 0 || TxBatchSize <= 0 || TotalDataSize <= 0 {
		return fmt.Errorf("InjectSpeed, TxBatchSize and TotalDataSize must be positive")
	}
	if math.IsNaN(TxAlloBeta) || math.IsInf(TxAlloBeta, 0) || TxAlloBeta < 0 || TxAlloBeta > math.MaxFloat32 {
		return fmt.Errorf("TxAlloBeta must be finite, nonnegative and representable as float32")
	}
	if TxAlloMaxIterations < 0 {
		return fmt.Errorf("TxAlloMaxIterations must be nonnegative")
	}
	return nil
}
