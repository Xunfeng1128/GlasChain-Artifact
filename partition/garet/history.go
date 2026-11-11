package garet

import (
	"errors"
	"fmt"
	"math"
	"time"
)

const (
	DefaultSlotNS     = int64(8 * time.Second)
	DefaultCycleSlots = 6
)

// ErrLateSample means a receipt belongs to an expired or already predicted
// source cycle. Callers can count it separately while retaining its execution
// Gas in their common metrics; it must not enter a later prediction instead.
var ErrLateSample = errors.New("GARET sample belongs to an expired or frozen cycle")

// Prediction keeps the common denominator separate, so ordering and cumulative
// shard loads remain exact integers. WeightedGas / WeightDenominator is the
// predicted Gas per slot, not total Gas per relocation cycle.
type Prediction struct {
	WeightedGas       []uint64
	WeightDenominator uint64
	Cycle             int64
	SourceStartNS     int64
	SourceEndNS       int64
}

// History uses timestamps relative to the first injection. Callers serialize
// Record and Predict with their own lock. It retains at most three cycles:
// the current cycle and the two preceding cycles. Empty slots contribute zero.
type History struct {
	groups        int
	slotNS        int64
	cycleSlots    int
	cycleNS       int64
	denominator   uint64
	watermarkNS   int64
	lastPredictNS int64
	cached        *Prediction
	cycles        map[int64][][]uint64
}

func NewHistory(groups int, slotNS int64, cycleSlots int) (*History, error) {
	if groups <= 0 || slotNS <= 0 || cycleSlots <= 0 {
		return nil, fmt.Errorf("history groups, slot duration, and cycle slots must be positive")
	}
	if uint64(cycleSlots) > uint64(math.MaxInt64/slotNS) {
		return nil, fmt.Errorf("history cycle duration overflows int64")
	}
	n := uint64(cycleSlots)
	a, b := n, n+1
	if a%2 == 0 {
		a /= 2
	} else {
		b /= 2
	}
	if b == 0 || a > math.MaxUint64/b {
		return nil, fmt.Errorf("history weight denominator overflows uint64")
	}
	return &History{groups: groups, slotNS: slotNS, cycleSlots: cycleSlots, cycleNS: slotNS * int64(cycleSlots), denominator: a * b, watermarkNS: -1, lastPredictNS: -1, cycles: make(map[int64][][]uint64)}, nil
}

func (history *History) advance(relativeNS int64) {
	if relativeNS <= history.watermarkNS {
		return
	}
	history.watermarkNS = relativeNS
	minimumCycle := relativeNS/history.cycleNS - 2
	for cycle := range history.cycles {
		if cycle < minimumCycle {
			delete(history.cycles, cycle)
		}
	}
}

// Record adds one actually executed stage's Gas to its owner group. The caller
// supplies receipt time, not historical block time. Out-of-order receipts are
// accepted within retained, unfrozen cycles without moving the clock backward.
func (history *History) Record(relativeNS int64, group int, gas uint64) error {
	if relativeNS < 0 {
		return fmt.Errorf("history timestamp must be nonnegative")
	}
	if group < 0 || group >= history.groups {
		return fmt.Errorf("group %d outside [0,%d)", group, history.groups)
	}
	cycle := relativeNS / history.cycleNS
	if cycle < history.watermarkNS/history.cycleNS-2 || (history.cached != nil && cycle <= history.cached.Cycle) {
		return fmt.Errorf("%w: receipt cycle %d, clock cycle %d", ErrLateSample, cycle, history.watermarkNS/history.cycleNS)
	}
	slot := int((relativeNS % history.cycleNS) / history.slotNS)
	values := history.cycles[cycle]
	if values != nil && values[slot] != nil && math.MaxUint64-values[slot][group] < gas {
		return fmt.Errorf("Gas accumulation overflows uint64 for cycle %d slot %d group %d", cycle, slot, group)
	}
	history.advance(relativeNS)
	if gas == 0 {
		return nil
	}
	if values == nil {
		values = make([][]uint64, history.cycleSlots)
		history.cycles[cycle] = values
	}
	if values[slot] == nil {
		values[slot] = make([]uint64, history.groups)
	}
	values[slot][group] += gas
	return nil
}

// Predict skips the most recently closed cycle. At time t in cycle k it uses
// cycle k-2; at least two complete cycles are required. Its first successful
// prediction freezes that source cycle, and repeats return independent copies
// of the cached result. Prediction times must not precede prior predictions;
// a newer Record timestamp does not by itself invalidate a pending Predict.
func (history *History) Predict(nowNS int64) (Prediction, bool, error) {
	if nowNS < 0 || nowNS < history.lastPredictNS {
		return Prediction{}, false, fmt.Errorf("prediction timestamp %d is negative or precedes prior prediction %d", nowNS, history.lastPredictNS)
	}
	currentCycle := nowNS / history.cycleNS
	if currentCycle < 2 {
		history.advance(nowNS)
		history.lastPredictNS = nowNS
		return Prediction{}, false, nil
	}
	sourceCycle := currentCycle - 2
	if history.cached != nil && sourceCycle == history.cached.Cycle {
		history.advance(nowNS)
		history.lastPredictNS = nowNS
		return copyPrediction(*history.cached), true, nil
	}
	if sourceCycle < history.watermarkNS/history.cycleNS-2 {
		return Prediction{}, false, fmt.Errorf("prediction source cycle %d expired before clock cycle %d", sourceCycle, history.watermarkNS/history.cycleNS)
	}
	prediction := Prediction{WeightedGas: make([]uint64, history.groups), WeightDenominator: history.denominator, Cycle: sourceCycle, SourceStartNS: sourceCycle * history.cycleNS, SourceEndNS: (sourceCycle + 1) * history.cycleNS}
	for slot, values := range history.cycles[sourceCycle] {
		weight := uint64(slot + 1)
		for group, gas := range values {
			if gas > math.MaxUint64/weight {
				return Prediction{}, false, fmt.Errorf("weighted Gas overflows uint64 for slot %d group %d", slot, group)
			}
			weighted := gas * weight
			if math.MaxUint64-prediction.WeightedGas[group] < weighted {
				return Prediction{}, false, fmt.Errorf("predicted Gas overflows uint64 for group %d", group)
			}
			prediction.WeightedGas[group] += weighted
		}
	}
	history.advance(nowNS)
	history.lastPredictNS = nowNS
	frozen := copyPrediction(prediction)
	history.cached = &frozen
	return prediction, true, nil
}

func copyPrediction(prediction Prediction) Prediction {
	prediction.WeightedGas = append([]uint64(nil), prediction.WeightedGas...)
	return prediction
}
