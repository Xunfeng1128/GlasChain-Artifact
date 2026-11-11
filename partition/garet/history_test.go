package garet

import (
	"errors"
	"math"
	"reflect"
	"testing"
)

func TestHistoryWeightedLagAndBoundaries(t *testing.T) {
	history, err := NewHistory(2, 10, 3)
	if err != nil {
		t.Fatal(err)
	}
	for _, record := range []struct {
		at    int64
		group int
		gas   uint64
	}{{0, 0, 10}, {10, 0, 20}, {29, 0, 30}, {29, 1, 7}, {30, 0, 1000}, {59, 0, 1000}} {
		if err := history.Record(record.at, record.group, record.gas); err != nil {
			t.Fatal(err)
		}
	}
	if _, ready, err := history.Predict(59); err != nil || ready {
		t.Fatalf("premature prediction %v %v", ready, err)
	}
	prediction, ready, err := history.Predict(60)
	if err != nil || !ready {
		t.Fatalf("prediction %v %v", ready, err)
	}
	// (10*1 + 20*2 + 30*3)/6 = 140/6; slot 2's 7 Gas is 21/6.
	if !reflect.DeepEqual(prediction.WeightedGas, []uint64{140, 21}) || prediction.WeightDenominator != 6 || prediction.Cycle != 0 || prediction.SourceStartNS != 0 || prediction.SourceEndNS != 30 {
		t.Fatalf("wrong prediction %+v", prediction)
	}
	repeat, ready, err := history.Predict(60)
	if err != nil || !ready || !reflect.DeepEqual(repeat, prediction) {
		t.Fatalf("repeat changed prediction %+v %v", repeat, err)
	}
	if err := history.Record(60, 0, 9000); err != nil {
		t.Fatal(err)
	}
	if _, _, err := history.Predict(59); err == nil {
		t.Fatal("backward prediction accepted")
	}
	if err := history.Record(59, 0, 1); err != nil {
		t.Fatalf("retained out-of-order receipt rejected: %v", err)
	}
	next, ready, err := history.Predict(90)
	if err != nil || !ready || next.WeightedGas[0] != 4003 || next.Cycle != 1 {
		t.Fatalf("cycle boundary: %+v %v", next, err)
	}
}

func TestHistoryPredictWinsLockBeforeRecentReceipt(t *testing.T) {
	history, _ := NewHistory(1, 10, 3)
	if _, ready, err := history.Predict(60); err != nil || !ready {
		t.Fatalf("initial prediction: %v", err)
	}
	// Both receipts were timestamped before the prediction callback won the
	// caller's lock. They belong to cycle 1, which has not yet been predicted.
	if err := history.Record(59, 0, 7); err != nil {
		t.Fatal(err)
	}
	if err := history.Record(30, 0, 5); err != nil {
		t.Fatal(err)
	}
	if history.watermarkNS != 60 {
		t.Fatal("late receipt moved retention boundary backward")
	}
	// A newer receipt can also win the lock before a pending prediction in
	// the same clock cycle; it does not make the pending timestamp invalid.
	if err := history.Record(70, 0, 100); err != nil {
		t.Fatal(err)
	}
	if _, ready, err := history.Predict(61); err != nil || !ready {
		t.Fatalf("pending prediction: %v", err)
	}
	prediction, ready, err := history.Predict(90)
	if err != nil || !ready || prediction.WeightedGas[0] != 26 {
		t.Fatalf("normal out-of-order receipts were lost: %+v %v", prediction, err)
	}
}

func TestHistoryFrozenPredictionRejectsLateSamplesAndCannotBeMutated(t *testing.T) {
	history, _ := NewHistory(1, 10, 3)
	if err := history.Record(0, 0, 4); err != nil {
		t.Fatal(err)
	}
	first, _, err := history.Predict(60)
	if err != nil {
		t.Fatal(err)
	}
	if err := history.Record(29, 0, 99); !errors.Is(err, ErrLateSample) {
		t.Fatalf("frozen sample did not return sentinel: %v", err)
	}
	first.WeightedGas[0] = 999
	second, _, err := history.Predict(61)
	if err != nil || second.WeightedGas[0] != 4 {
		t.Fatalf("cached prediction mutated: %+v %v", second, err)
	}
	second.WeightedGas[0] = 888
	third, _, err := history.Predict(61)
	if err != nil || third.WeightedGas[0] != 4 {
		t.Fatalf("repeat prediction mutated cache: %+v %v", third, err)
	}
	if err := history.Record(30, 0, 3); err != nil {
		t.Fatal(err)
	}
	next, _, err := history.Predict(90)
	if err != nil || next.WeightedGas[0] != 3 {
		t.Fatalf("late frozen Gas leaked into later prediction: %+v %v", next, err)
	}
}

func TestHistoryFastForwardAndExpiredReceipts(t *testing.T) {
	history, _ := NewHistory(1, 10, 3)
	if err := history.Record(0, 0, 4); err != nil {
		t.Fatal(err)
	}
	if err := history.Record(180, 0, 9); err != nil {
		t.Fatal(err)
	}
	// Clock cycle 6 retains cycles 4..6, regardless of callback arrival order.
	if err := history.Record(119, 0, 7); !errors.Is(err, ErrLateSample) {
		t.Fatalf("expired sample did not return sentinel: %v", err)
	}
	if err := history.Record(120, 0, 5); err != nil {
		t.Fatal(err)
	}
	if len(history.cycles) > 3 || history.watermarkNS != 180 {
		t.Fatal("fast-forward retention changed")
	}
	if _, _, err := history.Predict(90); err == nil {
		t.Fatal("expired prediction window silently treated as zero")
	}
	prediction, ready, err := history.Predict(180)
	if err != nil || !ready || prediction.Cycle != 4 || prediction.WeightedGas[0] != 5 {
		t.Fatalf("retained source after fast-forward: %+v %v", prediction, err)
	}
	if err := history.Record(149, 0, 1); !errors.Is(err, ErrLateSample) {
		t.Fatalf("just-frozen sample: %v", err)
	}
	if _, _, err := history.Predict(900); err != nil {
		t.Fatal(err)
	}
	if len(history.cycles) != 0 {
		t.Fatal("fast-forward retained obsolete observations")
	}
}

func TestHistoryZerosAndBoundedRetention(t *testing.T) {
	history, err := NewHistory(3, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	prediction, ready, err := history.Predict(4)
	if err != nil || !ready || !reflect.DeepEqual(prediction.WeightedGas, []uint64{0, 0, 0}) {
		t.Fatalf("empty history %+v %v", prediction, err)
	}
	for cycle := int64(2); cycle < 100; cycle++ {
		if err := history.Record(cycle*2, 0, 1); err != nil {
			t.Fatal(err)
		}
		if len(history.cycles) > 3 {
			t.Fatalf("retained %d cycles", len(history.cycles))
		}
	}
	if _, _, err := history.Predict(300); err != nil {
		t.Fatal(err)
	}
	if len(history.cycles) != 0 {
		t.Fatal("obsolete observations retained after empty cycles")
	}
}

func TestHistoryInvalidAndOverflow(t *testing.T) {
	for _, options := range []struct {
		groups int
		slot   int64
		cycle  int
	}{{0, 1, 1}, {1, 0, 1}, {1, 1, 0}, {1, math.MaxInt64, 2}} {
		if _, err := NewHistory(options.groups, options.slot, options.cycle); err == nil {
			t.Fatalf("invalid history accepted %+v", options)
		}
	}
	history, _ := NewHistory(1, 1, 2)
	if err := history.Record(-1, 0, 1); err == nil {
		t.Fatal("negative time accepted")
	}
	if err := history.Record(0, 1, 1); err == nil {
		t.Fatal("invalid group accepted")
	}
	if err := history.Record(0, 0, math.MaxUint64); err != nil {
		t.Fatal(err)
	}
	if err := history.Record(0, 0, 1); err == nil {
		t.Fatal("record addition overflow accepted")
	}
	if err := history.Record(1, 0, 1); err != nil {
		t.Fatal(err)
	}
	if _, _, err := history.Predict(4); err == nil {
		t.Fatal("prediction addition overflow accepted")
	}
	history, _ = NewHistory(1, 1, 2)
	if err := history.Record(1, 0, math.MaxUint64); err != nil {
		t.Fatal(err)
	}
	if _, _, err := history.Predict(4); err == nil {
		t.Fatal("prediction multiply overflow accepted")
	}
}
