package committee

import (
	"blockEmulator/core"
	"blockEmulator/supervisor/unifiedmetrics"
	"fmt"
	"time"
)

// injectionObservation adds the same dispatch observation point to each
// comparison method without changing its input batching or planner.
type injectionObservation struct {
	recorder          *unifiedmetrics.Recorder
	reconfigurationID uint64 // MsgSendingControl is the sole writer.
}

// reconfigurationObservation describes the synchronous supervisor control
// section. It is not a node freeze interval; already sent batches can still be
// executing or in transit. Draining observations have no injection pause rows.
type reconfigurationObservation struct {
	recorder *unifiedmetrics.Recorder
	event    unifiedmetrics.ReconfigurationEvent
}

func (o *injectionObservation) beginReconfiguration(targetEpoch uint64, injecting bool, basis string) reconfigurationObservation {
	return o.beginReconfigurationAt(targetEpoch, injecting, basis, time.Now())
}

func (o *injectionObservation) beginReconfigurationAt(targetEpoch uint64, injecting bool, basis string, at time.Time) reconfigurationObservation {
	if o.recorder == nil {
		return reconfigurationObservation{}
	}
	o.reconfigurationID++
	phase := "draining"
	if injecting {
		phase = "injecting"
	}
	span := reconfigurationObservation{recorder: o.recorder, event: unifiedmetrics.ReconfigurationEvent{
		ID: o.reconfigurationID, TargetEpoch: targetEpoch, Phase: phase, CompletionBasis: basis,
	}}
	span.record("trigger", "", at)
	if injecting {
		span.record("injection_pause_begin", "", at)
	}
	return span
}

func (span reconfigurationObservation) finish(status string) {
	span.finishAt(status, time.Now())
}

func (span reconfigurationObservation) finishAt(status string, at time.Time) {
	if span.recorder == nil {
		return
	}
	span.record("finish", status, at)
	if span.event.Phase == "injecting" && status != "failed" {
		span.record("injection_pause_end", status, at)
	}
}

func (span reconfigurationObservation) record(event, status string, at time.Time) {
	observation := span.event
	observation.Event, observation.Status = event, status
	if err := span.recorder.RecordReconfiguration(observation, at); err != nil {
		panic(fmt.Errorf("record unified reconfiguration: %w", err))
	}
}

// SetUnifiedRecorder is called once during supervisor initialization, before
// dispatch or receipt goroutines start. A nil recorder preserves legacy runs.
func (o *injectionObservation) SetUnifiedRecorder(recorder *unifiedmetrics.Recorder) {
	o.recorder = recorder
}

func (o *injectionObservation) observeInjection(txs []*core.Transaction, shard uint64) {
	o.observeInjectionAt(txs, shard, time.Now())
}

func (o *injectionObservation) observeInjectionAt(txs []*core.Transaction, shard uint64, at time.Time) {
	if o.recorder == nil || len(txs) == 0 {
		return
	}
	if err := o.recorder.RecordInjection(txs, shard, at); err != nil {
		// Continuing after a failed observation would create performance
		// results whose denominator does not describe the dispatched workload.
		panic(fmt.Errorf("record unified transaction injection: %w", err))
	}
}
