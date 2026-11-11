package pbft_all

import (
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/consensus_shard/pbft_all/pbft_log"
	"blockEmulator/params"
	"context"
	"io"
	"log"
	"sync"
	"testing"
	"time"
)

// Observe the production scheduler entering its cancellation select without
// adding timing hooks to production code or starting a ledger/network node.
type clpaLifecycleContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (ctx *clpaLifecycleContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.entered) })
	return ctx.Context.Done()
}

func clpaLifecycleFixture(t *testing.T) (*PbftConsensusNode, <-chan struct{}, <-chan struct{}, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	observed := &clpaLifecycleContext{Context: ctx, entered: make(chan struct{})}
	p := &PbftConsensusNode{
		NodeID:          1, // Follower: exercise the real scheduler without proposals.
		pbftChainConfig: &params.ChainConfig{BlockInterval: 8000, Nodes_perShard: 4},
		pStop:           make(chan uint64),
		pl:              &pbft_log.PbftLog{Plog: log.New(io.Discard, "", 0)},
	}
	cdm := dataSupport.NewCLPADataSupport()
	cdm.ControlEventsEnabled = true
	p.ihm = &CLPAPbftInsideExtraHandleMod{pbftNode: p, cdm: cdm}
	p.clpaContextOnce.Do(func() { p.clpaContext, p.clpaCancel = observed, cancel })
	p.lastCommitTime.Store(time.Now().Add(time.Hour).UnixMilli())
	p.pbftStage.Store(1)
	var finishOnce sync.Once
	finishShutdown := func() { finishOnce.Do(func() { close(p.pStop) }) }
	returned := make(chan struct{})
	started := make(chan struct{})
	go func() {
		close(started)
		p.Propose()
		close(returned)
	}()
	<-started
	t.Cleanup(func() {
		p.stopSignal.Store(true)
		cancel()
		finishShutdown()
		select {
		case <-returned:
		case <-time.After(time.Second):
			t.Error("Propose did not return after shutdown completed")
		}
	})
	return p, observed.entered, returned, finishShutdown
}

func requireCLPAProposeWaitingForShutdown(t *testing.T, returned <-chan struct{}, wait time.Duration) {
	t.Helper()
	select {
	case <-returned:
		t.Fatal("Propose returned before pStop closed: main could exit before StopAck and cleanup finished")
	case <-time.After(wait):
	}
}

func requireCLPAProposeReturnsAfterShutdown(t *testing.T, returned <-chan struct{}, finish func()) {
	t.Helper()
	finish()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("Propose remained blocked after shutdown completed")
	}
}

func TestCLPAProposeWaitsForShutdownAfterSchedulerCancellation(t *testing.T) {
	t.Parallel()
	p, entered, returned, finish := clpaLifecycleFixture(t)
	select {
	case <-entered: // The real five-second startup delay has finished.
	case <-time.After(7 * time.Second):
		t.Fatal("Propose did not enter the CLPA scheduler")
	}
	// waitToStop does these before sending StopAck and closing the database.
	// Cancelling control I/O must remain prompt without ending process lifetime.
	p.stopSignal.Store(true)
	p.cancelCLPAControl()
	requireCLPAProposeWaitingForShutdown(t, returned, 150*time.Millisecond)
	requireCLPAProposeReturnsAfterShutdown(t, returned, finish)
}

func TestCLPAProposeWaitsForShutdownWhenStoppedDuringStartup(t *testing.T) {
	t.Parallel()
	p, entered, returned, finish := clpaLifecycleFixture(t)
	p.stopSignal.Store(true)
	p.cancelCLPAControl()
	// Exercise the early return immediately after the production startup wait;
	// keeping shutdown incomplete past that wait distinguishes the regression.
	requireCLPAProposeWaitingForShutdown(t, returned, 5500*time.Millisecond)
	select {
	case <-entered:
		t.Fatal("a node stopped during startup entered the proposal scheduler")
	default:
	}
	requireCLPAProposeReturnsAfterShutdown(t, returned, finish)
}
