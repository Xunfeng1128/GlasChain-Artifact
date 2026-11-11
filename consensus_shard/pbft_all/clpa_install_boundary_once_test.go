package pbft_all

import (
	"blockEmulator/message"
	"blockEmulator/params"
	"bytes"
	"log"
	"runtime"
	"sync"
	"testing"
	"time"
)

// Delay only the test logger after recordCLPAPartitionInstall has stored its
// boundary. This separates the two formerly adjacent stores without adding a
// production clock hook or depending on both calls happening in one millisecond.
type clpaInstallBoundaryGateWriter struct {
	node     *PbftConsensusNode
	observed chan int64
	release  <-chan struct{}
	once     sync.Once
}

func (w *clpaInstallBoundaryGateWriter) Write(data []byte) (int, error) {
	if bytes.Contains(data, []byte(" installed: epoch=")) {
		w.once.Do(func() {
			w.observed <- w.node.lastPartitionCommitTime.Load()
			<-w.release
		})
	}
	return len(data), nil
}

// Observe the clock crossing the recorded millisecond before releasing the
// commit. Merely sleeping for a small duration could let a duplicate store
// escape detection when both stores happened to have the same timestamp.
func clpaInstallWaitPastBoundary(t *testing.T, boundary int64) {
	t.Helper()
	if boundary <= 0 {
		t.Fatalf("installation did not set a boundary: %d", boundary)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().UnixMilli() <= boundary {
		if time.Now().After(deadline) {
			t.Fatalf("wall clock did not pass installation boundary %d", boundary)
		}
		runtime.Gosched()
	}
}

func TestCLPASharedCommitRecordsInstallationBoundaryOnce(t *testing.T) {
	before := params.CLPAReconfigurationProfile
	t.Cleanup(func() { params.CLPAReconfigurationProfile = before })
	for _, profile := range []string{params.CLPAReconfigurationProfileFixed, params.CLPAReconfigurationProfileBlockResume, params.CLPAReconfigurationProfileInstallClock} {
		t.Run(profile, func(t *testing.T) {
			params.CLPAReconfigurationProfile = profile
			checkCLPASharedCommitRecordsInstallationBoundaryOnce(t)
		})
	}
}

func checkCLPASharedCommitRecordsInstallationBoundaryOnce(t *testing.T) {
	for _, mode := range []struct {
		name     string
		snapshot bool
	}{{"CLPA", false}, {"GLAS_Pending", true}} {
		for _, scheduler := range []struct {
			name string
			poll bool
		}{{"event", false}, {"poll", true}} {
			t.Run(mode.name+"/"+scheduler.name, func(t *testing.T) {
				node, outside, _ := clpaInstallFixture(t, 0)
				outside.cdm.PendingSnapshotMode = mode.snapshot
				outside.cdm.ControlPolling = scheduler.poll
				gate := make(chan struct{})
				writer := &clpaInstallBoundaryGateWriter{
					node: node, observed: make(chan int64, 1), release: gate,
				}
				node.pl.Plog = log.New(writer, "", 0)
				var release sync.Once
				var done <-chan struct{}
				t.Cleanup(func() {
					release.Do(func() { close(gate) })
					if done != nil {
						select {
						case <-done:
						case <-time.After(3 * time.Second):
							t.Error("commit did not finish after releasing its installation log")
						}
					}
				})
				request := clpaInstallRequest(1, 1, nil)
				done = clpaInstallCommit(t, node, request)
				var boundary int64
				select {
				case boundary = <-writer.observed:
				case <-done:
					t.Fatal("commit completed without recording its installation boundary")
				case <-time.After(3 * time.Second):
					t.Fatal("commit did not reach its installation log")
				}
				clpaInstallWaitPastBoundary(t, boundary)
				release.Do(func() { close(gate) })
				awaitPBFTOrderingCall(t, done)
				if got := node.lastPartitionCommitTime.Load(); got != boundary {
					t.Fatalf("normal commit rewrote its recorded installation boundary: first=%d final=%d", boundary, got)
				}
				if node.sequenceID != 2 || outside.cdm.AccountTransferRound != 1 {
					t.Fatal("installation did not finish its consensus/partition transition")
				}

				// A duplicate quorum and repeated installation observation must not
				// restart the ordinary block deadline either.
				node.handleCommit(pbftOrderingJSON(t, message.Commit{
					SeqID: 1, Digest: getDigest(request), SenderNode: node.RunningNode,
				}))
				node.recordCLPAPartitionInstall(request, 1)
				if got := node.lastPartitionCommitTime.Load(); got != boundary {
					t.Fatalf("duplicate installation reset the boundary: first=%d replay=%d", boundary, got)
				}
			})
		}
	}
}

func TestCLPASharedCatchUpStillRecordsInstallationBoundary(t *testing.T) {
	before := params.CLPAReconfigurationProfile
	t.Cleanup(func() { params.CLPAReconfigurationProfile = before })
	for _, profile := range []string{params.CLPAReconfigurationProfileFixed, params.CLPAReconfigurationProfileBlockResume, params.CLPAReconfigurationProfileInstallClock} {
		t.Run(profile, func(t *testing.T) {
			params.CLPAReconfigurationProfile = profile
			checkCLPASharedCatchUpStillRecordsInstallationBoundary(t)
		})
	}
}

func checkCLPASharedCatchUpStillRecordsInstallationBoundary(t *testing.T) {
	for _, mode := range []struct {
		name     string
		snapshot bool
	}{{"CLPA", false}, {"GLAS_Pending", true}} {
		for _, scheduler := range []struct {
			name string
			poll bool
		}{{"event", false}, {"poll", true}} {
			t.Run(mode.name+"/"+scheduler.name, func(t *testing.T) {
				node, outside, _ := clpaInstallFixture(t, 1)
				outside.cdm.PendingSnapshotMode = mode.snapshot
				outside.cdm.ControlPolling = scheduler.poll
				request := clpaInstallRequest(1, 1, nil)
				response := pbftOrderingJSON(t, message.SendOldMessage{
					SeqStartHeight: 1, SeqEndHeight: 1, OldRequest: []*message.Request{request},
				})
				node.catchUpPending, node.catchUpStart, node.catchUpEnd = true, 1, 1
				node.handleSendOldSeq(response)
				boundary := node.lastPartitionCommitTime.Load()
				if boundary <= 0 || node.sequenceID != 2 || outside.cdm.AccountTransferRound != 1 ||
					outside.cdm.LatestPartitionInstall == nil || outside.cdm.LatestPartitionInstall.Epoch != 1 {
					t.Fatalf("catch-up lost its installation boundary or receipt: boundary=%d seq=%d round=%d receipt=%+v",
						boundary, node.sequenceID, outside.cdm.AccountTransferRound, outside.cdm.LatestPartitionInstall)
				}
				clpaInstallWaitPastBoundary(t, boundary)
				node.handleSendOldSeq(response) // No outstanding recovery request.
				node.catchUpPending, node.catchUpStart, node.catchUpEnd = true, 1, 1
				node.handleSendOldSeq(response) // Fully overlapping recovery response.
				node.recordCLPAPartitionInstall(request, 1)
				if got := node.lastPartitionCommitTime.Load(); got != boundary {
					t.Fatalf("duplicate catch-up reset the installation boundary: first=%d replay=%d", boundary, got)
				}
				if node.sequenceID != 2 || outside.cdm.AccountTransferRound != 1 {
					t.Fatal("duplicate catch-up advanced the already installed partition")
				}
			})
		}
	}
}
