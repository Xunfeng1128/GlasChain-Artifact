package pbft_all

import (
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/shard"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

// this func is only invoked by main node
func (p *PbftConsensusNode) Propose() {
	// wait other nodes to start TCPlistening, sleep 5 sec.
	time.Sleep(5 * time.Second)
	if p.stopSignal.Load() {
		if p.clpaControlEnabled() {
			<-p.pStop // Keep main alive until StopAck and shutdown finish.
		}
		return
	}
	go p.monitorViewChanges()
	if p.clpaControlEnabled() {
		p.runCLPAProposalScheduler(p.broadcastGLASProposal)
		// Control cancellation stops proposals before the TCP shutdown goroutine
		// finishes. Propose owns process lifetime, so join completed shutdown.
		<-p.pStop
		return
	}

	nextRoundBeginSignal := make(chan bool)

	go func() {
		// go into the next round
		for !p.stopSignal.Load() {
			blockInterval := time.Duration(int64(p.pbftChainConfig.BlockInterval)) * time.Millisecond
			cycleStart := time.Now()
			time.Sleep(blockInterval)

			for {
				for p.pbftStage.Load() != 1 && !p.stopSignal.Load() {
					time.Sleep(time.Millisecond * 100)
				}
				if p.stopSignal.Load() {
					return
				}
				for !p.sequenceLock.TryLock() {
					if p.stopSignal.Load() {
						return
					}
					time.Sleep(time.Millisecond * 100)
				}
				p.sequenceLock.Unlock()

				partitionCommitTime := p.lastPartitionCommitTime.Load()
				if partitionCommitTime > cycleStart.UnixMilli() {
					nextTickTime := time.UnixMilli(partitionCommitTime).Add(blockInterval)
					if wait := time.Until(nextTickTime); wait > 0 {
						p.pl.Plog.Printf("DIAG tick reset after partition commit: seq=%d, partitionCommitTime=%d, wait=%s\n", p.sequenceID, partitionCommitTime, wait)
						time.Sleep(wait)
						continue
					}
				}
				break
			}

			p.pl.Plog.Printf("DIAG tick fired: seq=%d, pbftStage=%d, view=%d\n", p.sequenceID, p.pbftStage.Load(), p.view.Load())
			p.pl.Plog.Printf("DIAG tick sending nextRoundBeginSignal: seq=%d, pbftStage=%d, view=%d\n", p.sequenceID, p.pbftStage.Load(), p.view.Load())
			select {
			case nextRoundBeginSignal <- true:
			case <-p.pStop:
				return
			}
			p.pl.Plog.Printf("DIAG tick sent nextRoundBeginSignal: seq=%d, pbftStage=%d, view=%d\n", p.sequenceID, p.pbftStage.Load(), p.view.Load())
		}
	}()

	for {
		select {
		case <-nextRoundBeginSignal:
			p.pl.Plog.Printf("DIAG propose signal received: seq=%d, pbftStage=%d, view=%d\n", p.sequenceID, p.pbftStage.Load(), p.view.Load())
			go func() {
				// if this node is not leader, do not propose.
				if p.stopSignal.Load() || uint64(p.view.Load()) != p.NodeID {
					p.pl.Plog.Printf("DIAG propose ignored: not leader, seq=%d, pbftStage=%d, view=%d, node=%d\n", p.sequenceID, p.pbftStage.Load(), p.view.Load(), p.NodeID)
					return
				}

				p.pl.Plog.Printf("DIAG propose before sequenceLock: seq=%d, pbftStage=%d, view=%d\n", p.sequenceID, p.pbftStage.Load(), p.view.Load())
				p.sequenceLock.Lock()
				if p.stopSignal.Load() {
					p.sequenceLock.Unlock()
					return
				}
				p.pl.Plog.Printf("DIAG propose acquired sequenceLock: seq=%d, pbftStage=%d, view=%d\n", p.sequenceID, p.pbftStage.Load(), p.view.Load())
				p.pl.Plog.Printf("S%dN%d get sequenceLock locked, now trying to propose...\n", p.ShardID, p.NodeID)
				// propose
				// implement interface to generate propose
				shouldPropose, r := p.ihm.HandleinPropose()
				if !shouldPropose || r == nil {
					p.sequenceLock.Unlock()
					p.pl.Plog.Printf("S%dN%d skipped proposal at seq=%d (intentional pause or control wait)\n", p.ShardID, p.NodeID, p.sequenceID)
					return
				}

				digest := getDigest(r)
				p.cacheRequest(p.sequenceID, string(digest), r)
				p.pl.Plog.Printf("S%dN%d put the request into the pool ...\n", p.ShardID, p.NodeID)

				ppmsg := message.PrePrepare{
					RequestMsg: r,
					Digest:     digest,
					SeqID:      p.sequenceID,
				}
				// marshal and broadcast
				ppbyte, err := json.Marshal(ppmsg)
				if err != nil {
					log.Panic()
				}
				msg_send := message.MergeMessage(message.CPrePrepare, ppbyte)
				networks.Broadcast(p.RunningNode.IPaddr, p.getNeighborNodes(), msg_send)
				networks.TcpDial(msg_send, p.RunningNode.IPaddr)
				p.pbftStage.Store(2)
			}()

		case <-p.pStop:
			p.pl.Plog.Printf("S%dN%d get stopSignal in Propose Routine, now stop...\n", p.ShardID, p.NodeID)
			return
		}
	}
}

func (p *PbftConsensusNode) monitorViewChanges() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-p.pStop:
			return
		case <-ticker.C:
		}
		if p.stopSignal.Load() {
			return
		}
		if pause, ok := p.ihm.(IntentionalConsensusPause); ok && pause.SuppressViewChange() {
			p.lastCommitTime.Store(time.Now().UnixMilli())
			continue
		}
		if time.Now().UnixMilli()-p.lastCommitTime.Load() > int64(params.PbftViewChangeTimeOut) {
			p.lastCommitTime.Store(time.Now().UnixMilli())
			go p.viewChangePropose()
		}
	}
}

func (p *PbftConsensusNode) glasProposalInterval() time.Duration {
	interval := time.Duration(p.pbftChainConfig.BlockInterval) * time.Millisecond
	if interval <= 0 {
		return time.Millisecond
	}
	return interval
}

func (p *PbftConsensusNode) broadcastGLASProposal(proposal *message.PrePrepare) {
	if p.stopSignal.Load() {
		return
	}
	payload, err := json.Marshal(proposal)
	if err != nil {
		log.Panic(err)
	}
	wire := message.MergeMessage(message.CPrePrepare, payload)
	networks.Broadcast(p.RunningNode.IPaddr, p.getNeighborNodes(), wire)
	networks.TcpDial(wire, p.RunningNode.IPaddr)
}

func resetGLASProposalTimer(timer *time.Timer, delay time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	if delay < 0 {
		delay = 0
	}
	timer.Reset(delay)
}

func (p *PbftConsensusNode) handlePrePrepare(content []byte) {
	p.RunningNode.PrintNode()
	fmt.Println("received the PrePrepare ...")
	// decode the message
	ppmsg := new(message.PrePrepare)
	err := json.Unmarshal(content, ppmsg)
	if err != nil {
		log.Panic(err)
	}

	curView := p.view.Load()
	p.pbftLock.Lock()
	defer p.pbftLock.Unlock()
	for p.pbftStage.Load() < 1 && ppmsg.SeqID >= p.sequenceID && p.view.Load() == curView && !p.stopSignal.Load() {
		p.conditionalVarpbftLock.Wait()
	}
	defer p.conditionalVarpbftLock.Broadcast()

	// if this message is out of date, return.
	if p.stopSignal.Load() || ppmsg.SeqID < p.sequenceID || p.view.Load() != curView {
		return
	}

	if ppmsg.RequestMsg == nil || string(getDigest(ppmsg.RequestMsg)) != string(ppmsg.Digest) {
		p.pl.Plog.Printf("S%dN%d : the digest is not consistent, so refuse to prepare. \n", p.ShardID, p.NodeID)
		return
	}
	// A duplicate must neither replace an accepted proposal nor repeat its
	// validation/Prepare broadcast. Future proposals remain in the existing cache.
	if ppmsg.SeqID == p.sequenceID && p.prePrepareAccepted {
		return
	}
	p.cacheRequest(ppmsg.SeqID, string(ppmsg.Digest), ppmsg.RequestMsg)
	if ppmsg.SeqID > p.sequenceID {
		p.pl.Plog.Printf("S%dN%d : cached early PrePrepare local_seq=%d received_seq=%d\n", p.ShardID, p.NodeID, p.sequenceID, ppmsg.SeqID)
		return
	}
	p.activateCachedRequestLocked()
}

// Called with pbftLock held, from receipt, normal commit and catch-up alike.
// Only one current proposal can be accepted. Reuse its cached payload rather
// than adding a replay queue or recursively calling a handler that takes the lock.
func (p *PbftConsensusNode) activateCachedRequestLocked() {
	if p.stopSignal.Load() || p.catchUpPending || p.prePrepareAccepted || p.pbftStage.Load() == 5 {
		return
	}
	p.requestMu.RLock()
	digest, indexed := p.height2Digest[p.sequenceID]
	request := p.requestPool[digest]
	p.requestMu.RUnlock()
	if !indexed || request == nil {
		return
	}
	proposal := &message.PrePrepare{RequestMsg: request, Digest: []byte(digest), SeqID: p.sequenceID}
	if !p.ihm.HandleinPrePrepare(proposal) {
		return
	}
	p.prePrepareAccepted = true
	// The proposer may already have reserved stage 2. Do not increment it to
	// stage 3 before Prepare quorum, or increment again on a duplicate proposal.
	p.pbftStage.Store(2)
	pre := message.Prepare{Digest: proposal.Digest, SeqID: proposal.SeqID, SenderNode: p.RunningNode}
	payload, err := json.Marshal(pre)
	if err != nil {
		log.Panic(err)
	}
	wire := message.MergeMessage(message.CPrepare, payload)
	networks.Broadcast(p.RunningNode.IPaddr, p.getNeighborNodes(), wire)
	networks.TcpDial(wire, p.RunningNode.IPaddr)
	p.pl.Plog.Printf("S%dN%d : has broadcast the prepare message seq=%d\n", p.ShardID, p.NodeID, p.sequenceID)
	p.conditionalVarpbftLock.Broadcast()
}

// Handle prepare messages here.
// If you want to do more operations in the prepare stage, you can implement the interface "ExtraOpInConsensus",
// and call the function: **ExtraOpInConsensus.HandleinPrepare**
func (p *PbftConsensusNode) handlePrepare(content []byte) {
	p.pl.Plog.Printf("S%dN%d : received the Prepare ...\n", p.ShardID, p.NodeID)
	// decode the message
	pmsg := new(message.Prepare)
	err := json.Unmarshal(content, pmsg)
	if err != nil {
		log.Panic(err)
	}
	if pmsg.SenderNode == nil || pmsg.SenderNode.ShardID != p.ShardID || (p.node_nums != 0 && pmsg.SenderNode.NodeID >= p.node_nums) {
		return
	}

	curView := p.view.Load()
	p.pbftLock.Lock()
	defer p.pbftLock.Unlock()
	for pmsg.SeqID >= p.sequenceID && p.view.Load() == curView && !p.stopSignal.Load() &&
		(pmsg.SeqID > p.sequenceID || p.catchUpPending || !p.prePrepareAccepted || p.pbftStage.Load() < 2) {
		p.conditionalVarpbftLock.Wait()
	}
	defer p.conditionalVarpbftLock.Broadcast()

	// if this message is out of date, return.
	if p.stopSignal.Load() || pmsg.SeqID < p.sequenceID || p.view.Load() != curView {
		return
	}

	request, ok := p.requestByDigest(string(pmsg.Digest))
	current, _ := p.requestAtSequence(p.sequenceID)
	if !ok || request != current {
		p.pl.Plog.Printf("S%dN%d : doesn't have the digest in the requst pool, refuse to commit\n", p.ShardID, p.NodeID)
	} else if p.sequenceID < pmsg.SeqID {
		p.pl.Plog.Printf("S%dN%d : inconsistent sequence ID, refuse to commit\n", p.ShardID, p.NodeID)
	} else {
		// if needed more operations, implement interfaces
		p.ihm.HandleinPrepare(pmsg)

		p.set2DMap(true, string(pmsg.Digest), pmsg.SenderNode)
		cnt := len(p.cntPrepareConfirm[string(pmsg.Digest)])

		// if the node has received 2f messages (itself included), and it haven't committed, then it commit
		p.lock.Lock()
		defer p.lock.Unlock()
		if uint64(cnt) >= 2*p.malicious_nums+1 && !p.isCommitBordcast[string(pmsg.Digest)] {
			p.pl.Plog.Printf("S%dN%d : is going to commit\n", p.ShardID, p.NodeID)
			// generate commit and broadcast
			c := message.Commit{
				Digest:     pmsg.Digest,
				SeqID:      pmsg.SeqID,
				SenderNode: p.RunningNode,
			}
			commitByte, err := json.Marshal(c)
			if err != nil {
				log.Panic()
			}
			msg_send := message.MergeMessage(message.CCommit, commitByte)
			networks.Broadcast(p.RunningNode.IPaddr, p.getNeighborNodes(), msg_send)
			networks.TcpDial(msg_send, p.RunningNode.IPaddr)
			p.isCommitBordcast[string(pmsg.Digest)] = true
			p.pl.Plog.Printf("S%dN%d : commit is broadcast\n", p.ShardID, p.NodeID)

			p.pbftStage.Store(3)
		}
	}
}

// Handle commit messages here.
// If you want to do more operations in the commit stage, you can implement the interface "ExtraOpInConsensus",
// and call the function: **ExtraOpInConsensus.HandleinCommit**
func (p *PbftConsensusNode) handleCommit(content []byte) {
	// decode the message
	cmsg := new(message.Commit)
	err := json.Unmarshal(content, cmsg)
	if err != nil {
		log.Panic(err)
	}
	if cmsg.SenderNode == nil || cmsg.SenderNode.ShardID != p.ShardID || (p.node_nums != 0 && cmsg.SenderNode.NodeID >= p.node_nums) {
		return
	}

	curView := p.view.Load()
	p.pbftLock.Lock()
	defer p.pbftLock.Unlock()
	defer p.conditionalVarpbftLock.Broadcast()

	if p.stopSignal.Load() || cmsg.SeqID < p.sequenceID || p.view.Load() != curView {
		return
	}

	p.pl.Plog.Printf("S%dN%d received the Commit from ...%d\n", p.ShardID, p.NodeID, cmsg.SenderNode.NodeID)
	p.set2DMap(false, string(cmsg.Digest), cmsg.SenderNode)
	for {
		if p.stopSignal.Load() || cmsg.SeqID < p.sequenceID || p.view.Load() != curView || p.isReply[string(cmsg.Digest)] {
			return
		}
		if uint64(len(p.cntCommitConfirm[string(cmsg.Digest)])) < 2*p.malicious_nums+1 {
			return
		}
		if p.catchUpPending {
			p.conditionalVarpbftLock.Wait()
			continue
		}
		request, ok := p.requestByDigest(string(cmsg.Digest))
		// A later committed quorum proves lag even when our local Prepare
		// phase never started. Use the existing recovery path, never skip
		// directly to the future sequence or execute its payload out of order.
		if cmsg.SeqID > p.sequenceID || !ok {
			p.requestCatchUp(p.sequenceID, cmsg.SeqID)
			return
		}
		current, _ := p.requestAtSequence(p.sequenceID)
		if request != current {
			return
		}
		// Current-sequence commits still require local proposal validation
		// and Prepare quorum. Early votes wake here when that phase advances.
		if !p.prePrepareAccepted || p.pbftStage.Load() < 3 {
			p.conditionalVarpbftLock.Wait()
			continue
		}
		p.lock.Lock()
		isPartitionReq := request.RequestType == message.PartitionReq
		if isPartitionReq {
			p.markCLPAStep4Quorum(cmsg)
		}
		p.pl.Plog.Printf("S%dN%d : has received 2f + 1 commits ... \n", p.ShardID, p.NodeID)
		var installed *message.AccountTransferMsg
		if inside, ok := p.ihm.(*CLPAPbftInsideExtraHandleMod); isPartitionReq && ok && inside.cdm.ControlEventsEnabled {
			installed = inside.applyPartitionRequest(request)
		} else {
			p.ihm.HandleinCommit(cmsg)
		}
		p.markRequestCommitted(cmsg.SeqID, string(cmsg.Digest))
		if installed != nil {
			p.recordDecodedCLPAPartitionInstall(installed, cmsg.SeqID)
		}
		if isPartitionReq {
			// Shared CLPA modes already recorded this boundary when retaining
			// the installation receipt, also used by the catch-up path.
			partitionCommitTime := p.lastPartitionCommitTime.Load()
			if !p.clpaControlEnabled() {
				partitionCommitTime = time.Now().UnixMilli()
				p.lastPartitionCommitTime.Store(partitionCommitTime)
			}
			p.pl.Plog.Printf("DIAG scheduler partition commit reset point: seq=%d, commitTime=%d\n", p.sequenceID, partitionCommitTime)
		}
		p.isReply[string(cmsg.Digest)] = true
		p.pl.Plog.Printf("S%dN%d: this round of pbft %d is end \n", p.ShardID, p.NodeID, p.sequenceID)
		p.sequenceID++
		p.pruneCommittedRequests(p.sequenceID)
		p.prePrepareAccepted = false
		p.pbftStage.Store(1)
		p.pl.Plog.Printf("commited\n")
		p.lastCommitTime.Store(time.Now().UnixMilli())
		p.lock.Unlock()
		if p.NodeID == uint64(p.view.Load()) {
			p.sequenceLock.Unlock()
			p.pl.Plog.Printf("S%dN%d get sequenceLock unlocked...\n", p.ShardID, p.NodeID)
		}
		p.activateCachedRequestLocked()
		p.wakeCLPAControl()
		return
	}
}

// Called with pbftLock held. A pending range replaces the old mutex held across
// network I/O: waiting for a second range must never block the response handler.
func (p *PbftConsensusNode) requestCatchUp(start, end uint64) {
	if end < start || end == ^uint64(0) {
		p.failCatchUp(fmt.Errorf("invalid requested range [%d,%d]", start, end))
	}
	p.catchUpPending, p.catchUpStart, p.catchUpEnd = true, start, end
	server := &shard.Node{NodeID: uint64(p.view.Load()), ShardID: p.ShardID,
		IPaddr: p.ip_nodeTable[p.ShardID][uint64(p.view.Load())]}
	request := message.RequestOldMessage{SeqStartHeight: start, SeqEndHeight: end,
		ServerNode: server, SenderNode: p.RunningNode}
	encoded, err := json.Marshal(request)
	if err != nil {
		log.Panic(err)
	}
	p.pl.Plog.Printf("S%dN%d : requesting message (seq %d to %d)\n", p.ShardID, p.NodeID, start, end)
	networks.TcpDial(message.MergeMessage(message.CRequestOldrequest, encoded), server.IPaddr)
}

// A cache-only run cannot repair arbitrarily old consensus/control history.
// Log to the node log as well as stderr and fail the process explicitly, rather
// than silently applying a prefix or pretending the experiment completed.
func (p *PbftConsensusNode) failCatchUp(err error) {
	diagnostic := fmt.Sprintf("PBFT-CATCHUP-FAILED S%dN%d: %v; complete request disk history is disabled", p.ShardID, p.NodeID, err)
	if p.pl != nil && p.pl.Plog != nil {
		p.pl.Plog.Println(diagnostic)
	}
	panic(diagnostic)
}

// this func is only invoked by the main node,
// if the request is correct, the main node will send
// block back to the message sender.
// now this function can send both block and partition
func (p *PbftConsensusNode) handleRequestOldSeq(content []byte) {
	if uint64(p.view.Load()) != p.NodeID {
		return
	}

	rom := new(message.RequestOldMessage)
	err := json.Unmarshal(content, rom)
	if err != nil {
		log.Panic()
	}
	p.pl.Plog.Printf("S%dN%d : received the old message requst from ...", p.ShardID, p.NodeID)
	rom.SenderNode.PrintNode()

	oldR, err := p.cachedRequestRange(rom.SeqStartHeight, rom.SeqEndHeight)
	if err != nil {
		p.failCatchUp(err)
	}
	p.pl.Plog.Printf("S%dN%d : has generated the message to be sent\n", p.ShardID, p.NodeID)

	p.ihm.HandleReqestforOldSeq(rom)

	// send the block back
	sb := message.SendOldMessage{
		SeqStartHeight: rom.SeqStartHeight,
		SeqEndHeight:   rom.SeqEndHeight,
		OldRequest:     oldR,
		SenderNode:     p.RunningNode,
	}
	sbByte, err := json.Marshal(sb)
	if err != nil {
		log.Panic()
	}
	msg_send := message.MergeMessage(message.CSendOldrequest, sbByte)
	networks.TcpDial(msg_send, rom.SenderNode.IPaddr)
	p.pl.Plog.Printf("S%dN%d : send blocks\n", p.ShardID, p.NodeID)
}

// node requst blocks and receive blocks from the main node
func (p *PbftConsensusNode) handleSendOldSeq(content []byte) {
	som := new(message.SendOldMessage)
	err := json.Unmarshal(content, som)
	if err != nil {
		log.Panic()
	}
	p.pl.Plog.Printf("S%dN%d : has received the SendOldMessage message\n", p.ShardID, p.NodeID)
	p.pbftLock.Lock()
	defer p.pbftLock.Unlock()
	defer p.conditionalVarpbftLock.Broadcast()
	if p.stopSignal.Load() || !p.catchUpPending {
		return
	}
	if som.SeqStartHeight != p.catchUpStart || som.SeqEndHeight > p.catchUpEnd {
		p.pl.Plog.Println("refusing catch-up response outside the outstanding range")
		return
	}
	if som.SeqEndHeight != p.catchUpEnd || som.SeqEndHeight < som.SeqStartHeight ||
		som.SeqEndHeight == ^uint64(0) || uint64(len(som.OldRequest)) != som.SeqEndHeight-som.SeqStartHeight+1 {
		p.failCatchUp(fmt.Errorf("incomplete response for requested range [%d,%d]: got [%d,%d] with %d records", p.catchUpStart, p.catchUpEnd, som.SeqStartHeight, som.SeqEndHeight, len(som.OldRequest)))
	}
	for idx, request := range som.OldRequest {
		if request == nil {
			p.failCatchUp(fmt.Errorf("missing payload at sequence %d", som.SeqStartHeight+uint64(idx)))
		}
	}
	entrySequence := p.sequenceID
	if t := p.clpaStep4; t != nil && t.Seq <= som.SeqEndHeight {
		p.clpaStep4 = nil // Replay has no local proposal-to-quorum interval.
	}

	// implement interface for new consensus
	// Legacy modes do not all skip overlapping requests themselves. Pass only
	// unapplied records so duplicate responses cannot reapply a partition move.
	if som.SeqEndHeight >= entrySequence {
		replay := *som
		if replay.SeqStartHeight < entrySequence {
			replay.OldRequest = replay.OldRequest[entrySequence-replay.SeqStartHeight:]
			replay.SeqStartHeight = entrySequence
		}
		if !p.ihm.HandleforSequentialRequest(&replay) {
			p.failCatchUp(fmt.Errorf("could not apply complete requested range [%d,%d]", replay.SeqStartHeight, replay.SeqEndHeight))
		}
	}
	beginSeq := som.SeqStartHeight
	for idx, r := range som.OldRequest {
		p.cacheRequest(uint64(idx)+beginSeq, string(getDigest(r)), r)
		p.markRequestCommitted(uint64(idx)+beginSeq, string(getDigest(r)))
		if uint64(idx)+beginSeq >= entrySequence {
			p.recordCLPAPartitionInstall(r, uint64(idx)+beginSeq)
		}
		p.isReply[string(getDigest(r))] = true
		p.pl.Plog.Printf("this round of pbft %d is end \n", uint64(idx)+beginSeq)
	}
	// A delayed duplicate catch-up response must never move the local PBFT
	// sequence backwards. Normal catch-up still advances to end+1; an overlapping
	// or stale response leaves a newer local sequence unchanged.
	p.sequenceID = pbftSequenceAfterCatchUp(entrySequence, som.SeqEndHeight)
	p.pruneCommittedRequests(p.sequenceID)
	p.catchUpPending = false
	// Normal commits cannot pass a pending catch-up. Do not reset an already
	// advanced proposal on an overlapping/stale response used by legacy modes.
	if som.SeqEndHeight >= entrySequence {
		p.prePrepareAccepted = false
		p.pbftStage.Store(1)
	}
	p.activateCachedRequestLocked()
	p.wakeCLPAControl()
}

func pbftSequenceAfterCatchUp(current, receivedEnd uint64) uint64 {
	next := receivedEnd + 1
	if next > current {
		return next
	}
	return current
}
