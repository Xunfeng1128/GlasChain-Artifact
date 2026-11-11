package pbft_all

import (
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"encoding/json"
	"log"
	"time"
)

type ViewChangeData struct {
	NextView, SeqID int
	ViewVersion     uint64
}

// The worker cannot recover a leader's local priority marks after a view change.
func (p *PbftConsensusNode) failGLASPoolViewChange(reason string) {
	if params.GLASPendingFullWindowEarlyEnabled() {
		log.Panicf("GLAS full-window early reporting cannot recover leader-local priority marks after a view change (%s)", reason)
	}
}

// propose a view change request
func (p *PbftConsensusNode) viewChangePropose() {
	p.pbftLock.Lock()
	defer p.pbftLock.Unlock()
	if p.stopSignal.Load() {
		return
	}
	p.failGLASPoolViewChange("local view-change timeout")
	// load pbftStage as 5, i.e., making a view change
	p.pbftStage.Store(5)

	p.pl.Plog.Println("Main node is time out, now trying to view change. ")

	vcmsg := message.ViewChangeMsg{
		CurView:  int(p.view.Load()),
		NextView: int(p.view.Load()+1) % int(p.node_nums),
		SeqID:    int(p.sequenceID),
		FromNode: p.NodeID,
	}
	// marshal and broadcast
	vcbyte, err := json.Marshal(vcmsg)
	if err != nil {
		log.Panic()
	}
	msg_send := message.MergeMessage(message.ViewChangePropose, vcbyte)
	networks.Broadcast(p.RunningNode.IPaddr, p.getNeighborNodes(), msg_send)
	networks.TcpDial(msg_send, p.RunningNode.IPaddr)

	p.pl.Plog.Println("View change message is broadcasted. ")
}

// handle view change messages.
func (p *PbftConsensusNode) handleViewChangeMsg(content []byte) {
	vcmsg := new(message.ViewChangeMsg)
	err := json.Unmarshal(content, vcmsg)
	if err != nil {
		log.Panic(err)
	}
	p.pbftLock.Lock()
	defer p.pbftLock.Unlock()
	if p.stopSignal.Load() || vcmsg.SeqID < 0 || uint64(vcmsg.SeqID) < p.sequenceID {
		return
	}
	vcData := ViewChangeData{NextView: vcmsg.NextView, SeqID: vcmsg.SeqID}
	if _, ok := p.viewChangeMap[vcData]; !ok {
		p.viewChangeMap[vcData] = make(map[uint64]bool)
	}
	p.viewChangeMap[vcData][vcmsg.FromNode] = true

	p.pl.Plog.Println("Received view change message from Node", vcmsg.FromNode)

	// if cnt = 2*f+1, then broadcast newView msg
	if len(p.viewChangeMap[vcData]) == 2*int(p.malicious_nums)+1 {
		p.failGLASPoolViewChange("view-change quorum")
		nvmsg := message.NewViewMsg{
			CurView:  int(p.view.Load()),
			NextView: int(p.view.Load()+1) % int(p.node_nums),
			NewSeqID: int(p.sequenceID),
			FromNode: p.NodeID,
		}
		nvbyte, err := json.Marshal(nvmsg)
		if err != nil {
			log.Panic()
		}
		msg_send := message.MergeMessage(message.NewChange, nvbyte)
		networks.Broadcast(p.RunningNode.IPaddr, p.getNeighborNodes(), msg_send)
		networks.TcpDial(msg_send, p.RunningNode.IPaddr)
	}
}

func (p *PbftConsensusNode) handleNewViewMsg(content []byte) {
	nvmsg := new(message.NewViewMsg)
	err := json.Unmarshal(content, nvmsg)
	if err != nil {
		log.Panic(err)
	}
	p.pbftLock.Lock()
	var wakeProposal bool
	defer func() {
		p.pbftLock.Unlock()
		if wakeProposal {
			p.wakeCLPAControl()
		}
	}()
	if p.stopSignal.Load() || nvmsg.NewSeqID < 0 || uint64(nvmsg.NewSeqID) < p.sequenceID {
		return
	}
	vcData := ViewChangeData{NextView: nvmsg.NextView, SeqID: nvmsg.NewSeqID}
	if _, ok := p.newViewMap[vcData]; !ok {
		p.newViewMap[vcData] = make(map[uint64]bool)
	}
	p.newViewMap[vcData][nvmsg.FromNode] = true

	p.pl.Plog.Println("Received new view message from Node", nvmsg.FromNode)

	// if cnt = 2*f+1, then step into the next view.
	if len(p.newViewMap[vcData]) == 2*int(p.malicious_nums)+1 {
		p.failGLASPoolViewChange("new-view quorum")
		p.view.Store(int32(vcData.NextView))
		p.clpaStep4 = nil // A proposal from the previous view is not a timing sample.
		p.sequenceID = uint64(nvmsg.NewSeqID)
		p.prePrepareAccepted = false
		p.pbftStage.Store(1)
		p.lastCommitTime.Store(time.Now().UnixMilli())
		wakeProposal = true
		p.pl.Plog.Println("New view is updated.")
	}
}
