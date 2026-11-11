package pbft_all

import (
	"blockEmulator/message"
	"fmt"
	"sort"
)

// Recent committed requests are the only catch-up history. Blocks and account
// state remain in their ordinary databases, but complete PBFT/control requests
// are not archived. Older catch-up must fail explicitly; pending requests are
// never evicted by these bounds.
const pbftRequestCacheCount = 16
const pbftRequestCacheBytes = 32 << 20

func (p *PbftConsensusNode) cacheRequest(sequence uint64, digest string, request *message.Request) {
	p.requestMu.Lock()
	defer p.requestMu.Unlock()
	if p.requestPool == nil {
		p.requestPool = make(map[string]*message.Request)
	}
	if p.height2Digest == nil {
		p.height2Digest = make(map[uint64]string)
	}
	if old, ok := p.height2Digest[sequence]; ok && old != digest {
		if p.committedRequests[sequence] {
			panic(fmt.Sprintf("cannot replace committed PBFT request at sequence %d", sequence))
		}
		// A replacement proposal must not leave its superseded payload alive
		// after its final sequence reference disappears.
		delete(p.height2Digest, sequence)
		p.removeUnreferencedRequest(old)
		if p.supersededRequests == nil {
			p.supersededRequests = make(map[string]bool)
		}
		p.supersededRequests[old] = true
	}
	p.requestPool[digest] = request
	p.height2Digest[sequence] = digest
}

// Called with requestMu held. Different sequences may reference the same
// proposal, including a still-pending proposal after a view change.
func (p *PbftConsensusNode) removeUnreferencedRequest(digest string) {
	for _, other := range p.height2Digest {
		if other == digest {
			return
		}
	}
	delete(p.requestPool, digest)
}

func (p *PbftConsensusNode) requestByDigest(digest string) (*message.Request, bool) {
	p.requestMu.RLock()
	request, ok := p.requestPool[digest]
	p.requestMu.RUnlock()
	return request, ok && request != nil
}

func (p *PbftConsensusNode) requestAtSequence(sequence uint64) (*message.Request, bool) {
	p.requestMu.RLock()
	digest, indexed := p.height2Digest[sequence]
	request, cached := p.requestPool[digest]
	p.requestMu.RUnlock()
	return request, indexed && cached && request != nil
}

func (p *PbftConsensusNode) markRequestCommitted(sequence uint64, digest string) {
	p.requestMu.Lock()
	defer p.requestMu.Unlock()
	if p.height2Digest[sequence] != digest || p.requestPool[digest] == nil {
		panic(fmt.Sprintf("cannot mark uncached PBFT request committed at sequence %d", sequence))
	}
	if p.committedRequests == nil {
		p.committedRequests = make(map[uint64]bool)
	}
	p.committedRequests[sequence] = true
}

// Take one consistent snapshot of the complete range. Never send a prefix and
// leave a lagging node requesting an irrecoverably evicted suffix forever.
func (p *PbftConsensusNode) cachedRequestRange(start, end uint64) ([]*message.Request, error) {
	if end < start || end == ^uint64(0) {
		return nil, fmt.Errorf("invalid PBFT catch-up range [%d,%d]", start, end)
	}
	p.requestMu.RLock()
	defer p.requestMu.RUnlock()
	if end-start+1 > uint64(len(p.height2Digest)) {
		return nil, fmt.Errorf("PBFT catch-up range [%d,%d] is unavailable in recent request cache (no disk history)", start, end)
	}
	requests := make([]*message.Request, 0, int(end-start+1))
	for sequence := start; sequence <= end; sequence++ {
		digest, ok := p.height2Digest[sequence]
		request := p.requestPool[digest]
		if !ok || request == nil {
			return nil, fmt.Errorf("PBFT catch-up sequence %d in [%d,%d] is unavailable in recent request cache (no disk history)", sequence, start, end)
		}
		requests = append(requests, request)
	}
	return requests, nil
}

// Called under pbftLock after execution committed and advanced the sequence.
// An increased view/sequence alone is not evidence that pending work committed.
// Sequence checks reject delayed votes before touching
// the removed confirmation maps; active/uncommitted consensus is left intact.
func (p *PbftConsensusNode) pruneCommittedRequests(nextSequence uint64) {
	p.requestMu.Lock()
	defer p.requestMu.Unlock()
	for key := range p.viewChangeMap {
		if key.SeqID >= 0 && uint64(key.SeqID) < nextSequence {
			delete(p.viewChangeMap, key)
		}
	}
	for key := range p.newViewMap {
		if key.SeqID >= 0 && uint64(key.SeqID) < nextSequence {
			delete(p.newViewMap, key)
		}
	}
	// A view replacement can orphan the old proposal's votes without that
	// proposal ever committing. These small indexes must not grow by round.
	// Only known replaced proposals qualify: votes for a not-yet-arrived
	// PrePrepare must remain pending rather than being mistaken for garbage.
	for digest := range p.supersededRequests {
		if p.requestPool[digest] == nil {
			delete(p.cntPrepareConfirm, digest)
			delete(p.cntCommitConfirm, digest)
			delete(p.isCommitBordcast, digest)
			delete(p.isReply, digest)
			delete(p.supersededRequests, digest)
		}
	}
	var committed []uint64
	pendingDigests := make(map[string]bool)
	for sequence, digest := range p.height2Digest {
		if !p.committedRequests[sequence] {
			pendingDigests[digest] = true
		}
	}
	for sequence, digest := range p.height2Digest {
		if sequence < nextSequence && p.committedRequests[sequence] {
			committed = append(committed, sequence)
			if !pendingDigests[digest] {
				delete(p.cntPrepareConfirm, digest)
				delete(p.cntCommitConfirm, digest)
				delete(p.isCommitBordcast, digest)
				delete(p.isReply, digest)
			}
		}
	}
	sort.Slice(committed, func(i, j int) bool { return committed[i] > committed[j] })
	count, byteCount := 0, 0
	for _, sequence := range committed {
		digest := p.height2Digest[sequence]
		request := p.requestPool[digest]
		size := 0
		if request != nil {
			size = len(request.Msg.Content)
		}
		if count < pbftRequestCacheCount && byteCount+size <= pbftRequestCacheBytes {
			count++
			byteCount += size
			continue
		}
		delete(p.height2Digest, sequence)
		delete(p.committedRequests, sequence)
		p.removeUnreferencedRequest(digest)
	}
}

func (p *PbftConsensusNode) requestCacheStats() (int, int) {
	p.requestMu.RLock()
	defer p.requestMu.RUnlock()
	bytes := 0
	for _, request := range p.requestPool {
		if request != nil {
			bytes += len(request.Msg.Content)
		}
	}
	return len(p.requestPool), bytes
}
