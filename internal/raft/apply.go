package raft

import "time"

// This file is what turns a "replicated log" into a "replicated state
// machine": once an entry's index is <= CommitIndex, every node -
// leader and followers alike - applies it, in the same order, to its
// own local Store. Because every node applies the same commands in
// the same order, they all converge to the same state.

// applyTickInterval controls how often the apply loop checks whether
// CommitIndex has moved past LastApplied.
//
// TODO: polling is simple but adds up to applyTickInterval of latency
// between an entry committing and it being applied. A sync.Cond or a
// channel signaled wherever CommitIndex is set (in replication.go and
// heartbeat.go) would remove that latency; left as polling for
// clarity until performance actually demands the change.
const applyTickInterval = 10 * time.Millisecond

// runApplyLoop runs for the lifetime of the node, applying newly
// committed entries as they appear.
func (n *Node) runApplyLoop() {
	ticker := time.NewTicker(applyTickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			n.applyCommitted()
		case <-n.stopCh:
			return
		}
	}
}

// applyCommitted applies every entry between LastApplied and
// CommitIndex, strictly in order, advancing LastApplied as it goes.
func (n *Node) applyCommitted() {
	for {
		n.mu.Lock()
		if n.LastApplied >= n.CommitIndex {
			n.mu.Unlock()
			return
		}
		nextIdx := n.LastApplied + 1
		entry, ok := n.Log.Get(nextIdx)
		store := n.Store
		n.mu.Unlock()

		if !ok {
			// CommitIndex should never point past what's actually in
			// the log - if it does, something upstream is broken.
			// Bail out rather than spin forever on a bad index.
			return
		}

		if store != nil {
			switch entry.Command.Op {
			case OpPut:
				store.Put(entry.Command.Key, entry.Command.Value)
			case OpDelete:
				store.Delete(entry.Command.Key)
			}
		}

		n.mu.Lock()
		if nextIdx > n.LastApplied {
			n.LastApplied = nextIdx
		}
		n.mu.Unlock()
	}
}
