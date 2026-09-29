package raft

import "time"

// This file covers the AppendEntries RPC (Raft paper Figure 2):
//   - the follower-side handler, which does both jobs of AppendEntries:
//     acting as a heartbeat AND accepting replicated log entries
//   - the leader-side loop that fires AppendEntries at every follower
//     on a fixed interval (see replication.go for what each call sends)

// AppendEntriesArgs is sent by the leader to replicate log entries,
// and, with Entries left empty, purely as a heartbeat.
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderID     PeerID     // so followers know who the leader is
	PrevLogIndex int        // index of the entry immediately before Entries
	PrevLogTerm  int        // term of that entry
	Entries      []LogEntry // nil/empty = heartbeat only
	LeaderCommit int        // leader's commit index
}

// AppendEntriesReply is the response to an AppendEntries RPC.
type AppendEntriesReply struct {
	Term    int  // follower's current term, so a stale leader can step down
	Success bool // true if follower had an entry matching PrevLogIndex/PrevLogTerm
}

// HandleAppendEntries implements the receiver side of AppendEntries,
// following Raft paper Figure 2 step by step.
func (n *Node) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	// 1. Reject a leader from an older term - it has been replaced
	// and doesn't know it yet. The reply carries our term so it can
	// find out.
	if args.Term < n.CurrentTerm {
		return AppendEntriesReply{Term: n.CurrentTerm, Success: false}
	}

	if args.Term > n.CurrentTerm {
		n.CurrentTerm = args.Term
		n.VotedFor = ""
	}

	// A valid AppendEntries from a current-or-newer-term leader means
	// there IS a leader, so we're a Follower - even if we were a
	// Candidate or a stale Leader a moment ago - and we restart our
	// election countdown. This happens even if the log check below
	// fails, because we did hear from a legitimate leader.
	n.State = Follower
	n.resetElectionTimer()

	// 2. Log matching check: we must already have an entry at
	// PrevLogIndex with term PrevLogTerm, otherwise our log has a gap
	// or has diverged from the leader's. Reply false and the leader
	// will retry with an earlier PrevLogIndex.
	prevTerm, ok := n.Log.TermAt(args.PrevLogIndex)
	if !ok || prevTerm != args.PrevLogTerm {
		return AppendEntriesReply{Term: n.CurrentTerm, Success: false}
	}

	// 3 + 4. Walk the new entries. Skip ones we already have (same
	// index and term - this happens with duplicate/retried RPCs). At
	// the first conflict (same index, different term), delete that
	// entry and everything after it, then append the rest.
	for i, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx <= n.Log.LastIndex() {
			if existing, _ := n.Log.TermAt(idx); existing == e.Term {
				continue // already have this exact entry
			}
			n.Log.TruncateFrom(idx) // conflict: drop it and all that follow
		}
		n.Log.AppendAll(args.Entries[i:])
		break
	}

	// 5. Advance our commit index toward the leader's, but never past
	// the last entry this RPC verified we share with the leader, and
	// never backwards (a delayed old RPC must not lower it).
	if args.LeaderCommit > n.CommitIndex {
		lastNew := args.PrevLogIndex + len(args.Entries)
		newCommit := args.LeaderCommit
		if lastNew < newCommit {
			newCommit = lastNew
		}
		if newCommit > n.CommitIndex {
			n.CommitIndex = newCommit
		}
	}

	return AppendEntriesReply{Term: n.CurrentTerm, Success: true}
}

// runHeartbeats is started once, when a node becomes Leader (see
// becomeLeaderLocked in election.go). Every heartbeatInterval it
// triggers a replication round to every peer, until this node stops
// being Leader for the given term - either by stepping down (saw a
// higher term) or by Stop() being called.
//
// Each round doubles as the heartbeat: if a peer is already up to
// date, replicateTo sends an AppendEntries with no entries.
func (n *Node) runHeartbeats(term int) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		n.mu.Lock()
		stillLeader := n.State == Leader && n.CurrentTerm == term
		peers := append([]PeerID{}, n.Peers...)
		n.mu.Unlock()

		if !stillLeader {
			return
		}

		for _, peer := range peers {
			go n.replicateTo(peer, term)
		}

		select {
		case <-ticker.C:
		case <-n.stopCh:
			return
		}
	}
}
