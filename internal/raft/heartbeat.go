package raft

import "time"

// This file covers the AppendEntries RPC (Raft paper Figure 2): the
// receiver-side handler, and the leader-side loop that sends periodic
// heartbeats (AppendEntries with no entries) so followers know a
// leader is alive and don't start unnecessary elections.

// AppendEntriesArgs is sent by the leader, both to replicate log
// entries and, with Entries left empty, purely as a heartbeat.
type AppendEntriesArgs struct {
	Term         int
	LeaderID     PeerID
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry // nil/empty = heartbeat only
	LeaderCommit int
}

// AppendEntriesReply is the response to an AppendEntries RPC.
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// HandleAppendEntries implements the receiver side of AppendEntries.
// Right now this only implements the heartbeat / term-checking half.
//
// TODO(phase 6.2): implement real log matching - check
// args.PrevLogIndex/PrevLogTerm against this node's own log, truncate
// any conflicting suffix, append args.Entries, and advance CommitIndex
// based on args.LeaderCommit (capped at the index of the last new
// entry), per Raft paper Figure 2's AppendEntries RPC rules.
func (n *Node) HandleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.CurrentTerm {
		return AppendEntriesReply{Term: n.CurrentTerm, Success: false}
	}

	if args.Term > n.CurrentTerm {
		n.CurrentTerm = args.Term
		n.VotedFor = ""
	}

	// A valid AppendEntries from a current-or-newer-term leader means
	// there IS a leader right now, so we should be a Follower - even
	// if we were a Candidate, or a stale Leader ourselves, a moment
	// ago (this is how a partitioned-then-healed old leader learns to
	// stand down).
	n.State = Follower
	n.resetElectionTimer()

	return AppendEntriesReply{Term: n.CurrentTerm, Success: true}
}

// runHeartbeats is started once, when a node becomes Leader (see
// becomeLeaderLocked in election.go), and keeps sending heartbeats
// until this node stops being Leader for the given term - either by
// stepping down (saw a higher term) or by Stop() being called.
func (n *Node) runHeartbeats(term int) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()

	for {
		n.mu.Lock()
		stillLeader := n.State == Leader && n.CurrentTerm == term
		peers := append([]PeerID{}, n.Peers...)
		commitIndex := n.CommitIndex
		n.mu.Unlock()

		if !stillLeader {
			return
		}

		args := AppendEntriesArgs{
			Term:         term,
			LeaderID:     n.ID,
			LeaderCommit: commitIndex,
		}

		for _, peer := range peers {
			peer := peer
			go func() {
				replyAny, err := n.Network.Send(string(n.ID), string(peer), "Raft.AppendEntries", args)
				if err != nil {
					return // peer unreachable - try again next tick
				}
				reply, ok := replyAny.(AppendEntriesReply)
				if !ok {
					return
				}
				n.mu.Lock()
				if reply.Term > n.CurrentTerm {
					// A peer is ahead of us - we're not the real
					// leader anymore, step down.
					n.CurrentTerm = reply.Term
					n.State = Follower
					n.VotedFor = ""
				}
				n.mu.Unlock()
			}()
		}

		select {
		case <-ticker.C:
		case <-n.stopCh:
			return
		}
	}
}
