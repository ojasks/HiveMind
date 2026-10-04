package raft

// This file is the LEADER side of log replication: accepting a client
// command, sending each follower whatever entries it is missing, and
// deciding when an entry is safely committed (stored on a majority).

// Propose asks the cluster to replicate command. Only the leader
// accepts proposals: on a Leader it appends the command to its own log
// and returns the entry's index and term with isLeader = true; on any
// other node it returns isLeader = false.
//
// Returning does NOT mean the command is committed - only that the
// leader has accepted it. It becomes committed (CommitIndex >= index)
// once a majority of nodes store it, which happens asynchronously via
// the heartbeat loop.
func (n *Node) Propose(command Command) (index int, term int, isLeader bool) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.State != Leader {
		return 0, n.CurrentTerm, false
	}

	entry := n.Log.Append(n.CurrentTerm, command)
	n.persistNewEntryLocked(entry) // durable before we tell the caller we accepted it

	// With no peers (single-node cluster) the leader alone is a
	// majority, so commit right away. With peers this is a no-op until
	// followers report back.
	n.advanceCommitIndexLocked()

	return entry.Index, entry.Term, true
}

// replicateTo sends one AppendEntries RPC to peer, containing every
// entry from nextIndex[peer] onward (possibly none, which makes it a
// pure heartbeat), then processes the reply. Called once per peer per
// heartbeat tick.
func (n *Node) replicateTo(peer PeerID, term int) {
	// Build the request under the lock, then release it before the
	// network call so we never hold n.mu while waiting on a peer.
	n.mu.Lock()
	if n.State != Leader || n.CurrentTerm != term {
		n.mu.Unlock()
		return
	}
	next := n.nextIndex[peer]
	prevLogIndex := next - 1
	prevLogTerm, _ := n.Log.TermAt(prevLogIndex)
	entries := n.Log.Slice(next)
	args := AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.ID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.CommitIndex,
	}
	n.mu.Unlock()

	replyAny, err := n.Network.Send(string(n.ID), string(peer), "Raft.AppendEntries", args)
	if err != nil {
		return // peer unreachable - the next tick retries
	}
	reply, err := decodeArg[AppendEntriesReply](replyAny)
	if err != nil {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()

	if reply.Term > n.CurrentTerm {
		// A peer has seen a newer term: we are no longer the leader.
		n.CurrentTerm = reply.Term
		n.State = Follower
		n.VotedFor = ""
		n.persistTermAndVoteLocked()
		return
	}
	if n.State != Leader || n.CurrentTerm != term {
		return // we stepped down or a new term began while the RPC was in flight
	}

	if reply.Success {
		// The follower now matches our log through the last entry we
		// sent. Only ever move matchIndex forward.
		newMatch := prevLogIndex + len(entries)
		if newMatch > n.matchIndex[peer] {
			n.matchIndex[peer] = newMatch
		}
		n.nextIndex[peer] = n.matchIndex[peer] + 1
		n.advanceCommitIndexLocked()
		return
	}

	// Rejected: the follower's log doesn't contain our PrevLogIndex
	// entry. Step back one entry and retry next tick; repeat until we
	// find where the two logs agree. The check on nextIndex guards
	// against two overlapping RPCs both decrementing for one failure.
	if n.nextIndex[peer] == next && next > 1 {
		n.nextIndex[peer] = next - 1
	}
}

// advanceCommitIndexLocked moves CommitIndex forward to the highest
// entry stored on a majority of nodes (counting the leader itself).
// Caller must already hold n.mu.
func (n *Node) advanceCommitIndexLocked() {
	majority := (len(n.Peers)+1)/2 + 1

	for idx := n.Log.LastIndex(); idx > n.CommitIndex; idx-- {
		term, _ := n.Log.TermAt(idx)
		if term != n.CurrentTerm {
			// Raft paper §5.4.2: a leader only commits entries from
			// its OWN term by counting replicas. Older-term entries
			// become committed indirectly, once a current-term entry
			// after them is committed. Entries below this one are
			// older still, so stop looking.
			break
		}

		count := 1 // the leader itself has every entry in its own log
		for _, p := range n.Peers {
			if n.matchIndex[p] >= idx {
				count++
			}
		}
		if count >= majority {
			n.CommitIndex = idx
			return
		}
	}
}
