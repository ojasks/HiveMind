package raft

import "time"

// This file implements Raft leader election: the RequestVote RPC
// (Raft paper Figure 2, §5.2, §5.4) and the candidate-side logic that
// solicits votes from peers and becomes leader on winning a majority.

// RequestVoteArgs is sent by a candidate to gather votes.
type RequestVoteArgs struct {
	Term         int    // candidate's term
	CandidateID  PeerID // candidate requesting vote
	LastLogIndex int    // index of candidate's last log entry
	LastLogTerm  int    // term of candidate's last log entry
}

// RequestVoteReply is the response to a RequestVote RPC.
type RequestVoteReply struct {
	Term        int  // currentTerm, for the candidate to update itself
	VoteGranted bool // true means candidate received the vote
}

// HandleRequestVote implements the receiver side of RequestVote, per
// Raft paper §5.2 / §5.4.
func (n *Node) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	// 1. Reject outright if the candidate's term is stale.
	if args.Term < n.CurrentTerm {
		return RequestVoteReply{Term: n.CurrentTerm, VoteGranted: false}
	}

	// 2. If the candidate's term is newer than ours, we're behind:
	// update our term, revert to Follower, and clear our previous vote
	// since a vote only applies within a single term.
	if args.Term > n.CurrentTerm {
		n.CurrentTerm = args.Term
		n.State = Follower
		n.VotedFor = ""
	}

	// 3. Only grant the vote if we haven't already voted for someone
	// else this term, AND the candidate's log is at least as up to
	// date as ours - the safety check (§5.4.1) that stops a node with
	// stale/missing data from becoming leader. Compare by term first,
	// then by index as a tiebreaker within the same term.
	logIsUpToDate := args.LastLogTerm > n.Log.LastTerm() ||
		(args.LastLogTerm == n.Log.LastTerm() && args.LastLogIndex >= n.Log.LastIndex())

	// TODO(phase 6.3): CurrentTerm and VotedFor must be persisted to
	// disk (via internal/wal) BEFORE this function returns a granted
	// vote. Until persistence is wired in, a crash immediately after
	// granting a vote can make this node forget it voted and grant a
	// second, conflicting vote in the same term after restart - a real
	// safety violation, not a hypothetical one. Left deliberately
	// unimplemented here so persistence can be added and tested as its
	// own unit; do not ship this to a real cluster without it.

	if (n.VotedFor == "" || n.VotedFor == args.CandidateID) && logIsUpToDate {
		n.VotedFor = args.CandidateID
		n.resetElectionTimer() // 5. we just heard from a legitimate candidate
		return RequestVoteReply{Term: n.CurrentTerm, VoteGranted: true}
	}

	return RequestVoteReply{Term: n.CurrentTerm, VoteGranted: false}
}

// StartElection is called when a node's election timeout fires, per
// Raft paper §5.2: become a Candidate, vote for self, and ask every
// peer for their vote.
func (n *Node) StartElection() {
	n.mu.Lock()
	n.CurrentTerm++
	term := n.CurrentTerm
	n.State = Candidate
	n.VotedFor = n.ID
	lastLogIndex := n.Log.LastIndex()
	lastLogTerm := n.Log.LastTerm()
	peers := append([]PeerID{}, n.Peers...)
	n.mu.Unlock()

	// Restart our own timeout too: if this election stalls (e.g. a
	// split vote with no majority), we shouldn't wait a full period
	// before trying again.
	n.resetElectionTimer()

	if len(peers) == 0 {
		// A "cluster" of one node is trivially its own majority.
		n.mu.Lock()
		if n.State == Candidate && n.CurrentTerm == term {
			n.becomeLeaderLocked()
		}
		n.mu.Unlock()
		return
	}

	args := RequestVoteArgs{
		Term:         term,
		CandidateID:  n.ID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	replies := make(chan RequestVoteReply, len(peers))

	for _, peer := range peers {
		peer := peer
		go func() {
			replyAny, err := n.Network.Send(string(n.ID), string(peer), "Raft.RequestVote", args)
			if err != nil {
				return // peer unreachable/dropped - it simply doesn't vote
			}
			if reply, ok := replyAny.(RequestVoteReply); ok {
				replies <- reply
			}
		}()
	}

	votes := 1 // we vote for ourselves
	majority := (len(peers)+1)/2 + 1
	deadline := time.After(electionTimeoutMax)

	for i := 0; i < len(peers); i++ {
		select {
		case reply := <-replies:
			n.mu.Lock()
			if reply.Term > n.CurrentTerm {
				// Someone is ahead of us in term - abandon this
				// election and fall in line as a Follower.
				n.CurrentTerm = reply.Term
				n.State = Follower
				n.VotedFor = ""
				n.mu.Unlock()
				return
			}
			if n.State != Candidate || n.CurrentTerm != term {
				// We've moved on (stepped down, or started a newer
				// election) since sending this request - the reply is
				// stale, ignore it.
				n.mu.Unlock()
				return
			}
			if reply.VoteGranted {
				votes++
			}
			if votes >= majority {
				n.becomeLeaderLocked()
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()
		case <-n.stopCh:
			return
		case <-deadline:
			// Didn't hear back from enough peers in time (e.g. they're
			// partitioned). Give up - the election timer will fire
			// again and we'll retry with a new term.
			return
		}
	}
}

// becomeLeaderLocked transitions the node to Leader and starts its
// heartbeat loop. Caller must already hold n.mu.
func (n *Node) becomeLeaderLocked() {
	n.State = Leader
	// TODO(phase 6.2): initialize nextIndex[peer] = len(log)+1 and
	// matchIndex[peer] = 0 for every peer here, per Raft paper Figure
	// 2, once log replication (not just heartbeats) is implemented.
	go n.runHeartbeats(n.CurrentTerm)
}
