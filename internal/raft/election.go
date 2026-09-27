package raft

// This file defines the RequestVote RPC shape from the Raft paper
// (Figure 2, "RequestVote RPC") and stubs out where the election logic
// will live. Nothing here runs an actual election yet — see the TODOs.

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

// HandleRequestVote implements the receiver side of RequestVote.
//
// TODO(phase 6.1): implement per Raft paper §5.2 / §5.4:
//  1. Reply false if args.Term < n.CurrentTerm.
//  2. If args.Term > n.CurrentTerm, update n.CurrentTerm, revert to
//     Follower, and clear VotedFor.
//  3. Grant the vote only if VotedFor is empty or already equals
//     args.CandidateID, AND the candidate's log is at least as
//     up-to-date as this node's own log (the actual safety check
//     that prevents electing a leader with stale data).
//  4. Persist CurrentTerm/VotedFor (via wal) before replying — see the
//     TODO on Node in node.go.
//  5. Reset the node's election timer on granting a vote.
func (n *Node) HandleRequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Placeholder: currently never grants a vote. Replace with the real
	// logic described above.
	return RequestVoteReply{
		Term:        n.CurrentTerm,
		VoteGranted: false,
	}
}

// StartElection is called when a Follower's election timeout fires.
//
// TODO(phase 6.1): implement per Raft paper §5.2:
//  1. Increment CurrentTerm, transition to Candidate, vote for self.
//  2. Reset election timer (with randomized timeout to avoid repeated
//     split votes — this randomization is not optional).
//  3. Send RequestVote RPCs in parallel to all peers (via
//     internal/transport).
//  4. If votes received from a majority (including self): become
//     Leader, and immediately start sending heartbeats.
//  5. If AppendEntries received from a new leader with Term >=
//     CurrentTerm: step down to Follower.
//  6. If election timeout elapses with no winner: start a new election
//     (new term).
func (n *Node) StartElection() {
	n.mu.Lock()
	defer n.mu.Unlock()

	// TODO: implement. See docstring above.
	_ = n // placeholder to keep the receiver used until implemented
}
