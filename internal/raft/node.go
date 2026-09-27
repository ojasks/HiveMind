// Package raft will eventually contain a from-scratch Raft consensus
// implementation, built directly against the Raft paper (Ongaro &
// Ousterhout, "In Search of an Understandable Consensus Algorithm").
//
// Nothing in this package does real consensus yet. What's here is the
// scaffolding — the state a node needs to track, and the shape of the
// operations it will perform — so that election.go and log replication
// can be filled in incrementally without restructuring everything.
//
// Recommended build order (see docs/architecture.md):
//  1. Leader election only, against internal/transport's fake network.
//  2. Log replication (AppendEntries) with no persistence.
//  3. Persist currentTerm/votedFor/log via internal/wal before
//     acknowledging votes/entries — this is the most common place
//     naive Raft implementations violate safety.
//  4. Commit index + apply loop into internal/kv.
package raft

import "sync"

// State is the role a Raft node currently believes it holds.
type State string

const (
	Follower  State = "Follower"
	Candidate State = "Candidate"
	Leader    State = "Leader"
)

// PeerID identifies another node in the cluster.
type PeerID string

// Node holds all Raft state for a single cluster member.
//
// TODO(phase 6.3 - persistence): currentTerm, votedFor, and the log must
// be persisted (via internal/wal) BEFORE a node responds to a
// RequestVote or AppendEntries RPC. If a node forgets who it voted for
// after a crash, it can vote twice in the same term and violate Raft's
// safety guarantees. Do not skip this when wiring persistence in.
type Node struct {
	mu sync.Mutex

	ID    PeerID
	Peers []PeerID

	// --- Persistent state (must survive restarts once wal is wired in) ---
	CurrentTerm int
	VotedFor    PeerID // "" if none
	Log         *Log

	// --- Volatile state on all servers ---
	State       State
	CommitIndex int
	LastApplied int

	// --- Volatile state on leaders only (reinitialized after election) ---
	// nextIndex/matchIndex per peer — populated once log replication
	// (phase 6.2) is implemented.
	nextIndex  map[PeerID]int
	matchIndex map[PeerID]int
}

// NewNode constructs a fresh Raft node starting as a Follower in term 0.
func NewNode(id PeerID, peers []PeerID) *Node {
	return &Node{
		ID:          id,
		Peers:       peers,
		CurrentTerm: 0,
		VotedFor:    "",
		Log:         NewLog(),
		State:       Follower,
		CommitIndex: 0,
		LastApplied: 0,
		nextIndex:   make(map[PeerID]int),
		matchIndex:  make(map[PeerID]int),
	}
}

// Status is a read-only snapshot of a node's state, intended for the
// future `hivectl status` command and for observability/logging.
type Status struct {
	ID          PeerID
	State       State
	CurrentTerm int
	VotedFor    PeerID
	CommitIndex int
	LastApplied int
	LogLength   int
}

// Status returns a snapshot of the node's current state.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return Status{
		ID:          n.ID,
		State:       n.State,
		CurrentTerm: n.CurrentTerm,
		VotedFor:    n.VotedFor,
		CommitIndex: n.CommitIndex,
		LastApplied: n.LastApplied,
		LogLength:   n.Log.LastIndex(),
	}
}
