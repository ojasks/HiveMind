// Package raft is a from-scratch Raft consensus implementation, built
// directly against the Raft paper (Ongaro & Ousterhout, "In Search of
// an Understandable Consensus Algorithm").
//
// Build order (see docs/architecture.md):
//  1. Leader election (this + election.go + timer.go), tested against
//     internal/transport's FakeNetwork.
//  2. Log replication (heartbeat.go currently only does the heartbeat
//     half of AppendEntries - see its TODOs).
//  3. Persist currentTerm/votedFor/log via internal/wal before
//     acknowledging votes/entries - see the TODO in election.go. This
//     is the most common place naive Raft implementations violate
//     safety, so don't skip it.
//  4. Commit index + apply loop into internal/kv.
package raft

import (
	"sync"

	"hivemind/internal/transport"
)

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

	ID      PeerID
	Peers   []PeerID
	Network transport.Network // how this node talks to its peers

	// --- Persistent state (must survive restarts once wal is wired in) ---
	CurrentTerm int
	VotedFor    PeerID // "" if none
	Log         *Log

	// --- Volatile state on all servers ---
	State       State
	CommitIndex int
	LastApplied int

	// --- Volatile state on leaders only (reinitialized after election) ---
	// nextIndex/matchIndex per peer - populated once log replication
	// (phase 6.2) is implemented.
	nextIndex  map[PeerID]int
	matchIndex map[PeerID]int

	// --- Election timer plumbing (see timer.go) ---
	resetCh chan struct{} // signals "restart the election countdown"
	stopCh  chan struct{} // signals "shut down all background goroutines"
}

// NewNode constructs a fresh Raft node starting as a Follower in term 0.
// network is how this node will reach its peers - typically a
// transport.FakeNetwork in tests, or a real RPC transport in
// production. Call Start() after construction to begin participating
// in elections.
func NewNode(id PeerID, peers []PeerID, network transport.Network) *Node {
	return &Node{
		ID:          id,
		Peers:       peers,
		Network:     network,
		CurrentTerm: 0,
		VotedFor:    "",
		Log:         NewLog(),
		State:       Follower,
		CommitIndex: 0,
		LastApplied: 0,
		nextIndex:   make(map[PeerID]int),
		matchIndex:  make(map[PeerID]int),
		resetCh:     make(chan struct{}, 1),
		stopCh:      make(chan struct{}),
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
