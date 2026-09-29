// Package raft is a from-scratch Raft consensus implementation, built
// directly against the Raft paper (Ongaro & Ousterhout, "In Search of
// an Understandable Consensus Algorithm").
//
// Build order (see docs/architecture.md):
//  1. Leader election (election.go + timer.go), tested against
//     internal/transport's FakeNetwork. DONE.
//  2. Log replication: Propose, AppendEntries, nextIndex/matchIndex,
//     majority-based commit index (replication.go, heartbeat.go).
//     DONE.
//  3. Apply loop: committed entries applied to internal/kv on every
//     node (apply.go). DONE.
//  4. Persist CurrentTerm/VotedFor/Log via persist.go BEFORE a node
//     acts on a change (replying to an RPC, or asking for votes as a
//     new candidate). DONE.
//  5. server.Server routes Put/Delete through Propose instead of
//     writing to kv/wal directly (see the TODO in server.go). NOT
//     DONE YET.
//  6. Real RPC replacing transport.FakeNetwork, for actual
//     multi-process/multi-machine operation. NOT DONE YET.
package raft

import (
	"sync"

	"hivemind/internal/kv"
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
// CurrentTerm, VotedFor, and Log are persisted (see persist.go, and
// the Persister field below) before this node acts on any change to
// them - replying to an RPC, or asking peers for votes as a new
// candidate. This is what stops a crash from making a node forget who
// it voted for and casting a second, conflicting vote in the same term
// after restarting.
type Node struct {
	mu sync.Mutex

	ID      PeerID
	Peers   []PeerID
	Network transport.Network // how this node talks to its peers

	// Store is where committed commands are applied (see apply.go). It
	// is nil by default - set it directly (n.Store = someStore) before
	// calling Start() if you want committed entries to actually reach
	// a kv.Store. Consensus-only tests can leave it nil: the apply
	// loop still advances LastApplied, it just has nowhere to write.
	Store *kv.Store

	// Persister is where CurrentTerm/VotedFor/Log are made durable
	// (see persist.go). Nil by default. If set, assign it and call
	// Restore() BEFORE Start(), so any state from a previous run is
	// loaded before elections/RPCs can begin.
	Persister Persister

	// --- Persistent state (see Persister above - durable across restarts) ---
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
