// Package server ties kv, raft, and (for now) nothing else together
// into a single node that hivectl can talk to.
//
// As of this package, Put/Delete go through Raft: Server.Put proposes
// a command via raft.Node.Propose and BLOCKS until that command is
// actually applied to the local kv.Store (or times out) - see
// propose() below for exactly why waiting for "applied" (not just
// "committed") is the right choice here. Get reads directly from that
// same kv.Store, which raft.Node's own apply loop keeps up to date as
// entries commit (see internal/raft/apply.go).
//
// A NOTE ON internal/wal: Server no longer uses it. Once raft.Node has
// its own Persister (internal/raft/persist.go) durably storing the
// replicated log, that log alone is sufficient to rebuild the KV
// store's state on restart - REPLAYING the log through the SAME apply
// loop that handles normal operation, rather than maintaining a
// second, separate durability mechanism for the same data. internal/wal
// itself is untouched and still a perfectly good, independent example
// of a write-ahead log; server.go simply doesn't need two of them.
package server

import (
	"fmt"
	"time"

	"hivemind/internal/kv"
	"hivemind/internal/raft"
	"hivemind/internal/transport"
)

// commitWaitTimeout bounds how long Put/Delete wait for a proposed
// command to actually commit before giving up and returning an error.
// startupTimeout bounds the one-time waits New() does (for this node
// to become leader, and to catch up applying anything already
// committed) - generous because it only happens once per process, and
// the election timeout alone (up to 300ms) already eats into it.
const (
	commitWaitTimeout = 2 * time.Second
	startupTimeout    = 3 * time.Second
)

// Server is a single HiveMind node: a KV store kept in sync by a
// raft.Node. Today that raft.Node runs as a cluster of exactly one
// (see New) - real multi-process clustering needs real RPC in place of
// transport.FakeNetwork, which is a later phase (see docs/architecture.md).
// Running as a cluster of one already exercises the real Put -> Propose
// -> commit -> apply -> Get path end to end, including surviving a
// process restart - it just can't yet tolerate losing a NODE, only a
// process restarting with its data intact.
type Server struct {
	store     *kv.Store
	raft      *raft.Node
	persister *raft.FilePersister
}

// New creates a Server whose data lives under dataDir: raft's own
// persisted term/vote/log (see internal/raft/persist.go). id names this
// node (used as its raft.PeerID).
//
// New does three things in order, and does not return until all three
// are done - so by the time New returns, Get already reflects
// everything that was durably committed before this process started:
//  1. Restore whatever term/vote/log a previous run left on disk.
//  2. Start the node and wait for it to become Leader. For a
//     single-node cluster this is automatic (see raft.Node.StartElection)
//     but still takes up to the ~300ms election timeout, since nothing
//     shortcuts that wait today.
//  3. Propose one empty "barrier" command and wait for it to be
//     applied. This step exists to close a subtle gap: CommitIndex is
//     NOT persisted (correctly - see the Raft paper), so after a
//     restart it starts at 0 even though this node's OWN past entries
//     were genuinely committed before the crash. Raft's rule "only
//     count replicas toward commitment for entries from the CURRENT
//     term" (safety property §5.4.2) means old entries can't become
//     re-recognized as committed on their own - they need ONE new-term
//     entry to commit first, which then implicitly re-confirms
//     everything before it. An empty Command (Op == "") is a no-op to
//     the KV store (see apply.go's switch), so this barrier changes
//     nothing except unlocking that recognition.
func New(id string, dataDir string) (*Server, error) {
	persister, err := raft.NewFilePersister(dataDir)
	if err != nil {
		return nil, fmt.Errorf("server: creating persister: %w", err)
	}

	store := kv.NewStore()
	// A single-node cluster still needs SOME transport.Network to
	// satisfy raft.Node's constructor, even though - with zero peers -
	// it never actually sends a message over it. Swapping this for a
	// real network transport is exactly what multi-process clustering
	// needs later.
	net := transport.NewFakeNetwork()

	node := raft.NewNode(raft.PeerID(id), nil, net) // nil peers = cluster of one
	node.Store = store
	node.Persister = persister

	if err := node.Restore(); err != nil {
		return nil, fmt.Errorf("server: restoring raft state: %w", err)
	}
	node.Start()

	if err := waitForLeadership(node, startupTimeout); err != nil {
		return nil, fmt.Errorf("server: %w", err)
	}

	if _, _, ok := node.Propose(raft.Command{}); !ok {
		return nil, fmt.Errorf("server: startup barrier command was rejected (not leader?)")
	}
	if err := waitForFullyApplied(node, startupTimeout); err != nil {
		return nil, fmt.Errorf("server: %w", err)
	}

	return &Server{store: store, raft: node, persister: persister}, nil
}

// Close releases this server's resources: stops the raft node's
// background goroutines and closes its persistence file handle.
func (s *Server) Close() error {
	s.raft.Stop()
	return s.persister.Close()
}

// Put durably, replicatedly writes key=value. It does not return until
// the write has actually committed - see propose() for why.
func (s *Server) Put(key, value string) error {
	return s.propose(raft.Command{Op: raft.OpPut, Key: key, Value: value})
}

// Delete durably, replicatedly removes key.
func (s *Server) Delete(key string) error {
	return s.propose(raft.Command{Op: raft.OpDelete, Key: key})
}

// Get returns the current value for key, read from the local KV store
// that raft.Node's apply loop keeps in sync with the committed log.
func (s *Server) Get(key string) (string, error) {
	return s.store.Get(key)
}

// KeyCount returns the number of keys currently stored.
func (s *Server) KeyCount() int {
	return s.store.Len()
}

// RaftStatus returns a snapshot of this node's underlying Raft state -
// term, role, commit index, and so on - for the `hivectl status` command.
func (s *Server) RaftStatus() raft.Status {
	return s.raft.Status()
}

// propose submits cmd through Raft and BLOCKS until it is not just
// committed but actually APPLIED - i.e. until Get would see it - rather
// than returning as soon as the leader merely accepts or commits it.
//
// Why block on APPLIED and not just committed: "committed" means a
// majority durably has the entry and it can never be lost - but the
// apply loop (apply.go) that copies committed entries into the actual
// kv.Store runs on its own timer (every applyTickInterval), separately
// from commit. Returning success as soon as an entry commits, without
// waiting for it to also be applied, would let a caller's immediate
// Get() race the apply loop and see "not found" for a write Put just
// reported as successful - which is exactly what happened before this
// fix (see the test failures that caught it).
func (s *Server) propose(cmd raft.Command) error {
	index, _, isLeader := s.raft.Propose(cmd)
	if !isLeader {
		// TODO(multi-node phase): once this can be a real multi-node
		// cluster, a non-leader should redirect the client to the
		// current leader instead of just failing outright.
		return fmt.Errorf("server: this node is not currently the leader")
	}

	deadline := time.Now().Add(commitWaitTimeout)
	for time.Now().Before(deadline) {
		if s.raft.Status().CommitIndex >= index {
			return nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return fmt.Errorf("server: command was proposed but was not applied within %v", commitWaitTimeout)
}

// waitForLeadership blocks until n reports itself as Leader, or
// timeout elapses.
func waitForLeadership(n *raft.Node, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if n.Status().State == raft.Leader {
			return nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return fmt.Errorf("node did not become leader within %v", timeout)
}

// waitForFullyApplied blocks until n's apply loop has caught
// LastApplied up to CommitIndex - i.e. every committed entry has
// actually reached the KV store - or timeout elapses.
func waitForFullyApplied(n *raft.Node, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		st := n.Status()
		if st.LastApplied >= st.CommitIndex {
			return nil
		}
		time.Sleep(2 * time.Millisecond)
	}
	return fmt.Errorf("node did not finish applying committed entries within %v", timeout)
}
