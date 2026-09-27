// Package transport provides the network layer Raft nodes use to talk to
// each other. The FakeNetwork implementation here is intentionally the
// FIRST transport to build against: it lets you test leader election and
// log replication with full, deterministic control over message
// delivery (drop, delay, duplicate) without any real sockets — no
// flakiness from actual timing, and no need for Docker just to run a
// unit test.
//
// Real RPC (e.g. over gRPC or net/rpc) should be a separate
// implementation of the same Network interface, swapped in only once
// Raft's correctness has been validated against FakeNetwork.
package transport

import (
	"fmt"
	"sync"
)

// Network is the interface raft.Node depends on to send RPCs to peers.
// Both FakeNetwork (for tests) and a future real RPC transport should
// implement this.
//
// TODO(phase 7): once this is wired into raft.Node, calls should be
// context-aware (context.Context) so RPCs can be cancelled/timed out.
type Network interface {
	// Send delivers an RPC named method with payload args to peer id,
	// and returns whatever handler is registered for that method on
	// the receiving node.
	Send(to string, method string, args any) (reply any, err error)
}

// Handler processes an incoming RPC on a given node.
type Handler func(args any) (reply any, err error)

// FakeNetwork simulates a cluster's network in-process. Each node
// registers handlers for the RPC methods it supports (e.g.
// "Raft.RequestVote", "Raft.AppendEntries"); other nodes call Send to
// reach them.
//
// Fault injection: set Drop[nodeID] = true to make all messages TO that
// node silently fail, simulating a crashed or partitioned node. This is
// deliberately simple; extend it (per-link partitions, delay
// distributions, message duplication) as failure-testing needs grow —
// see docs/architecture.md phase 11.
type FakeNetwork struct {
	mu       sync.Mutex
	handlers map[string]map[string]Handler // nodeID -> method -> handler
	Drop     map[string]bool               // nodeID -> drop all inbound messages
}

// NewFakeNetwork returns an empty simulated network.
func NewFakeNetwork() *FakeNetwork {
	return &FakeNetwork{
		handlers: make(map[string]map[string]Handler),
		Drop:     make(map[string]bool),
	}
}

// Register attaches a handler for method on the given node.
func (f *FakeNetwork) Register(nodeID, method string, h Handler) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.handlers[nodeID] == nil {
		f.handlers[nodeID] = make(map[string]Handler)
	}
	f.handlers[nodeID][method] = h
}

// Send simulates delivering an RPC to nodeID.
//
// TODO(phase 11): add configurable delay and message duplication here
// once basic election + replication work correctly, so failure tests
// can exercise "delayed but not lost" and "duplicated" message cases
// too, not just outright drops.
func (f *FakeNetwork) Send(to string, method string, args any) (any, error) {
	f.mu.Lock()
	dropped := f.Drop[to]
	handler, ok := f.handlers[to][method]
	f.mu.Unlock()

	if dropped {
		return nil, fmt.Errorf("transport: message to %s dropped (simulated partition)", to)
	}
	if !ok {
		return nil, fmt.Errorf("transport: no handler for %s.%s", to, method)
	}
	return handler(args)
}

// Partition marks nodeID as unreachable (all inbound messages dropped)
// until Heal is called. This is the building block for `hivectl
// partition` in the finished CLI.
func (f *FakeNetwork) Partition(nodeID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Drop[nodeID] = true
}

// Heal removes a previously simulated partition for nodeID.
func (f *FakeNetwork) Heal(nodeID string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Drop[nodeID] = false
}
