package raft

import (
	"math/rand"
	"time"
)

// This file is the node's lifecycle and its election clock: how long a
// Follower waits without hearing from a leader before it decides to
// start an election.

const (
	// electionTimeoutMin/Max bound the randomized election timeout.
	// Randomization matters: if every node used the same fixed timeout,
	// a leader failure would make all followers become candidates in
	// the same instant, split the vote every time, and never converge.
	electionTimeoutMin = 150 * time.Millisecond
	electionTimeoutMax = 300 * time.Millisecond

	// heartbeatInterval is how often a Leader pings followers. It must
	// be comfortably shorter than electionTimeoutMin or followers will
	// time out between heartbeats and trigger needless elections.
	heartbeatInterval = 50 * time.Millisecond
)

// Start registers this node's RPC handlers on its Network and launches
// its background goroutines: the election timer and the apply loop
// (see apply.go). Call this once per node after construction, and
// after setting n.Store if you want committed entries applied to a
// kv.Store.
func (n *Node) Start() {
	// decodeArg (rpcwire.go) is what lets these SAME two closures serve
	// requests whether args arrives as an already-typed Go value
	// (transport.FakeNetwork, in-process) or as raw JSON bytes off a
	// real connection (transport.RPCNetwork) - see its doc comment.
	n.Network.Register(string(n.ID), "Raft.RequestVote", func(args any) (any, error) {
		a, err := decodeArg[RequestVoteArgs](args)
		if err != nil {
			return nil, err
		}
		return n.HandleRequestVote(a), nil
	})
	n.Network.Register(string(n.ID), "Raft.AppendEntries", func(args any) (any, error) {
		a, err := decodeArg[AppendEntriesArgs](args)
		if err != nil {
			return nil, err
		}
		return n.HandleAppendEntries(a), nil
	})

	go n.runElectionTimer()
	go n.runApplyLoop()
}

// Stop shuts down this node's background goroutines (election timer,
// and its heartbeat loop if it's currently a Leader).
func (n *Node) Stop() {
	close(n.stopCh)
}

// resetElectionTimer signals the election timer loop to restart its
// countdown. Safe to call from any goroutine, including while holding
// n.mu - it never blocks (the channel send is best-effort: if a reset
// is already pending, a second one is redundant).
func (n *Node) resetElectionTimer() {
	select {
	case n.resetCh <- struct{}{}:
	default:
	}
}

func randomElectionTimeout() time.Duration {
	span := electionTimeoutMax - electionTimeoutMin
	return electionTimeoutMin + time.Duration(rand.Int63n(int64(span)))
}

// runElectionTimer is "if we haven't heard from a leader (or granted a
// vote) recently, become a candidate and start an election." It runs
// for the lifetime of the node, one iteration per timeout period.
func (n *Node) runElectionTimer() {
	for {
		timeout := randomElectionTimeout()
		select {
		case <-time.After(timeout):
			n.mu.Lock()
			isLeader := n.State == Leader
			n.mu.Unlock()
			if !isLeader {
				n.StartElection()
			}
			// Leaders don't run elections against themselves; their
			// own heartbeat loop (see heartbeat.go) is what keeps
			// followers from timing out in the first place.
		case <-n.resetCh:
			// Heard from a leader, or just granted a vote - restart
			// the countdown from zero.
		case <-n.stopCh:
			return
		}
	}
}
