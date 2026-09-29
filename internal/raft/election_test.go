package raft

import (
	"testing"
	"time"

	"hivemind/internal/transport"
)

// newTestCluster wires up one Node per id, all sharing a single
// FakeNetwork, and starts each node's election timer. It registers a
// t.Cleanup so every node's goroutines are stopped when the test ends,
// even on failure.
func newTestCluster(t *testing.T, ids []PeerID) (*transport.FakeNetwork, map[PeerID]*Node) {
	t.Helper()

	net := transport.NewFakeNetwork()
	nodes := make(map[PeerID]*Node, len(ids))

	for _, id := range ids {
		var peers []PeerID
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		nodes[id] = NewNode(id, peers, net)
	}

	for _, n := range nodes {
		n.Start()
	}

	t.Cleanup(func() {
		for _, n := range nodes {
			n.Stop()
		}
	})

	return net, nodes
}

// leaders returns the IDs of every node currently claiming to be Leader.
func leaders(nodes map[PeerID]*Node) []PeerID {
	var result []PeerID
	for id, n := range nodes {
		if n.Status().State == Leader {
			result = append(result, id)
		}
	}
	return result
}

// waitForSingleLeader polls the cluster until exactly one leader
// exists, or fails the test after timeout.
func waitForSingleLeader(t *testing.T, nodes map[PeerID]*Node, timeout time.Duration) PeerID {
	t.Helper()

	deadline := time.After(timeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if ls := leaders(nodes); len(ls) == 1 {
				return ls[0]
			}
		case <-deadline:
			t.Fatalf("no single leader elected within %v (current leaders: %v)", timeout, leaders(nodes))
			return ""
		}
	}
}

// TestLeaderElection_SingleLeaderElected is the first real correctness
// test for the project: three nodes, no faults, should converge on
// exactly one leader and stay there.
func TestLeaderElection_SingleLeaderElected(t *testing.T) {
	ids := []PeerID{"node1", "node2", "node3"}
	_, nodes := newTestCluster(t, ids)

	leader := waitForSingleLeader(t, nodes, 2*time.Second)
	t.Logf("elected leader: %s", leader)

	// Let the cluster run a bit longer and confirm the leader is
	// stable (no flapping between leaders).
	time.Sleep(150 * time.Millisecond)
	if ls := leaders(nodes); len(ls) != 1 || ls[0] != leader {
		t.Fatalf("expected leader to remain %s, got leaders: %v", leader, ls)
	}
}

// TestLeaderElection_ReelectsAfterPartition simulates killing the
// leader (via a network partition) and checks the remaining two nodes
// elect a new leader among themselves.
func TestLeaderElection_ReelectsAfterPartition(t *testing.T) {
	ids := []PeerID{"node1", "node2", "node3"}
	net, nodes := newTestCluster(t, ids)

	firstLeader := waitForSingleLeader(t, nodes, 2*time.Second)
	t.Logf("original leader: %s", firstLeader)

	net.Partition(string(firstLeader))
	t.Logf("partitioned %s - waiting for remaining nodes to re-elect", firstLeader)

	deadline := time.After(2 * time.Second)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			var newLeaders []PeerID
			for id, n := range nodes {
				if id != firstLeader && n.Status().State == Leader {
					newLeaders = append(newLeaders, id)
				}
			}
			if len(newLeaders) == 1 {
				t.Logf("new leader after partition: %s", newLeaders[0])
				return
			}
		case <-deadline:
			t.Fatalf("no new leader elected among remaining nodes within timeout")
		}
	}
}

// TestLeaderElection_OldLeaderStepsDownAfterHeal covers the other half
// of a partition: once the network heals, the old leader (which spent
// the partition still believing it was leader) must discover the newer
// term, step down, and leave the cluster with exactly one leader.
func TestLeaderElection_OldLeaderStepsDownAfterHeal(t *testing.T) {
	ids := []PeerID{"node1", "node2", "node3"}
	net, nodes := newTestCluster(t, ids)

	oldLeader := waitForSingleLeader(t, nodes, 2*time.Second)
	net.Partition(string(oldLeader))

	newLeader := waitForLeaderExcluding(t, nodes, oldLeader, 2*time.Second)
	newTerm := nodes[newLeader].Status().CurrentTerm
	t.Logf("old leader %s partitioned; new leader %s in term %d", oldLeader, newLeader, newTerm)

	// Cut off from everyone, the old leader cannot know it was replaced.
	if st := nodes[oldLeader].Status(); st.State != Leader {
		t.Fatalf("partitioned old leader should still believe it is Leader, got %s", st.State)
	}

	net.Heal(string(oldLeader))

	waitFor(t, 2*time.Second, "old leader to step down and adopt the new term", func() bool {
		st := nodes[oldLeader].Status()
		return st.State == Follower && st.CurrentTerm >= newTerm
	})

	if ls := leaders(nodes); len(ls) != 1 {
		t.Fatalf("expected exactly one leader after heal, got %v", ls)
	}
}
