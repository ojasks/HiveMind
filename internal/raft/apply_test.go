package raft

import (
	"testing"
	"time"

	"hivemind/internal/kv"
	"hivemind/internal/transport"
)

// newTestClusterWithStores is like newTestCluster but also gives every
// node its own kv.Store, wired in before Start() so the apply loop has
// somewhere to write from its very first tick.
func newTestClusterWithStores(t *testing.T, ids []PeerID) (*transport.FakeNetwork, map[PeerID]*Node, map[PeerID]*kv.Store) {
	t.Helper()

	net := transport.NewFakeNetwork()
	nodes := make(map[PeerID]*Node, len(ids))
	stores := make(map[PeerID]*kv.Store, len(ids))

	for _, id := range ids {
		var peers []PeerID
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		n := NewNode(id, peers, net)
		s := kv.NewStore()
		n.Store = s
		nodes[id] = n
		stores[id] = s
	}

	for _, n := range nodes {
		n.Start()
	}

	t.Cleanup(func() {
		for _, n := range nodes {
			n.Stop()
		}
	})

	return net, nodes, stores
}

// Several commands proposed on the leader - including a delete and an
// overwrite - end up applied, in order, to every node's KV store.
func TestApply_CommittedCommandsReachAllStores(t *testing.T) {
	ids := []PeerID{"node1", "node2", "node3"}
	_, nodes, stores := newTestClusterWithStores(t, ids)

	leader := waitForSingleLeader(t, nodes, 2*time.Second)

	commands := []Command{
		putCmd("name", "Ojas"),
		putCmd("age", "22"),
		{Op: OpDelete, Key: "age"},
		putCmd("age", "23"), // overwrite after the delete
	}
	for _, cmd := range commands {
		if _, _, ok := nodes[leader].Propose(cmd); !ok {
			t.Fatalf("Propose(%+v) on leader %s was rejected", cmd, leader)
		}
	}

	waitFor(t, 2*time.Second, "all stores to converge on the final state", func() bool {
		for _, s := range stores {
			if name, err := s.Get("name"); err != nil || name != "Ojas" {
				return false
			}
			if age, err := s.Get("age"); err != nil || age != "23" {
				return false
			}
		}
		return true
	})
}

// A follower partitioned during the writes still ends up with identical
// state once healed and caught up - proving replication AND application
// both survive a temporarily missing node.
func TestApply_PartitionedFollowerConvergesAfterHeal(t *testing.T) {
	ids := []PeerID{"node1", "node2", "node3"}
	net, nodes, stores := newTestClusterWithStores(t, ids)

	leader := waitForSingleLeader(t, nodes, 2*time.Second)
	lagging := followersOf(nodes, leader)[0]

	net.Partition(string(lagging))

	commands := []Command{putCmd("x", "1"), putCmd("y", "2")}
	for _, cmd := range commands {
		if _, _, ok := nodes[leader].Propose(cmd); !ok {
			t.Fatalf("Propose(%+v) on leader %s was rejected", cmd, leader)
		}
	}

	waitFor(t, 2*time.Second, "leader's store to reflect both writes", func() bool {
		x, errX := stores[leader].Get("x")
		y, errY := stores[leader].Get("y")
		return errX == nil && x == "1" && errY == nil && y == "2"
	})

	net.Heal(string(lagging))

	waitFor(t, 5*time.Second, "partitioned follower's store to catch up", func() bool {
		x, errX := stores[lagging].Get("x")
		y, errY := stores[lagging].Get("y")
		return errX == nil && x == "1" && errY == nil && y == "2"
	})
}
