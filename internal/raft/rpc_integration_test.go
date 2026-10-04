package raft

import (
	"testing"
	"time"

	"hivemind/internal/kv"
	"hivemind/internal/transport"
)

// This is the payoff test for real RPC: the exact same election and
// replication logic already proven against transport.FakeNetwork,
// running instead over actual TCP connections on loopback - as close
// as a single test process can get to "these are really separate
// reachable nodes" without spawning separate OS processes.
func TestRPC_ThreeNodeClusterOverRealTCP(t *testing.T) {
	ids := []PeerID{"node1", "node2", "node3"}

	// Each node gets its OWN RPCNetwork (unlike FakeNetwork, which was
	// one shared in-memory switchboard for the whole test cluster).
	networks := make(map[PeerID]*transport.RPCNetwork, len(ids))
	for _, id := range ids {
		net, err := transport.NewRPCNetwork(string(id), "127.0.0.1:0")
		if err != nil {
			t.Fatalf("NewRPCNetwork(%s): %v", id, err)
		}
		networks[id] = net
		t.Cleanup(func() { net.Close() })
	}

	// Every node needs to be told every OTHER node's real address
	// before it can send anything - bookkeeping that had no equivalent
	// with FakeNetwork's shared map.
	for _, id := range ids {
		for _, peer := range ids {
			if peer != id {
				networks[id].SetPeer(string(peer), networks[peer].Addr())
			}
		}
	}

	nodes := make(map[PeerID]*Node, len(ids))
	stores := make(map[PeerID]*kv.Store, len(ids))
	for _, id := range ids {
		var peers []PeerID
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		n := NewNode(id, peers, networks[id])
		s := kv.NewStore()
		n.Store = s
		nodes[id] = n
		stores[id] = s
	}

	for _, n := range nodes {
		n.Start()
		t.Cleanup(n.Stop)
	}

	leader := waitForSingleLeader(t, nodes, 3*time.Second)
	t.Logf("elected leader over real TCP: %s", leader)

	if _, _, ok := nodes[leader].Propose(putCmd("name", "Ojas")); !ok {
		t.Fatalf("Propose on leader %s was rejected", leader)
	}

	waitFor(t, 2*time.Second, "all stores to converge over real TCP", func() bool {
		for _, s := range stores {
			if v, err := s.Get("name"); err != nil || v != "Ojas" {
				return false
			}
		}
		return true
	})
}
