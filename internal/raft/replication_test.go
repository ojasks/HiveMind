package raft

import (
	"testing"
	"time"
)

// A command proposed on the leader ends up in every node's log and is
// committed everywhere.
func TestReplication_ProposeReplicatesToAllNodes(t *testing.T) {
	ids := []PeerID{"node1", "node2", "node3"}
	_, nodes := newTestCluster(t, ids)

	leader := waitForSingleLeader(t, nodes, 2*time.Second)

	idx, _, ok := nodes[leader].Propose(putCmd("x", "10"))
	if !ok {
		t.Fatalf("Propose on leader %s was rejected", leader)
	}
	if idx != 1 {
		t.Fatalf("expected first proposal at index 1, got %d", idx)
	}

	waitFor(t, 2*time.Second, "all nodes to log and commit 'PUT x 10'", func() bool {
		for _, n := range nodes {
			cmds := logCommands(n)
			if len(cmds) != 1 || cmds[0] != putCmd("x", "10") || n.Status().CommitIndex != 1 {
				return false
			}
		}
		return true
	})
}

// Only the leader accepts proposals.
func TestReplication_FollowerRejectsPropose(t *testing.T) {
	ids := []PeerID{"node1", "node2", "node3"}
	_, nodes := newTestCluster(t, ids)

	leader := waitForSingleLeader(t, nodes, 2*time.Second)
	follower := followersOf(nodes, leader)[0]

	if _, _, ok := nodes[follower].Propose(putCmd("x", "10")); ok {
		t.Fatalf("follower %s accepted a proposal", follower)
	}
}

// With one follower down, the leader plus the other follower are still
// a majority (2 of 3), so writes commit. When the downed follower
// returns, the leader backs up nextIndex until the logs line up and
// fills in everything it missed.
func TestReplication_CommitsWithOneFollowerDownAndCatchesUp(t *testing.T) {
	ids := []PeerID{"node1", "node2", "node3"}
	net, nodes := newTestCluster(t, ids)

	leader := waitForSingleLeader(t, nodes, 2*time.Second)
	lagging := followersOf(nodes, leader)[0]

	net.Partition(string(lagging))

	for _, cmd := range []Command{putCmd("a", "1"), putCmd("b", "2")} {
		if _, _, ok := nodes[leader].Propose(cmd); !ok {
			t.Fatalf("Propose(%+v) on leader %s was rejected", cmd, leader)
		}
	}

	waitFor(t, 2*time.Second, "leader to commit both entries with one follower down", func() bool {
		return nodes[leader].Status().CommitIndex == 2
	})
	if got := len(logCommands(nodes[lagging])); got != 0 {
		t.Fatalf("partitioned follower should have no entries, has %d", got)
	}

	net.Heal(string(lagging))

	// Leadership may change here (the returning node's inflated term can
	// force a new election), but any leader has both entries and will
	// bring the lagging node up to date.
	waitFor(t, 5*time.Second, "lagging follower to catch up", func() bool {
		cmds := logCommands(nodes[lagging])
		return len(cmds) == 2 && cmds[0] == putCmd("a", "1") && cmds[1] == putCmd("b", "2")
	})
}

// A leader that cannot reach a majority accepts writes into its log
// but must never commit them.
func TestReplication_LeaderWithoutMajorityCannotCommit(t *testing.T) {
	ids := []PeerID{"node1", "node2", "node3"}
	net, nodes := newTestCluster(t, ids)

	leader := waitForSingleLeader(t, nodes, 2*time.Second)
	for _, f := range followersOf(nodes, leader) {
		net.Partition(string(f))
	}

	if _, _, ok := nodes[leader].Propose(putCmd("x", "1")); !ok {
		t.Fatalf("Propose on leader %s was rejected", leader)
	}

	time.Sleep(500 * time.Millisecond) // roughly ten heartbeat rounds

	st := nodes[leader].Status()
	if st.CommitIndex != 0 {
		t.Fatalf("leader committed without a majority: CommitIndex = %d", st.CommitIndex)
	}
	if st.LogLength != 1 {
		t.Fatalf("leader should still hold the uncommitted entry, LogLength = %d", st.LogLength)
	}
}

// The core safety scenario. An isolated old leader accepts a write that
// can never commit. Meanwhile the majority elects a new leader and
// commits a different write at the same log index. After the heal, the
// old leader's conflicting entry must be deleted and replaced.
func TestReplication_DivergentLogIsOverwrittenAfterHeal(t *testing.T) {
	ids := []PeerID{"node1", "node2", "node3"}
	net, nodes := newTestCluster(t, ids)

	oldLeader := waitForSingleLeader(t, nodes, 2*time.Second)
	net.Partition(string(oldLeader))

	// Still believing it is leader, the isolated node accepts a write.
	if _, _, ok := nodes[oldLeader].Propose(putCmd("stale", "stale")); !ok {
		t.Fatalf("isolated old leader %s rejected a proposal", oldLeader)
	}

	newLeader := waitForLeaderExcluding(t, nodes, oldLeader, 2*time.Second)
	if _, _, ok := nodes[newLeader].Propose(putCmd("fresh", "fresh")); !ok {
		t.Fatalf("Propose on new leader %s was rejected", newLeader)
	}
	waitFor(t, 2*time.Second, "new leader to commit FRESH", func() bool {
		return nodes[newLeader].Status().CommitIndex == 1
	})

	net.Heal(string(oldLeader))

	waitFor(t, 5*time.Second, "old leader to step down and replace STALE with FRESH", func() bool {
		cmds := logCommands(nodes[oldLeader])
		return nodes[oldLeader].Status().State == Follower && len(cmds) == 1 && cmds[0] == putCmd("fresh", "fresh")
	})
}
