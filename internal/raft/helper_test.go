package raft

import (
	"testing"
	"time"
)

// waitFor polls cond every 10ms until it returns true, failing the test
// with desc if timeout elapses first.
func waitFor(t *testing.T, timeout time.Duration, desc string, cond func() bool) {
	t.Helper()

	deadline := time.After(timeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if cond() {
				return
			}
		case <-deadline:
			t.Fatalf("timed out after %v waiting for: %s", timeout, desc)
		}
	}
}

// waitForLeaderExcluding waits until exactly one node other than
// `exclude` is a Leader and returns it. Needed after a partition,
// because the isolated old leader still believes it is the leader, so
// counting every node would see two.
func waitForLeaderExcluding(t *testing.T, nodes map[PeerID]*Node, exclude PeerID, timeout time.Duration) PeerID {
	t.Helper()

	var found PeerID
	waitFor(t, timeout, "a single leader other than "+string(exclude), func() bool {
		var ls []PeerID
		for id, n := range nodes {
			if id != exclude && n.Status().State == Leader {
				ls = append(ls, id)
			}
		}
		if len(ls) == 1 {
			found = ls[0]
			return true
		}
		return false
	})
	return found
}

// followersOf returns every node ID except the leader's.
func followersOf(nodes map[PeerID]*Node, leader PeerID) []PeerID {
	var out []PeerID
	for id := range nodes {
		if id != leader {
			out = append(out, id)
		}
	}
	return out
}

// logCommands returns the commands in a node's log, in order.
func logCommands(n *Node) []string {
	n.mu.Lock()
	defer n.mu.Unlock()

	var cmds []string
	for i := 1; i <= n.Log.LastIndex(); i++ {
		e, _ := n.Log.Get(i)
		cmds = append(cmds, e.Command)
	}
	return cmds
}
