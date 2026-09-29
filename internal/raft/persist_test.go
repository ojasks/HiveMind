package raft

import "testing"

// Direct test of FilePersister: save, append, and rewrite all round-
// trip correctly through Load.
func TestPersist_FilePersisterRoundTrip(t *testing.T) {
	dir := t.TempDir()
	p, err := NewFilePersister(dir)
	if err != nil {
		t.Fatalf("NewFilePersister: %v", err)
	}

	if err := p.SaveTermAndVote(3, "node2"); err != nil {
		t.Fatalf("SaveTermAndVote: %v", err)
	}

	e1 := LogEntry{Index: 1, Term: 1, Command: putCmd("a", "1")}
	e2 := LogEntry{Index: 2, Term: 1, Command: putCmd("b", "2")}
	if err := p.AppendEntry(e1); err != nil {
		t.Fatalf("AppendEntry e1: %v", err)
	}
	if err := p.AppendEntry(e2); err != nil {
		t.Fatalf("AppendEntry e2: %v", err)
	}

	term, votedFor, entries, err := p.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if term != 3 || votedFor != "node2" {
		t.Fatalf("expected term=3 votedFor=node2, got term=%d votedFor=%q", term, votedFor)
	}
	if len(entries) != 2 || entries[0] != e1 || entries[1] != e2 {
		t.Fatalf("unexpected entries after append: %+v", entries)
	}

	// Simulate a follower resolving a conflict: rewrite with e1 kept,
	// but a different entry at index 2.
	e2b := LogEntry{Index: 2, Term: 2, Command: putCmd("b", "999")}
	if err := p.RewriteLog([]LogEntry{e1, e2b}); err != nil {
		t.Fatalf("RewriteLog: %v", err)
	}

	_, _, entries2, err := p.Load()
	if err != nil {
		t.Fatalf("Load after rewrite: %v", err)
	}
	if len(entries2) != 2 || entries2[0] != e1 || entries2[1] != e2b {
		t.Fatalf("expected rewritten log [e1, e2b], got %+v", entries2)
	}
}

// The scenario that actually matters: a node's term, vote, and log
// survive it being destroyed and a fresh Node reading the same
// directory - simulating a process crash and restart.
func TestPersist_NodeRestoresStateAfterRestart(t *testing.T) {
	dir := t.TempDir()

	persister, err := NewFilePersister(dir)
	if err != nil {
		t.Fatalf("NewFilePersister: %v", err)
	}

	// "First run": grant a vote, then (as if leader) accept two writes
	// directly into the log - bypassing Start()/Propose, since this
	// test is only about persistence, not networking.
	n1 := NewNode("node1", nil, nil)
	n1.Persister = persister
	if err := n1.Restore(); err != nil {
		t.Fatalf("Restore (empty): %v", err)
	}

	reply := n1.HandleRequestVote(RequestVoteArgs{Term: 5, CandidateID: "node9"})
	if !reply.VoteGranted {
		t.Fatalf("expected vote granted, got %+v", reply)
	}

	n1.mu.Lock()
	n1.State = Leader
	e1 := n1.Log.Append(n1.CurrentTerm, putCmd("x", "1"))
	n1.persistNewEntryLocked(e1)
	e2 := n1.Log.Append(n1.CurrentTerm, putCmd("y", "2"))
	n1.persistNewEntryLocked(e2)
	n1.mu.Unlock()

	if err := persister.Close(); err != nil {
		t.Fatalf("closing persister: %v", err)
	}

	// "Restart": a brand new Node and a brand new Persister pointed at
	// the same directory.
	persister2, err := NewFilePersister(dir)
	if err != nil {
		t.Fatalf("NewFilePersister (reopen): %v", err)
	}
	n2 := NewNode("node1", nil, nil)
	n2.Persister = persister2
	if err := n2.Restore(); err != nil {
		t.Fatalf("Restore (after restart): %v", err)
	}

	st := n2.Status()
	if st.CurrentTerm != 5 {
		t.Fatalf("expected restored term 5, got %d", st.CurrentTerm)
	}
	if st.VotedFor != "node9" {
		t.Fatalf("expected restored vote for node9, got %q", st.VotedFor)
	}
	if st.LogLength != 2 {
		t.Fatalf("expected 2 restored log entries, got %d", st.LogLength)
	}
	got1, _ := n2.Log.Get(1)
	got2, _ := n2.Log.Get(2)
	if got1.Command != putCmd("x", "1") || got2.Command != putCmd("y", "2") {
		t.Fatalf("restored entries don't match: %+v, %+v", got1, got2)
	}
}
