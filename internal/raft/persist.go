package raft

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// This file is Raft's safety net against crashes: CurrentTerm,
// VotedFor, and the Log must survive a process restart, or Raft's
// safety guarantees don't actually hold. Concretely: if a node forgets
// it already voted in term 5 and a restart resets it to "haven't
// voted," it can grant a second, conflicting vote in term 5 - two
// nodes could then both believe they won the same election. Persisting
// before a node acts on a change is what closes that gap.

// Persister is how a Node's CurrentTerm/VotedFor/Log reach stable
// storage. FilePersister below is a real, working implementation;
// tests that only care about in-memory consensus can simply leave a
// Node's Persister field nil (see Node.Restore and the persist*Locked
// helpers, which all no-op when Persister is nil).
type Persister interface {
	// SaveTermAndVote durably records CurrentTerm and VotedFor.
	SaveTermAndVote(term int, votedFor PeerID) error
	// AppendEntry durably records one freshly-created log entry.
	AppendEntry(e LogEntry) error
	// RewriteLog durably replaces the ENTIRE on-disk log with entries.
	// Used after a follower truncates a conflicting suffix, where
	// rewriting from scratch is simpler and safer than trying to
	// truncate an append-only file in place.
	RewriteLog(entries []LogEntry) error
	// Load returns whatever is currently on disk: zero values if
	// nothing has ever been saved.
	Load() (term int, votedFor PeerID, entries []LogEntry, err error)
}

// persistedState is the on-disk shape of the small state file.
type persistedState struct {
	Term     int    `json:"term"`
	VotedFor PeerID `json:"votedFor"`
}

// FilePersister is a Persister backed by two files in a directory:
//   - raft-state.json: CurrentTerm + VotedFor, rewritten atomically
//     (write to a temp file, fsync, rename) every time either changes.
//     Small and rewritten often, so a full atomic overwrite is cheap.
//   - raft-log.jsonl: one JSON object per line, mirroring the style of
//     internal/wal - appended to on the fast path (a new entry), and
//     fully rewritten only on the rarer conflict-resolution path.
type FilePersister struct {
	mu        sync.Mutex
	stateFile string
	logFile   string
	log       *os.File // kept open in append mode
}

// NewFilePersister creates dir if needed and opens (creating if
// necessary) the files inside it. Call Load() afterward to pick up
// whatever a previous run left behind.
func NewFilePersister(dir string) (*FilePersister, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("raft: creating persist dir %s: %w", dir, err)
	}
	logPath := filepath.Join(dir, "raft-log.jsonl")
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("raft: opening %s: %w", logPath, err)
	}
	return &FilePersister{
		stateFile: filepath.Join(dir, "raft-state.json"),
		logFile:   logPath,
		log:       f,
	}, nil
}

// SaveTermAndVote atomically overwrites the state file: write to a
// temp file, fsync, then rename over the original. A crash mid-write
// can only ever leave the OLD file intact or the NEW file fully
// written - never a half-written, corrupt one.
func (p *FilePersister) SaveTermAndVote(term int, votedFor PeerID) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := json.Marshal(persistedState{Term: term, VotedFor: votedFor})
	if err != nil {
		return fmt.Errorf("raft: marshaling state: %w", err)
	}

	tmp := p.stateFile + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("raft: opening temp state file: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("raft: writing temp state file: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("raft: fsyncing temp state file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("raft: closing temp state file: %w", err)
	}
	if err := os.Rename(tmp, p.stateFile); err != nil {
		return fmt.Errorf("raft: renaming state file: %w", err)
	}
	return nil
}

// AppendEntry appends one entry and fsyncs before returning, so a
// successful AppendEntry means the entry is durable even if the
// process is killed immediately after.
func (p *FilePersister) AppendEntry(e LogEntry) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	line, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("raft: marshaling log entry: %w", err)
	}
	line = append(line, '\n')
	if _, err := p.log.Write(line); err != nil {
		return fmt.Errorf("raft: writing log entry: %w", err)
	}
	return p.log.Sync()
}

// RewriteLog atomically replaces the entire on-disk log with entries.
//
// TODO: this is O(log length) rather than O(1). A production system
// would keep the log in numbered segment files and only touch the
// affected segment; left as a full rewrite here since this project's
// logs are small and correctness matters far more than speed for a
// first implementation.
func (p *FilePersister) RewriteLog(entries []LogEntry) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	tmp := p.logFile + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("raft: opening temp log file: %w", err)
	}
	for _, e := range entries {
		line, err := json.Marshal(e)
		if err != nil {
			f.Close()
			return fmt.Errorf("raft: marshaling entry %d: %w", e.Index, err)
		}
		line = append(line, '\n')
		if _, err := f.Write(line); err != nil {
			f.Close()
			return fmt.Errorf("raft: writing entry %d: %w", e.Index, err)
		}
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("raft: fsyncing temp log file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("raft: closing temp log file: %w", err)
	}

	// The live handle still points at the old (soon to be unlinked)
	// file, so close it, rename, then reopen against the new file.
	if err := p.log.Close(); err != nil {
		return fmt.Errorf("raft: closing old log handle: %w", err)
	}
	if err := os.Rename(tmp, p.logFile); err != nil {
		return fmt.Errorf("raft: renaming log file: %w", err)
	}
	f2, err := os.OpenFile(p.logFile, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("raft: reopening log file: %w", err)
	}
	p.log = f2
	return nil
}

// Load reads whatever is currently on disk: zero-value term/vote if
// the state file doesn't exist yet, and every log entry in order.
func (p *FilePersister) Load() (term int, votedFor PeerID, entries []LogEntry, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if data, readErr := os.ReadFile(p.stateFile); readErr == nil {
		var st persistedState
		if err := json.Unmarshal(data, &st); err != nil {
			return 0, "", nil, fmt.Errorf("raft: decoding state file: %w", err)
		}
		term, votedFor = st.Term, st.VotedFor
	} else if !os.IsNotExist(readErr) {
		return 0, "", nil, fmt.Errorf("raft: reading state file: %w", readErr)
	}

	if _, err := p.log.Seek(0, 0); err != nil {
		return 0, "", nil, fmt.Errorf("raft: seeking log file: %w", err)
	}
	scanner := bufio.NewScanner(p.log)
	for scanner.Scan() {
		var e LogEntry
		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			return 0, "", nil, fmt.Errorf("raft: decoding log entry: %w", err)
		}
		entries = append(entries, e)
	}
	if err := scanner.Err(); err != nil {
		return 0, "", nil, fmt.Errorf("raft: scanning log file: %w", err)
	}
	if _, err := p.log.Seek(0, 2); err != nil {
		return 0, "", nil, fmt.Errorf("raft: seeking to end of log file: %w", err)
	}

	return term, votedFor, entries, nil
}

// Close releases the persister's open file handle.
func (p *FilePersister) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.log.Close()
}

// --- Node-side helpers: where persistence actually gets triggered ---
//
// Each of these is a no-op if n.Persister is nil, so existing
// consensus-only tests (which never set a Persister) are unaffected.
// Every call site is chosen to match the Raft paper's rule: persist
// BEFORE acting on the change - replying to an RPC, or asking peers
// for votes as a new candidate.

// persistTermAndVoteLocked writes CurrentTerm/VotedFor to disk. Caller
// must hold n.mu.
func (n *Node) persistTermAndVoteLocked() {
	if n.Persister == nil {
		return
	}
	if err := n.Persister.SaveTermAndVote(n.CurrentTerm, n.VotedFor); err != nil {
		// TODO: a production node should treat a failed persist as
		// fatal - it can no longer safely make promises to the
		// cluster - rather than silently continuing. Logged instead
		// of ignored here so a real failure is at least visible.
		fmt.Fprintf(os.Stderr, "raft: %s: failed to persist term/vote: %v\n", n.ID, err)
	}
}

// persistNewEntryLocked durably appends one freshly-created entry.
// Caller must hold n.mu.
func (n *Node) persistNewEntryLocked(e LogEntry) {
	if n.Persister == nil {
		return
	}
	if err := n.Persister.AppendEntry(e); err != nil {
		fmt.Fprintf(os.Stderr, "raft: %s: failed to persist log entry %d: %v\n", n.ID, e.Index, err)
	}
}

// persistLogRewriteLocked rewrites the entire on-disk log to match
// n.Log. Used after a follower's log is truncated and/or extended in
// HandleAppendEntries. Caller must hold n.mu.
func (n *Node) persistLogRewriteLocked() {
	if n.Persister == nil {
		return
	}
	if err := n.Persister.RewriteLog(n.Log.AllEntries()); err != nil {
		fmt.Fprintf(os.Stderr, "raft: %s: failed to persist rewritten log: %v\n", n.ID, err)
	}
}

// Restore loads previously persisted CurrentTerm, VotedFor and Log
// from n.Persister, if one is set. Call this once, before Start(), on
// a node that might be recovering from a crash or restart. If
// Persister is nil (the default), Restore is a no-op and the node
// starts fresh, exactly as before.
func (n *Node) Restore() error {
	if n.Persister == nil {
		return nil
	}
	term, votedFor, entries, err := n.Persister.Load()
	if err != nil {
		return fmt.Errorf("raft: %s: restoring: %w", n.ID, err)
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	n.CurrentTerm = term
	n.VotedFor = votedFor
	if len(entries) > 0 {
		n.Log = RestoreLog(entries)
	}
	return nil
}
