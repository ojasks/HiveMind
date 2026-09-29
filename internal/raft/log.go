package raft

// LogEntry is a single entry in the replicated Raft log. This mirrors
// Figure 2 of the Raft paper: every entry carries the term in which it
// was created by the leader, plus the command itself.
type LogEntry struct {
	Index   int     // position in the log, 1-indexed
	Term    int     // term when entry was created by the leader
	Command Command // the write operation this entry represents
}

// Log is the in-memory representation of a node's replicated log.
// Persistence to disk (see persist.go) and conflict-resolution
// truncation (TruncateFrom, used from HandleAppendEntries) are both
// implemented; log compaction/snapshots are not yet.
type Log struct {
	entries []LogEntry
}

// NewLog returns an empty log. Raft log indices start at 1, so entries[0]
// is reserved as a sentinel and never returned by public methods.
func NewLog() *Log {
	return &Log{
		entries: make([]LogEntry, 1), // index 0 is unused sentinel
	}
}

// LastIndex returns the index of the last entry in the log, or 0 if empty.
func (l *Log) LastIndex() int {
	return len(l.entries) - 1
}

// LastTerm returns the term of the last entry in the log, or 0 if empty.
func (l *Log) LastTerm() int {
	if l.LastIndex() == 0 {
		return 0
	}
	return l.entries[l.LastIndex()].Term
}

// Append adds a new entry to the end of the log and returns its index.
//
// TODO(phase 6): real AppendEntries handling needs to detect and resolve
// conflicts (truncate the suffix of the log when an existing entry
// conflicts with a new one at the same index) per the Raft log matching
// property. This method intentionally does not do that yet.
func (l *Log) Append(term int, command Command) LogEntry {
	entry := LogEntry{
		Index:   l.LastIndex() + 1,
		Term:    term,
		Command: command,
	}
	l.entries = append(l.entries, entry)
	return entry
}

// Get returns the entry at index, and whether it exists.
func (l *Log) Get(index int) (LogEntry, bool) {
	if index <= 0 || index > l.LastIndex() {
		return LogEntry{}, false
	}
	return l.entries[index], true
}

// TermAt returns the term of the entry at index. Index 0 is the
// sentinel "position before the first entry" and has term 0, which is
// what lets the very first AppendEntries (PrevLogIndex 0) pass the
// log-matching check. ok is false if index is negative or past the end.
func (l *Log) TermAt(index int) (term int, ok bool) {
	if index < 0 || index > l.LastIndex() {
		return 0, false
	}
	return l.entries[index].Term, true
}

// TruncateFrom deletes the entry at index and everything after it. Used
// by a follower when a leader's entry conflicts with what it already has.
func (l *Log) TruncateFrom(index int) {
	if index < 1 || index > l.LastIndex() {
		return
	}
	l.entries = l.entries[:index]
}

// AppendAll appends entries received from a leader to the end of the
// log. Each entry's Index is re-stamped from its actual position so the
// invariant "entries[i].Index == i" can never be broken by a bad RPC.
func (l *Log) AppendAll(entries []LogEntry) {
	for _, e := range entries {
		e.Index = len(l.entries)
		l.entries = append(l.entries, e)
	}
}

// RestoreLog builds a Log from previously persisted entries (which do
// not include the index-0 sentinel - NewLog adds that back).
func RestoreLog(entries []LogEntry) *Log {
	l := NewLog()
	l.entries = append(l.entries, entries...)
	return l
}

// AllEntries returns a copy of every real entry in the log (excluding
// the index-0 sentinel) - the exact shape RewriteLog persists to disk.
func (l *Log) AllEntries() []LogEntry {
	out := make([]LogEntry, l.LastIndex())
	copy(out, l.entries[1:])
	return out
}

// Slice returns a copy of the entries from index `from` through the
// end of the log (nil if there are none). Used by the leader to build
// the Entries field of an AppendEntries RPC.
func (l *Log) Slice(from int) []LogEntry {
	if from < 1 {
		from = 1
	}
	if from > l.LastIndex() {
		return nil
	}
	out := make([]LogEntry, l.LastIndex()-from+1)
	copy(out, l.entries[from:])
	return out
}
