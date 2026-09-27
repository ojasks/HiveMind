// Package wal implements a simple write-ahead log.
//
// The WAL is what lets a node survive a crash without losing committed
// writes: every operation is appended (and fsync'd) to disk *before* it's
// considered durable. On startup, a node replays the WAL to reconstruct
// its state before rejoining the cluster.
//
// This is a real, working append-only log (JSON-lines on disk), not a
// stub — Put/Delete are functional today. What's still missing, and
// belongs in later phases, is:
//   - truncating/compacting the WAL once a snapshot covers its contents
//   - fsync policy tuning (currently fsyncs every write, which is safe
//     but slow — batching is a later optimization)
package wal

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
)

// OpType identifies the kind of operation recorded in a WAL entry.
type OpType string

const (
	OpPut    OpType = "PUT"
	OpDelete OpType = "DELETE"
)

// Entry is a single record in the write-ahead log.
//
// Index is a monotonically increasing sequence number. Once Raft is
// wired in, this will become the Raft log index/term instead of a
// locally-assigned counter.
type Entry struct {
	Index int    `json:"index"`
	Op    OpType `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// WAL is an append-only, crash-durable log backed by a single file.
type WAL struct {
	mu       sync.Mutex
	file     *os.File
	nextIdx  int
	path     string
}

// Open opens (creating if necessary) the WAL file at path and returns a
// ready-to-use WAL. It does NOT replay existing entries — call Replay
// explicitly so the caller controls exactly when/how entries are applied.
func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: opening %s: %w", path, err)
	}
	return &WAL{
		file:    f,
		nextIdx: 1,
		path:    path,
	}, nil
}

// Close closes the underlying file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

// Append writes a new entry to the log and fsyncs before returning, so
// that a successful Append means the entry is durable even if the
// process is killed immediately after.
func (w *WAL) Append(op OpType, key, value string) (Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	entry := Entry{
		Index: w.nextIdx,
		Op:    op,
		Key:   key,
		Value: value,
	}

	line, err := json.Marshal(entry)
	if err != nil {
		return Entry{}, fmt.Errorf("wal: marshaling entry: %w", err)
	}
	line = append(line, '\n')

	if _, err := w.file.Write(line); err != nil {
		return Entry{}, fmt.Errorf("wal: writing entry: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return Entry{}, fmt.Errorf("wal: fsyncing entry: %w", err)
	}

	w.nextIdx++
	return entry, nil
}

// Replay reads every entry currently in the WAL, in order, and calls
// apply for each one. Typical use on startup:
//
//	store := kv.NewStore()
//	w, _ := wal.Open("hivemind.wal")
//	w.Replay(func(e wal.Entry) {
//	    switch e.Op {
//	    case wal.OpPut:
//	        store.Put(e.Key, e.Value)
//	    case wal.OpDelete:
//	        store.Delete(e.Key)
//	    }
//	})
//
// TODO(snapshots): once snapshotting exists, Replay should start from
// the snapshot's index instead of from the beginning of the file.
func (w *WAL) Replay(apply func(Entry)) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if _, err := w.file.Seek(0, 0); err != nil {
		return fmt.Errorf("wal: seeking to start: %w", err)
	}

	scanner := bufio.NewScanner(w.file)
	maxIdx := 0
	for scanner.Scan() {
		var e Entry
		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			return fmt.Errorf("wal: decoding entry: %w", err)
		}
		apply(e)
		if e.Index > maxIdx {
			maxIdx = e.Index
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("wal: scanning file: %w", err)
	}

	// Resume appending after whatever we just replayed, and make sure
	// the file offset is back at the end for subsequent Appends.
	w.nextIdx = maxIdx + 1
	if _, err := w.file.Seek(0, 2); err != nil {
		return fmt.Errorf("wal: seeking to end: %w", err)
	}

	return nil
}
