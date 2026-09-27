// Package kv implements the underlying key-value store that sits at the
// bottom of the HiveMind stack. On a single node this is just a
// thread-safe in-memory map. Once Raft is wired in, the raft.Node's
// "apply" callback will call into this store so that every node ends up
// with the same state after applying the same sequence of commands.
package kv

import (
	"errors"
	"sync"
)

// ErrKeyNotFound is returned by Get when the key does not exist.
var ErrKeyNotFound = errors.New("kv: key not found")

// Store is a simple thread-safe in-memory key-value store.
//
// TODO(raft): once the replicated log exists, writes to Store should only
// ever happen via the state machine's Apply() path (i.e. after a command
// has been committed by Raft), never directly from client requests. This
// type on its own is intentionally unaware of Raft, WAL, or replication.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewStore creates an empty in-memory store.
func NewStore() *Store {
	return &Store{
		data: make(map[string]string),
	}
}

// Put sets key to value, overwriting any existing value.
func (s *Store) Put(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

// Get returns the value for key, or ErrKeyNotFound if it doesn't exist.
func (s *Store) Get(key string) (string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	if !ok {
		return "", ErrKeyNotFound
	}
	return v, nil
}

// Delete removes key. It is not an error to delete a key that doesn't exist.
func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
}

// Len returns the number of keys currently stored. Mostly useful for tests
// and the future `hivectl status` command.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.data)
}

// Snapshot returns a copy of the entire keyspace. This will become the
// input to raft's future snapshotting mechanism (see internal/raft) once
// log compaction is implemented.
func (s *Store) Snapshot() map[string]string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]string, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out
}

// Restore replaces the entire keyspace with snapshot. Used when a node
// recovers by loading a snapshot instead of replaying the full log.
func (s *Store) Restore(snapshot map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = make(map[string]string, len(snapshot))
	for k, v := range snapshot {
		s.data[k] = v
	}
}
