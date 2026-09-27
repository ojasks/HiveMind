// Package server ties the individual pieces (kv, wal, and eventually
// raft) together into a single node that hivectl can talk to.
//
// As it stands, Server is a fully working SINGLE-NODE, PERSISTENT
// key-value store: writes go to the WAL first (durable before
// acknowledging), then to the in-memory store, and on startup the WAL is
// replayed to reconstruct state. This corresponds to the end of Phase 3
// in docs/architecture.md.
//
// What's NOT here yet is anything Raft-related — Put/Delete currently
// apply directly rather than going through a replicated log. See the
// TODO on Put/Delete below for exactly what changes once raft.Node is
// wired in.
package server

import (
	"fmt"

	"hivemind/internal/kv"
	"hivemind/internal/wal"
)

// Server is a single HiveMind node: a KV store fronted by a WAL for
// durability. Networking, clustering, and consensus are layered on top
// of this in later phases.
type Server struct {
	store *kv.Store
	log   *wal.WAL

	// TODO(phase 6.4): once Raft exists, add:
	//   raftNode *raft.Node
	// and change Put/Delete below to submit the command to raftNode
	// instead of applying directly. The actual mutation of `store`
	// should then only happen inside the function passed to
	// raftNode's Apply/commit callback, so that all nodes apply
	// commands in the same committed order rather than each node
	// accepting writes independently.
}

// New creates a Server backed by a WAL file at walPath, replaying any
// existing entries to reconstruct prior state.
func New(walPath string) (*Server, error) {
	w, err := wal.Open(walPath)
	if err != nil {
		return nil, fmt.Errorf("server: opening wal: %w", err)
	}

	store := kv.NewStore()

	if err := w.Replay(func(e wal.Entry) {
		switch e.Op {
		case wal.OpPut:
			store.Put(e.Key, e.Value)
		case wal.OpDelete:
			store.Delete(e.Key)
		}
	}); err != nil {
		return nil, fmt.Errorf("server: replaying wal: %w", err)
	}

	return &Server{store: store, log: w}, nil
}

// Close releases the server's resources (currently just the WAL file).
func (s *Server) Close() error {
	return s.log.Close()
}

// Put durably writes key=value: first to the WAL, then to the in-memory
// store, matching the "write op to WAL -> apply to state machine" flow
// from docs/architecture.md.
//
// TODO(phase 6.4 - see struct comment): this should submit the command
// through raft.Node once clustering exists, rather than writing directly.
func (s *Server) Put(key, value string) error {
	if _, err := s.log.Append(wal.OpPut, key, value); err != nil {
		return fmt.Errorf("server: put: %w", err)
	}
	s.store.Put(key, value)
	return nil
}

// Get returns the current value for key.
func (s *Server) Get(key string) (string, error) {
	return s.store.Get(key)
}

// Delete durably removes key.
//
// TODO(phase 6.4): same note as Put — route through Raft once it exists.
func (s *Server) Delete(key string) error {
	if _, err := s.log.Append(wal.OpDelete, key, ""); err != nil {
		return fmt.Errorf("server: delete: %w", err)
	}
	s.store.Delete(key)
	return nil
}

// KeyCount returns the number of keys currently stored. Used by the
// `hivectl status` stub for now; will grow to include Raft state
// (term, leader, commit index, etc.) once that exists.
func (s *Server) KeyCount() int {
	return s.store.Len()
}
