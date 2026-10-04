package raft

import (
	"encoding/json"
	"fmt"
)

// decodeArg bridges two different transports behind the same generic
// Network interface (Send/Register pass values around as `any`):
//
//   - transport.FakeNetwork calls a handler with the ORIGINAL, already
//     concrete Go value (e.g. a real RequestVoteArgs struct) - nothing
//     is serialized in-process, so a direct type assertion succeeds
//     immediately.
//   - transport.RPCNetwork receives requests/replies as raw bytes off
//     an actual network connection, and can only pass them along as
//     json.RawMessage - it has no way to know which concrete raft
//     type ("RequestVoteArgs"? "AppendEntriesReply"?) to decode into,
//     because the transport package doesn't (and can't, without
//     creating an import cycle - raft already imports transport) know
//     about raft's own types.
//
// decodeArg tries the fast path first (v is already a T - true for
// FakeNetwork, so nothing changes there) and falls back to decoding v
// as JSON (true for RPCNetwork). Every place that receives an RPC
// argument or reply uses this, so the exact same registered handlers
// and call sites work correctly against EITHER transport unchanged.
func decodeArg[T any](v any) (T, error) {
	if typed, ok := v.(T); ok {
		return typed, nil
	}

	var zero T
	raw, ok := v.(json.RawMessage)
	if !ok {
		return zero, fmt.Errorf("raft: unexpected value of type %T", v)
	}

	var decoded T
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return zero, fmt.Errorf("raft: decoding %T: %w", decoded, err)
	}
	return decoded, nil
}
