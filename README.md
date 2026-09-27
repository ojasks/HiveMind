# HiveMind

A distributed key-value database written in Go, built from scratch to
understand and demonstrate how distributed systems maintain agreement,
survive failures, and recover. See [docs/architecture.md](docs/architecture.md)
for the full design and build-order roadmap.

## Status

Single-node, persistent (WAL-backed) key-value store with a CLI.
Clustering and Raft consensus are scaffolded (`internal/raft`,
`internal/transport`) but not yet implemented — see the TODOs in those
packages for exactly what's next.

## Quick start

```bash
go build ./...          # sanity check everything compiles

go run ./cmd/hivectl put name Ojas
go run ./cmd/hivectl get name
go run ./cmd/hivectl status
go run ./cmd/hivectl delete name
```

Each command opens `hivemind.wal` in the current directory, replays it
to rebuild state, and appends new writes to it — so state survives
between runs. A good first test: run a few `put`s, then `kill -9` the
process mid-write and confirm `get` still returns the last *completed*
write on the next run.

## Layout

```
cmd/hivectl/          CLI entrypoint
internal/kv/          in-memory key-value store
internal/wal/         write-ahead log (durability)
internal/raft/        Raft consensus (scaffolding — see TODOs)
internal/transport/   fake in-process network for testing Raft
internal/server/      wires kv + wal (+ raft, later) into one node
docs/                 architecture and design docs
```

## Running tests

No tests exist yet — Phase 9 in the architecture doc. A reasonable
first test to write: unit tests for `internal/wal` (append, replay,
crash-mid-write simulation) and `internal/kv`.
