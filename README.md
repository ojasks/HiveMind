# HiveMind

A distributed key-value database written in Go, built from scratch to
understand and demonstrate how distributed systems maintain agreement,
survive failures, and recover. See [docs/architecture.md](docs/architecture.md)
for the full design and build-order roadmap.

## Status

Leader election, log replication, commit-index advancement, and an
apply loop are all implemented and tested (`internal/raft`). Raft's own
state (term/vote/log) is now durably persisted to disk
(`internal/raft/persist.go`), and `internal/server` routes `Put`/`Delete`
through Raft's `Propose`, blocking until each write actually commits.

Today's cluster size is one node (`internal/transport.FakeNetwork`
stands in for real networking) - so this already exercises the full
propose -> replicate -> commit -> apply -> persist -> restart path, it
just can't yet tolerate losing an entire *node*, only a process
restarting with its data intact. Real multi-process clustering needs
real RPC in place of `FakeNetwork` - see the TODOs in
`internal/raft/node.go` for what's left.

## Quick start

```bash
go build ./...          # sanity check everything compiles

go run ./cmd/hivectl put name Ojas
go run ./cmd/hivectl get name
go run ./cmd/hivectl status
go run ./cmd/hivectl delete name
```

Each command starts a fresh single-node Raft cluster backed by
`./hivemind-data/` (created on first run), so there's a genuine but
brief startup cost every invocation: winning its own leader election
(up to ~300ms) plus a one-time "barrier" commit that makes any
previously-committed data visible again (see the doc comment on
`server.New` for why that step exists). State survives a `kill -9`
between commands - not because of a separate WAL file anymore, but
because Raft's own persisted log gets replayed through the same apply
loop that handles normal writes.

## Layout

```
cmd/hivectl/          CLI entrypoint
internal/kv/          in-memory key-value store
internal/wal/         write-ahead log (durability) - a standalone
                       example; internal/server no longer uses it, now
                       that raft.Node persists its own log (see below)
internal/raft/        Raft consensus: election, replication, apply
                       loop, and persistence - see its TODOs for what's
                       still ahead (real RPC, multi-node)
internal/transport/   fake in-process network (stands in for real RPC)
internal/server/      wires kv + raft into one node; Put/Delete go
                       through Raft and block until committed
docs/                 architecture and design docs
```

## Running tests

```bash
go test ./... -v -race
```

`internal/raft` has the most coverage: election, replication, apply,
and persistence, including simulated network partitions and a full
crash/restart. `internal/server` has its own tests, including one that
restarts a server mid-test and confirms its data survived.
