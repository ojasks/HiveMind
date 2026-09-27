# HiveMind Architecture

HiveMind is a distributed key-value database written in Go, built from
scratch to understand and demonstrate how distributed systems maintain
agreement, survive failures, and recover. It is not meant to compete
with etcd or CockroachDB — the goal is a serious, technically deep
implementation you can explain from first principles.

## Layers

```
                 Client
                   |
                   v
            Client / CLI (hivectl)
                   |
                   v
              RPC Layer (internal/transport)
                   |
                   v
             Raft Consensus (internal/raft)
                   |
          +--------+--------+
          |                 |
      Replicated Log    Leader Election
          |
          v
       State Machine
          |
          v
     Key-Value Store (internal/kv)
          |
          v
     Persistent Storage
          |
      +---+----+
      |        |
     WAL    Snapshots
  (internal/wal)
```

## Current status

- `internal/kv` — working, in-memory, thread-safe.
- `internal/wal` — working, append-only, fsync'd, replay-on-startup.
- `internal/server` — working single-node server wiring kv + wal
  together, with a documented TODO for where Raft plugs in.
- `cmd/hivectl` — working CLI against the local single-node server.
- `internal/raft` — scaffolding only (types, state, RPC shapes). No
  election or replication logic yet.
- `internal/transport` — a `FakeNetwork` for deterministic, in-process
  testing of Raft (drop/partition simulation) before real RPC exists.

## What HiveMind explicitly does NOT aim for (v1)

SQL, transactions, sharding, multi-region replication, secondary
indexes, a query optimizer, authentication, or a complex UI. The
priority is making the distributed core excellent, not building a
general-purpose database.

## Build order

1. **Understand the problem.**
2. **Single-node KV store.** (done — `internal/kv`)
3. **Persistence.** (done — `internal/wal`, `internal/server`)
4. **Networking.** Real RPC (e.g. gRPC) replacing `transport.FakeNetwork`
   for production use; FakeNetwork remains for tests.
5. **Multiple nodes into a cluster.**
6. **Raft**, in sub-phases:
   1. Leader election only, tested against `transport.FakeNetwork`.
   2. Log replication (`AppendEntries`), no persistence yet.
   3. Persistence of `currentTerm`/`votedFor`/log via `internal/wal`
      *before* acknowledging votes or entries. This is the most common
      place naive Raft implementations break safety (a node that
      forgets its vote can double-vote in the same term after a
      restart).
   4. Commit index + apply loop wired into `internal/kv`.
7. **Failure and recovery** — kill/restart nodes, verify catch-up.
8. **Snapshots and log compaction.**
9. **Testing**: unit, integration (3- and 5-node clusters), failure
   (kill leader, drop/delay/partition messages), and concurrency tests.
10. **Benchmarks, Docker Compose, documentation, polish.**

## Read/write consistency (decide before Phase 6.4)

Writes go through Raft and are only visible once committed. Reads need
an explicit decision, documented here once made:

- **Stale reads allowed (simplest):** any node answers `GET` from its
  local state, which may lag the leader.
- **Linearizable reads:** only the leader answers `GET`, using a
  read-index or lease mechanism to avoid serving stale data after a
  leadership change.

## Not planned even later (avoid silent scope creep)

Dynamic cluster membership changes (adding/removing nodes at runtime)
are out of scope alongside the Phase-1 exclusions above, unless
explicitly revisited after the core is solid.
