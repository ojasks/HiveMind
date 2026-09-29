package raft

// CommandOp identifies the kind of write a Command represents. This
// intentionally mirrors wal.OpType - once persistence (phase 6.3) is
// wired in, encoding a Command to/from the WAL is a direct field-for-
// field mapping, not a translation.
type CommandOp string

const (
	OpPut    CommandOp = "PUT"
	OpDelete CommandOp = "DELETE"
)

// Command is the payload of a LogEntry: one write operation the
// cluster is agreeing on the order of. This replaces the earlier
// opaque string placeholder now that log replication (phase 6.2) is
// done and something concrete needs to flow through it.
type Command struct {
	Op    CommandOp
	Key   string
	Value string // unused for OpDelete
}
