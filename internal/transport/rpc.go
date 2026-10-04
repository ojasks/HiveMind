// Package transport's RPCNetwork is a real, working alternative to
// FakeNetwork: instead of calling another node's handler directly
// in-process, it sends an actual JSON message over an actual TCP
// connection to that peer's listening address, and reads back an
// actual response. This is what turns "a Raft implementation proven
// correct in one process" into an actual distributed system runnable
// as separate processes - on one machine (different ports) or several
// (different hosts).
package transport

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"
)

const (
	// dialTimeout bounds how long Send waits to connect to a peer.
	// Short on purpose: a genuinely unreachable peer (crashed, or
	// partitioned) should fail fast, the same way FakeNetwork's
	// simulated drops fail immediately rather than hanging.
	dialTimeout = 500 * time.Millisecond
	// rpcTimeout bounds the whole round trip (connect + send request +
	// receive response), so a peer that accepts a connection but never
	// replies can't hang the caller forever either.
	rpcTimeout = 2 * time.Second
)

// rpcRequest is the wire format sent from Send to a peer's listener.
type rpcRequest struct {
	Method string          `json:"method"`
	Args   json.RawMessage `json:"args"`
}

// rpcResponse is the wire format sent back. Result is left as raw
// bytes rather than decoded into a concrete type here, for the same
// reason Args is: this package doesn't know raft's types, and doesn't
// need to - see raft.decodeArg, which decodes it on the receiving end.
type rpcResponse struct {
	Result json.RawMessage `json:"result,omitempty"`
	Error  string          `json:"error,omitempty"`
}

// Compile-time check that RPCNetwork actually implements Network.
var _ Network = (*RPCNetwork)(nil)

// RPCNetwork is a Network backed by real TCP sockets. Each node runs
// one of these: it listens on its own address for incoming RPCs, and
// uses its peers map to know which address to dial for each peer ID.
//
// Each call is a fresh, one-shot connection (dial, write one request,
// read one response, close) rather than a long-lived multiplexed
// connection.
//
// TODO: connection reuse/pooling would reduce per-call latency (one
// TCP handshake per RPC adds up under load); left as one-shot
// connections here since correctness and readability matter more than
// throughput for a first working version.
type RPCNetwork struct {
	mu       sync.RWMutex
	selfID   string
	handlers map[string]Handler // method -> handler, this node's own only
	peers    map[string]string  // peer ID -> "host:port"
	listener net.Listener
}

// NewRPCNetwork starts listening on listenAddr (e.g. "127.0.0.1:9001",
// or ":0" to let the OS assign a free port - see Addr()) and returns a
// ready-to-use RPCNetwork. Call SetPeer for every peer this node needs
// to reach before calling Send to them.
func NewRPCNetwork(selfID string, listenAddr string) (*RPCNetwork, error) {
	l, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("transport: listening on %s: %w", listenAddr, err)
	}
	n := &RPCNetwork{
		selfID:   selfID,
		handlers: make(map[string]Handler),
		peers:    make(map[string]string),
		listener: l,
	}
	go n.serve()
	return n, nil
}

// Addr returns the address this network is actually listening on -
// useful when NewRPCNetwork was given ":0" and the OS picked the port,
// which is how tests avoid picking (and colliding on) a fixed port.
func (n *RPCNetwork) Addr() string {
	return n.listener.Addr().String()
}

// SetPeer records the network address to dial for peerID. Call this
// once for every peer before Send needs to reach them.
func (n *RPCNetwork) SetPeer(peerID string, addr string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.peers[peerID] = addr
}

// Register attaches a handler for method. nodeID is accepted (to match
// the Network interface FakeNetwork also implements) but unused here:
// an RPCNetwork instance only ever serves requests addressed to ITS
// OWN node, so there is only ever one "nodeID" it could sensibly mean.
func (n *RPCNetwork) Register(nodeID string, method string, h Handler) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handlers[method] = h
}

// Send dials to's registered address, sends method and args as one
// JSON request, and returns the decoded response - as json.RawMessage
// on success, since this package can't know (and doesn't need to know)
// which concrete raft type the caller will eventually decode it into.
// from is accepted for interface compatibility with FakeNetwork (which
// uses it to simulate partitions) but unused here: a real dropped
// connection or timeout IS the real-network equivalent of a partition,
// no simulation needed.
func (n *RPCNetwork) Send(from string, to string, method string, args any) (any, error) {
	n.mu.RLock()
	addr, ok := n.peers[to]
	n.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("transport: no known address for peer %q", to)
	}

	conn, err := net.DialTimeout("tcp", addr, dialTimeout)
	if err != nil {
		return nil, fmt.Errorf("transport: dialing %s (%s): %w", to, addr, err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(rpcTimeout))

	argsBytes, err := json.Marshal(args)
	if err != nil {
		return nil, fmt.Errorf("transport: marshaling args: %w", err)
	}

	if err := json.NewEncoder(conn).Encode(rpcRequest{Method: method, Args: argsBytes}); err != nil {
		return nil, fmt.Errorf("transport: sending request to %s: %w", to, err)
	}

	var resp rpcResponse
	if err := json.NewDecoder(conn).Decode(&resp); err != nil {
		return nil, fmt.Errorf("transport: reading response from %s: %w", to, err)
	}
	if resp.Error != "" {
		return nil, fmt.Errorf("transport: %s: %s", to, resp.Error)
	}
	return resp.Result, nil
}

// serve accepts incoming connections until the listener is closed.
func (n *RPCNetwork) serve() {
	for {
		conn, err := n.listener.Accept()
		if err != nil {
			return // listener closed - normal shutdown, not an error to report
		}
		go n.handleConn(conn)
	}
}

// handleConn processes exactly one request/response on conn, matching
// the one-shot-connection-per-call convention Send uses.
func (n *RPCNetwork) handleConn(conn net.Conn) {
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(rpcTimeout))

	var req rpcRequest
	if err := json.NewDecoder(conn).Decode(&req); err != nil {
		return // malformed or dropped request - nothing sensible to reply with
	}

	n.mu.RLock()
	handler, ok := n.handlers[req.Method]
	n.mu.RUnlock()

	var resp rpcResponse
	switch {
	case !ok:
		resp.Error = fmt.Sprintf("no handler registered for method %q", req.Method)
	default:
		result, err := handler(req.Args) // handler decodes req.Args itself (raft.decodeArg)
		switch {
		case err != nil:
			resp.Error = err.Error()
		default:
			resultBytes, mErr := json.Marshal(result)
			if mErr != nil {
				resp.Error = fmt.Sprintf("marshaling result: %v", mErr)
			} else {
				resp.Result = resultBytes
			}
		}
	}

	_ = json.NewEncoder(conn).Encode(resp) // best-effort: nothing to do if this fails
}

// Close stops accepting new connections. Safe to call once; a second
// call will return an error from the underlying listener, which callers
// can safely ignore.
func (n *RPCNetwork) Close() error {
	return n.listener.Close()
}
