package transport

import (
	"encoding/json"
	"strings"
	"testing"
)

// A basic round trip: node A registers a handler, node B sends it a
// request over a real (loopback) TCP connection, and gets the real
// reply back.
func TestRPCNetwork_SendAndReceive(t *testing.T) {
	a, err := NewRPCNetwork("a", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewRPCNetwork(a): %v", err)
	}
	defer a.Close()

	b, err := NewRPCNetwork("b", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewRPCNetwork(b): %v", err)
	}
	defer b.Close()

	b.SetPeer("a", a.Addr())

	type echoArgs struct{ Message string }
	type echoReply struct{ Echoed string }

	a.Register("a", "Test.Echo", func(args any) (any, error) {
		// In-process (FakeNetwork), args would already be the typed
		// struct; over a real RPCNetwork it arrives as raw JSON bytes,
		// so decode it the same way raft.decodeArg does.
		raw, ok := args.(json.RawMessage)
		if !ok {
			t.Errorf("expected args to arrive as json.RawMessage, got %T", args)
		}
		var decoded echoArgs
		if err := json.Unmarshal(raw, &decoded); err != nil {
			return nil, err
		}
		return echoReply{Echoed: decoded.Message}, nil
	})

	replyAny, err := b.Send("b", "a", "Test.Echo", echoArgs{Message: "hello over real TCP"})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}

	raw, ok := replyAny.(json.RawMessage)
	if !ok {
		t.Fatalf("expected reply to be json.RawMessage, got %T", replyAny)
	}
	var reply echoReply
	if err := json.Unmarshal(raw, &reply); err != nil {
		t.Fatalf("decoding reply: %v", err)
	}
	if reply.Echoed != "hello over real TCP" {
		t.Fatalf("got Echoed=%q, want %q", reply.Echoed, "hello over real TCP")
	}
}

// Sending to an address nothing is listening on should fail promptly,
// not hang - this is what stands in for a "partitioned" or "crashed"
// peer on a real network.
func TestRPCNetwork_SendToDeadPeerFailsFast(t *testing.T) {
	a, err := NewRPCNetwork("a", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewRPCNetwork(a): %v", err)
	}
	defer a.Close()

	a.SetPeer("ghost", "127.0.0.1:1") // port 1 - nothing listens there

	_, err = a.Send("a", "ghost", "Test.Echo", struct{}{})
	if err == nil {
		t.Fatal("expected Send to a dead peer to fail, got nil error")
	}
}

// Sending to a peer ID with no known address should fail immediately
// with a clear error, not attempt to dial anything.
func TestRPCNetwork_SendToUnknownPeerFails(t *testing.T) {
	a, err := NewRPCNetwork("a", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewRPCNetwork(a): %v", err)
	}
	defer a.Close()

	_, err = a.Send("a", "nobody", "Test.Echo", struct{}{})
	if err == nil || !strings.Contains(err.Error(), "no known address") {
		t.Fatalf("expected a 'no known address' error, got %v", err)
	}
}
