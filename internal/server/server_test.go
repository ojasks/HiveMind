package server

import (
	"errors"
	"testing"

	"hivemind/internal/kv"
)

func TestServer_PutAndGet(t *testing.T) {
	dir := t.TempDir()

	srv, err := New("node1", dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer srv.Close()

	if err := srv.Put("name", "Ojas"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := srv.Put("age", "22"); err != nil {
		t.Fatalf("Put: %v", err)
	}

	name, err := srv.Get("name")
	if err != nil || name != "Ojas" {
		t.Fatalf("Get(name) = %q, %v; want Ojas, nil", name, err)
	}
	age, err := srv.Get("age")
	if err != nil || age != "22" {
		t.Fatalf("Get(age) = %q, %v; want 22, nil", age, err)
	}

	if got := srv.KeyCount(); got != 2 {
		t.Fatalf("KeyCount() = %d, want 2", got)
	}
}

func TestServer_DeleteRemovesKey(t *testing.T) {
	dir := t.TempDir()

	srv, err := New("node1", dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer srv.Close()

	if err := srv.Put("temp", "value"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := srv.Delete("temp"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if _, err := srv.Get("temp"); !errors.Is(err, kv.ErrKeyNotFound) {
		t.Fatalf("Get(temp) after delete: got err=%v, want ErrKeyNotFound", err)
	}
}

// This is the scenario the whole persistence layer exists for: data
// written before a process dies is still there after it restarts -
// with NO new write needed to "unlock" it (see the New() doc comment
// on the startup barrier for why that's not automatic).
func TestServer_SurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	srv1, err := New("node1", dir)
	if err != nil {
		t.Fatalf("New (first run): %v", err)
	}
	if err := srv1.Put("name", "Ojas"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := srv1.Put("role", "leader"); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := srv1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// "Restart": a brand new Server pointed at the same directory,
	// simulating the process having crashed and come back up.
	srv2, err := New("node1", dir)
	if err != nil {
		t.Fatalf("New (after restart): %v", err)
	}
	defer srv2.Close()

	name, err := srv2.Get("name")
	if err != nil || name != "Ojas" {
		t.Fatalf("Get(name) after restart = %q, %v; want Ojas, nil (no new write happened first)", name, err)
	}
	role, err := srv2.Get("role")
	if err != nil || role != "leader" {
		t.Fatalf("Get(role) after restart = %q, %v; want leader, nil", role, err)
	}

	// The restarted server should also still accept new writes.
	if err := srv2.Put("age", "22"); err != nil {
		t.Fatalf("Put after restart: %v", err)
	}
	age, err := srv2.Get("age")
	if err != nil || age != "22" {
		t.Fatalf("Get(age) after restart-write = %q, %v; want 22, nil", age, err)
	}
}
