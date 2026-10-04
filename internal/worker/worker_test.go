package worker

import (
	"net"
	"net/rpc"
	"testing"

	"github.com/TiyaGarg06/DistribuQ/internal/scheduler"
)

// startScheduler runs a real scheduler behind a real net/rpc server on
// a random local port and returns it with its address. dispatched
// receives the address of every worker a task is dispatched to.
func startScheduler(t *testing.T, dispatched *[]string) (*scheduler.Scheduler, string) {
	t.Helper()

	s := scheduler.NewScheduler(func(workerID, addr string, task *scheduler.Task) error {
		*dispatched = append(*dispatched, addr)
		return nil
	})
	srv := rpc.NewServer()
	if err := srv.RegisterName("SchedulerService", &scheduler.SchedulerService{S: s}); err != nil {
		t.Fatalf("register SchedulerService: %v", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go srv.ServeConn(conn)
		}
	}()
	return s, ln.Addr().String()
}

func workerCount(s *scheduler.Scheduler) int {
	_, n := s.Snapshot()
	return n
}

// TestHeartbeatRegistersUnknownWorker is the "reaped or new leader"
// case: the scheduler has never heard of this worker, rejects its
// heartbeat, and the worker responds by registering with its address.
func TestHeartbeatRegistersUnknownWorker(t *testing.T) {
	var dispatched []string
	s, addr := startScheduler(t, &dispatched)

	w := &Worker{ID: "w1", SchedulerAddr: addr, Addr: "localhost:9001"}

	if n := workerCount(s); n != 0 {
		t.Fatalf("setup: expected 0 workers, got %d", n)
	}
	w.sendHeartbeat()
	if n := workerCount(s); n != 1 {
		t.Fatalf("expected worker to re-register after rejected heartbeat, got %d workers", n)
	}

	// It registered with the right address: a task is dispatched there.
	if _, err := s.SubmitTask("t1", "payload"); err != nil {
		t.Fatalf("SubmitTask: %v", err)
	}
	if len(dispatched) != 1 || dispatched[0] != "localhost:9001" {
		t.Errorf("expected dispatch to localhost:9001, got %v", dispatched)
	}

	// A heartbeat from a now-known worker is a plain heartbeat.
	w.sendHeartbeat()
	if n := workerCount(s); n != 1 {
		t.Errorf("expected still 1 worker, got %d", n)
	}
}

// TestHeartbeatWithoutAddrDoesNotRegister checks that a worker with no
// Addr set doesn't register itself with an empty address (which would
// make it look available while being unreachable).
func TestHeartbeatWithoutAddrDoesNotRegister(t *testing.T) {
	var dispatched []string
	s, addr := startScheduler(t, &dispatched)

	w := &Worker{ID: "w1", SchedulerAddr: addr}
	w.sendHeartbeat()

	if n := workerCount(s); n != 0 {
		t.Errorf("expected no registration without an Addr, got %d workers", n)
	}
}

// TestHeartbeatUnreachableSchedulerDoesNotPanic checks that network
// failures are survivable: nothing registers and nothing blows up.
func TestHeartbeatUnreachableSchedulerDoesNotPanic(t *testing.T) {
	// Grab a free port, then close it so nothing is listening there.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	ln.Close()

	w := &Worker{ID: "w1", SchedulerAddr: addr, Addr: "localhost:9001"}
	w.sendHeartbeat() // must just log and return
}

// TestRegister checks the explicit registration call, including the
// error when the scheduler is unreachable.
func TestRegister(t *testing.T) {
	var dispatched []string
	s, addr := startScheduler(t, &dispatched)

	w := &Worker{ID: "w1", SchedulerAddr: addr, Addr: "localhost:9001"}
	if err := w.Register(); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if n := workerCount(s); n != 1 {
		t.Errorf("expected 1 registered worker, got %d", n)
	}

	// Registering twice is a refresh, not a duplicate.
	if err := w.Register(); err != nil {
		t.Fatalf("second Register: %v", err)
	}
	if n := workerCount(s); n != 1 {
		t.Errorf("expected still 1 worker after re-registering, got %d", n)
	}

	bad := &Worker{ID: "w2", SchedulerAddr: "127.0.0.1:1", Addr: "localhost:9002"}
	if err := bad.Register(); err == nil {
		t.Error("expected an error registering with an unreachable scheduler")
	}
}
