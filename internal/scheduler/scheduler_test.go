package scheduler

import (
	"sync"
	"testing"
	"time"
)

// fakeDispatch records dispatched tasks per worker instead of making a
// real network call, so scheduling logic can be tested in isolation.
type fakeDispatch struct {
	mu  sync.Mutex
	log []string // "workerID:taskID"
}

func (f *fakeDispatch) fn(workerID, addr string, task *Task) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.log = append(f.log, workerID+":"+task.ID)
	return nil
}

func (f *fakeDispatch) count(workerID string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, e := range f.log {
		if len(e) >= len(workerID) && e[:len(workerID)] == workerID {
			n++
		}
	}
	return n
}

func TestDispatchPicksLeastLoadedWorker(t *testing.T) {
	fd := &fakeDispatch{}
	s := NewScheduler(fd.fn)

	s.RegisterWorker("w1", "localhost:9001")
	s.RegisterWorker("w2", "localhost:9002")

	// Submit 4 tasks; they should split evenly 2/2 across two idle
	// workers since dispatch always picks the least-loaded one.
	for i := 0; i < 4; i++ {
		s.SubmitTask(taskID(i), "payload")
	}

	c1, c2 := fd.count("w1"), fd.count("w2")
	if c1 != 2 || c2 != 2 {
		t.Errorf("expected even 2/2 split, got w1=%d w2=%d", c1, c2)
	}
}

func TestNoWorkersAvailable(t *testing.T) {
	fd := &fakeDispatch{}
	s := NewScheduler(fd.fn)

	task := s.SubmitTask("t1", "payload")
	if task.Status != TaskQueued {
		t.Errorf("expected task to remain queued with no workers, got %s", task.Status)
	}
}

func TestDeadWorkerTasksAreReassigned(t *testing.T) {
	fd := &fakeDispatch{}
	s := NewScheduler(fd.fn)

	s.RegisterWorker("w1", "localhost:9001")
	task := s.SubmitTask("t1", "payload")

	if task.WorkerID != "w1" {
		t.Fatalf("expected task assigned to w1, got %q", task.WorkerID)
	}

	// A second, healthy worker joins before w1 goes silent.
	s.RegisterWorker("w2", "localhost:9002")

	// Simulate w1 going silent past WorkerTimeout by manipulating time
	// indirectly: we sleep past the timeout instead of mocking time,
	// keeping the test simple and avoiding a fake clock dependency.
	origTimeout := WorkerTimeout
	setWorkerTimeoutForTest(t, 50*time.Millisecond)
	defer setWorkerTimeoutForTest(t, origTimeout)

	time.Sleep(100 * time.Millisecond)
	s.Heartbeat("w2") // w2 stays alive; only w1 has gone silent
	s.reapDeadWorkers()

	tasks, workerCount := s.Snapshot()
	if workerCount != 1 {
		t.Errorf("expected 1 surviving worker after reaping w1, got %d", workerCount)
	}
	if tasks["t1"] != TaskAssigned && tasks["t1"] != TaskQueued {
		t.Errorf("expected t1 to be reassigned (queued/assigned), got %s", tasks["t1"])
	}
	// It should have been redispatched to w2 since w1 is dead.
	if fd.count("w2") != 1 {
		t.Errorf("expected orphaned task to be redispatched to w2, got count=%d", fd.count("w2"))
	}
}

func taskID(i int) string {
	return "t" + string(rune('0'+i))
}

// setWorkerTimeoutForTest allows tests to shrink WorkerTimeout so tests
// run fast; production code always uses the default constant.
func setWorkerTimeoutForTest(t *testing.T, d time.Duration) {
	t.Helper()
	workerTimeoutMu.Lock()
	defer workerTimeoutMu.Unlock()
	WorkerTimeout = d
}

var workerTimeoutMu sync.Mutex
