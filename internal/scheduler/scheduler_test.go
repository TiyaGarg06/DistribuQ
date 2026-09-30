package scheduler

import (
	"errors"
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

// taskState returns a task's status and attempt count under the
// scheduler's lock so tests don't read fields racily.
func taskState(s *Scheduler, id string) (TaskStatus, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.tasks[id]
	return t.Status, t.Attempts
}

// activeTaskCount returns how many in-flight tasks a worker is
// currently charged with.
func activeTaskCount(s *Scheduler, workerID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.workers[workerID].ActiveTasks)
}

// TestDispatchFailuresExhaustRetries checks that when every dispatch
// attempt errors out, the task is retried exactly MaxRetries times and
// then marked permanently failed instead of retrying forever.
func TestDispatchFailuresExhaustRetries(t *testing.T) {
	calls := 0
	alwaysFail := func(workerID, addr string, task *Task) error {
		calls++
		return errors.New("dispatch boom")
	}
	s := NewScheduler(alwaysFail)
	s.RegisterWorker("w1", "localhost:9001")

	s.SubmitTask("t1", "payload")

	status, attempts := taskState(s, "t1")
	if status != TaskFailed {
		t.Errorf("expected task to be failed after exhausting retries, got %s", status)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts (MaxRetries), got %d", attempts)
	}
	if calls != 3 {
		t.Errorf("expected dispatchFn to be called 3 times, got %d", calls)
	}
	if n := activeTaskCount(s, "w1"); n != 0 {
		t.Errorf("expected failed dispatches to leave w1 with 0 active tasks, got %d", n)
	}
}

// TestFailTaskRetriesThenFails checks that worker-reported failures
// requeue and redispatch the task until MaxRetries is reached, after
// which the task is marked permanently failed.
func TestFailTaskRetriesThenFails(t *testing.T) {
	fd := &fakeDispatch{}
	s := NewScheduler(fd.fn)
	s.RegisterWorker("w1", "localhost:9001")

	s.SubmitTask("t1", "payload") // attempt 1

	// First and second failures should each trigger a redispatch.
	for want := 2; want <= 3; want++ {
		if err := s.FailTask("t1"); err != nil {
			t.Fatalf("FailTask: %v", err)
		}
		status, attempts := taskState(s, "t1")
		if status != TaskAssigned || attempts != want {
			t.Fatalf("after failure, expected assigned with %d attempts, got %s with %d", want, status, attempts)
		}
	}

	// Third failure: attempts are exhausted, so it must stay failed.
	if err := s.FailTask("t1"); err != nil {
		t.Fatalf("FailTask: %v", err)
	}
	status, attempts := taskState(s, "t1")
	if status != TaskFailed {
		t.Errorf("expected task to be permanently failed, got %s", status)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
	if got := len(fd.log); got != 3 {
		t.Errorf("expected exactly 3 dispatches, got %d", got)
	}
	if n := activeTaskCount(s, "w1"); n != 0 {
		t.Errorf("expected w1 to have 0 active tasks after final failure, got %d", n)
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
