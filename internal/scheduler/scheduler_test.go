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

	task, err := s.SubmitTask("t1", "payload")
	if err != nil {
		t.Fatalf("SubmitTask: %v", err)
	}
	if task.Status != TaskQueued {
		t.Errorf("expected task to remain queued with no workers, got %s", task.Status)
	}
}

func TestDeadWorkerTasksAreReassigned(t *testing.T) {
	fd := &fakeDispatch{}
	s := NewScheduler(fd.fn)

	s.RegisterWorker("w1", "localhost:9001")
	task, err := s.SubmitTask("t1", "payload")
	if err != nil {
		t.Fatalf("SubmitTask: %v", err)
	}

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

// taskWorker returns the ID of the worker a task is currently
// assigned to ("" if none).
func taskWorker(s *Scheduler, id string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tasks[id].WorkerID
}

// newReassignedTask builds the classic stale-report scenario: task t1
// is dispatched to w1, w1 then goes silent and is reaped, and t1 is
// redispatched to w2. From here on, w1 is a "zombie": it may still
// report on t1, but t1 now belongs to w2.
func newReassignedTask(t *testing.T) (*Scheduler, *fakeDispatch) {
	t.Helper()
	fd := &fakeDispatch{}
	s := NewScheduler(fd.fn)

	s.RegisterWorker("w1", "localhost:9001")
	s.SubmitTask("t1", "payload")
	s.RegisterWorker("w2", "localhost:9002")

	// Make w1 look like it stopped heartbeating long ago, then reap it.
	s.mu.Lock()
	s.workers["w1"].LastHeartbeat = time.Now().Add(-time.Hour)
	s.mu.Unlock()
	s.reapDeadWorkers()

	status, attempts := taskState(s, "t1")
	if status != TaskAssigned || attempts != 2 || taskWorker(s, "t1") != "w2" {
		t.Fatalf("setup: expected t1 reassigned to w2 (attempt 2), got %s on %q (attempt %d)",
			status, taskWorker(s, "t1"), attempts)
	}
	return s, fd
}

// TestStaleCompleteIsRejected checks that a completion report from a
// worker that no longer owns the task is ignored, and that the real
// owner can still complete it.
func TestStaleCompleteIsRejected(t *testing.T) {
	s, _ := newReassignedTask(t)

	err := s.CompleteTaskBy("t1", "w1")
	if !errors.Is(err, ErrStaleReport) {
		t.Fatalf("expected ErrStaleReport from zombie worker, got %v", err)
	}
	status, _ := taskState(s, "t1")
	if status != TaskAssigned || taskWorker(s, "t1") != "w2" {
		t.Errorf("stale completion must not change task state, got %s on %q", status, taskWorker(s, "t1"))
	}

	if err := s.CompleteTaskBy("t1", "w2"); err != nil {
		t.Fatalf("current assignee should be able to complete the task, got %v", err)
	}
	if status, _ := taskState(s, "t1"); status != TaskCompleted {
		t.Errorf("expected task completed, got %s", status)
	}
	if n := activeTaskCount(s, "w2"); n != 0 {
		t.Errorf("expected w2 to have 0 active tasks after completing, got %d", n)
	}
}

// TestStaleFailIsRejected checks that a failure report from a worker
// that no longer owns the task does not burn a retry or trigger a
// spurious redispatch.
func TestStaleFailIsRejected(t *testing.T) {
	s, fd := newReassignedTask(t)
	dispatchesBefore := len(fd.log)

	err := s.FailTaskBy("t1", "w1")
	if !errors.Is(err, ErrStaleReport) {
		t.Fatalf("expected ErrStaleReport from zombie worker, got %v", err)
	}
	status, attempts := taskState(s, "t1")
	if status != TaskAssigned || attempts != 2 || taskWorker(s, "t1") != "w2" {
		t.Errorf("stale failure must not change task state, got %s on %q (attempt %d)",
			status, taskWorker(s, "t1"), attempts)
	}
	if len(fd.log) != dispatchesBefore {
		t.Errorf("stale failure must not trigger a redispatch, got %d extra dispatch(es)", len(fd.log)-dispatchesBefore)
	}

	// The real owner reporting failure still requeues and redispatches.
	if err := s.FailTaskBy("t1", "w2"); err != nil {
		t.Fatalf("current assignee failure report should be accepted, got %v", err)
	}
	if _, attempts := taskState(s, "t1"); attempts != 3 {
		t.Errorf("expected owner's failure to trigger attempt 3, got attempt %d", attempts)
	}
}

// TestReportsForUnknownTask checks that reporting on a task the
// scheduler has never seen returns ErrTaskNotFound rather than
// ErrStaleReport or a panic.
func TestReportsForUnknownTask(t *testing.T) {
	s := NewScheduler((&fakeDispatch{}).fn)

	if err := s.CompleteTaskBy("nope", "w1"); !errors.Is(err, ErrTaskNotFound) {
		t.Errorf("CompleteTaskBy: expected ErrTaskNotFound, got %v", err)
	}
	if err := s.FailTaskBy("nope", "w1"); !errors.Is(err, ErrTaskNotFound) {
		t.Errorf("FailTaskBy: expected ErrTaskNotFound, got %v", err)
	}
}

// TestSubmitRejectsDuplicateID checks that resubmitting an existing
// task ID is rejected and leaves the original task (still in flight on
// a worker) completely untouched, and that IDs stay reserved even after
// the task has completed.
func TestSubmitRejectsDuplicateID(t *testing.T) {
	fd := &fakeDispatch{}
	s := NewScheduler(fd.fn)
	s.RegisterWorker("w1", "localhost:9001")

	if _, err := s.SubmitTask("t1", "original"); err != nil {
		t.Fatalf("first submit: %v", err)
	}

	task, err := s.SubmitTask("t1", "overwrite attempt")
	if !errors.Is(err, ErrDuplicateTask) {
		t.Fatalf("expected ErrDuplicateTask, got %v", err)
	}
	if task != nil {
		t.Errorf("expected nil task on rejected submit, got %+v", task)
	}

	s.mu.Lock()
	payload := s.tasks["t1"].Payload
	s.mu.Unlock()
	if payload != "original" {
		t.Errorf("duplicate submit overwrote the payload: got %q", payload)
	}
	status, attempts := taskState(s, "t1")
	if status != TaskAssigned || attempts != 1 {
		t.Errorf("duplicate submit disturbed the running task: %s, attempt %d", status, attempts)
	}
	if len(fd.log) != 1 {
		t.Errorf("duplicate submit must not dispatch again, got %d dispatches", len(fd.log))
	}

	// The ID stays reserved after completion.
	if err := s.CompleteTask("t1"); err != nil {
		t.Fatalf("CompleteTask: %v", err)
	}
	if _, err := s.SubmitTask("t1", "again"); !errors.Is(err, ErrDuplicateTask) {
		t.Errorf("expected ErrDuplicateTask for a completed task's id, got %v", err)
	}
}

// TestSubmitRejectsEmptyID checks that an empty task ID is rejected and
// nothing is queued.
func TestSubmitRejectsEmptyID(t *testing.T) {
	fd := &fakeDispatch{}
	s := NewScheduler(fd.fn)
	s.RegisterWorker("w1", "localhost:9001")

	task, err := s.SubmitTask("", "payload")
	if !errors.Is(err, ErrEmptyTaskID) {
		t.Fatalf("expected ErrEmptyTaskID, got %v", err)
	}
	if task != nil {
		t.Errorf("expected nil task on rejected submit, got %+v", task)
	}
	if tasks, _ := s.Snapshot(); len(tasks) != 0 {
		t.Errorf("expected no tasks to be queued, got %d", len(tasks))
	}
	if len(fd.log) != 0 {
		t.Errorf("expected no dispatches, got %d", len(fd.log))
	}
}

// TestStopIsIdempotent checks that Stop can be called repeatedly
// without panicking (closing a closed channel would), that it ends a
// running MonitorWorkers loop, and that a monitor started after Stop
// returns immediately instead of hanging.
func TestStopIsIdempotent(t *testing.T) {
	s := NewScheduler((&fakeDispatch{}).fn)

	done := make(chan struct{})
	go func() {
		s.MonitorWorkers(10 * time.Millisecond)
		close(done)
	}()

	s.Stop()
	s.Stop() // must not panic

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("MonitorWorkers did not return after Stop")
	}

	// Stop is also safe to call concurrently.
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.Stop()
		}()
	}
	wg.Wait()

	// A monitor started after Stop exits right away.
	late := make(chan struct{})
	go func() {
		s.MonitorWorkers(10 * time.Millisecond)
		close(late)
	}()
	select {
	case <-late:
	case <-time.After(time.Second):
		t.Fatal("MonitorWorkers started after Stop did not return")
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
