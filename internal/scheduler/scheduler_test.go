package scheduler

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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

// TestReapRejectsLateReportFromReapedWorker checks that once a dead
// worker's tasks have been taken back, a completion report from that
// worker (arriving while the reaper is still redispatching) is
// rejected as stale rather than accepted and then clobbered.
func TestReapRejectsLateReportFromReapedWorker(t *testing.T) {
	var s *Scheduler
	var staleResult error
	var observed bool

	dispatch := func(workerID, addr string, task *Task) error {
		// Only intercept redispatches (to w2) made by the reaper, and
		// only once: while the reaper is mid-redispatch, the reaped
		// worker w1 reports completion of the *other* orphaned task.
		if workerID == "w2" && !observed {
			observed = true
			other := "t1"
			if task.ID == "t1" {
				other = "t2"
			}
			staleResult = s.CompleteTaskBy(other, "w1")
		}
		return nil
	}
	s = NewScheduler(dispatch)

	s.RegisterWorker("w1", "localhost:9001")
	s.SubmitTask("t1", "payload")
	s.SubmitTask("t2", "payload")
	s.RegisterWorker("w2", "localhost:9002")

	s.mu.Lock()
	s.workers["w1"].LastHeartbeat = time.Now().Add(-time.Hour)
	s.mu.Unlock()
	s.reapDeadWorkers()

	if !observed {
		t.Fatal("test setup: reaper never redispatched to w2")
	}
	if !errors.Is(staleResult, ErrStaleReport) {
		t.Errorf("late report from a reaped worker should be stale, got %v", staleResult)
	}
	for _, id := range []string{"t1", "t2"} {
		status, attempts := taskState(s, id)
		if status != TaskAssigned || attempts != 2 || taskWorker(s, id) != "w2" {
			t.Errorf("%s: expected assigned to w2 on attempt 2, got %s on %q (attempt %d)",
				id, status, taskWorker(s, id), attempts)
		}
	}
}

// TestReapFailsOrphanWhenRetriesExhausted checks that an orphaned task
// with no attempts left goes straight to failed and is never
// redispatched.
func TestReapFailsOrphanWhenRetriesExhausted(t *testing.T) {
	fd := &fakeDispatch{}
	s := NewScheduler(fd.fn)

	s.RegisterWorker("w1", "localhost:9001")
	s.SubmitTask("t1", "payload")
	s.mu.Lock()
	s.tasks["t1"].Attempts = s.tasks["t1"].MaxRetries // pretend retries are used up
	s.workers["w1"].LastHeartbeat = time.Now().Add(-time.Hour)
	s.mu.Unlock()
	s.RegisterWorker("w2", "localhost:9002")

	dispatchesBefore := len(fd.log)
	s.reapDeadWorkers()

	if status, _ := taskState(s, "t1"); status != TaskFailed {
		t.Errorf("expected exhausted orphan to be failed, got %s", status)
	}
	if len(fd.log) != dispatchesBefore {
		t.Errorf("exhausted orphan must not be redispatched, got %d extra dispatch(es)", len(fd.log)-dispatchesBefore)
	}
}

// TestHeartbeatUnknownWorker checks that heartbeats from workers the
// scheduler doesn't know about are rejected (instead of silently
// ignored), and that registering again makes them valid.
func TestHeartbeatUnknownWorker(t *testing.T) {
	s := NewScheduler((&fakeDispatch{}).fn)

	if err := s.Heartbeat("ghost"); !errors.Is(err, ErrUnknownWorker) {
		t.Errorf("never-registered worker: expected ErrUnknownWorker, got %v", err)
	}

	s.RegisterWorker("w1", "localhost:9001")
	if err := s.Heartbeat("w1"); err != nil {
		t.Errorf("registered worker: expected nil, got %v", err)
	}

	// Reaped workers are forgotten, so their heartbeats are rejected...
	s.mu.Lock()
	s.workers["w1"].LastHeartbeat = time.Now().Add(-time.Hour)
	s.mu.Unlock()
	s.reapDeadWorkers()
	if err := s.Heartbeat("w1"); !errors.Is(err, ErrUnknownWorker) {
		t.Errorf("reaped worker: expected ErrUnknownWorker, got %v", err)
	}

	// ...until they register again.
	s.RegisterWorker("w1", "localhost:9001")
	if err := s.Heartbeat("w1"); err != nil {
		t.Errorf("re-registered worker: expected nil, got %v", err)
	}
}

// stressJob is one dispatch that the simulated workers must act on.
type stressJob struct {
	taskID   string
	workerID string
	attempt  int
}

// TestConcurrentSubmitAndComplete hammers the scheduler from many
// goroutines at once -- concurrent submits, workers completing and
// failing tasks (which triggers retries and redispatch), plus
// heartbeats and snapshots -- and then checks the invariants that
// must hold no matter how the goroutines interleave. Its main job is
// to give `go test -race` real contention to chew on.
func TestConcurrentSubmitAndComplete(t *testing.T) {
	const (
		numWorkers    = 4
		numTasks      = 200
		numSubmitters = 8
		numActors     = 8 // goroutines playing the part of workers
		failEvery     = 5 // every 5th task fails its first attempt once
	)

	// Which tasks fail their first attempt. Built up front and only
	// read afterwards, so it is safe to share between goroutines.
	ids := make([]string, numTasks)
	failsFirst := make(map[string]bool)
	for i := range ids {
		ids[i] = fmt.Sprintf("task-%03d", i)
		if i%failEvery == 0 {
			failsFirst[ids[i]] = true
		}
	}

	// The dispatch function hands each dispatch to the actors over a
	// channel. It runs outside the scheduler's lock (dispatch releases
	// it first), so the buffer only needs to cover the worst case of
	// every task being dispatched twice, with room to spare.
	jobs := make(chan stressJob, numTasks*3)
	var dispatches int64
	s := NewScheduler(func(workerID, addr string, task *Task) error {
		atomic.AddInt64(&dispatches, 1)
		jobs <- stressJob{taskID: task.ID, workerID: workerID, attempt: task.Attempts}
		return nil
	})

	workerIDs := make([]string, numWorkers)
	for i := range workerIDs {
		workerIDs[i] = fmt.Sprintf("w%d", i+1)
		s.RegisterWorker(workerIDs[i], fmt.Sprintf("localhost:%d", 9001+i))
	}

	// Simulated workers: complete each job, except that the first
	// attempt of "failing" tasks is reported as a failure instead.
	var actors sync.WaitGroup
	for i := 0; i < numActors; i++ {
		actors.Add(1)
		go func() {
			defer actors.Done()
			for job := range jobs {
				var err error
				if job.attempt == 1 && failsFirst[job.taskID] {
					err = s.FailTaskBy(job.taskID, job.workerID)
				} else {
					err = s.CompleteTaskBy(job.taskID, job.workerID)
				}
				if err != nil {
					t.Errorf("report for %s from %s: %v", job.taskID, job.workerID, err)
				}
			}
		}()
	}

	// Background noise: heartbeats and snapshots while all of that runs.
	stop := make(chan struct{})
	var background sync.WaitGroup
	for _, id := range workerIDs {
		background.Add(1)
		go func(id string) {
			defer background.Done()
			for {
				select {
				case <-stop:
					return
				default:
					if err := s.Heartbeat(id); err != nil {
						t.Errorf("heartbeat for %s: %v", id, err)
						return
					}
				}
			}
		}(id)
	}
	background.Add(1)
	go func() {
		defer background.Done()
		for {
			select {
			case <-stop:
				return
			default:
				s.Snapshot()
			}
		}
	}()

	// Concurrent submitters, each taking every numSubmitters-th task.
	var submitters sync.WaitGroup
	for g := 0; g < numSubmitters; g++ {
		submitters.Add(1)
		go func(g int) {
			defer submitters.Done()
			for i := g; i < numTasks; i += numSubmitters {
				if _, err := s.SubmitTask(ids[i], "payload"); err != nil {
					t.Errorf("submit %s: %v", ids[i], err)
				}
			}
		}(g)
	}
	submitters.Wait()

	// Wait for every task to complete.
	deadline := time.Now().Add(10 * time.Second)
	for {
		tasks, _ := s.Snapshot()
		allDone := len(tasks) == numTasks
		counts := map[TaskStatus]int{}
		for _, st := range tasks {
			counts[st]++
			if st != TaskCompleted {
				allDone = false
			}
		}
		if allDone {
			break
		}
		if time.Now().After(deadline) {
			close(stop)
			t.Fatalf("timed out waiting for completion: %d tasks known, statuses %v", len(tasks), counts)
		}
		time.Sleep(time.Millisecond)
	}

	// Everything has completed, so no more dispatches can happen.
	close(stop)
	close(jobs)
	actors.Wait()
	background.Wait()

	for i, id := range ids {
		status, attempts := taskState(s, id)
		wantAttempts := 1
		if i%failEvery == 0 {
			wantAttempts = 2
		}
		if status != TaskCompleted || attempts != wantAttempts {
			t.Errorf("%s: expected completed after %d attempt(s), got %s after %d", id, wantAttempts, status, attempts)
		}
	}
	for _, id := range workerIDs {
		if n := activeTaskCount(s, id); n != 0 {
			t.Errorf("%s still has %d active task(s) after everything completed", id, n)
		}
	}
	wantDispatches := int64(numTasks + numTasks/failEvery)
	if got := atomic.LoadInt64(&dispatches); got != wantDispatches {
		t.Errorf("expected %d dispatches (every task once, failing tasks twice), got %d", wantDispatches, got)
	}
}

// TestConcurrentDuplicateSubmitOnlyOneWins races many goroutines
// submitting the same task ID and checks that exactly one of them wins
// and the task is dispatched exactly once.
func TestConcurrentDuplicateSubmitOnlyOneWins(t *testing.T) {
	const goroutines = 20

	fd := &fakeDispatch{}
	s := NewScheduler(fd.fn)
	s.RegisterWorker("w1", "localhost:9001")

	var accepted, rejected int32
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start // release everyone at once to maximise the race
			_, err := s.SubmitTask("same-id", "payload")
			switch {
			case err == nil:
				atomic.AddInt32(&accepted, 1)
			case errors.Is(err, ErrDuplicateTask):
				atomic.AddInt32(&rejected, 1)
			default:
				t.Errorf("unexpected error: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()

	if accepted != 1 || rejected != goroutines-1 {
		t.Errorf("expected 1 accepted and %d rejected, got %d accepted and %d rejected", goroutines-1, accepted, rejected)
	}
	if got := len(fd.log); got != 1 {
		t.Errorf("expected the task to be dispatched exactly once, got %d dispatches", got)
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
