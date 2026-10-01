// Package scheduler implements task queueing, worker registration, and
// least-loaded task dispatch, plus failure detection that reassigns
// in-flight tasks when a worker stops sending heartbeats.
package scheduler

import (
	"errors"
	"sync"
	"time"
)

type TaskStatus string

const (
	TaskQueued    TaskStatus = "queued"
	TaskAssigned  TaskStatus = "assigned"
	TaskRunning   TaskStatus = "running"
	TaskCompleted TaskStatus = "completed"
	TaskFailed    TaskStatus = "failed"
)

type Task struct {
	ID         string
	Payload    string
	Status     TaskStatus
	WorkerID   string
	Attempts   int
	MaxRetries int
}

type WorkerInfo struct {
	ID            string
	Addr          string
	LastHeartbeat time.Time
	ActiveTasks   map[string]bool
}

var (
	ErrNoWorkersAvailable = errors.New("scheduler: no healthy workers available")
	ErrTaskNotFound       = errors.New("scheduler: task not found")

	// ErrEmptyTaskID is returned by SubmitTask when the task ID is "".
	ErrEmptyTaskID = errors.New("scheduler: task id must not be empty")

	// ErrDuplicateTask is returned by SubmitTask when a task with the
	// same ID was already submitted. Task IDs are unique for the life
	// of the scheduler, including after the task completes or fails;
	// accepting a duplicate would silently overwrite a task that may
	// still be running on a worker.
	ErrDuplicateTask = errors.New("scheduler: a task with this id already exists")

	// ErrStaleReport is returned when a worker reports the outcome of a
	// task that is no longer assigned to it (for example, the worker was
	// declared dead, the task was reassigned, and then the old worker
	// woke up and reported late). The report is ignored so it can't
	// clobber the state of the task's current assignment.
	ErrStaleReport = errors.New("scheduler: stale report from a worker the task is not assigned to")
)

// WorkerTimeout is how long a worker can go without a heartbeat before
// it's considered dead and its in-flight tasks are reassigned.
// It's a var (not const) so tests can shrink it instead of sleeping
// for the full production timeout.
var WorkerTimeout = 3 * time.Second

type Scheduler struct {
	mu sync.Mutex

	tasks   map[string]*Task
	workers map[string]*WorkerInfo

	// dispatchFn sends a task to a worker; injected so it can be swapped
	// (RPC today, gRPC later) without touching scheduling logic.
	dispatchFn func(workerID, addr string, task *Task) error

	// stopCh is closed by Stop to end MonitorWorkers. stopOnce makes
	// Stop idempotent: closing an already-closed channel panics, and
	// shutdown paths (signal handlers, deferred cleanup, tests) can
	// easily end up calling Stop more than once.
	stopCh   chan struct{}
	stopOnce sync.Once
}

func NewScheduler(dispatchFn func(workerID, addr string, task *Task) error) *Scheduler {
	return &Scheduler{
		tasks:      make(map[string]*Task),
		workers:    make(map[string]*WorkerInfo),
		dispatchFn: dispatchFn,
		stopCh:     make(chan struct{}),
	}
}

// RegisterWorker adds (or refreshes) a worker in the pool.
func (s *Scheduler) RegisterWorker(id, addr string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if w, ok := s.workers[id]; ok {
		w.LastHeartbeat = time.Now()
		w.Addr = addr
		return
	}
	s.workers[id] = &WorkerInfo{
		ID:            id,
		Addr:          addr,
		LastHeartbeat: time.Now(),
		ActiveTasks:   make(map[string]bool),
	}
}

// Heartbeat refreshes a worker's liveness timestamp.
func (s *Scheduler) Heartbeat(workerID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if w, ok := s.workers[workerID]; ok {
		w.LastHeartbeat = time.Now()
	}
}

// SubmitTask queues a task and attempts immediate dispatch to the
// least-loaded healthy worker. It rejects an empty ID (ErrEmptyTaskID)
// and an ID that has been used before (ErrDuplicateTask); in both
// cases nothing is queued and existing tasks are left untouched.
func (s *Scheduler) SubmitTask(id, payload string) (*Task, error) {
	if id == "" {
		return nil, ErrEmptyTaskID
	}

	s.mu.Lock()
	if _, exists := s.tasks[id]; exists {
		s.mu.Unlock()
		return nil, ErrDuplicateTask
	}
	task := &Task{ID: id, Payload: payload, Status: TaskQueued, MaxRetries: 3}
	s.tasks[id] = task
	s.mu.Unlock()

	_ = s.dispatch(task)
	return task, nil
}

// dispatch picks the least-loaded worker and hands the task to it. If
// dispatchFn fails, the tentative assignment is rolled back and the
// task is requeued (or marked failed past MaxRetries); if requeued,
// dispatch immediately retries against a different worker rather than
// leaving the task sitting as TaskQueued with nothing to ever pick it
// back up. Retries are naturally bounded: each attempt increments
// task.Attempts, and once Attempts reaches MaxRetries the task is
// marked TaskFailed instead of requeued, which stops the recursion.
func (s *Scheduler) dispatch(task *Task) error {
	s.mu.Lock()
	worker := s.pickLeastLoadedWorkerLocked()
	if worker == nil {
		s.mu.Unlock()
		return ErrNoWorkersAvailable
	}
	task.Status = TaskAssigned
	task.WorkerID = worker.ID
	task.Attempts++
	worker.ActiveTasks[task.ID] = true
	addr := worker.Addr
	workerID := worker.ID
	s.mu.Unlock()

	if err := s.dispatchFn(workerID, addr, task); err != nil {
		s.mu.Lock()
		if w, ok := s.workers[workerID]; ok {
			delete(w.ActiveTasks, task.ID)
		}
		s.requeueOrFailLocked(task)
		requeued := task.Status == TaskQueued
		s.mu.Unlock()

		if requeued {
			s.dispatch(task)
		}
		return err
	}
	return nil
}

// requeueOrFailLocked decides a task's fate after it comes off a
// worker without completing (dispatch failure, execution failure, or
// worker death): retry it if attempts remain, otherwise mark it
// permanently failed. Callers must hold s.mu and must have already
// cleared the task out of its former worker's ActiveTasks.
func (s *Scheduler) requeueOrFailLocked(task *Task) {
	if task.Attempts < task.MaxRetries {
		task.Status = TaskQueued
		task.WorkerID = ""
	} else {
		task.Status = TaskFailed
		task.WorkerID = ""
	}
}

func (s *Scheduler) pickLeastLoadedWorkerLocked() *WorkerInfo {
	var best *WorkerInfo
	now := time.Now()
	for _, w := range s.workers {
		if now.Sub(w.LastHeartbeat) > WorkerTimeout {
			continue // treat as dead, skip
		}
		if best == nil || len(w.ActiveTasks) < len(best.ActiveTasks) {
			best = w
		}
	}
	return best
}

// CompleteTask marks a task finished and frees the worker's slot. It
// accepts the report regardless of which worker sent it; prefer
// CompleteTaskBy, which rejects stale reports.
func (s *Scheduler) CompleteTask(taskID string) error {
	return s.completeTask(taskID, "", false)
}

// CompleteTaskBy marks a task finished, but only if workerID is the
// worker the task is currently assigned to. A report from any other
// worker (e.g. one that was declared dead and whose task has since
// been reassigned) is ignored and ErrStaleReport is returned. A
// duplicate report from the current assignee for an already-completed
// task is treated as a harmless no-op.
func (s *Scheduler) CompleteTaskBy(taskID, workerID string) error {
	return s.completeTask(taskID, workerID, true)
}

func (s *Scheduler) completeTask(taskID, workerID string, checkWorker bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	task, ok := s.tasks[taskID]
	if !ok {
		return ErrTaskNotFound
	}
	if checkWorker && task.WorkerID != workerID {
		return ErrStaleReport
	}
	task.Status = TaskCompleted
	if w, ok := s.workers[task.WorkerID]; ok {
		delete(w.ActiveTasks, taskID)
	}
	return nil
}

// FailTask is called when a worker reports that a task's execution
// failed (as opposed to a dispatch-time failure, handled in dispatch,
// or a worker going silent, handled in reapDeadWorkers). It frees the
// worker's slot and, if attempts remain, requeues the task and
// immediately attempts redispatch to a different worker (mirroring
// reapDeadWorkers); once MaxRetries is exhausted it's marked
// permanently failed instead. Without the redispatch step, a requeued
// task would sit as TaskQueued forever, since nothing else
// periodically retries queued tasks.
//
// It accepts the report regardless of which worker sent it; prefer
// FailTaskBy, which rejects stale reports.
func (s *Scheduler) FailTask(taskID string) error {
	return s.failTask(taskID, "", false)
}

// FailTaskBy is FailTask restricted to the worker the task is
// currently assigned to. Reports from any other worker return
// ErrStaleReport and change nothing.
func (s *Scheduler) FailTaskBy(taskID, workerID string) error {
	return s.failTask(taskID, workerID, true)
}

func (s *Scheduler) failTask(taskID, workerID string, checkWorker bool) error {
	s.mu.Lock()
	task, ok := s.tasks[taskID]
	if !ok {
		s.mu.Unlock()
		return ErrTaskNotFound
	}
	if checkWorker && task.WorkerID != workerID {
		s.mu.Unlock()
		return ErrStaleReport
	}
	if w, ok := s.workers[task.WorkerID]; ok {
		delete(w.ActiveTasks, taskID)
	}
	s.requeueOrFailLocked(task)
	requeued := task.Status == TaskQueued
	s.mu.Unlock()

	if requeued {
		s.dispatch(task)
	}
	return nil
}

// MonitorWorkers should run in a goroutine. It periodically checks for
// workers that have gone silent past WorkerTimeout, marks them dead,
// and reassigns their in-flight tasks to healthy workers -- this is
// the core fault-recovery behavior of the system.
func (s *Scheduler) MonitorWorkers(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.reapDeadWorkers()
		}
	}
}

// Stop ends MonitorWorkers. It is safe to call more than once and from
// multiple goroutines; only the first call has any effect.
func (s *Scheduler) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})
}

func (s *Scheduler) reapDeadWorkers() {
	now := time.Now()

	var orphaned []*Task
	s.mu.Lock()
	for id, w := range s.workers {
		if now.Sub(w.LastHeartbeat) <= WorkerTimeout {
			continue
		}
		// Worker is dead: collect its in-flight tasks for reassignment.
		for taskID := range w.ActiveTasks {
			if t, ok := s.tasks[taskID]; ok && t.Status != TaskCompleted {
				orphaned = append(orphaned, t)
			}
		}
		delete(s.workers, id)
	}
	s.mu.Unlock()

	for _, t := range orphaned {
		s.mu.Lock()
		t.Status = TaskQueued
		t.WorkerID = ""
		requeue := t.Attempts < t.MaxRetries
		s.mu.Unlock()

		if requeue {
			s.dispatch(t)
		} else {
			s.mu.Lock()
			t.Status = TaskFailed
			s.mu.Unlock()
		}
	}
}

// Snapshot returns a copy of current task states, useful for tests and
// for the dashboard/CLI to display cluster state.
func (s *Scheduler) Snapshot() (tasks map[string]TaskStatus, workerCount int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tasks = make(map[string]TaskStatus, len(s.tasks))
	for id, t := range s.tasks {
		tasks[id] = t.Status
	}
	return tasks, len(s.workers)
}
