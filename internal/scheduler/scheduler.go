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
	task := &Task{ID: id, Payload: payload, Status: TaskQueued,
