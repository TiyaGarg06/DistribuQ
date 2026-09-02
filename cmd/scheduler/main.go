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

	stopCh chan struct{}
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
// least-loaded healthy worker.
func (s *Scheduler) SubmitTask(id, payload string) *Task {
	s.mu.Lock()
	task := &Task{ID: id, Payload: payload, Status: TaskQueued, MaxRetries: 3}
	s.tasks[id] = task
	s.mu.Unlock()

	_ = s.dispatch(task)
	return task
}

// dispatch picks the least-loaded worker and hands the task to it. If
// dispatchFn fails, the tentative assignment is rolled back and the
// task is requeued (or marked failed past MaxRetries) so it never gets
// stuck as TaskAssigned on a worker that never actually received it.
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
		if task.Attempts < task.MaxRetries {
			task.Status = TaskQueued
			task.WorkerID = ""
		} else {
			task.Status = TaskFailed
		}
		s.mu.Unlock()
		return err
	}
	return nil
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

// CompleteTask marks a task finished and frees the worker's slot.
func (s *Scheduler) CompleteTask(taskID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	task, ok := s.tasks[taskID]
	if !ok {
		return ErrTaskNotFound
	}
	task.Status = TaskCompleted
	if w, ok := s.workers[task.WorkerID]; ok {
		delete(w.ActiveTasks, taskID)
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

func (s *Scheduler) Stop() {
	close(s.stopCh)
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
