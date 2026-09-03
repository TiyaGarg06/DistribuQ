// Package worker implements the worker-side RPC service: it accepts
// tasks pushed by the scheduler, executes them, and reports completion.
package worker

import (
	"log"
	"net/rpc"
	"time"
)

type ExecuteArgs struct {
	TaskID  string
	Payload string
}

type ExecuteReply struct {
	Accepted bool
}

// TaskExecutor runs a task's payload. Swappable so tests can inject a
// no-op executor instead of doing real work.
type TaskExecutor func(taskID, payload string) error

type Worker struct {
	ID            string
	SchedulerAddr string
	Execute       TaskExecutor
}

// WorkerService is the RPC-exported wrapper the scheduler calls into.
type WorkerService struct {
	w *Worker
}

// NewWorkerService wraps a Worker so it can be registered on an
// rpc.Server and receive ExecuteTask calls from the scheduler.
func NewWorkerService(w *Worker) *WorkerService {
	return &WorkerService{w: w}
}

func (ws *WorkerService) ExecuteTask(args *ExecuteArgs, reply *ExecuteReply) error {
	reply.Accepted = true
	go func() {
		if err := ws.w.Execute(args.TaskID, args.Payload); err != nil {
			log.Printf("worker %s: task %s failed: %v", ws.w.ID, args.TaskID, err)
			if rerr := ws.w.reportFailure(args.TaskID, err); rerr != nil {
				log.Printf("worker %s: failed to report failure for %s: %v", ws.w.ID, args.TaskID, rerr)
			}
			return
		}
		if err := ws.w.reportCompletion(args.TaskID); err != nil {
			log.Printf("worker %s: failed to report completion for %s: %v", ws.w.ID, args.TaskID, err)
		}
	}()
	return nil
}

// reportCompletion calls back into the scheduler's RPC service to mark
// a task done.
func (w *Worker) reportCompletion(taskID string) error {
	client, err := rpc.Dial("tcp", w.SchedulerAddr)
	if err != nil {
		return err
	}
	defer client.Close()

	var reply struct{ OK bool }
	return client.Call("SchedulerService.CompleteTask", &CompleteArgs{TaskID: taskID, WorkerID: w.ID}, &reply)
}

type CompleteArgs struct {
	TaskID   string
	WorkerID string
}

// reportFailure calls back into the scheduler's RPC service to report
// that this task's execution failed, so the scheduler can requeue it
// (or mark it permanently failed) instead of leaving it stuck as
// assigned to this worker forever.
func (w *Worker) reportFailure(taskID string, cause error) error {
	client, err := rpc.Dial("tcp", w.SchedulerAddr)
	if err != nil {
		return err
	}
	defer client.Close()

	var reply struct{ OK bool }
	return client.Call("SchedulerService.FailTask", &FailArgs{TaskID: taskID, WorkerID: w.ID, Cause: cause.Error()}, &reply)
}

type FailArgs struct {
	TaskID   string
	WorkerID string
	Cause    string
}

// StartHeartbeatLoop periodically pings the scheduler so it knows this
// worker is alive; missing heartbeats trigger task reassignment on the
// scheduler side (see scheduler.MonitorWorkers).
func (w *Worker) StartHeartbeatLoop(interval time.Duration, stopCh <-chan struct{}) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			w.sendHeartbeat()
		}
	}
}

func (w *Worker) sendHeartbeat() {
	client, err := rpc.Dial("tcp", w.SchedulerAddr)
	if err != nil {
		log.Printf("worker %s: heartbeat dial failed: %v", w.ID, err)
		return
	}
	defer client.Close()

	var reply struct{ OK bool }
	_ = client.Call("SchedulerService.Heartbeat", &HeartbeatArgs{WorkerID: w.ID}, &reply)
}

type HeartbeatArgs struct {
	WorkerID string
}
