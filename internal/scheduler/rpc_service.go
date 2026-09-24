package scheduler

import "errors"

// ErrNotLeader is returned by SchedulerService RPC methods that
// require leadership (currently SubmitTask) when this replica is not
// the current Raft leader. Callers (e.g. cmd/submit) can use this to
// detect a misdirected request and retry against the real leader.
var ErrNotLeader = errors.New("scheduler: this replica is not the leader")

// SchedulerService is the RPC-exported wrapper around a Scheduler,
// registered on every scheduler replica so workers can register,
// heartbeat, and report task completion over the network. IsLeader
// gates the operations (currently just SubmitTask) that must only be
// accepted by the current Raft leader -- without it, a client pointed
// at a follower would have that follower independently track and
// dispatch tasks against the shared worker pool.
type SchedulerService struct {
	S        *Scheduler
	IsLeader func() bool
}

func (s *SchedulerService) requireLeader() bool {
	return s.IsLeader == nil || s.IsLeader()
}

type RegisterArgs struct {
	WorkerID string
	Addr     string
}

type OKReply struct{ OK bool }

func (s *SchedulerService) RegisterWorker(args *RegisterArgs, reply *OKReply) error {
	s.S.RegisterWorker(args.WorkerID, args.Addr)
	reply.OK = true
	return nil
}

type HeartbeatArgs struct {
	WorkerID string
}

func (s *SchedulerService) Heartbeat(args *HeartbeatArgs, reply *OKReply) error {
	s.S.Heartbeat(args.WorkerID)
	reply.OK = true
	return nil
}

type CompleteArgs struct {
	TaskID   string
	WorkerID string
}

func (s *SchedulerService) CompleteTask(args *CompleteArgs, reply *OKReply) error {
	err := s.S.CompleteTask(args.TaskID)
	reply.OK = err == nil
	return err
}

// FailArgs carries the reason a worker gives for reporting a task as
// failed. Cause is informational only (logged), not parsed by the
// scheduler.
type FailArgs struct {
	TaskID   string
	WorkerID string
	Cause    string
}

func (s *SchedulerService) FailTask(args *FailArgs, reply *OKReply) error {
	err := s.S.FailTask(args.TaskID)
	reply.OK = err == nil
	return err
}

type SubmitArgs struct {
	TaskID  string
	Payload string
}

func (s *SchedulerService) SubmitTask(args *SubmitArgs, reply *OKReply) error {
	if !s.requireLeader() {
		reply.OK = false
		return ErrNotLeader
	}
	s.S.SubmitTask(args.TaskID, args.Payload)
	reply.OK = true
	return nil
}

// StatusArgs is empty -- GetStatus takes no parameters, it just
// reports this replica's current local view of the world.
type StatusArgs struct{}

// StatusReply mirrors Scheduler.Snapshot()'s return values so they
// can cross the RPC boundary.
type StatusReply struct {
	Tasks       map[string]TaskStatus
	WorkerCount int
}

// GetStatus reports this replica's local task states and worker
// count. Available on every replica, not just the leader: a follower
// answering truthfully about its own (possibly stale, since there's
// no real log replication yet) state is still useful for confirming
// it's alive and roughly caught up.
func (s *SchedulerService) GetStatus(args *StatusArgs, reply *StatusReply) error {
	tasks, workerCount := s.S.Snapshot()
	reply.Tasks = tasks
	reply.WorkerCount = workerCount
	return nil
}
