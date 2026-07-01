package scheduler

// SchedulerService is the RPC-exported wrapper around a Scheduler,
// registered on the leader's rpc.Server so workers can register,
// heartbeat, and report task completion over the network.
type SchedulerService struct {
	S *Scheduler
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

type SubmitArgs struct {
	TaskID  string
	Payload string
}

func (s *SchedulerService) SubmitTask(args *SubmitArgs, reply *OKReply) error {
	s.S.SubmitTask(args.TaskID, args.Payload)
	reply.OK = true
	return nil
}
