// Command worker runs a single worker node: it registers itself with
// the scheduler, sends periodic heartbeats, and executes tasks pushed
// to it over RPC. Each task's payload is run as a shell command (see
// internal/worker.NewShellExecutor).
package main

import (
	"flag"
	"log"
	"net"
	"net/rpc"
	"time"

	"github.com/TiyaGarg06/DistribuQ/internal/worker"
)

func main() {
	id := flag.String("id", "", "unique ID for this worker")
	addr := flag.String("addr", ":9000", "address to listen on for RPC")
	schedulerAddr := flag.String("scheduler", "localhost:8000", "address of the scheduler leader")
	timeout := flag.Duration("timeout", worker.DefaultExecTimeout, "max duration a single task's command may run before being killed")
	flag.Parse()

	if *id == "" {
		log.Fatal("worker: -id is required")
	}

	w := &worker.Worker{
		ID:            *id,
		SchedulerAddr: *schedulerAddr,
		Addr:          *addr,
		Execute:       worker.NewShellExecutor(*id, *timeout),
	}

	server := rpc.NewServer()
	if err := server.RegisterName("WorkerService", worker.NewWorkerService(w)); err != nil {
		log.Fatalf("failed to register WorkerService: %v", err)
	}

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", *addr, err)
	}
	log.Printf("[%s] worker listening on %s, reporting to scheduler at %s", *id, *addr, *schedulerAddr)

	stopCh := make(chan struct{})
	go w.StartHeartbeatLoop(1*time.Second, stopCh)

	// If the scheduler isn't reachable yet, that's fine: the heartbeat
	// loop registers this worker as soon as the scheduler answers.
	if err := w.Register(); err != nil {
		log.Printf("worker %s: initial registration failed, will retry via heartbeats: %v", *id, err)
	}

	server.Accept(listener)
}
