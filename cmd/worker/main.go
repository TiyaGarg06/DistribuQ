// Command worker runs a single worker node: it registers itself with
// the scheduler, sends periodic heartbeats, and executes tasks pushed
// to it over RPC.
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
	flag.Parse()

	if *id == "" {
		log.Fatal("worker: -id is required")
	}

	w := &worker.Worker{
		ID:            *id,
		SchedulerAddr: *schedulerAddr,
		Execute: func(taskID, payload string) error {
			// Placeholder task execution -- real task logic (e.g.
			// running a job, processing a batch) goes here. Simulated
			// work delay stands in for now.
			log.Printf("[%s] executing task %s (payload=%q)", *id, taskID, payload)
			time.Sleep(200 * time.Millisecond)
			log.Printf("[%s] finished task %s", *id, taskID)
			return nil
		},
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

	registerWithScheduler(*schedulerAddr, *id, *addr)

	server.Accept(listener)
}

func registerWithScheduler(schedulerAddr, id, addr string) {
	client, err := rpc.Dial("tcp", schedulerAddr)
	if err != nil {
		log.Printf("worker %s: could not reach scheduler to register: %v", id, err)
		return
	}
	defer client.Close()

	args := struct {
		WorkerID string
		Addr     string
	}{WorkerID: id, Addr: addr}
	var reply struct{ OK bool }
	if err := client.Call("SchedulerService.RegisterWorker", &args, &reply); err != nil {
		log.Printf("worker %s: registration failed: %v", id, err)
	}
}
