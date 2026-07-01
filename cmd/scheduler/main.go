// Command scheduler runs one replica of the DistribuQ scheduler. Multiple
// replicas form a cluster; exactly one becomes leader (via the raft
// package) and accepts task submissions, while followers stand by to
// take over if the leader crashes.
package main

import (
	"flag"
	"log"
	"net"
	"net/rpc"
	"strings"
	"time"

	"github.com/TiyaGarg06/DistribuQ/internal/raft"
	"github.com/TiyaGarg06/DistribuQ/internal/scheduler"
)

func main() {
	id := flag.String("id", "", "unique ID for this scheduler replica")
	addr := flag.String("addr", ":8000", "address to listen on for RPC")
	peersFlag := flag.String("peers", "", "comma-separated id=addr pairs for other scheduler replicas")
	flag.Parse()

	if *id == "" {
		log.Fatal("scheduler: -id is required")
	}

	peerAddrs := parsePeers(*peersFlag)
	var peerIDs []string
	for pid := range peerAddrs {
		peerIDs = append(peerIDs, pid)
	}

	dispatch := func(workerID, workerAddr string, task *scheduler.Task) error {
		client, err := rpc.Dial("tcp", workerAddr)
		if err != nil {
			return err
		}
		defer client.Close()

		args := struct {
			TaskID  string
			Payload string
		}{TaskID: task.ID, Payload: task.Payload}
		var reply struct{ Accepted bool }
		return client.Call("WorkerService.ExecuteTask", &args, &reply)
	}

	sched := scheduler.NewScheduler(dispatch)
	go sched.MonitorWorkers(500 * time.Millisecond)

	transport := raft.NewRPCTransport(peerAddrs)
	node := raft.NewNode(*id, peerIDs, transport)
	node.OnBecomeLeader(func() {
		log.Printf("[%s] elected LEADER (term %d) -- now accepting task submissions", *id, node.Term())
	})
	node.OnBecomeFollower(func() {
		log.Printf("[%s] stepped down to FOLLOWER (leader is %q)", *id, node.LeaderID())
	})

	server := rpc.NewServer()
	if err := server.RegisterName("RaftService", &raft.RaftService{Node: node}); err != nil {
		log.Fatalf("failed to register RaftService: %v", err)
	}
	if err := server.RegisterName("SchedulerService", &scheduler.SchedulerService{S: sched}); err != nil {
		log.Fatalf("failed to register SchedulerService: %v", err)
	}

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", *addr, err)
	}
	log.Printf("[%s] scheduler replica listening on %s", *id, *addr)

	go node.Run()
	server.Accept(listener)
}

func parsePeers(s string) map[string]string {
	peers := make(map[string]string)
	if s == "" {
		return peers
	}
	for _, pair := range strings.Split(s, ",") {
		parts := strings.SplitN(pair, "=", 2)
		if len(parts) != 2 {
			continue
		}
		peers[parts[0]] = parts[1]
	}
	return peers
}
