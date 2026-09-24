// Command status queries a scheduler replica for its current view of
// task states and worker count.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/rpc"
	"sort"

	"github.com/TiyaGarg06/DistribuQ/internal/scheduler"
)

func main() {
	addr := flag.String("scheduler", "localhost:8000", "scheduler replica address to query")
	flag.Parse()

	client, err := rpc.Dial("tcp", *addr)
	if err != nil {
		log.Fatalf("dial %s failed: %v", *addr, err)
	}
	defer client.Close()

	var reply scheduler.StatusReply
	if err := client.Call("SchedulerService.GetStatus", &scheduler.StatusArgs{}, &reply); err != nil {
		log.Fatalf("status query failed: %v", err)
	}

	fmt.Printf("workers: %d\n", reply.WorkerCount)
	if len(reply.Tasks) == 0 {
		fmt.Println("tasks: (none)")
		return
	}

	ids := make([]string, 0, len(reply.Tasks))
	for id := range reply.Tasks {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	fmt.Println("tasks:")
	for _, id := range ids {
		fmt.Printf("  %-20s %s\n", id, reply.Tasks[id])
	}
}
