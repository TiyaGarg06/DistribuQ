// Command submit is a small CLI client for submitting a task to a
// running scheduler leader, used for manual testing/demo purposes.
package main

import (
	"flag"
	"log"
	"net/rpc"
)

func main() {
	addr := flag.String("scheduler", "localhost:8000", "scheduler address")
	id := flag.String("id", "", "task id")
	payload := flag.String("payload", "hello", "task payload")
	flag.Parse()

	client, err := rpc.Dial("tcp", *addr)
	if err != nil {
		log.Fatalf("dial failed: %v", err)
	}
	defer client.Close()

	args := struct{ TaskID, Payload string }{TaskID: *id, Payload: *payload}
	var reply struct{ OK bool }
	if err := client.Call("SchedulerService.SubmitTask", &args, &reply); err != nil {
		log.Fatalf("submit failed: %v", err)
	}
	log.Printf("submitted task %s: ok=%v", *id, reply.OK)
}
