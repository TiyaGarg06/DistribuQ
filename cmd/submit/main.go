// Command submit is a small CLI client for submitting a task to a
// running scheduler cluster, used for manual testing/demo purposes.
// Since only the current Raft leader accepts submissions (see
// SchedulerService.SubmitTask), this tries each given address in turn
// until one succeeds, rather than requiring the caller to already
// know which replica is leader.
package main

import (
	"flag"
	"log"
	"net/rpc"
	"strings"

	"github.com/TiyaGarg06/DistribuQ/internal/scheduler"
)

func main() {
	addrsFlag := flag.String("schedulers", "localhost:8000", "comma-separated scheduler replica addresses to try")
	id := flag.String("id", "", "task id (required)")
	payload := flag.String("payload", "hello", "task payload")
	flag.Parse()

	if *id == "" {
		log.Fatal("submit: -id is required")
	}

	addrs := strings.Split(*addrsFlag, ",")
	args := struct{ TaskID, Payload string }{TaskID: *id, Payload: *payload}

	var lastErr error
	for _, addr := range addrs {
		addr = strings.TrimSpace(addr)
		if addr == "" {
			continue
		}

		client, err := rpc.Dial("tcp", addr)
		if err != nil {
			lastErr = err
			log.Printf("dial %s failed: %v, trying next", addr, err)
			continue
		}

		var reply struct{ OK bool }
		callErr := client.Call("SchedulerService.SubmitTask", &args, &reply)
		client.Close()

		if callErr == nil {
			log.Printf("submitted task %s to %s: ok=%v", *id, addr, reply.OK)
			return
		}

		lastErr = callErr
		// net/rpc serializes errors as plain strings and reconstructs
		// them as generic errors on the client side, so the original
		// sentinel value doesn't survive the round trip -- errors.Is
		// against scheduler.ErrNotLeader would never match here.
		// Comparing the message text is the reliable option for a
		// net/rpc-based service.
		if callErr.Error() == scheduler.ErrNotLeader.Error() {
			log.Printf("%s is not the leader, trying next", addr)
			continue
		}
		log.Printf("submit to %s failed: %v, trying next", addr, callErr)
	}
	log.Fatalf("submit failed against all %d address(es): %v", len(addrs), lastErr)
}
