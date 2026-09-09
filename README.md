
The `raft.Transport` and scheduler `dispatchFn` are both interfaces/injected
functions, not hardwired to a specific RPC library. Communication currently
runs over Go's standard library `net/rpc` (chosen so the algorithmic core
could be built and tested without external network access); migrating the
transport layer to gRPC is the next step and requires no changes to the
election or scheduling logic itself, only a new `Transport` implementation.

## Running locally

```bash
# terminal 1 — start a single scheduler replica
go run ./cmd/scheduler -id s1 -addr :8000

# terminal 2/3 — start workers, pointing at the scheduler
go run ./cmd/worker -id w1 -addr :9001 -scheduler localhost:8000
go run ./cmd/worker -id w2 -addr :9002 -scheduler localhost:8000

# terminal 4 — submit a task (payload is run as a shell command)
go run ./cmd/submit -id task1 -payload "echo hello && sleep 1" -schedulers localhost:8000
```

To run a multi-scheduler cluster (to see leader election / failover),
start multiple `cmd/scheduler` processes with `-peers id1=addr1,id2=addr2`
pointing at each other.

## Tests

```bash
go test ./... -v
```

Covers: single-leader election under normal operation, follower
convergence on the current leader, re-election after the leader stops,
even task distribution across idle workers, task reassignment when a
worker stops sending heartbeats, and the worker's shell-command
executor (success, non-zero exit, timeout, empty payload).

## Roadmap

- [ ] Swap `net/rpc` transport for gRPC (`.proto` definitions + generated
      stubs implementing the existing `Transport` interface)
- [ ] Persist raft term/vote state so a restarted node doesn't double-vote
- [x] Real task execution backends (not just the simulated placeholder)
- [ ] Chaos test harness (random worker/leader kills under load)

## License

MIT
