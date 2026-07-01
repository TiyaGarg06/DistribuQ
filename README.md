# DistribuQ

A distributed, fault-tolerant task scheduler in Go. A cluster of scheduler
replicas elects a single leader via a simplified Raft-inspired protocol;
the leader dispatches tasks to a pool of workers using a least-loaded
assignment strategy, monitors worker liveness via heartbeats, and
automatically reassigns in-flight tasks if a worker (or the leader
itself) crashes.

**Status: early / in progress.** Core election and scheduling logic is
implemented and tested (see `internal/raft` and `internal/scheduler`).
Worker execution is currently a placeholder (see `cmd/worker/main.go`)
pending real workload integration.

## Why

Built to get hands-on with distributed systems fundamentals — consensus,
failure detection, and fault recovery — that don't come up naturally in
application/ML-focused project work.

## Architecture

```
cmd/
  scheduler/   entrypoint for a scheduler replica (raft node + task dispatcher)
  worker/      entrypoint for a worker node
  submit/      CLI to submit a task to the current leader (for testing/demo)
internal/
  raft/        leader election: RequestVote/Heartbeat RPCs, term handling,
               randomized election timeouts, transport-agnostic (interface-based)
  scheduler/   worker registry, least-loaded task dispatch, heartbeat-based
               failure detection, automatic task reassignment on worker death
  worker/      worker-side RPC service: task execution + heartbeat loop
```

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

# terminal 4 — submit a task
go run ./cmd/submit -id task1 -payload "hello" -scheduler localhost:8000
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
even task distribution across idle workers, and task reassignment when
a worker stops sending heartbeats.

## Roadmap

- [ ] Swap `net/rpc` transport for gRPC (`.proto` definitions + generated
      stubs implementing the existing `Transport` interface)
- [ ] Persist raft term/vote state so a restarted node doesn't double-vote
- [ ] Real task execution backends (not just the simulated placeholder)
- [ ] Chaos test harness (random worker/leader kills under load)

## License

MIT
