# DistribuQ

A fault-tolerant distributed task scheduler written in Go. Multiple scheduler replicas elect a leader; the leader dispatches tasks to a pool of workers, detects dead workers through heartbeats, and reassigns their in-flight tasks.

Zero external dependencies: standard library only.

## Architecture

```
                 ┌────────────┐   RequestVote / Heartbeat   ┌────────────┐
   submit ─────▶ │ scheduler  │ ◀───────────────────────▶ │ scheduler  │
   (client)      │  (leader)  │                             │ (follower) │
                 └─────┬──────┘                             └────────────┘
          dispatch     │  ▲ register / heartbeat /
          (net/rpc)    │  │ complete / fail
                 ┌─────▼──┴───┐   ┌────────────┐
                 │  worker 1  │   │  worker 2  │ ...
                 └────────────┘   └────────────┘
```

| Package | Responsibility |
| --- | --- |
| `internal/raft` | Raft-style leader election (terms, randomized timeouts, RequestVote, heartbeats, majority quorum) behind a `Transport` interface |
| `internal/scheduler` | Task queue, least-loaded dispatch, retries (max 3 attempts), dead-worker detection and task reassignment |
| `internal/worker` | Worker RPC service, heartbeat loop, shell-command executor with timeout |
| `cmd/scheduler` | Runs one scheduler replica |
| `cmd/worker` | Runs one worker |
| `cmd/submit` | CLI to submit a task; tries each replica until it finds the leader |
| `cmd/status` | CLI to view a replica's tasks and worker count |

The `raft.Transport` interface and the scheduler's `dispatchFn` are injected, not hardwired to an RPC library. Communication currently uses Go's standard library `net/rpc`. Moving to gRPC means writing a new `Transport` implementation, with no changes to the election or scheduling logic.

## Task lifecycle

```
queued ──▶ assigned ──▶ completed
   ▲           │
   └───────────┤  worker failure / dispatch error / dead worker
               ▼  (requeued while attempts < MaxRetries)
             failed
```

## Running locally

```bash
# terminal 1: start a single scheduler replica
go run ./cmd/scheduler -id s1 -addr :8000

# terminals 2 and 3: start workers, pointing at the scheduler
go run ./cmd/worker -id w1 -addr :9001 -scheduler localhost:8000
go run ./cmd/worker -id w2 -addr :9002 -scheduler localhost:8000

# terminal 4: submit a task (payload is run as a shell command)
go run ./cmd/submit -id task1 -payload "echo hello && sleep 1" -schedulers localhost:8000

# check cluster state
go run ./cmd/status -scheduler localhost:8000
```

To run a multi-scheduler cluster (leader election and failover), start several `cmd/scheduler` processes with `-peers id1=addr1,id2=addr2` pointing at each other.

## Tests

```bash
go test -race ./... -v
```

Covers: single-leader election, follower convergence on the current leader, re-election after the leader stops, least-loaded dispatch, task reassignment when a worker stops sending heartbeats, and the shell executor (success, non-zero exit, timeout, empty payload).

## Current limitations

This is a working prototype. Known gaps:

- **Election only, no log replication.** Task state lives in the leader's memory and is not replicated. If the leader crashes, a new leader is elected but starts with an empty task queue.
- **Raft term/vote is not persisted**, so a restarted node can vote twice in the same term.
- **Workers register with a single scheduler address**, so after a failover they must be pointed at the new leader.
- **At-least-once execution.** A slow (not dead) worker declared dead can cause a task to run twice.
- **No authentication or TLS.** Task payloads run via `sh -c`, so only run this on a trusted network.

## Roadmap

- [x] Real task execution backend (shell executor)
- [x] Status CLI
- [ ] Ignore stale completion/failure reports from non-assigned workers
- [ ] Persist Raft term/vote state
- [ ] Workers accept multiple scheduler addresses and re-register on failover
- [ ] Replicate task state to followers
- [ ] Swap `net/rpc` for gRPC
- [ ] Chaos test harness (random worker/leader kills under load)

## License

MIT
