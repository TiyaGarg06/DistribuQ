// Package raft implements a simplified Raft-inspired leader election
// protocol used to elect a single active scheduler among a cluster of
// scheduler replicas, so the system tolerates a scheduler crash without
// losing the ability to dispatch tasks.
package raft

import (
	"math/rand"
	"sync"
	"time"
)

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}

// Transport abstracts sending RequestVote / Heartbeat RPCs to a peer.
// A concrete implementation (net/rpc today, gRPC later) satisfies this
// interface so the election algorithm itself stays transport-agnostic.
type Transport interface {
	RequestVote(peerID string, args *RequestVoteArgs) (*RequestVoteReply, error)
	Heartbeat(peerID string, args *HeartbeatArgs) (*HeartbeatReply, error)
}

type RequestVoteArgs struct {
	Term        int
	CandidateID string
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type HeartbeatArgs struct {
	Term     int
	LeaderID string
}

type HeartbeatReply struct {
	Term    int
	Success bool
}

// Node is a single participant in the leader election cluster.
type Node struct {
	mu sync.Mutex

	ID    string
	Peers []string

	currentTerm int
	votedFor    string
	state       State
	leaderID    string

	transport Transport

	electionResetAt time.Time
	electionTimeout time.Duration

	stopCh chan struct{}

	// onBecomeLeader / onBecomeFollower let the scheduler react to
	// role changes (e.g. start/stop accepting task submissions).
	onBecomeLeader   func()
	onBecomeFollower func()
}

func NewNode(id string, peers []string, transport Transport) *Node {
	return &Node{
		ID:              id,
		Peers:           peers,
		state:           Follower,
		transport:       transport,
		electionTimeout: randomElectionTimeout(),
		stopCh:          make(chan struct{}),
	}
}

func (n *Node) OnBecomeLeader(fn func())   { n.onBecomeLeader = fn }
func (n *Node) OnBecomeFollower(fn func()) { n.onBecomeFollower = fn }

func randomElectionTimeout() time.Duration {
	// 150-300ms range, randomized per node so simultaneous elections
	// (split votes) are rare, mirroring the standard Raft approach.
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

func (n *Node) State() State {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state
}

func (n *Node) Term() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

func (n *Node) LeaderID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

// Run starts the election timeout loop and heartbeat loop (if leader).
// It blocks until Stop() is called, so callers should run it in a
// goroutine.
func (n *Node) Run() {
	n.resetElectionTimer()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-n.stopCh:
			return
		case <-ticker.C:
			n.tick()
		}
	}
}

func (n *Node) Stop() {
	close(n.stopCh)
}

func (n *Node) tick() {
	n.mu.Lock()
	state := n.state
	timedOut := time.Since(n.electionResetAt) > n.electionTimeout
	n.mu.Unlock()

	switch state {
	case Leader:
		n.sendHeartbeats()
	default:
		if timedOut {
			n.startElection()
		}
	}
}

func (n *Node) resetElectionTimer() {
	n.mu.Lock()
	n.electionResetAt = time.Now()
	n.electionTimeout = randomElectionTimeout()
	n.mu.Unlock()
}

// startElection transitions this node to Candidate and requests votes
// from all peers in parallel. If it wins a majority, it becomes Leader.
func (n *Node) startElection() {
	n.mu.Lock()
	n.state = Candidate
	n.currentTerm++
	n.votedFor = n.ID
	term := n.currentTerm
	peers := append([]string{}, n.Peers...)
	n.mu.Unlock()

	n.resetElectionTimer()

	votes := 1 // vote for self
	var voteMu sync.Mutex
	var wg sync.WaitGroup

	for _, peer := range peers {
		wg.Add(1)
		go func(peerID string) {
			defer wg.Done()
			reply, err := n.transport.RequestVote(peerID, &RequestVoteArgs{
				Term:        term,
				CandidateID: n.ID,
			})
			if err != nil || reply == nil {
				return
			}
			n.mu.Lock()
			if reply.Term > n.currentTerm {
				n.currentTerm = reply.Term
				n.state = Follower
				n.votedFor = ""
			}
			n.mu.Unlock()

			if reply.VoteGranted {
				voteMu.Lock()
				votes++
				voteMu.Unlock()
			}
		}(peer)
	}
	wg.Wait()

	n.mu.Lock()
	defer n.mu.Unlock()

	majority := (len(peers)+1)/2 + 1
	if n.state == Candidate && n.currentTerm == term && votes >= majority {
		n.state = Leader
		n.leaderID = n.ID
		if n.onBecomeLeader != nil {
			go n.onBecomeLeader()
		}
	}
}

func (n *Node) sendHeartbeats() {
	n.mu.Lock()
	term := n.currentTerm
	peers := append([]string{}, n.Peers...)
	n.mu.Unlock()

	for _, peer := range peers {
		go func(peerID string) {
			reply, err := n.transport.Heartbeat(peerID, &HeartbeatArgs{
				Term:     term,
				LeaderID: n.ID,
			})
			if err != nil || reply == nil {
				return
			}
			n.mu.Lock()
			if reply.Term > n.currentTerm {
				n.currentTerm = reply.Term
				n.state = Follower
				n.leaderID = ""
				n.electionResetAt = time.Now()
				n.mu.Unlock()
				if n.onBecomeFollower != nil {
					n.onBecomeFollower()
				}
				return
			}
			n.mu.Unlock()
		}(peer)
	}
}

// HandleRequestVote is invoked (via the transport's RPC server side)
// when a peer asks this node for its vote.
func (n *Node) HandleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	if args.Term < n.currentTerm {
		return &RequestVoteReply{Term: n.currentTerm, VoteGranted: false}
	}
	if args.Term > n.currentTerm {
		n.currentTerm = args.Term
		n.state = Follower
		n.votedFor = ""
	}

	granted := n.votedFor == "" || n.votedFor == args.CandidateID
	if granted {
		n.votedFor = args.CandidateID
		n.electionResetAt = time.Now()
	}
	return &RequestVoteReply{Term: n.currentTerm, VoteGranted: granted}
}

// HandleHeartbeat is invoked when this node receives a leader heartbeat.
func (n *Node) HandleHeartbeat(args *HeartbeatArgs) *HeartbeatReply {
	n.mu.Lock()

	if args.Term < n.currentTerm {
		reply := &HeartbeatReply{Term: n.currentTerm, Success: false}
		n.mu.Unlock()
		return reply
	}

	wasLeaderOrCandidate := n.state != Follower
	n.currentTerm = args.Term
	n.state = Follower
	n.leaderID = args.LeaderID
	n.electionResetAt = time.Now()
	n.mu.Unlock()

	if wasLeaderOrCandidate && n.onBecomeFollower != nil {
		n.onBecomeFollower()
	}
	return &HeartbeatReply{Term: args.Term, Success: true}
}
