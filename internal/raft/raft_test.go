package raft

import (
	"sync"
	"testing"
	"time"
)

// fakeTransport routes RequestVote/Heartbeat calls directly to in-memory
// Node instances, so election behavior can be tested deterministically
// without opening real sockets.
type fakeTransport struct {
	mu    sync.Mutex
	nodes map[string]*Node
}

func newFakeTransport() *fakeTransport {
	return &fakeTransport{nodes: make(map[string]*Node)}
}

func (f *fakeTransport) register(n *Node) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nodes[n.ID] = n
}

func (f *fakeTransport) RequestVote(peerID string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	f.mu.Lock()
	peer, ok := f.nodes[peerID]
	f.mu.Unlock()
	if !ok {
		return nil, errNoPeer
	}
	return peer.HandleRequestVote(args), nil
}

func (f *fakeTransport) Heartbeat(peerID string, args *HeartbeatArgs) (*HeartbeatReply, error) {
	f.mu.Lock()
	peer, ok := f.nodes[peerID]
	f.mu.Unlock()
	if !ok {
		return nil, errNoPeer
	}
	return peer.HandleHeartbeat(args), nil
}

var errNoPeer = &peerError{"no such peer"}

type peerError struct{ msg string }

func (e *peerError) Error() string { return e.msg }

func newCluster(t *testing.T, ids []string) (*fakeTransport, []*Node) {
	t.Helper()
	ft := newFakeTransport()
	nodes := make([]*Node, len(ids))
	for i, id := range ids {
		var peers []string
		for _, other := range ids {
			if other != id {
				peers = append(peers, other)
			}
		}
		n := NewNode(id, peers, ft)
		nodes[i] = n
		ft.register(n)
	}
	return ft, nodes
}

func TestSingleLeaderElected(t *testing.T) {
	_, nodes := newCluster(t, []string{"n1", "n2", "n3"})

	for _, n := range nodes {
		go n.Run()
		defer n.Stop()
	}

	deadline := time.After(2 * time.Second)
	for {
		leaders := 0
		for _, n := range nodes {
			if n.State() == Leader {
				leaders++
			}
		}
		if leaders == 1 {
			break // exactly one leader elected -- test passes
		}
		if leaders > 1 {
			t.Fatalf("split brain: %d nodes claim leadership simultaneously", leaders)
		}
		select {
		case <-deadline:
			t.Fatal("no leader elected within timeout")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestFollowersRecognizeLeader(t *testing.T) {
	_, nodes := newCluster(t, []string{"n1", "n2", "n3"})

	for _, n := range nodes {
		go n.Run()
		defer n.Stop()
	}

	time.Sleep(1 * time.Second)

	var leaderID string
	for _, n := range nodes {
		if n.State() == Leader {
			leaderID = n.ID
		}
	}
	if leaderID == "" {
		t.Fatal("no leader elected")
	}

	for _, n := range nodes {
		if n.State() == Follower && n.LeaderID() != leaderID {
			t.Errorf("follower %s does not recognize leader %s (sees %q)", n.ID, leaderID, n.LeaderID())
		}
	}
}

func TestNewLeaderElectedAfterLeaderStops(t *testing.T) {
	_, nodes := newCluster(t, []string{"n1", "n2", "n3"})

	for _, n := range nodes {
		go n.Run()
	}

	// Wait for initial leader.
	time.Sleep(1 * time.Second)

	var firstLeader *Node
	for _, n := range nodes {
		if n.State() == Leader {
			firstLeader = n
		}
	}
	if firstLeader == nil {
		t.Fatal("no initial leader elected")
	}
	firstLeader.Stop()

	// A surviving node should take over within a couple of election
	// timeout windows.
	deadline := time.After(2 * time.Second)
	for {
		newLeader := ""
		for _, n := range nodes {
			if n == firstLeader {
				continue
			}
			if n.State() == Leader {
				newLeader = n.ID
			}
		}
		if newLeader != "" && newLeader != firstLeader.ID {
			for _, n := range nodes {
				if n != firstLeader {
					n.Stop()
				}
			}
			return // success
		}
		select {
		case <-deadline:
			t.Fatal("no new leader elected after original leader stopped")
		case <-time.After(20 * time.Millisecond):
		}
	}
}
