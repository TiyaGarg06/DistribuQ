package raft

import (
	"fmt"
	"net/rpc"
	"sync"
)

// RPCTransport implements Transport using Go's standard library net/rpc.
//
// NOTE: this is an intentional placeholder transport. The election
// algorithm in raft.go is fully decoupled from the transport (see the
// Transport interface), so swapping this for a gRPC-based transport
// later is a matter of writing a GRPCTransport that implements the
// same interface -- no changes needed to the election logic itself.
type RPCTransport struct {
	mu        sync.Mutex
	peerAddrs map[string]string
	conns     map[string]*rpc.Client
}

func NewRPCTransport(peerAddrs map[string]string) *RPCTransport {
	return &RPCTransport{
		peerAddrs: peerAddrs,
		conns:     make(map[string]*rpc.Client),
	}
}

func (t *RPCTransport) client(peerID string) (*rpc.Client, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if c, ok := t.conns[peerID]; ok {
		return c, nil
	}
	addr, ok := t.peerAddrs[peerID]
	if !ok {
		return nil, fmt.Errorf("raft: unknown peer %q", peerID)
	}
	c, err := rpc.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	t.conns[peerID] = c
	return c, nil
}

// invalidate drops a cached (likely dead) connection so the next call
// re-dials -- this is what lets the cluster recover automatically once
// a crashed peer comes back up.
func (t *RPCTransport) invalidate(peerID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c, ok := t.conns[peerID]; ok {
		c.Close()
		delete(t.conns, peerID)
	}
}

func (t *RPCTransport) RequestVote(peerID string, args *RequestVoteArgs) (*RequestVoteReply, error) {
	c, err := t.client(peerID)
	if err != nil {
		return nil, err
	}
	reply := &RequestVoteReply{}
	if err := c.Call("RaftService.RequestVote", args, reply); err != nil {
		t.invalidate(peerID)
		return nil, err
	}
	return reply, nil
}

func (t *RPCTransport) Heartbeat(peerID string, args *HeartbeatArgs) (*HeartbeatReply, error) {
	c, err := t.client(peerID)
	if err != nil {
		return nil, err
	}
	reply := &HeartbeatReply{}
	if err := c.Call("RaftService.Heartbeat", args, reply); err != nil {
		t.invalidate(peerID)
		return nil, err
	}
	return reply, nil
}

// RaftService is the RPC-exported wrapper around a Node, registered on
// each cluster member's rpc.Server so peers can call RequestVote/Heartbeat
// on it.
type RaftService struct {
	Node *Node
}

func (s *RaftService) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	*reply = *s.Node.HandleRequestVote(args)
	return nil
}

func (s *RaftService) Heartbeat(args *HeartbeatArgs, reply *HeartbeatReply) error {
	*reply = *s.Node.HandleHeartbeat(args)
	return nil
}
