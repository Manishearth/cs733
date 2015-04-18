// This package contains a toy implementation of the Raft consensus algorithm
package raft

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"time"
)

// This file contains most of the core code for the Raft interface
// but not the actual working

type Lsn uint64  // Log sequence number, unique for all time.
type Data string // String for now, we can have more structured data (or byte data) later

// A log entry
type LogEntry struct {
	Lsn  Lsn
	Data Data
	Term uint
}

// A single Raft instance
type RaftServer struct {
	Id          uint
	CommitCh    chan LogEntry       // Committed entries go here
	EventCh     chan Signal         // RPCs, RPC responses, and other external messages go here
	Network     CommunicationHelper // Information about the network
	Log         []LogEntry
	Term        uint
	VotedFor    int
	CommitIndex int
	LastApplied uint
	N           uint // number of servers
	Leader      uint // Last known leader
	Persistence PersistenceHandler
}

// Interface for specifying a state
type State interface {
	__stateAssert() // To get type safety
}

// RPC-like async interface for sending a signal to another
// Raft server
type CommunicationHelper interface {
	Send(signal Signal, id uint)
	Setup() // In case it needs to register rpcs
}

type Signal interface {
	__signalAssert() // To get type safety
}
type Response interface {
	__responseAssert() // To get type safety
	__signalAssert()   // All Responses are signals
}

// A message, with an ack
//
// The ack is not always needed, but this model becomes useful for replacing
// stuff with an RPC later
type ChanMessage struct {
	ack    chan Response
	signal Signal
}

// A specific communication helper designed to work
// for a channel-based network. Can be swapped with
// an RPC framework
type ChanCommunicationHelper struct {
	chans []chan Signal
}

func (c ChanCommunicationHelper) Send(signal Signal, id uint) {
	c.chans[id] <- signal
}

func (c ChanCommunicationHelper) Setup() {
}

// Handles Raft persistence. Save() will be called
// whenever persistent state needs updating
// Be sure to run blocking IO in a separate goroutine
type PersistenceHandler interface {
	Save(raft *RaftServer)
}

type NoPersistence struct{}

func (p NoPersistence) Save(raft *RaftServer) {
	// do nothing
}

// Makes a number of raft servers as a single network
func MakeRafts(count uint) []RaftServer {
	rand.Seed(time.Now().UTC().UnixNano())
	network := make([]chan Signal, count)
	for i := uint(0); i < count; i++ {
		network[i] = make(chan Signal, 1000)
	}

	servers := make([]RaftServer, count)

	for i := uint(0); i < count; i++ {
		commit := make(chan LogEntry, 100)
		log := make([]LogEntry, 0)
		servers[i] = RaftServer{
			Id:          i,
			CommitCh:    commit,
			EventCh:     network[i],
			Network:     ChanCommunicationHelper{network},
			Log:         log,
			Term:        0,
			VotedFor:    -1,
			CommitIndex: -1,
			LastApplied: 0,
			N:           count,
			Leader:      0,
			Persistence: NoPersistence{},
		}
	}
	return servers
}

// RPC based network communication helper
// To be used when different raft instances are spread out across servers
// This will *not* handle restarting the raft instance after a server crash,
// use a cronjob or setup job to do somethng similar
type NetCommunicationHelper struct {
	Id      uint          // Self id
	Hosts   []string      // Hostnames of all servers
	Ports   []uint        // Ports of all servers
	Handler NetRPCHandler // Handle to parent raft event chan
}

type NetRPCHandler struct {
	EventCh chan<- Signal
}

func (c NetCommunicationHelper) Send(signal Signal, id uint) {
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%v:%v", c.Hosts[id], c.Ports[id]))
	if err != nil {
		// Server unavailable
		// Nothing we need to worry about, raft will find out anyway
		return
	}
	var reply bool
	err = client.Call("NetRPCHandler.Recv", &signal, &reply)
	if err != nil {
		// The message type wasn't registered or something
		log.Fatal("RPC internal error ", err)
	}
	client.Close()
	// We don't care about the RPC reply
}

func (c NetCommunicationHelper) Setup() {
	Register()
	rpc.Register(&c.Handler)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", fmt.Sprintf(":%v", c.Ports[c.Id]))
	if e != nil {
		// Someone else is using the port, or something
		// We failed to setup, so we should just crash
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *NetRPCHandler) Recv(arg *Signal, reply *bool) error {
	c.EventCh <- *arg
	*reply = true
	return nil
}

func Register() {
	gob.Register(TimeoutEvent{})
	gob.Register(ClientAppendEvent{})
	gob.Register(ClientAppendResponse{})
	gob.Register(AppendRPCEvent{})
	gob.Register(AppendRPCResponse{})
	gob.Register(DebugEvent{})
	gob.Register(DebugResponse{})
	gob.Register(VoteRequestEvent{})
	gob.Register(VoteResponse{})
	gob.Register(HeartBeatEvent{})
	gob.Register(DisconnectEvent{})
	gob.Register(ReconnectEvent{})
	gob.Register(LogEntry{Lsn: Lsn(0), Data: Data(""), Term: 0})
}
