// This package contains a toy implementation of the Raft consensus algorithm
package raft

import (
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

// A generic log entry
type LogEntry interface {
	Lsn() Lsn
	Data() Data
	// Committed() bool
	Term() uint
}

// Simple string entry, can be swapped
// for a better structure if needed
type StringEntry struct {
	lsn  Lsn
	data Data
	// committed bool
	term uint
}

func (e StringEntry) Lsn() Lsn {
	return e.lsn
}

func (e StringEntry) Data() Data {
	return e.data
}

func (e StringEntry) Term() uint {
	return e.term
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
	EventCh chan<- Signal // Handle to parent raft event chan
}

func (c NetCommunicationHelper) Send(signal Signal, id uint) {
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%v:%v", c.Hosts[id], c.Ports[id]))
	if err != nil {
		// Server unavailable
		// Nothing we need to worry about, raft will find out anyway
		return
	}
	var reply bool
	client.Call("NetCommunicationHelper.RecvRPC", &signal, &reply)
	// We don't care about the RPC reply
}

func (c NetCommunicationHelper) Setup() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", fmt.Sprintf(":%v", c.Ports[c.Id]))
	if e != nil {
		// Someone else is using the port, or something
		// We failed to setup, so we should just crash
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *NetCommunicationHelper) RecvRPC(arg *Signal, reply *bool) error {
	c.EventCh <- *arg
	*reply = true
	return nil
}
