package raft

import (
	"math/rand"
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
	EventCh     chan ChanMessage    // RPCs, RPC responses, and other external messages go here
	Network     CommunicationHelper // Information about the network
	Log         []LogEntry
	Term        uint
	VotedFor    int
	CommitIndex int
	LastApplied uint
}

// Interface for specifying a state
type State interface {
	__stateAssert() // To get type safety
}

// RPC-like async interface for sending a signal to another
// Raft server
type CommunicationHelper interface {
	Send(signal Signal, id uint) chan Response
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
	chans []chan ChanMessage
}

func (c ChanCommunicationHelper) Send(signal Signal, id uint) chan Response {
	ack := make(chan Response, 1)
	message := ChanMessage{ack, signal}
	c.chans[id] <- message
	return ack
}

// Makes a number of raft servers as a single network
func MakeRafts(count uint) []RaftServer {
	rand.Seed(time.Now().UTC().UnixNano())
	if count != 5 {
		panic("Current code is hardcoded for 5 servers")
	}
	network := make([]chan ChanMessage, count)
	for i := uint(0); i < count; i++ {
		network[i] = make(chan ChanMessage, 1000)
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
		}
	}
	return servers
}
