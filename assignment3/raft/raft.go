package raft

// This file contains most of the code for the Raft interface

type Lsn uint64  // Log sequence number, unique for all time.
type Data string // String for now, we can have more structured data (or byte data) later

type LogEntry interface {
	Lsn() Lsn
	Data() Data
	// Committed() bool
	Term() uint
}

// Simple string entry
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

/*
func (e StringEntry) Committed() bool {
	return e.committed
}
*/

func (e StringEntry) Term() uint {
	return e.term
}

type RaftServer struct {
	Id          uint
	CommitCh    chan Data
	EventCh     chan ChanMessage
	Network     CommunicationHelper
	Log         []LogEntry
	Term        uint
	VotedFor    int
	CommitIndex int
	LastApplied uint
}

type State interface{}

type CommunicationHelper interface {
	Send(signal Signal, id uint) chan Response
}

type Signal interface{}
type Response interface{}

type ChanMessage struct {
	ack    chan Response
	signal Signal
}

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
	network := make([]chan ChanMessage, count)
	for i := uint(0); i < count; i++ {
		network[i] = make(chan ChanMessage, 1000)
	}

	servers := make([]RaftServer, count)

	for i := uint(0); i < count; i++ {
		commit := make(chan Data, 1000)
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
