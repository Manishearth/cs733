package raft

import (
	"strconv"
	"math/rand"
	"sync"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"log"
)

// This file contains most of the code for the Raft interface
// and SharedLog


type Lsn uint64      // Log sequence number, unique for all time.
type ErrRedirect int // See Log.Append. Implements Error interface.


// I have changed the API here so
// that I can use typed messages
type LogEntry struct {
	Lsn Lsn
	Data Message
	Committed bool
	votes int
}

type SharedLog interface {
	// Each data item is wrapped in a LogEntry with a unique
	// lsn. The only error that will be returned is ErrRedirect,
	// to indicate the server id of the leader. Append initiates
	// a local disk write and a broadcast to the other replicas,
	// and returns without waiting for the result.
	Append(data Message) (LogEntry, error)
}

// --------------------------------------
// Raft setup
type ServerConfig struct {
	Id         int    // Id of server. Must be unique
	Hostname   string // string name or ip of host
	ClientPort int    // int port at which server listens to client messages.
	LogPort    int    // tcp port for inter-replica protocol messages.
}

type ClusterConfig struct {
	Path    string         // Directory for persistent log
	Servers []ServerConfig // All servers in this cluster
}

// Raft implements the SharedLog interface.
type Raft struct {
	SelfId   int
	Config   ClusterConfig
	CommitCh chan LogEntry
	lock sync.Mutex
	log []LogEntry // Ordered set of log entries
	commitIndex int // Redundancy to avoid searching
}

// Creates a raft object. This implements the SharedLog interface.
// commitCh is the channel that the kvstore waits on for committed messages.
// When the process starts, the local disk log is read and all committed
// entries are recovered and replayed
func NewRaft(config *ClusterConfig, thisServerId int, commitCh chan LogEntry) (*Raft, error) {
	r := Raft{
		SelfId:   thisServerId,
		Config:   *config,
		CommitCh: commitCh,
		lock: sync.Mutex{},
		log: *new([]LogEntry),
		commitIndex: 0,
	}
	if thisServerId == 0 {
		go r.heartbeat()
	} else {
		go r.appendListener()
	}
	return &r, nil
}

// ErrRedirect as an Error object
func (e ErrRedirect) Error() string {
	return "Redirect to server " + strconv.Itoa(int(e))
}

// RPC
func (t *ClusterConfig) GetSetup(args *int, reply *ClusterConfig) error {
	reply.Path = t.Path
	reply.Servers = t.Servers
	return nil
}

func (t *Raft) Append(data Message) (*LogEntry, error) {
	if t.SelfId != 0 {
		return nil, ErrRedirect(t.Config.Servers[0].ClientPort)
	}
	logEntry := LogEntry {
		// Effectively a uuid
		Lsn: Lsn(rand.Int63()),
		Data: data,
		Committed: false,
		votes: 0,
	}
	go t.initAppend(logEntry)
	return &logEntry, nil
}

type RaftRPC Raft
type AppendEntriesRPCArg struct {
	Index int
	Entry LogEntry
}

type AppendEntriesRPCRet struct {
	Ack bool
	CurrentIndex int
}

func (t *Raft) initAppend(logEntry LogEntry) {

	t.lock.Lock()
	t.log = append(t.log, logEntry)
	t.lock.Unlock()
}

func (t *Raft) heartbeat() {
	followerStatus := make([]int, len(t.Config.Servers))
	commitStatus := make([]int, len(t.Config.Servers))
	for {
		// We can freely use the slice here
		// since the other goroutines are only appending to it,
		// and all indices that appear here will be lesser
		t.lock.Lock()
		l := len(t.log)
		t.lock.Unlock()
		for i:=1; i<len(t.Config.Servers); i++ {
			if followerStatus[i] == l {
				// Up to date
				continue
			}

			var ret AppendEntriesRPCRet
			args := AppendEntriesRPCArg {
				Index: followerStatus[i],
				Entry: t.log[followerStatus[i]],
			}
			server := t.Config.Servers[i]
			client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%v:%v", server.Hostname, server.LogPort))
			if err != nil {
				// Can't connect, we can try later
				continue
			}
			err = client.Call("RaftRPC.AppendEntriesRPC", &args, &ret)
			defer client.Close()
			if ret.Ack == true {
				t.log[followerStatus[i]].votes++
				followerStatus[i] = followerStatus[i] + 1
			} else {
				if followerStatus[i] >= ret.CurrentIndex {
					followerStatus[i] = ret.CurrentIndex
				} else {
					// Looks like the follower got the rpcs after all,
					// just that we missed the acks. We can fill them in
					for j := followerStatus[i]; j < ret.CurrentIndex; j++ {
						t.log[j].votes++
					}
				}
			}
			
		}
		
		for i := t.commitIndex; i < l; i++ {
			if t.log[i].votes < 2 {
				break;
			}
			t.log[i].Committed = true
			t.commitIndex++
			t.CommitCh <- t.log[i]
		}

		// Check commits
		for i:=1; i<len(t.Config.Servers); i++ {
			if commitStatus[i] == l {
				// Up to date
				continue
			}
			if !t.log[commitStatus[i]].Committed {
				// Up to date
				continue
			}
			server := t.Config.Servers[i]
			client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%v:%v", server.Hostname, server.LogPort))
			defer client.Close()
			if err != nil {
				// Can't connect, we can try later
				continue
			}
			var ret AppendEntriesRPCRet
			args := AppendEntriesRPCArg {
				Index: commitStatus[i],
				Entry: t.log[commitStatus[i]],
			}
			err = client.Call("RaftRPC.AppendEntriesRPC", &args, &ret)
			if ret.Ack == true {
				commitStatus[i] = commitStatus[i] + 1
			}			
			
		}		
	}
}

func (t *Raft) appendListener() {
	rpc.Register((*RaftRPC)(t))
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", fmt.Sprintf(":%v", t.Config.Servers[t.SelfId].LogPort))
	if e != nil {
		log.Fatal("listen error:", e)
	}
	http.Serve(l, nil)
}

func (t1 *RaftRPC) AppendEntriesRPC(args *AppendEntriesRPCArg, ret *AppendEntriesRPCRet) error {
	t:= (*Raft)(t1)
	if args.Entry.Committed && args.Index < len(t.log) {
		ret.Ack = true
		t.lock.Lock()
		// We may have missed some commits
		// Just fill them in, since currently if the leader
		// says that it is committed, it is committed
		for i := t.commitIndex+1; i<= args.Index; i++ {
			t.log[i].Committed = true
		}
		t.commitIndex = args.Index
		t.lock.Unlock()		
	} else if args.Index == len(t.log) {
		// Simple append
		t.lock.Lock()
		t.log = append(t.log, args.Entry)
		t.lock.Unlock()
		ret.Ack = true
		ret.CurrentIndex = args.Index+1
	} else {
		// Server isn't on the same page as we are
		// Ask it to send something we need
		ret.Ack = false
		ret.CurrentIndex = len(t.log)
	}
	return nil
}
