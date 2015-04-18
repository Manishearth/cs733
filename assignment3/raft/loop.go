package raft

import (
	"log"
	"math/rand"
	"sort"
	"time"
)

// This file contains most of the code for the raft event loop
// along with the various events

type Follower struct {
	// empty
}

func (Follower) __stateAssert() {}

type Candidate struct {
}

func (Candidate) __stateAssert() {}

type Leader struct {
	// empty
}
type Disconnected struct {
	// empty
}

func (Leader) __stateAssert()       {}
func (Disconnected) __stateAssert() {}

func (raft *RaftServer) Loop() {
	state := State(Follower{}) // begin life as a follower
	if raft.Id == 0 && raft.Term == 0 {
		state = State(Leader{}) // No need to have an election initially
	}
	raft.Network.Setup()
	for {
		log.Printf("Server %v is now in state %T with term %v\n", raft.Id, state, raft.Term)
		switch state.(type) {
		case Follower:
			state = raft.follower()
		case Candidate:
			state = raft.candidate()
		case Leader:
			state = raft.leader()
		case Disconnected:
			state = raft.disconnected()
		default:
			return
		}
	}
}

// Times out the server
type TimeoutEvent struct{}

func (TimeoutEvent) __signalAssert() {}

// Append data to a leader's log
// Fails for non-leaders
type ClientAppendEvent struct {
	ack  chan ClientAppendResponse
	data Data
}

func (ClientAppendEvent) __signalAssert() {}

type ClientAppendResponse struct {
	Queued bool // was it queued?
	Lsn    Lsn  // log sequence number for entry
}

func (ClientAppendResponse) __signalAssert()   {}
func (ClientAppendResponse) __responseAssert() {}

// AppendRequestRPC
type AppendRPCEvent struct {
	term         uint       // the term
	leaderId     uint       // who the leader is
	prevLogIndex int        // log index before the entries
	prevLogTerm  uint       // term of last log entry before the sent entries
	entries      []LogEntry // entries being sent
	leaderCommit int        // commit index of leader
}

func (AppendRPCEvent) __signalAssert() {}

type DebugEvent struct {
	ack chan DebugResponse
}

func (DebugEvent) __signalAssert() {}

type DebugResponse struct {
	Log         []LogEntry
	Term        uint
	CommitIndex int
}

func (DebugResponse) __signalAssert()   {}
func (DebugResponse) __responseAssert() {}

// Response to AppendRequestRPC
type AppendRPCResponse struct {
	term         uint // follower term
	success      bool // did the AppendRequestRPC succeed?
	followerId   uint // id of follower
	prevLogIndex int  // last log index before append
	count        uint // amount of entries appended
}

func (AppendRPCResponse) __signalAssert()   {}
func (AppendRPCResponse) __responseAssert() {}

func (raft *RaftServer) follower() State {
	// Start a timer to timeout the follower
	timer := time.AfterFunc(time.Duration(500+rand.Intn(100))*time.Millisecond, func() { raft.EventCh <- TimeoutEvent{} })
Loop:
	for event := range raft.EventCh {
		switch event.(type) {
		case DebugEvent:
			event.(DebugEvent).ack <- DebugResponse{Log: raft.Log, Term: raft.Term, CommitIndex: raft.CommitIndex}
		case DisconnectEvent:
			return Disconnected{}
		case TimeoutEvent:
			// In case of timeouts, stop all existing timers, and become a candidate
			timer.Stop()
			raft.Term++
			return Candidate{}
		case VoteRequestEvent:
			// The network seems alive, reset timer
			timer.Reset(time.Duration(500+rand.Intn(100)) * time.Millisecond)
			msg := event.(VoteRequestEvent)
			if msg.term < raft.Term {
				// Our term is better
				raft.Network.Send(VoteResponse{term: raft.Term, voteGranted: false}, msg.candidateId)
				continue Loop
			}
			if msg.term > raft.Term {
				// Their term is better, we should update ourselves
				raft.Term = msg.term
				raft.VotedFor = -1
			}
			if raft.VotedFor != -1 {
				// We already voted
				raft.Network.Send(VoteResponse{term: raft.Term, voteGranted: false}, msg.candidateId)
				continue Loop
			}
			if len(raft.Log) > 0 {
				if msg.lastLogTerm < raft.Log[len(raft.Log)-1].Term() {
					// Our log has better entries
					raft.Network.Send(VoteResponse{term: raft.Term, voteGranted: false}, msg.candidateId)
					continue Loop
				}
				if msg.lastLogTerm == raft.Log[len(raft.Log)-1].Term() {
					if int(msg.lastLogIndex) < len(raft.Log)-1 {
						// Our log has more entries
						raft.Network.Send(VoteResponse{term: raft.Term, voteGranted: false}, msg.candidateId)
						continue Loop
					}
				}
			}
			// If we reach this far, we are behind on the log and should vote
			raft.Network.Send(VoteResponse{term: raft.Term, voteGranted: true, voterId: raft.Id}, msg.candidateId)
			raft.VotedFor = int(msg.candidateId)
		case ClientAppendEvent:
			event.(ClientAppendEvent).ack <- ClientAppendResponse{false, Lsn(0)}
		case AppendRPCEvent:
			// The network seems alive, reset timer
			timer.Reset(time.Duration(500+rand.Intn(100)) * time.Millisecond)
			msg := event.(AppendRPCEvent)
			if msg.term < raft.Term {
				// Our term is better
				raft.Network.Send(AppendRPCResponse{raft.Term, false, raft.Id, 0, 0}, msg.leaderId)
				continue Loop
			} else if raft.Term < msg.term {
				// Update to their term
				raft.Term = msg.term
				log.Printf("Server %v is now in state %T with term %v\n", raft.Id, Follower{}, raft.Term)
			}
			if len(raft.Log)-1 < msg.prevLogIndex {
				// We're behind on their expectations of log size
				raft.Network.Send(AppendRPCResponse{raft.Term, false, raft.Id, 0, 0}, msg.leaderId)
				continue Loop
			}
			if len(raft.Log) > 0 {
				if raft.Log[len(raft.Log)-1].Term() != msg.prevLogTerm {
					// We're behind on their expectations of log term
					raft.Network.Send(AppendRPCResponse{raft.Term, false, raft.Id, 0, 0}, msg.leaderId)
					continue Loop
				}
			}
			delete := false // should we delete entries?
			for i := 1; i <= len(msg.entries); i++ {
				if len(raft.Log) > msg.prevLogIndex+i {
					if raft.Log[msg.prevLogIndex+i].Term() != msg.entries[i-1].Term() {
						// uh oh, entries mismatch, we should clean it up
						delete = true
						raft.Log[msg.prevLogIndex+i] = msg.entries[i-1]
					}
				} else {
					// Append new entries to the log
					raft.Log = append(raft.Log, msg.entries[i-1])
				}

			}
			if delete == true && len(raft.Log) > msg.prevLogIndex+len(msg.entries) {
				// Truncate any entries past what we already overwrote
				raft.Log = raft.Log[:msg.prevLogIndex+len(msg.entries)+1]
			}
			if len(raft.Log) > 0 {
				lastCommit := min(raft.CommitIndex, msg.prevLogIndex+len(msg.entries))
				lastCommitNew := min(int(msg.leaderCommit), len(raft.Log)-1)
				if msg.leaderCommit > lastCommit {
					// We have new things to commit; commit them
					for i := lastCommit + 1; i <= lastCommitNew; i++ {
						raft.CommitCh <- raft.Log[i]
					}
				}
				raft.CommitIndex = lastCommitNew
			}
			raft.Network.Send(AppendRPCResponse{raft.Term, true, raft.Id, msg.prevLogIndex, uint(len(msg.entries))}, msg.leaderId)
		default:
			//
		}

	}
	return Follower{}
}

// VoteRequestRPC
type VoteRequestEvent struct {
	candidateId  uint // candidate requesting vote
	term         uint // term of candidate
	lastLogIndex int  // index of last log entry of candidate
	lastLogTerm  uint // term of last log entry of candidate
}

func (VoteRequestEvent) __signalAssert() {}

// Response to VoteRequestRPC
type VoteResponse struct {
	term        uint // term of voter
	voteGranted bool // was the vote granted?
	voterId     uint // id of voter
}

func (VoteResponse) __signalAssert()   {}
func (VoteResponse) __responseAssert() {}

func (raft *RaftServer) candidate() State {
	votes := make([]uint, raft.N) // 2 = yes, 1 = no, 0 = not voted
	votes[raft.Id] = 2            // vote for self
	lastLogTerm := uint(0)
	if len(raft.Log) > 0 {
		lastLogTerm = raft.Log[uint(len(raft.Log)-1)].Term()
	}
	for i := uint(0); i < raft.N; i++ {
		if uint(i) != raft.Id {
			// ask everyone for votes
			go raft.voteRequest(raft.Term, uint(i), len(raft.Log)-1, lastLogTerm)
		}
	}
	// Set a random timeout for the candidate state
	timer := time.AfterFunc(time.Duration(150+rand.Intn(150))*time.Millisecond, func() { raft.EventCh <- TimeoutEvent{} })
Loop:
	for event := range raft.EventCh {
		switch event.(type) {
		case TimeoutEvent:
			timer.Stop()
			return Follower{}
		case DebugEvent:
			event.(DebugEvent).ack <- DebugResponse{Log: raft.Log, Term: raft.Term, CommitIndex: raft.CommitIndex}
		case DisconnectEvent:
			return Disconnected{}
		case VoteRequestEvent:
			timer.Reset(time.Duration(150+rand.Intn(150)) * time.Millisecond)
			msg := event.(VoteRequestEvent)
			if raft.Term >= msg.term {
				// They're on a lower term, deny
				raft.Network.Send(VoteResponse{term: raft.Term, voteGranted: false}, msg.candidateId)
			} else {
				// They're on a higher term, vote for them and update self
				raft.Term = msg.term
				raft.Network.Send(VoteResponse{term: raft.Term, voteGranted: true}, msg.candidateId)
				raft.VotedFor = int(msg.candidateId)
				return Follower{}
			}
		case AppendRPCEvent:
			msg := event.(AppendRPCEvent)
			raft.Network.Send(AppendRPCResponse{raft.Term, false, raft.Id, 0, 0}, msg.leaderId)
		case ClientAppendEvent:
			event.(ClientAppendEvent).ack <- ClientAppendResponse{false, Lsn(0)}
		case VoteResponse:
			timer.Reset(time.Duration(150+rand.Intn(150)) * time.Millisecond)
			msg := event.(VoteResponse)
			if msg.term != raft.Term {
				// Not the votes we are looking for
				continue Loop
			}

			// record the vote
			if msg.voteGranted {
				votes[msg.voterId] = 2
			} else {
				votes[msg.voterId] = 1
			}
			yescount := uint(0)
			nocount := uint(0)
			for i := uint(0); i < raft.N; i++ {
				if votes[i] == 2 {
					yescount++
				} else if votes[i] == 3 {
					nocount++
				}
			}
			if yescount > raft.N/2 {
				// We have a majority
				timer.Stop()
				return Leader{}
			} else if nocount > raft.N/2 {
				// We have a majority of no votes, give up
				timer.Stop()
				// raft.Term++
				return Follower{}
			}
		default:

		}
	}
	return Candidate{}
}

func (raft *RaftServer) voteRequest(term uint, id uint, lastLogIndex int, lastLogTerm uint) {
	raft.Network.Send(VoteRequestEvent{candidateId: raft.Id, term: term, lastLogIndex: lastLogIndex, lastLogTerm: lastLogTerm}, id)
}

func (raft *RaftServer) appendRequest(term uint, id uint, prevLogIndex int, prevLogTerm uint, entries []LogEntry, leaderCommit int) {
	raft.Network.Send(AppendRPCEvent{term: term, leaderId: raft.Id, prevLogIndex: prevLogIndex, prevLogTerm: prevLogTerm, entries: entries, leaderCommit: leaderCommit}, id)
}

// Instructs leader to send out heartbeats
type HeartBeatEvent struct{}

func (HeartBeatEvent) __signalAssert() {}

func (raft *RaftServer) leader() State {
	nextIndex := make([]uint, raft.N)
	matchIndex := make([]int, raft.N)
	for i := uint(0); i < raft.N; i++ {
		nextIndex[i] = uint(len(raft.Log))
		matchIndex[i] = -1
	}
	// start a ticker for the heartbeat
	heartbeat := time.NewTicker(time.Duration(100) * time.Millisecond)
	quit := make(chan bool, 1)
	go func() {
		// Keep sending heartbeats, but if we get a quit message,
		// stop
		for {
			select {
			case <-heartbeat.C:
				raft.EventCh <- HeartBeatEvent{}
			case <-quit:
				heartbeat.Stop()
				return
			}
		}
	}()

	for event := range raft.EventCh {
		switch event.(type) {
		case DisconnectEvent:
			quit <- true
			return Disconnected{}
		case HeartBeatEvent:
			for i := uint(0); i < raft.N; i++ {
				if uint(i) == raft.Id {
					// ignore self; we already know that self is up to date
					continue
				}
				if len(raft.Log) >= int(nextIndex[i]) {
					// figure out the AppendRequestRPC params, and update self
					prevLogTerm := raft.Term
					newEntries := make([]LogEntry, 0)
					if len(raft.Log) > 0 && nextIndex[i] > 0 {
						prevLogTerm = raft.Log[nextIndex[i]-1].Term()
					}
					if uint(len(raft.Log)) > nextIndex[i] {
						newEntries = raft.Log[nextIndex[i]:]
					}
					go raft.appendRequest(raft.Term, uint(i), int(nextIndex[i])-1, prevLogTerm, newEntries, raft.CommitIndex)
					nextIndex[i] = uint(len(raft.Log))
				}
			}
		case DebugEvent:
			event.(DebugEvent).ack <- DebugResponse{Log: raft.Log, Term: raft.Term, CommitIndex: raft.CommitIndex}
		case VoteRequestEvent:
			msg := event.(VoteRequestEvent)
			if raft.Term >= msg.term {
				// Deny if we're on the same footing or a better term
				raft.Network.Send(VoteResponse{term: raft.Term, voteGranted: false}, msg.candidateId)
			} else {
				// If it's on a better term, grant vote and become follower
				raft.Term = msg.term
				raft.Network.Send(VoteResponse{term: raft.Term, voteGranted: true}, msg.candidateId)
				raft.VotedFor = int(msg.candidateId)
				quit <- true
				return Follower{}
			}
		case AppendRPCEvent:
			// If we get an AppendRequestRPC from someone with a better term
			// deny it for now, but become a follower
			msg := event.(AppendRPCEvent)
			if raft.Term < msg.term {
				raft.Term = msg.term
				raft.Network.Send(AppendRPCResponse{raft.Term, false, raft.Id, 0, 0}, msg.leaderId)
				quit <- true
				return Follower{}
			}
		case ClientAppendEvent:
			msg := event.(ClientAppendEvent)
			lsn := Lsn(0)
			if len(raft.Log) > 0 {
				// Lsn should be 0 or `(lsn of last log entry) + 1`
				lsn = raft.Log[len(raft.Log)-1].Lsn()
			}
			lsn++
			raft.Log = append(raft.Log, StringEntry{lsn: lsn, data: msg.data, term: raft.Term})
			msg.ack <- ClientAppendResponse{true, lsn}
		case AppendRPCResponse:
			msg := event.(AppendRPCResponse)
			id := msg.followerId
			if !msg.success {
				// message failed, decrement nextIndex
				if nextIndex[id] > 0 {
					nextIndex[id] = nextIndex[id] - 1
				} else {
					nextIndex[id] = 0
				}
				continue
			}
			if msg.term > raft.Term {
				// we're behind
				quit <- true
				return Follower{}
			}
			if msg.prevLogIndex+int(msg.count)+1 > int(matchIndex[id]) {
				// update matchIndex if necessary
				matchIndex[id] = msg.prevLogIndex + int(msg.count)
			}

			// make a copy so we can sort it
			matchCopy := make([]int, raft.N)
			copy(matchCopy, matchIndex)
			sort.IntSlice(matchCopy).Sort()
			// If there exists an N such that N > commitIndex, a majority
			// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
			// set commitIndex = N .
		Inner:
			for i := int(raft.N/2 + 1); i >= 0; i-- {
				// First N/2+1 of sorted matchindex
				// slice all are less than or equal to a majority of
				// matchindices
				if matchCopy[i] > raft.CommitIndex {
					for j := raft.CommitIndex + 1; j <= matchCopy[i] && j < len(raft.Log); j++ {
						raft.CommitCh <- raft.Log[j]
					}
					raft.CommitIndex = matchCopy[i]
					break Inner
				}
			}
		default:

		}
	}
	return Leader{}
}

// Disconnect server from network
type DisconnectEvent struct{}

// Reconnect server to network
type ReconnectEvent struct{}

func (DisconnectEvent) __signalAssert() {}
func (ReconnectEvent) __signalAssert()  {}

func (raft *RaftServer) disconnected() State {
	for event := range raft.EventCh {
		switch event.(type) {
		case ReconnectEvent:
			return Follower{}
		default:
			// drop everything
		}
	}
	return Disconnected{}
}

func min(a int, b int) int {
	if a <= b {
		return a
	} else {
		return b
	}
}
