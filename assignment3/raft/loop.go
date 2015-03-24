package raft

import (
	"fmt"
	"math/rand"
	"sort"
	"time"
)

type Follower struct {
	// empty
}

type Candidate struct {
}

type Leader struct {
	// empty
}

type Disconnected struct {
	// empty
}

func (raft *RaftServer) loop() {
	state := State(Follower{}) // begin life as a follower
	if raft.Id == 0 {
		state = State(Leader{}) // No need to have an election initially
	}

	for {
		fmt.Printf("Server %v is now in state %T with term %v\n", raft.Id, state, raft.Term)
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

type TimeoutEvent struct{}

type ClientAppendEvent struct {
	data Data
}

type ClientAppendResponse struct {
	Queued bool
	Lsn    Lsn
}

type AppendRPCEvent struct {
	term         uint
	leaderId     uint
	prevLogIndex int
	prevLogTerm  uint
	entries      []LogEntry
	leaderCommit int
}

type DebugEvent struct{}
type DebugResponse struct {
	Log         []LogEntry
	Term        uint
	CommitIndex int
}

type AppendRPCResponse struct {
	term         uint
	success      bool
	followerId   uint
	prevLogIndex int
	count        uint
}

func (raft *RaftServer) follower() State {
	timer := time.AfterFunc(time.Duration(500+rand.Intn(100))*time.Millisecond, func() { raft.EventCh <- ChanMessage{make(chan Response), TimeoutEvent{}} })
Loop:
	for event := range raft.EventCh {
		switch event.signal.(type) {
		case DebugEvent:
			event.ack <- DebugResponse{Log: raft.Log, Term: raft.Term, CommitIndex: raft.CommitIndex}
		case DisconnectEvent:
			return Disconnected{}
		case TimeoutEvent:
			timer.Stop()
			raft.Term++
			return Candidate{}
		case VoteRequestEvent:
			timer.Reset(time.Duration(500+rand.Intn(100)) * time.Millisecond)
			msg := event.signal.(VoteRequestEvent)
			if msg.term < raft.Term {
				event.ack <- VoteResponse{term: raft.Term, voteGranted: false}
				continue Loop
			}
			if msg.term > raft.Term {
				raft.Term = msg.term
				raft.VotedFor = -1
			}
			if raft.VotedFor != -1 {
				event.ack <- VoteResponse{term: raft.Term, voteGranted: false, voterId: raft.Id}
				continue Loop
			}
			if len(raft.Log) > 0 {
				if msg.lastLogTerm < raft.Log[len(raft.Log)-1].Term() {
					event.ack <- VoteResponse{term: raft.Term, voteGranted: false, voterId: raft.Id}
					continue Loop
				}
				if msg.lastLogTerm == raft.Log[len(raft.Log)-1].Term() {
					if int(msg.lastLogIndex) < len(raft.Log)-1 {
						event.ack <- VoteResponse{term: raft.Term, voteGranted: false, voterId: raft.Id}
						continue Loop
					}
				}
			}
			// If we reach this far, we are behind on the log and should vote
			event.ack <- VoteResponse{term: raft.Term, voteGranted: true, voterId: raft.Id}
			raft.VotedFor = int(msg.candidateId)
		case ClientAppendEvent:
			event.ack <- ClientAppendResponse{false, Lsn(0)}
		case AppendRPCEvent:

			timer.Reset(time.Duration(500+rand.Intn(100)) * time.Millisecond)
			msg := event.signal.(AppendRPCEvent)
			if msg.term < raft.Term {
				event.ack <- AppendRPCResponse{raft.Term, false, raft.Id, 0, 0}
				continue Loop
			} else if raft.Term < msg.term {
				raft.Term = msg.term
				fmt.Printf("Server %v is now in state %T with term %v\n", raft.Id, Follower{}, raft.Term)
			}
			if len(raft.Log)-1 < msg.prevLogIndex {
				event.ack <- AppendRPCResponse{raft.Term, false, raft.Id, 0, 0}
				continue Loop
			}
			if len(raft.Log) > 0 {
				if raft.Log[len(raft.Log)-1].Term() != msg.prevLogTerm {
					event.ack <- AppendRPCResponse{raft.Term, false, raft.Id, 0, 0}
					continue Loop
				}
			}
			delete := false
			for i := 1; i <= len(msg.entries); i++ {
				if len(raft.Log) > msg.prevLogIndex+i {
					if raft.Log[msg.prevLogIndex+i].Term() != msg.entries[i-1].Term() {
						delete = true
						raft.Log[msg.prevLogIndex+i] = msg.entries[i-1]
					}
				} else {
					raft.Log = append(raft.Log, msg.entries[i-1])
				}

			}
			if delete == true && len(raft.Log) > msg.prevLogIndex+len(msg.entries) {
				raft.Log = raft.Log[:msg.prevLogIndex+len(msg.entries)+1]
			}
			if len(raft.Log) > 0 {
				lastCommit := min(raft.CommitIndex, msg.prevLogIndex+len(msg.entries))
				lastCommitNew := min(int(msg.leaderCommit), len(raft.Log)-1)
				if msg.leaderCommit > lastCommit {
					for i := lastCommit + 1; i <= lastCommitNew; i++ {
						raft.CommitCh <- raft.Log[i]
					}
				}
				raft.CommitIndex = lastCommitNew
			}
			event.ack <- AppendRPCResponse{raft.Term, true, raft.Id, msg.prevLogIndex, uint(len(msg.entries))}
		default:
		}

	}
	return Follower{}
}

type VoteRequestEvent struct {
	candidateId  uint
	term         uint
	lastLogIndex int
	lastLogTerm  uint
}

type VoteResponse struct {
	term        uint
	voteGranted bool
	voterId     uint
}

func (raft *RaftServer) candidate() State {
	votes := make([]uint, 5)
	votes[raft.Id] = 2 // 2 = yes, 1 = no
	lastLogTerm := uint(0)
	if len(raft.Log) > 0 {
		lastLogTerm = raft.Log[uint(len(raft.Log)-1)].Term()
	}
	for i := 0; i < 5; i++ {
		if uint(i) != raft.Id {
			go raft.voteRequest(raft.Term, uint(i), len(raft.Log)-1, lastLogTerm)
		}
	}
	timer := time.AfterFunc(time.Duration(150+rand.Intn(150))*time.Millisecond, func() { raft.EventCh <- ChanMessage{make(chan Response), TimeoutEvent{}} })
Loop:
	for event := range raft.EventCh {
		switch event.signal.(type) {
		case TimeoutEvent:
			timer.Stop()
			//raft.Term++
			return Follower{}
		case DebugEvent:
			event.ack <- DebugResponse{Log: raft.Log, Term: raft.Term, CommitIndex: raft.CommitIndex}
		case DisconnectEvent:
			return Disconnected{}
		case VoteRequestEvent:
			timer.Reset(time.Duration(150+rand.Intn(150)) * time.Millisecond)
			msg := event.signal.(VoteRequestEvent)
			if raft.Term >= msg.term {
				event.ack <- VoteResponse{term: raft.Term, voteGranted: false}
			} else {
				raft.Term = msg.term
				event.ack <- VoteResponse{term: raft.Term, voteGranted: true}
				raft.VotedFor = int(msg.candidateId)
				return Follower{}
			}
		case AppendRPCEvent:
			event.ack <- AppendRPCResponse{raft.Term, false, raft.Id, 0, 0}
		case ClientAppendEvent:
			event.ack <- ClientAppendResponse{false, Lsn(0)}
		case VoteResponse:
			timer.Reset(time.Duration(150+rand.Intn(150)) * time.Millisecond)
			msg := event.signal.(VoteResponse)
			if msg.term != raft.Term {
				continue Loop
			}

			if msg.voteGranted {
				votes[msg.voterId] = 2
			} else {
				votes[msg.voterId] = 1
			}
			yescount := 0
			nocount := 0
			for i := 0; i < 5; i++ {
				if votes[i] == 2 {
					yescount++
				} else if votes[i] == 3 {
					nocount++
				}
			}
			if yescount > 2 {
				timer.Stop()
				return Leader{}
			} else if nocount > 2 {
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
	ack := raft.Network.Send(VoteRequestEvent{candidateId: raft.Id, term: term, lastLogIndex: lastLogIndex, lastLogTerm: lastLogTerm}, id)
	resp := <-ack
	if raft.Term != term {
		return
	}
	raft.EventCh <- ChanMessage{make(chan Response), resp}
}

func (raft *RaftServer) appendRequest(term uint, id uint, prevLogIndex int, prevLogTerm uint, entries []LogEntry, leaderCommit int) {
	ack := raft.Network.Send(AppendRPCEvent{term: term, leaderId: raft.Id, prevLogIndex: prevLogIndex, prevLogTerm: prevLogTerm, entries: entries, leaderCommit: leaderCommit}, id)
	resp := <-ack
	if raft.Term != term {
		return
	}
	raft.EventCh <- ChanMessage{make(chan Response), resp}
}

type HeartBeatEvent struct{}

func (raft *RaftServer) leader() State {
	nextIndex := make([]uint, 5)
	matchIndex := make([]int, 5)
	for i := 0; i < 5; i++ {
		nextIndex[i] = uint(len(raft.Log))
		matchIndex[i] = -1
	}
	heartbeat := time.NewTicker(time.Duration(100) * time.Millisecond)
	quit := make(chan bool, 1)
	go func() {
		for {
			select {
			case <-heartbeat.C:
				raft.EventCh <- ChanMessage{make(chan Response, 1), HeartBeatEvent{}}
			case <-quit:
				heartbeat.Stop()
				return
			}
		}
	}()

	for event := range raft.EventCh {
		switch event.signal.(type) {
		case DisconnectEvent:
			quit <- true
			return Disconnected{}
		case HeartBeatEvent:
			for i := 0; i < 5; i++ {
				if uint(i) == raft.Id {
					continue
				}
				if len(raft.Log) >= int(nextIndex[i]) {
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
			event.ack <- DebugResponse{Log: raft.Log, Term: raft.Term, CommitIndex: raft.CommitIndex}
		case VoteRequestEvent:
			msg := event.signal.(VoteRequestEvent)
			if raft.Term >= msg.term {
				event.ack <- VoteResponse{term: raft.Term, voteGranted: false}
			} else {
				raft.Term = msg.term
				event.ack <- VoteResponse{term: raft.Term, voteGranted: true}
				raft.VotedFor = int(msg.candidateId)
				quit <- true
				return Follower{}
			}
		case AppendRPCEvent:
			msg := event.signal.(AppendRPCEvent)
			if raft.Term < msg.term {
				raft.Term = msg.term
				event.ack <- AppendRPCResponse{raft.Term, false, raft.Id, 0, 0}
				quit <- true
				return Follower{}
			}
		case ClientAppendEvent:
			msg := event.signal.(ClientAppendEvent)
			lsn := Lsn(0)
			if len(raft.Log) > 0 {
				lsn = raft.Log[len(raft.Log)-1].Lsn()
			}
			lsn++
			raft.Log = append(raft.Log, StringEntry{lsn: lsn, data: msg.data, term: raft.Term})
			event.ack <- ClientAppendResponse{true, lsn}
		case AppendRPCResponse:
			msg := event.signal.(AppendRPCResponse)
			id := msg.followerId
			if !msg.success {
				if nextIndex[id] > 0 {
					nextIndex[id] = nextIndex[id] - 1
				} else {
					nextIndex[id] = 0
				}
				continue
			}
			if msg.term > raft.Term {
				quit <- true
				return Follower{}
			}
			if msg.prevLogIndex+int(msg.count)+1 > int(matchIndex[id]) {
				matchIndex[id] = msg.prevLogIndex + int(msg.count)
			}
			matchCopy := make([]int, 5)
			copy(matchCopy, matchIndex)
			sort.IntSlice(matchCopy).Sort()
			// If there exists an N such that N > commitIndex, a majority
			// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
			// set commitIndex = N .
		Inner:
			for i := 3; i >= 0; i-- {
				// First three elements of sorted matchindex
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

type DisconnectEvent struct{}
type ReconnectEvent struct{}

func (raft *RaftServer) disconnected() State {
	for event := range raft.EventCh {
		switch event.signal.(type) {
		case ReconnectEvent:
			return Follower{}
		default:
			// drop
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
