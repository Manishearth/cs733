package raft

import (
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

func (raft *RaftServer) loop() {
	state := State(Follower{}) // begin life as a follower
	if raft.Id == 0 {
		state = State(Leader{}) // No need to have an election initially
	}

	for {
		switch state.(type) {
		case Follower:
			state = raft.follower()
		case Candidate:
			state = raft.candidate()
		case Leader:
			state = raft.leader()
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
	term       uint
	success    bool
	followerId uint
}

func (raft *RaftServer) follower() State {
	timer := time.AfterFunc(time.Duration(500)*time.Millisecond, func() { raft.EventCh <- ChanMessage{make(chan Response), TimeoutEvent{}} })
Loop:
	for event := range raft.EventCh {
		switch event.signal.(type) {
		case DebugEvent:
			event.ack <- DebugResponse{Log: raft.Log, Term: raft.Term, CommitIndex: raft.CommitIndex}
		case TimeoutEvent:
			timer.Stop()
			raft.Term++
			return Candidate{}
		case VoteRequestEvent:
			timer.Reset(time.Duration(500) * time.Millisecond)
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

			timer.Reset(time.Duration(500) * time.Millisecond)
			msg := event.signal.(AppendRPCEvent)
			if msg.term < raft.Term {
				event.ack <- AppendRPCResponse{raft.Term, false, raft.Id}
				continue Loop
			}
			if len(raft.Log)-1 < msg.prevLogIndex {
				event.ack <- AppendRPCResponse{raft.Term, false, raft.Id}
				continue Loop
			}
			if len(raft.Log) > 0 {
				if raft.Log[len(raft.Log)-1].Term() != msg.prevLogTerm {
					event.ack <- AppendRPCResponse{raft.Term, false, raft.Id}
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
			if len(msg.entries) > 0 {
				lastCommit := min(raft.CommitIndex, msg.prevLogIndex+len(msg.entries))
				lastCommitNew := min(int(msg.leaderCommit), len(raft.Log))
				if msg.leaderCommit > lastCommit {
					for i := lastCommit + 1; i <= lastCommitNew; i++ {
						raft.CommitCh <- raft.Log[i].Data()
					}
				}
				raft.CommitIndex = lastCommitNew
				event.ack <- AppendRPCResponse{raft.Term, true, raft.Id}
			}
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
	votes := make([]bool, 5)
	votes[raft.Id] = true
	lastLogTerm := uint(0)
	if len(raft.Log) > 0 {
		lastLogTerm = uint(len(raft.Log) - 1)
	}
	for i := 0; i < 5; i++ {
		if uint(i) != raft.Id {
			go raft.voteRequest(raft.Term, uint(i), len(raft.Log)-1, lastLogTerm)
		}
	}
	timer := time.AfterFunc(time.Duration(1000)*time.Millisecond, func() { raft.EventCh <- ChanMessage{make(chan Response), TimeoutEvent{}} })
Loop:
	for event := range raft.EventCh {
		switch event.signal.(type) {
		case TimeoutEvent:
			raft.Term++
			return Candidate{}
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
				return Follower{}
			}
		case AppendRPCEvent:
			event.ack <- AppendRPCResponse{raft.Term, false, raft.Id}
		case ClientAppendEvent:
			event.ack <- ClientAppendResponse{false, Lsn(0)}
		case VoteResponse:
			timer.Reset(time.Duration(1000) * time.Millisecond)
			msg := event.signal.(VoteResponse)
			if msg.term != raft.Term {
				continue Loop
			}
			votes[msg.voterId] = true
			count := 0
			for i := 0; i < 5; i++ {
				if votes[i] {
					count++
				}
			}
			if count > 2 {
				return Leader{}
			} else {
				raft.Term++
				return Candidate{}
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
	matchIndex := make([]uint, 5)
	for i := 0; i < 5; i++ {
		nextIndex[i] = uint(len(raft.Log))
	}
	heartbeat := time.NewTicker(time.Duration(500) * time.Millisecond)
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
		case HeartBeatEvent:
			for i := 0; i < 5; i++ {
				if len(raft.Log) >= int(nextIndex[i]) {
					// stub
					go appendRequest(raft.Term, i)
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
				event.ack <- AppendRPCResponse{raft.Term, false, raft.Id}
				quit <- true
				return Follower{}
			}
		case ClientAppendEvent:
			lsn := Lsn(0)
			if len(raft.Log) > 0 {
				lsn = raft.Log[len(raft.Log)-1].Lsn()
			}
			lsn++
			event.ack <- ClientAppendResponse{true, lsn}
		case AppendRPCResponse:
			msg := event.signal.(AppendRPCResponse)
			// stub
		default:

		}
	}
	return Leader{}
}

func min(a int, b int) int {
	if a <= b {
		return a
	} else {
		return b
	}
}
