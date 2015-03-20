package raft

import (
	"time"
)

type Follower struct {
	// empty
}

type Candidate struct {
	// empty
}

type Leader struct {
	// empty
}

func (raft *RaftServer) loop() {
	state := State(Follower{}) // begin life as a follower

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
type VoteRequestEvent struct {
	candidateId uint
	term uint
	lastLogIndex uint
	lastLogTerm uint
}

type VoteResponse struct {
	term uint
	voteGranted bool
}

type ClientAppendEvent struct {
	data Data
}

type ClientAppendResponse struct {
	Queued bool
	Lsn Lsn
}

type AppendRPCEvent struct {
	term uint
	leaderId uint
	prevLogIndex int
	prevLogTerm uint
	entries []LogEntry
	leaderCommit uint
}

type AppendRPCResponse struct {
	term uint
	success bool
}

func (raft *RaftServer) follower() State {
	timer := time.AfterFunc(time.Duration(500)*time.Millisecond, func() { raft.EventCh <- ChanMessage{make(chan Response), TimeoutEvent{}} })
	for event := range raft.EventCh {
		switch event.signal.(type) {
		case TimeoutEvent: 
			return Candidate{}
		case VoteRequestEvent:
			timer.Reset(time.Duration(500)*time.Millisecond)
			msg := event.signal.(VoteRequestEvent)
			if msg.term < raft.Term {
				event.ack <- VoteResponse {term: raft.Term, voteGranted: false}
				return Follower {}
			}
			if msg.term > raft.Term {
				raft.Term = msg.term
				raft.VotedFor = -1
			}
			if raft.VotedFor != -1 {
				event.ack <- VoteResponse {term: raft.Term, voteGranted: false}
				return Follower {}
			}
			if len(raft.Log) > 0 {
				if msg.lastLogTerm < raft.Log[len(raft.Log)-1].Term() {
					event.ack <- VoteResponse {term: raft.Term, voteGranted: false}
					return Follower {}				
				}
				if msg.lastLogTerm == raft.Log[len(raft.Log)-1].Term() {
					if int(msg.lastLogIndex) < len(raft.Log)-1 {
						event.ack <- VoteResponse {term: raft.Term, voteGranted: false}
						return Follower {}
					}		
				}
			}
			// If we reach this far, we are behind on the log and should vote
			event.ack <- VoteResponse {term: raft.Term, voteGranted: true}
			raft.VotedFor = int(msg.candidateId)
			return Follower{}
		case ClientAppendEvent:
			event.ack <- ClientAppendResponse {false, Lsn(0)}
			return Follower {}
		case AppendRPCEvent:
			timer.Reset(time.Duration(500)*time.Millisecond)
			msg := event.signal.(AppendRPCEvent)
			if msg.term < raft.Term {
				event.ack <- AppendRPCResponse{raft.Term, false}
				return Follower {}
			}
			if len(raft.Log)-1 < msg.prevLogIndex{
				event.ack <- AppendRPCResponse{raft.Term, false}
				return Follower {}
			}
			if len(raft.Log) > 0 {
				if raft.Log[len(raft.Log)-1].Term() != msg.prevLogTerm {
					event.ack <- AppendRPCResponse{raft.Term, false}
					return Follower {}					
				}
			}
			delete := false
			for i := 1; i<=len(msg.entries); i++ {
				if len(raft.Log) > msg.prevLogIndex+i {
					if raft.Log[msg.prevLogIndex+i].Term() != msg.entries[i-1].Term() {
						delete = true
						raft.Log[msg.prevLogIndex + i] = msg.entries[i-1]
					}
				} else {
					raft.Log = append(raft.Log, msg.entries[i-1])
				}

			}
			if delete == true && len(raft.Log) > msg.prevLogIndex+len(msg.entries) {
				raft.Log = raft.Log[:msg.prevLogIndex+len(msg.entries)+1]
			}
			if len(msg.entries) > 0 {
				lastCommit := min(raft.CommitIndex, uint(msg.prevLogIndex+len(msg.entries)))
				lastCommitNew := min(msg.leaderCommit, uint(len(raft.Log)))
				if msg.leaderCommit > lastCommit {
					for i:= lastCommit+1; i<= lastCommitNew; i++ {
						raft.CommitCh <- raft.Log[i].Data()
					}
				}
				raft.CommitIndex = lastCommitNew
			}
			return Follower {}
		}

	}
	return Follower{}
}

func (raft *RaftServer) candidate() State {
	return Candidate{}
}

func (raft *RaftServer) leader() State {
	return Leader{}
}


func min(a uint, b uint) uint {
	if a >= b {
		return a
	} else {
		return b
	}
}