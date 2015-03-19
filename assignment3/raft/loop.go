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

func (raft *RaftServer) follower() State {
	timer := time.AfterFunc(time.Duration(5)*time.Second, func() { raft.EventCh <- TimeoutEvent{} })
	timer.Stop()
	return Follower{}
}

func (raft *RaftServer) candidate() State {
	return Candidate{}
}

func (raft *RaftServer) leader() State {
	return Leader{}
}
