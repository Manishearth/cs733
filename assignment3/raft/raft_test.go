package raft

import (
	"fmt"
	"testing"
	"time"
)

func TestInternalAppend(t *testing.T) {
	rafts := MakeRafts(5)
	for i := 0; i < 5; i++ {
		go rafts[i].loop()
	}
	entry := make([]LogEntry, 1)
	entry[0] = StringEntry{lsn: 0, data: "foo", term: 0}
	rafts[1].EventCh <- ChanMessage{make(chan Response, 1), AppendRPCEvent{0, 0, -1, 0, entry, -1}}
	entry = make([]LogEntry, 1)
	entry[0] = StringEntry{lsn: 0, data: "bar", term: 0}
	rafts[1].EventCh <- ChanMessage{make(chan Response, 1), AppendRPCEvent{0, 0, 0, 0, entry, -1}}
	entry = make([]LogEntry, 1)
	entry[0] = StringEntry{lsn: 0, data: "baz", term: 0}
	rafts[1].EventCh <- ChanMessage{make(chan Response, 1), AppendRPCEvent{0, 0, 1, 0, entry, 1}}
	wait := make(chan Response)
	rafts[1].EventCh <- ChanMessage{wait, DebugEvent{}}
	ret := (<-wait)
	debug := ret.(DebugResponse)
	expect(t, string(debug.Log[0].Data()), "foo")
	expect(t, string(debug.Log[1].Data()), "bar")
	expect(t, string(debug.Log[2].Data()), "baz")
	expect_num(t, int(debug.Term), 0)
	expect_num(t, debug.CommitIndex, 1)
}

func TestClientAppend(t *testing.T) {
	rafts := MakeRafts(5)
	for i := 0; i < 5; i++ {
		go rafts[i].loop()
	}
	lsnget := make(chan Response, 4)
	rafts[0].EventCh <- ChanMessage{lsnget, ClientAppendEvent{data: "foo"}}
	lsn1 := (<-lsnget).(ClientAppendResponse).Lsn
	rafts[0].EventCh <- ChanMessage{lsnget, ClientAppendEvent{data: "bar"}}
	lsn2 := (<-lsnget).(ClientAppendResponse).Lsn
	rafts[0].EventCh <- ChanMessage{lsnget, ClientAppendEvent{data: "baz"}}
	lsn3 := (<-lsnget).(ClientAppendResponse).Lsn

	for i := 1; i < 5; i++ {
		commit := <-rafts[i].CommitCh
		t.Logf("Found data %v, with lsn %v\n", commit.Data(), commit.Lsn())
		expect_num(t, int(lsn1), int(commit.Lsn()))
		expect(t, "foo", string(commit.Data()))
		commit = <-rafts[i].CommitCh
		t.Logf("Found data %v, with lsn %v\n", commit.Data(), commit.Lsn())
		expect_num(t, int(lsn2), int(commit.Lsn()))
		expect(t, "bar", string(commit.Data()))
		commit = <-rafts[i].CommitCh
		t.Logf("Found data %v, with lsn %v\n", commit.Data(), commit.Lsn())
		expect_num(t, int(lsn3), int(commit.Lsn()))
		expect(t, "baz", string(commit.Data()))
	}
}

func TestClientAppendDisconnect(t *testing.T) {
	rafts := MakeRafts(5)
	rafts[3].EventCh <- ChanMessage{make(chan Response), DisconnectEvent{}}
	rafts[4].EventCh <- ChanMessage{make(chan Response), DisconnectEvent{}}
	for i := 0; i < 5; i++ {
		go rafts[i].loop()
	}
	lsnget := make(chan Response, 4)
	rafts[0].EventCh <- ChanMessage{lsnget, ClientAppendEvent{data: "foo"}}
	lsn1 := (<-lsnget).(ClientAppendResponse).Lsn
	rafts[0].EventCh <- ChanMessage{lsnget, ClientAppendEvent{data: "bar"}}
	lsn2 := (<-lsnget).(ClientAppendResponse).Lsn
	rafts[0].EventCh <- ChanMessage{lsnget, ClientAppendEvent{data: "baz"}}
	lsn3 := (<-lsnget).(ClientAppendResponse).Lsn

	for i := 1; i < 3; i++ {
		commit := <-rafts[i].CommitCh
		t.Logf("Found data %v, with lsn %v\n", commit.Data(), commit.Lsn())
		expect_num(t, int(lsn1), int(commit.Lsn()))
		expect(t, "foo", string(commit.Data()))
		commit = <-rafts[i].CommitCh
		t.Logf("Found data %v, with lsn %v\n", commit.Data(), commit.Lsn())
		expect_num(t, int(lsn2), int(commit.Lsn()))
		expect(t, "bar", string(commit.Data()))
		commit = <-rafts[i].CommitCh
		t.Logf("Found data %v, with lsn %v\n", commit.Data(), commit.Lsn())
		expect_num(t, int(lsn3), int(commit.Lsn()))
		expect(t, "baz", string(commit.Data()))
	}
}

func TestClientAppendDisconnectFail(t *testing.T) {
	rafts := MakeRafts(5)
	rafts[3].EventCh <- ChanMessage{make(chan Response), DisconnectEvent{}}
	rafts[4].EventCh <- ChanMessage{make(chan Response), DisconnectEvent{}}
	// We lost quorum
	rafts[2].EventCh <- ChanMessage{make(chan Response), DisconnectEvent{}}
	for i := 0; i < 5; i++ {
		go rafts[i].loop()
	}
	lsnget := make(chan Response, 4)
	rafts[0].EventCh <- ChanMessage{lsnget, ClientAppendEvent{data: "foo"}}
	rafts[0].EventCh <- ChanMessage{lsnget, ClientAppendEvent{data: "bar"}}
	rafts[0].EventCh <- ChanMessage{lsnget, ClientAppendEvent{data: "baz"}}
	timer := time.NewTimer(time.Duration(1) * time.Second)
	select {
	case <-timer.C:
		return
	case <-rafts[1].CommitCh:
		t.Error("Data was committed when there was no quorum")
	}
}

func TestClientElection(t *testing.T) {
	rafts := MakeRafts(5)
	rafts[0].EventCh <- ChanMessage{make(chan Response), DisconnectEvent{}}
	for i := 0; i < 5; i++ {
		go rafts[i].loop()
	}
	time.Sleep(1 * time.Second)
	newleader := 0
	for i := 1; i < 5; i++ {
		lsnget := make(chan Response, 1)
		rafts[i].EventCh <- ChanMessage{lsnget, ClientAppendEvent{data: "foo"}}
		if (<-lsnget).(ClientAppendResponse).Queued {
			t.Logf("Successful ClientAppend on %v\n", i)
			newleader = i
		}
	}
	for i := 1; i < 5; i++ {
		commit := <-rafts[i].CommitCh
		t.Logf("Found data %v on server %v, with lsn %v\n", commit.Data(), i, commit.Lsn())
		expect(t, "foo", string(commit.Data()))
	}

	rafts[0].EventCh <- ChanMessage{make(chan Response), ReconnectEvent{}}
	{
		commit := <-rafts[0].CommitCh
		t.Logf("Found data %v on server %v, with lsn %v\n", commit.Data(), 0, commit.Lsn())
		expect(t, "foo", string(commit.Data()))
	}
	rafts[newleader].EventCh <- ChanMessage{make(chan Response), DisconnectEvent{}}
	// Election takes time sometimes
	time.Sleep(4 * time.Second)

	for i := 0; i < 5; i++ {
		if i == newleader {
			continue
		}
		lsnget := make(chan Response, 1)
		rafts[i].EventCh <- ChanMessage{lsnget, ClientAppendEvent{data: "bar"}}
		if (<-lsnget).(ClientAppendResponse).Queued {
			t.Logf("Successful ClientAppend on %v\n", i)
		}
	}
	for i := 0; i < 5; i++ {
		if i == newleader {
			continue
		}
		commit := <-rafts[i].CommitCh
		t.Logf("Found data %v on server %v, with lsn %v\n", commit.Data(), i, commit.Lsn())
		expect(t, "bar", string(commit.Data()))
	}
}

// Useful testing function
func expect(t *testing.T, a string, b string) {
	if a != b {
		t.Error(fmt.Sprintf("Expected %v, found %v", b, a))
	}
}

// Useful testing function
func expect_num(t *testing.T, a int, b int) {
	if a != b {
		t.Error(fmt.Sprintf("Expected %v, found %v", b, a))
	}
}
