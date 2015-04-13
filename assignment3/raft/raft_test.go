package raft

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// Tests if a single Raft server behaves
// correctly on AppendRPC
func TestInternalAppend(t *testing.T) {
	rafts := MakeRafts(5)
	for i := 0; i < 5; i++ {
		// We don't actually need the network here, just
		// the first follower
		go rafts[i].loop()
	}
	entry := make([]LogEntry, 1)
	entry[0] = StringEntry{lsn: 0, data: "foo", term: 0}
	rafts[1].EventCh <- AppendRPCEvent{0, 0, -1, 0, entry, -1}
	entry = make([]LogEntry, 1)
	entry[0] = StringEntry{lsn: 0, data: "bar", term: 0}
	rafts[1].EventCh <- AppendRPCEvent{0, 0, 0, 0, entry, -1}
	entry = make([]LogEntry, 1)
	entry[0] = StringEntry{lsn: 0, data: "baz", term: 0}
	rafts[1].EventCh <- AppendRPCEvent{0, 0, 1, 0, entry, 1}
	wait := make(chan DebugResponse)
	rafts[1].EventCh <- DebugEvent{wait}
	debug := (<-wait)
	expect(t, string(debug.Log[0].Data()), "foo")
	expect(t, string(debug.Log[1].Data()), "bar")
	expect(t, string(debug.Log[2].Data()), "baz")
	expect_num(t, int(debug.Term), 0)
	expect_num(t, debug.CommitIndex, 1)
}

// Tests that an append eventually propagates to the network
// rafts parameter lets one customize the type of network passed
// into this and introduce brokenness to test fault-tolerance
func testClientAppend(t *testing.T, rafts []RaftServer) {
	for i := 0; i < 5; i++ {
		go rafts[i].loop()
	}
	// Append three bits of data
	lsnget := make(chan ClientAppendResponse, 4)
	rafts[0].EventCh <- ClientAppendEvent{data: "foo", ack: lsnget}
	lsn1 := (<-lsnget).Lsn
	rafts[0].EventCh <- ClientAppendEvent{data: "bar", ack: lsnget}
	lsn2 := (<-lsnget).Lsn
	rafts[0].EventCh <- ClientAppendEvent{data: "baz", ack: lsnget}
	lsn3 := (<-lsnget).Lsn

	for i := 1; i < 5; i++ {
		// Check that the data eventually propagated
		// with the right lsn
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

func TestClientAppend(t *testing.T) {
	rafts := MakeRafts(5)
	testClientAppend(t, rafts)
}

// Tests that an append eventually propagates to the network
// for a network with a minority of servers dysfunctional
func testClientAppendDisconnect(t *testing.T, rafts []RaftServer) {
	rafts[3].EventCh <- DisconnectEvent{}
	rafts[4].EventCh <- DisconnectEvent{}
	for i := 0; i < 5; i++ {
		go rafts[i].loop()
	}
	lsnget := make(chan ClientAppendResponse, 4)
	rafts[0].EventCh <- ClientAppendEvent{data: "foo", ack: lsnget}
	lsn1 := (<-lsnget).Lsn
	rafts[0].EventCh <- ClientAppendEvent{data: "bar", ack: lsnget}
	lsn2 := (<-lsnget).Lsn
	rafts[0].EventCh <- ClientAppendEvent{data: "baz", ack: lsnget}
	lsn3 := (<-lsnget).Lsn

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

func TestClientAppendDisconnect(t *testing.T) {
	rafts := MakeRafts(5)
	testClientAppendDisconnect(t, rafts)
}

// Tests that an append does not propagate in a crippled network
func testClientAppendDisconnectFail(t *testing.T, rafts []RaftServer) {
	rafts[3].EventCh <- DisconnectEvent{}
	rafts[4].EventCh <- DisconnectEvent{}
	// We lose quorum here
	rafts[2].EventCh <- DisconnectEvent{}
	for i := 0; i < 5; i++ {
		go rafts[i].loop()
	}
	lsnget := make(chan ClientAppendResponse, 4)
	rafts[0].EventCh <- ClientAppendEvent{data: "foo", ack: lsnget}
	rafts[0].EventCh <- ClientAppendEvent{data: "bar", ack: lsnget}
	rafts[0].EventCh <- ClientAppendEvent{data: "baz", ack: lsnget}
	timer := time.NewTimer(time.Duration(1) * time.Second)
	select {
	case <-timer.C:
		return
	case <-rafts[1].CommitCh:
		t.Error("Data was committed when there was no quorum")
	}
}

func TestClientAppendDisconnectFail(t *testing.T) {
	rafts := MakeRafts(5)
	testClientAppendDisconnectFail(t, rafts)
}

// Tests that elections work and that the log
// persists over an election
func testElection(t *testing.T, rafts []RaftServer) {
	// Kill the leader
	rafts[0].EventCh <- DisconnectEvent{}
	for i := 0; i < 5; i++ {
		go rafts[i].loop()
	}
	// Wait sufficient time for the election to happen
	//time.Sleep(time.Duration(multiplier) * time.Second)
	newleader := 0
	// Keep hammering with appends until one of them turns out to hit a leader
	for newleader == 0 {
		for i := 1; i < 5; i++ {
			lsnget := make(chan ClientAppendResponse, 1)
			rafts[i].EventCh <- ClientAppendEvent{data: "foo", ack: lsnget}
			// If something was queued, then that must be the new leader
			if (<-lsnget).Queued {
				if newleader != 0 {
					t.Errorf("Both %v and %v were elected", i, newleader)
				}
				t.Logf("Successful ClientAppend on %v\n", i)
				newleader = i
			}
		}
	}

	for i := 1; i < 5; i++ {
		// Should get committed eventually
		commit := <-rafts[i].CommitCh
		t.Logf("Found data %v on server %v, with lsn %v\n", commit.Data(), i, commit.Lsn())
		expect(t, "foo", string(commit.Data()))
	}

	// Let's reconnect the old leader and check if it gets the new data
	rafts[0].EventCh <- ReconnectEvent{}
	{
		commit := <-rafts[0].CommitCh
		t.Logf("Found data %v on server %v, with lsn %v\n", commit.Data(), 0, commit.Lsn())
		expect(t, "foo", string(commit.Data()))
	}
	// ... and kill the new leader
	rafts[newleader].EventCh <- DisconnectEvent{}

	newnewleader := newleader
	// Keep hammering with appends until one of them turns out to hit a leader
	for newnewleader == newleader {
		for i := 0; i < 5; i++ {
			if i == newleader {
				// Don't even try for the dead leader
				continue
			}
			lsnget := make(chan ClientAppendResponse, 1)
			rafts[i].EventCh <- ClientAppendEvent{data: "bar", ack: lsnget}
			if (<-lsnget).Queued {
				t.Logf("Successful ClientAppend on %v\n", i)
				newnewleader = i
			}
		}
	}

	for i := 0; i < 5; i++ {
		if i == newleader {
			continue
		}
		// Check if the new data got committed
		commit := <-rafts[i].CommitCh
		t.Logf("Found data %v on server %v, with lsn %v\n", commit.Data(), i, commit.Lsn())
		expect(t, "bar", string(commit.Data()))
	}
}

func TestElection(t *testing.T) {
	rafts := MakeRafts(5)
	// Multiplier 3, with reliable channels we need
	// not worry about the election taking too long
	testElection(t, rafts)
}

// This "evil" communication helper simulates the dropping of RPC packets
// by refusing to send or returning bogus response channels every now and then
type EvilChanCommunicationHelper struct {
	chans []chan Signal
	t     *testing.T
}

func (c EvilChanCommunicationHelper) Send(signal Signal, id uint) {
	rand.Seed(time.Now().UTC().UnixNano())
	if rand.Intn(10) > 8 {
		// drop 10% of packets
		c.t.Logf("Dropped packet %T\n", signal)
		return // `signal` will never get sent
	}
	c.chans[id] <- signal
}

func TestClientAppendEvil(t *testing.T) {
	rafts := MakeRafts(5)
	for i := 0; i < 5; i++ {
		rafts[i].Network = EvilChanCommunicationHelper{chans: rafts[i].Network.(ChanCommunicationHelper).chans, t: t}
	}
	testClientAppend(t, rafts)
}

func TestClientAppendDisconnectEvil(t *testing.T) {
	rafts := MakeRafts(5)
	for i := 0; i < 5; i++ {
		rafts[i].Network = EvilChanCommunicationHelper{chans: rafts[i].Network.(ChanCommunicationHelper).chans, t: t}
	}
	testClientAppendDisconnect(t, rafts)
}

func TestElectionEvil(t *testing.T) {
	rafts := MakeRafts(5)
	for i := 0; i < 5; i++ {
		rafts[i].Network = EvilChanCommunicationHelper{chans: rafts[i].Network.(ChanCommunicationHelper).chans, t: t}
	}
	testElection(t, rafts)
}

// This "lazy" communication helper sometimes snoozes before doing its work
type LazyChanCommunicationHelper struct {
	chans []chan Signal
	t     *testing.T
}

func (c LazyChanCommunicationHelper) Send(signal Signal, id uint) {
	rand.Seed(time.Now().UTC().UnixNano())
	go (func() {
		duration := rand.Intn(400)
		c.t.Logf("Snoozing for %v ms\n", duration)
		time.Sleep(time.Duration(duration) * time.Millisecond)
		c.chans[id] <- signal
	})()
}

// Our tests rely on the order of [foo, bar, baz] being preserved
// which need not always happen with a lazy send, so we need to rewrite a test
func TestClientAppendLazy(t *testing.T) {
	rafts := MakeRafts(5)
	for i := 0; i < 5; i++ {
		rafts[i].Network = LazyChanCommunicationHelper{chans: rafts[i].Network.(ChanCommunicationHelper).chans, t: t}
	}
	for i := 0; i < 5; i++ {
		go rafts[i].loop()
	}
	// Append three bits of data
	lsnget := make(chan ClientAppendResponse, 4)
	rafts[0].EventCh <- ClientAppendEvent{data: "foo", ack: lsnget}
	rafts[0].EventCh <- ClientAppendEvent{data: "bar", ack: lsnget}
	rafts[0].EventCh <- ClientAppendEvent{data: "baz", ack: lsnget}

	data := make([]string, 3)
	commit := <-rafts[1].CommitCh
	t.Logf("Found data %v, with lsn %v\n", commit.Data(), commit.Lsn())
	data[0] = string(commit.Data())
	commit = <-rafts[1].CommitCh
	t.Logf("Found data %v, with lsn %v\n", commit.Data(), commit.Lsn())
	data[1] = string(commit.Data())
	commit = <-rafts[1].CommitCh
	t.Logf("Found data %v, with lsn %v\n", commit.Data(), commit.Lsn())
	data[2] = string(commit.Data())
	for i := 2; i < 5; i++ {
		// Check that the data eventually propagated
		// with the right lsn
		commit := <-rafts[i].CommitCh
		t.Logf("Found data %v, with lsn %v\n", commit.Data(), commit.Lsn())
		expect(t, data[0], string(commit.Data()))
		commit = <-rafts[i].CommitCh
		t.Logf("Found data %v, with lsn %v\n", commit.Data(), commit.Lsn())
		expect(t, data[1], string(commit.Data()))
		commit = <-rafts[i].CommitCh
		t.Logf("Found data %v, with lsn %v\n", commit.Data(), commit.Lsn())
		expect(t, data[2], string(commit.Data()))
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
