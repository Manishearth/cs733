package raft

import (
	"testing"
    "fmt"
)

func TestAppend(t *testing.T) {
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

