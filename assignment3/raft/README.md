## Raft implementation

This package contains an implementation of the [Raft consensus algorithm](https://ramcloud.stanford.edu/raft.pdf).

[Generated documentation](https://godoc.org/github.com/Manishearth/cs733/assignment3/raft)

Sample usage:


```go
rafts := MakeRafts(5) // 5 is hardcoded for now
for i := 0; i < 5; i++ {
    go rafts[i].loop() // set up event loop
}
lsnget := make(chan Response, 1)
// Send an Append event to the leader
rafts[0].EventCh <- ChanMessage{lsnget, ClientAppendEvent{data: "foo"}}
// Record the lsn
lsn := (<-lsnget).(ClientAppendResponse).Lsn

commit := <-rafts[1].CommitCh
fmt.Printf("Found data %v, with lsn %v\n", commit.Data(), commit.Lsn())

```

This commit will be of type `LogEntry`, and will contain a committed set of data on the first server

There are various events which you can directly send to a raft instance. Notable ones include `DisconnectEvent`, `ReconnectEvent`, and `DebugEvent`.

The first two will disconnect and reconnect the server from the network, and the last one will return debugging data through the ack channel specified in the `ChanMessage`

All the events that can be sent (see [documentation](https://godoc.org/github.com/Manishearth/cs733/assignment3/raft) for details on their contents):

 - `TimeoutEvent`
 - `ClientAppendEvent` (returns `ClientAppendResponse`)
 - `AppendRPCEvent` (returns `AppendRPCResponse`)
 - `DebugEvent` (returns `DebugResponse`)
 - `VoteRequestEvent` (returns `VoteResponse`)
 - `HeartBeatEvent`
 - `DisconnectEvent`
 - `ReconnectEvent`

All responses can be sent through the `EventCh` as well

To make this work on a network, one will have to replace `ChanCommunicationHelper` with a custom one that uses RPCs under the hood and knows about the mapping of server IP/port to server id. Everything else should stay mostly the same.


Use `go test -v` to run the tests. The test framework defines some custom communication helpers which can drop packets or sleep between packet delivery to test robustness.
