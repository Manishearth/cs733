package main

import (
	"encoding/gob"
	"github.com/Manishearth/cs733/assignment2/raft"
	"log"
	"net/rpc"
	"os"
	"strconv"
)

// A separate executable to be spawned for each server.
// The config is passed to this via an RPC rather than the json file (which is only read by main())
func main() {
	// For the RPC to work
	gob.Register(raft.Message{})
	gob.Register(raft.Set{})
	gob.Register(raft.Get{})
	gob.Register(raft.Cas{})
	gob.Register(raft.Getm{})
	// Fetch cluster config
	id, _ := strconv.Atoi(os.Args[1])
	port := os.Args[2]
	var config raft.ClusterConfig
	client, err := rpc.DialHTTP("tcp", "localhost"+port)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	err = client.Call("ClusterConfig.GetSetup", &id, &config)
	if err != nil {
		log.Fatal("rpc error:", err)
	}
	commitCh := make(chan raft.LogEntry)

	// Initialize SharedLog
	r, _ := raft.NewRaft(&config, id, commitCh)

	// Initialize the kv backend and the listener
	r.Listen()
}
