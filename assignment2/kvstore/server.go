package main

import (
	"github.com/Manishearth/cs733/assignment2/raft"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"encoding/gob"
)

func main() {
	gob.Register(raft.Message{})
	gob.Register(raft.Set{})
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

	r, _ := raft.NewRaft(&config, id, commitCh)

    

    r.Listen()
}


