package main

import (
	"fmt"
	"github.com/Manishearth/cs733/assignment2/raft"
	"log"
	"net/rpc"
	"os"
	"strconv"
)

func main() {
	// Fetch cluster config
	id, _ := strconv.Atoi(os.Args[1])
	port := os.Args[2]
	fmt.Printf("%v, %v", id, port)
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
	fmt.Printf("x: %v", r.Config.Servers[id].ClientPort)
}
