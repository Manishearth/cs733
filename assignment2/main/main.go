package main

import (
	"github.com/Manishearth/cs733/assignment2/raft"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/exec"
	"strconv"
)

const MAINPORT = ":8999"
const NUMSERVER = 5

func main() {
	var servers [NUMSERVER]raft.ServerConfig
	for i := 0; i < NUMSERVER; i++ {
		servers[i] = raft.ServerConfig{
			Id:         i,
			Hostname:   "localhost",
			ClientPort: 8000 + i,
			LogPort:    9000 + i,
		}
	}
	cluster := raft.ClusterConfig{
		Path:    "/tmp/out",
		Servers: servers[:],
	}

	// Configuration sync RPC
	// We do this to initialize the followers
	// with the config
	rpc.Register(&cluster)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", MAINPORT)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	for i := 0; i < NUMSERVER; i++ {
		cmd := exec.Command(os.Getenv("GOPATH")+"/bin/kvstore", strconv.Itoa(i), MAINPORT)
		cmd.Stderr = os.Stderr
		cmd.Stdout = os.Stdout
		cmd.Start()
	}

	// Sadly I haven't figured out how to make the http server stop serving after five counts,
	// so this process must block indefinitely
	cs := make(chan bool)
	<-cs
}
