package main

import (
	"encoding/json"
	"fmt"
	"github.com/Manishearth/cs733/assignment2/raft"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/exec"
	"strconv"
)

// The port used for propagating config files
const MAINPORT = ":8999"

// The main executable, which sets up all the servers
func main() {
	file, err := ioutil.ReadFile("./config.json")
	if err != nil {
		fmt.Printf("Could not find config file, please place config.json in working directory")
	}
	var cluster raft.ClusterConfig
	err = json.Unmarshal(file, &cluster)
	if err != nil {
		fmt.Printf("%v", err.Error())
	}

	// Configuration sync RPC
	// We do this to initialize the followers
	// with the config
	// They do not load from the same json for debugging purposes
	rpc.Register(&cluster)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", MAINPORT)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	for i := 0; i < len(cluster.Servers); i++ {
		cmd := exec.Command(os.Getenv("GOPATH")+"/bin/kvstore", strconv.Itoa(i), MAINPORT)
		cmd.Stderr = os.Stderr
		cmd.Stdout = os.Stdout
		cmd.Start()
	}

	// Block indefinitely for being able to get debugging output
	cs := make(chan bool)
	<-cs
}
