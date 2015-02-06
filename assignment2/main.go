package main

import (
	"fmt"
	"github.com/Manishearth/cs733/assignment2/raft"
)

func main() {
	var servers [5]raft.ServerConfig
	for i := 0; i < 5; i++ {
		servers[i] = ServerConfig{
			Id:         i,
			Hostname:   "localhost",
			ClientPort: 8000 + i,
			LogPort:    9000 + i,
		}
	}
}
