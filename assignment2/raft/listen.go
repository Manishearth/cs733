package raft

import (
	"bufio"
	"errors"
	"log"
	"net"
	"strconv"
	"strings"
	"fmt"
	"io"
)

// A value in the key-value store
type Value struct {
	val     []byte
	exptime int64
	version int64
}

// A generic message to be passed to the backend
type Message struct {
	// The actual command (Set, Get, Getm, Cas, Delete)
	// I would prefer to use an enum here, but that doesn't
	// seem to be an option
	Data interface{}
}

// Concrete message types

// Set a key with given expiry time
type Set struct {
	Key     string
	Exptime int64
	Value   []byte
}

// Get the value for a key
type Get struct {
	Key string
}

// Get the metadata for a key
type Getm struct {
	Key string
}

// Compare-and-swap a key's value iff the version matches
type Cas struct {
	Key     string
	Exptime int64
	Version int64
	Value   []byte
}

// Delete a key
type Delete struct {
	Key string
}

// Parse the input string and turn it into a concrete message type
func parse(inp string) (interface{}, int, error) {
	s := strings.Split(inp, " ")
	err := errors.New("ERR_CMD_ERR\r\n")
	n := -1
	var ret interface{} = nil
	switch s[0] {
	case "set":
		if len(s) != 4 {
			return ret, n, err
		}
		key := s[1]
		exp, e := strconv.Atoi(s[2])
		exptime := int64(exp)
		if e != nil {
			return ret, n, err
		}
		n, e := strconv.Atoi(s[3])
		if e != nil {
			return ret, n, err
		}
		ret = Set{key, exptime, make([]byte, 0)}
		return ret, n, nil
	case "get":
		if len(s) != 2 {
			return ret, n, err
		}
		ret = Get{s[1]}
		return ret, n, nil
	case "getm":
		if len(s) != 2 {
			return ret, n, err
		}
		ret = Getm{s[1]}
		return ret, n, nil
	case "delete":
		if len(s) != 2 {
			return ret, n, err
		}
		ret = Delete{s[1]}
		return ret, n, nil
	case "cas":
		if len(s) != 6 {
			return ret, n, err
		}
		key := s[1]
		exp, e := strconv.Atoi(s[2])
		exptime := int64(exp)
		if e != nil {
			return ret, n, err
		}
		ver, e := strconv.Atoi(s[3])
		version := int64(ver)
		if e != nil {
			return ret, n, err
		}
		n, e := strconv.Atoi(s[4])
		if e != nil {
			return ret, n, err
		}
		ret = Cas{key, exptime, version, make([]byte, 0)}
		return ret, n, nil
	default:
		return nil, n, err
	}
}

func (t *Raft) Listen() {
	for {
		listener, err := net.Listen("tcp", ":"+strconv.Itoa(t.Config.Servers[t.SelfId].ClientPort))

		if err != nil {
			log.Printf("Error setting up listener: %v", err.Error())
			continue
		}

		// This channel will queue messages from sockets
		messages := make(chan IndexedAck, 1000)

		// Start backend goroutine
		go t.Backend(messages)

		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Printf("Error starting connection: %v", err.Error())
				continue
			}
			defer conn.Close()
			// Spawn a socket goroutine
			go t.newSocket(conn, messages)
		}
	}
}

// Goroutine that handles interaction with a single connection
// It can send structured messages via the Message channel
func (t *Raft) newSocket(conn net.Conn, cs chan IndexedAck) {
	scanner := bufio.NewScanner(conn)
	// scanner.Scan() automatically breaks at \r\n for us
	for scanner.Scan() {
		// Create acknowledgement channel
		ack := make(chan string, 2)

		// Parse input
		data, n, err := parse(scanner.Text())
		if err != nil {
			conn.Write([]byte(err.Error()))
			continue
		}

		value := make([]byte, 0)
		// In case we are supposed to read a value of n bytes, read it
		if n != -1 {
			buf := make([]byte, n+2)
			_, err = io.ReadFull(conn,buf)
			if err != nil {
				conn.Write([]byte("ERR_CMD_ERR\r\n"))
				continue
			}
			value = buf[0:n]
		}

		switch data.(type) {
		case Set:
			set := data.(Set)
			set.Value = value
			data = set
		case Cas:
			cas := data.(Cas)
			cas.Value = value
			data = cas
		}

		message := Message{data}
		log, err := t.Append(message)
		if err != nil {
			server := t.Config.Servers[0]
			conn.Write([]byte(fmt.Sprintf("ERR_REDIRECT %v:%v", server.Hostname, server.ClientPort)))
			return
		}
		cs<- IndexedAck {
			lsn: log.Lsn,
			ack: ack,
		}
		// Wait for acknowledgement
		out := <-ack
		conn.Write([]byte(out+"\r\n"))
	}
}
