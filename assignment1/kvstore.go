package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"time"
)

const PORT = ":9000"

// A value in the key-value store
type Value struct {
	val     string
	exptime int64
	version int64
	// Time of creation; for checking expiry
	creation time.Time
}

// A generic message to be passed to the backend
type Message struct {
	// The "acknowledgement" channel:
	// will return acknowledgement from backend and
	// any associated text
	ack chan string
	// The actual command (Set, Get, Getm, Cas, Delete)
	// I would prefer to use an enum here, but that doesn't
	// seem to be an option
	data interface{}
}

// Concrete message types

// Set a key with given expiry time
type Set struct {
	key     string
	exptime int64
	noreply bool
	value   string
}

// Get the value for a key
type Get struct {
	key string
}

// Get the metadata for a key
type Getm struct {
	key string
}

// Compare-and-swap a key's value iff the version matches
type Cas struct {
	key     string
	exptime int64
	noreply bool
	version int64
	value   string
}

// Delete a key
type Delete struct {
	key string
}

// Internal message for cleaning up expired keys
type Cleanup struct {
}

func main() {
	for {
		listener, err := net.Listen("tcp", PORT)

		if err != nil {
			log.Printf("Error setting up listener: %v", err.Error())
			continue
		}

		// This channel will queue messages from sockets
		messages := make(chan Message, 1000)

		// Start backend goroutine
		go backend(messages)

		for {
			conn, err := listener.Accept()
			if err != nil {
				log.Printf("Error starting connection: %v", err.Error())
				continue
			}
			defer conn.Close()
			// Spawn a socket goroutine
			go newSocket(conn, messages)
		}
	}
}

// Goroutine that handles interaction with a single connection
// It can send structured messages via the Message channel
func newSocket(conn net.Conn, cs chan Message) {
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
		noreply := false

		value := ""
		// In case we are supposed to read a value of n bytes, read it
		if n != -1 {
			scanner.Scan()
			value = scanner.Text()
			if len(value) != n {
				conn.Write([]byte("ERR_CMD_ERR\r\n"))
				continue
			}
		}
		// Handle noreply
		switch data.(type) {
		case Set:
			set := data.(Set)
			if set.noreply {
				noreply = true
			}
			set.value = value
			data = set
		case Cas:
			cas := data.(Cas)
			if cas.noreply {
				noreply = true
			}
			cas.value = value
			data = cas
		}

		message := Message{ack, data}
		cs <- message
		// Wait for acknowledgement
		out := <-ack
		if !noreply {
			conn.Write([]byte(out + "\r\n"))
		}
	}
}

// Goroutine that handles interaction with the k-v store
// it will receive structured messages via the Message channel
func backend(cs chan Message) {
	// The actual keyvalue store
	store := make(map[string]Value)
	// Fake ack channel for including in messages for
	// the cleanup task
	fakeAck := make(chan string, 2)
	// Schedule the every-ten-seconds cleanup
	go cleanup(cs, fakeAck)
	// Spawn a task to queue a cleanup in a delayed fashion
	// To be canceled if under load
	timer := makeCleanup(cs, fakeAck)
	for message := range cs {
		// We're under load, cancel the task to queue a cleanup
		timer.Stop()
		switch message.data.(type) {
		case Set:
			{
				data := message.data.(Set)
				version := rand.Int63()
				exptime := data.exptime
				value := data.value
				store[data.key] = Value{value, exptime, version, time.Now()}
				message.ack <- "OK " + strconv.FormatInt(version, 10)
			}
		case Get:
			{
				data := message.data.(Get)
				value, ok := store[data.key]
				if ok {
					val := value.val
					message.ack <- fmt.Sprintf("VALUE %v\r\n%v", len(val), val)
				} else {
					message.ack <- "ERR_NOT_FOUND"
				}
			}
		case Getm:
			{
				data := message.data.(Getm)
				value, ok := store[data.key]
				if ok {
					val := value.val
					message.ack <- fmt.Sprintf("VALUE %v %v %v\r\n%v", value.version,
						value.exptime, len(val), val)
				} else {
					message.ack <- "ERR_NOT_FOUND"
				}
			}
		case Delete:
			{
				data := message.data.(Delete)
				_, ok := store[data.key]
				if ok {
					delete(store, data.key)
					message.ack <- "DELETED"
				} else {
					message.ack <- "ERR_NOT_FOUND"
				}
			}
		case Cas:
			{
				data := message.data.(Cas)
				val, ok := store[data.key]
				if ok {
					if val.version == data.version {
						version := rand.Int63()
						exptime := data.exptime
						value := data.value
						store[data.key] = Value{value, exptime, version, time.Now()}
						message.ack <- "OK " + strconv.FormatInt(version, 10)
					} else {
						message.ack <- "ERR_VERSION"
					}
				} else {
					message.ack <- "ERR_NOT_FOUND"
				}
			}
		case Cleanup:
			{
				now := time.Now()
				for key := range store {
					if store[key].creation.Add(time.Duration(store[key].exptime) *
						time.Second).Before(now) {
						delete(store, key)
					}
				}
			}
		default:
			// this should never happen
			message.ack <- "ERR_INTERNAL"
		}
		// Restart timer to wait for the next message
		// If there is no message in the next two seconds, a cleanup
		// task will be queued
		timer = makeCleanup(cs, fakeAck)
	}
}

// Make a cleanup-queueing task
// This will queue a cleanup task in 2 seconds if not interrupted
// The idea is to only run cleanup if the backend has not been under much load (no messages in 2 seconds)
// This way the possibly heavy cleanup task will be run when there is a free slot
// giving us a very rudimentary task priority system
func makeCleanup(cs chan Message, fakeAck chan string) *time.Timer {
	return time.AfterFunc(time.Second*time.Duration(2), func() { cs <- Message{ack: fakeAck, data: Cleanup{}} })
}

// Always queue a cleanup task every 10 seconds
// In case of constant load, makeCleanup will always be canceled
// but we we still want to clean up the store occasionally
// Once every ten seconds should not be much load
func cleanup(cs chan Message, fakeAck chan string) {
	for {
		cs <- Message{ack: fakeAck, data: Cleanup{}}
		time.Sleep(time.Second * time.Duration(10))
	}
}

// Parse the input string and turn it into a concrete message type
func parse(inp string) (interface{}, int, error) {
	s := strings.Split(inp, " ")
	err := errors.New("ERR_CMD_ERR\r\n")
	n := -1
	var ret interface{} = nil
	switch s[0] {
	case "set":
		noreply := false
		switch len(s) {
		case 4:
		case 5:
			if s[4] == "noreply" {
				noreply = true
			} else {
				return ret, n, err
			}
		default:
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
		ret = Set{key, exptime, noreply, ""}
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
		noreply := false
		switch len(s) {
		case 5:
		case 6:
			if s[5] == "noreply" {
				noreply = true
			} else {
				return ret, n, err
			}
		default:
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
		ret = Cas{key, exptime, noreply, version, ""}
		return ret, n, nil
	default:
		return nil, n, err
	}
}
