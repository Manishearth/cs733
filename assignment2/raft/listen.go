package raft

import (
    "bufio"
    "errors"
    "log"
    "net"
    "strconv"
    "strings"
)
// A value in the key-value store
type Value struct {
    val     []byte
    exptime int64
    version int64
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
    value   []byte
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
    version int64
    value   []byte
}

// Delete a key
type Delete struct {
    key string
}


// Parse the input string and turn it into a concrete message type
func parse(inp string) (interface{}, int, error) {
    s := strings.Split(inp, " ")
    err := errors.New("ERR_CMD_ERR\r\n")
    n := -1
    var ret interface{} = nil
    switch s[0] {
    case "set":
        if len(s) != 2 {
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
        ret = Set{key, exptime, make([]byte,0)}
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
        ret = Cas{key, exptime, version, make([]byte,0)}
        return ret, n, nil
    default:
        return nil, n, err
    }
}

func (t *Raft) listen() {
    for {
        listener, err := net.Listen("tcp", t.config[t.selfId].ClientPort)

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

        value := make([]byte, 0)
        // In case we are supposed to read a value of n bytes, read it
        if n != -1 {
            buf := make([]byte, n)
            _, err = conn.Read(buf)
            if err != nil {
                conn.Write([]byte("ERR_CMD_ERR\r\n"))
                continue                
            }
            // Check if there was some data left
            scanner.Scan()
            v := scanner.Text()
            if len(value) != 0 {
                conn.Write([]byte("ERR_CMD_ERR\r\n"))
                continue
            }
            value = buf
        }
        // Handle noreply
        switch data.(type) {
        case Set:
            set := data.(Set)
            set.value = value
            data = set
        case Cas:
            cas := data.(Cas)
            cas.value = value
            data = cas
        }

        message := Message{ack, data}
        cs <- message
        // Wait for acknowledgement
        out := <-ack
    }
}


func backend(cs chan Message) {

}