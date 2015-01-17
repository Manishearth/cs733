package main

import (
    "net"
    "bufio"
    "strings"
    "strconv"
    "errors"
    "math/rand"
    "fmt"
    "os"
)

const PORT = ":9000"

func main () {
    listener, err := net.Listen("tcp", PORT)

    if err != nil {
        fmt.Printf("Error setting up listener: %v", err.Error())
        os.Exit(1)
    }

    // This channel will queue messages from sockets
    messages := make(chan Message)

    // Start backend goroutine
    go backend(messages)

    for {
        conn, err := listener.Accept();
        if err != nil {
            fmt.Printf("Error starting connection: %v", err.Error())
            continue
        }
        defer conn.Close()
        // Spawn a socket goroutine
        go newSocket(conn, messages)
    }
}

// Goroutine that handles interaction with a single connection
// It can send structured messages via the Message channel
func newSocket(conn net.Conn, cs chan Message) {
    scanner := bufio.NewScanner(conn)
    // scanner.Scan() automatically breaks at \r\n for us
    for scanner.Scan() {
        // Create acknowledgement channel
        ack := make(chan string)

        // Parse input
        data, n, err := parse(scanner.Text())
        if err != nil {
            conn.Write([]byte(err.Error()))
            continue
        }
        message := Message {ack, "", data}

        // In case we are supposed to read a value of n bytes, read it
        if n != -1 {
            scanner.Scan()
            message.value = scanner.Text()
            if len(message.value) != n {
                conn.Write([]byte("ERR_CMD_ERR\r\n"))
                continue                
            }
        }
        cs <- message
        // Wait for acknowledgement
        out :=  <- ack

        // Handle noreply
        switch message.data.(type) {
            case Set:
                if message.data.(Set).noreply {
                    continue
                }
            case Cas:
                if message.data.(Cas).noreply {
                    continue
                }
        }
        conn.Write([]byte(out+"\r\n"))
    }
}

// Goroutine that handles interaction with the k-v store
// it will receive structured messages via the Message channel
func backend(cs chan Message) {
    // The actual keyvalue store
    store := make(map[string]Value)
    for {
        message :=  <- cs
        switch message.data.(type) {
            case Set: {
                data := message.data.(Set)
                version := rand.Int63()
                value := message.value
                exptime := data.exptime
                store[data.key] = Value {value, exptime, version}
                message.ack <- "OK "+strconv.FormatInt(version, 10)
            }
            case Get: {
                data := message.data.(Get)
                value, ok := store[data.key]
                if ok {
                    val := value.val
                    message.ack <- fmt.Sprintf("VALUE %v\r\n%v", len(val), val)
                } else {
                    message.ack <- "ERR_NOT_FOUND"
                }
            }
            case Getm: {
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
            case Delete: {
                data := message.data.(Delete)
                _, ok := store[data.key]
                if ok {
                    delete(store, data.key)
                    message.ack <- "DELETED"
                } else {
                    message.ack <- "ERR_NOT_FOUND"
                }
            }
            case Cas: {
                data := message.data.(Cas)
                value := message.value
                val, ok := store[data.key]
                if ok {
                    if val.version == data.version {
                        version := rand.Int63()
                        exptime := data.exptime
                        store[data.key] = Value {value, exptime, version}
                        message.ack <- "OK "+strconv.FormatInt(version, 10)
                    } else {
                        message.ack <- "ERR_VERSION"
                    }
                } else {
                    message.ack <- "ERR_NOT_FOUND"
                }                
            }
            default:
                message.ack <- "ERR_INTERNAL"
        }
    }
}

// A value in the key-value store
type Value struct {
    val string
    exptime int64
    version int64
}

// A generic message to be passed to the backend
type Message struct {
    // The "acknowledgement" channel:
    // will return acknowledgement from backend and
    // any associated text
    ack chan string
    // A vlaue, if any
    value string
    // The actual command (Set, Get, Getm, Cas, Delete)
    // I would prefer to use an enum here, but that doesn't
    // seem to be an option
    data interface{}
}

type Set struct {
    key string
    exptime int64
    noreply bool
}

type Get struct {
    key string
}

type Getm struct {
    key string
}

type Cas struct {
    key string
    exptime int64
    noreply bool
    version int64
}

type Delete struct {
    key string
}

func parse(inp string) (interface{}, int, error) {
    s := strings.Split(inp, " ")
    err := errors.New("ERR_CMD_ERR\r\n")
    n := -1
    var ret interface {} = nil
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
                default: return ret, n, err
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
            ret = Set {key, exptime, noreply}
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
                default: return ret, n, err
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
            ret = Cas {key, exptime, noreply, version}
            return ret, n, nil
        default: return nil, n, err
    }
}