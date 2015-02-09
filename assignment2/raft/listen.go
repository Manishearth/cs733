package raft

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
)

// This file contains most of the code on the "listening" side of the system

// Do all the setup for listening
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
		if len(s) != 5 {
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

// Goroutine that handles interaction with a single connection
func (t *Raft) newSocket(conn net.Conn, cs chan IndexedAck) {
	reader := bufio.NewReader(conn)
	// scanner.Scan() automatically breaks at \r\n for us
	for {
		// Create acknowledgement channel
		ack := make(chan []byte, 2)
		text, err := ReadString(reader)
		if err != nil {
			conn.Write([]byte("ERR_CMD_ERR\r\n"))
			continue
		}
		// Parse input
		data, n, err := parse(text)
		if err != nil {
			conn.Write([]byte(err.Error()))
			continue
		}

		value := make([]byte, 0)
		// In case we are supposed to read a value of n bytes, read it
		if n != -1 {
			buf, err := ReadBytes(reader, n)
			if err != nil {
				conn.Write([]byte("ERR_CMD_ERR\r\n"))
				continue
			}
			value = buf
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
			conn.Write([]byte(fmt.Sprintf("ERR_REDIRECT %v:%v\r\n", server.Hostname, server.ClientPort)))
			return
		}
		cs <- IndexedAck{
			lsn: log.Lsn,
			ack: ack,
		}
		// Wait for acknowledgement
		out := <-ack
		conn.Write(append(out, '\r', '\n'))
	}
}

// I couldn't find a library which lets me both
// read whole lines up to \r\n or \n, as well as
// do n-byte reads when I want to, so I wrote my own
func ReadString(r *bufio.Reader) (string, error) {
	buf := make([]byte, 0)
	for {
		by, err := r.ReadByte()
		if err != nil {
			continue
		}
		if by == byte('\r') {
			break
		}
		if by == '\n' {
			return string(buf), nil
		}
		buf = append(buf, by)
	}
	for {
		by, err := r.ReadByte()
		if err != nil {
			continue
		}
		if by == '\n' {
			return string(buf), nil
		} else {
			return string(buf), errors.New("ERR_CMD_ERR\r\n")
		}
	}

}
func ReadBytes(r *bufio.Reader, n int) ([]byte, error) {
	buf := make([]byte, 0)
	i := 0
	for {
		by, err := r.ReadByte()
		if err != nil {
			continue
		}
		buf = append(buf, by)
		i++
		if i == n {
			break
		}
	}
	for {
		by, err := r.ReadByte()
		if err != nil {
			continue
		}
		if by == byte('\r') {
			break
		}
		if by == '\n' {
			return buf, nil
		}

	}
	for {
		by, err := r.ReadByte()
		if err != nil {
			continue
		}
		if by == '\n' {
			return buf, nil
		}
	}
	return buf, errors.New("ERR_CMD_ERR\r\n")
}
