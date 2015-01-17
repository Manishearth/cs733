package main

import (
    "net"
    "bufio"
)

const PORT = ":9000"

func main () {
    l, _ := net.Listen("tcp", PORT)
    for {
        conn, _ := l.Accept();
        defer conn.Close()
        go newSocket(conn)
    }
}

func newSocket(conn net.Conn) {
    scanner := bufio.NewScanner(conn)
    for scanner.Scan() {
        conn.Write([]byte("banana"+scanner.Text()+"\r\n"))
    }
}