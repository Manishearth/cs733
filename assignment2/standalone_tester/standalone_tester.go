package main

import (
	"bufio"
	"fmt"
	"github.com/Manishearth/cs733/assignment2/raft"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// Hardcoded since my tests depend on the given config anyway
const PORT = ":8000"

func main() {
	TestTCP()
	SingleTCPBin()
	TestKill()
}

// Kills the listener of a given port on most linux machines
func kill(port string) {
	fmt.Printf("Killing server on port %v\n", port)
	c1 := exec.Command("netstat", "-tulpn")
	c2 := exec.Command("grep", port)
	c3 := exec.Command("sed", "s/^.*LISTEN\\s*//g")
	c4 := exec.Command("sed", "s|/.*$||g")
	c5 := exec.Command("xargs", "kill")
	c2.Stdin, _ = c1.StdoutPipe()
	c3.Stdin, _ = c2.StdoutPipe()
	c4.Stdin, _ = c3.StdoutPipe()
	c5.Stdin, _ = c4.StdoutPipe()
	c1.Start()
	c2.Start()
	c3.Start()
	c4.Start()
	c5.Run()
}

// Kills servers one by one until majority is lost
func TestKill() {
	fmt.Println("Testing resilience to unresponsive servers")
	conn, err := net.Dial("tcp", fmt.Sprintf("localhost%v", PORT))

	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()
	name := "killed"
	value := "killed"
	exptime := 4000
	scanner := bufio.NewScanner(conn)
	fmt.Fprintf(conn, "set %v %v %v\r\n%v\r\n", name, exptime, len(value), value)
	scanner.Scan()
	fmt.Fprintf(conn, "get %v\r\n", name)
	scanner.Scan()
	expect(scanner.Text(), fmt.Sprintf("VALUE %v", len(value)))
	scanner.Scan()
	expect(value, scanner.Text())
	kill(":8001")
	fmt.Fprintf(conn, "get %v\r\n", name)
	scanner.Scan()
	expect(scanner.Text(), fmt.Sprintf("VALUE %v", len(value)))
	scanner.Scan()
	expect(value, scanner.Text())
	kill(":8002")
	fmt.Fprintf(conn, "get %v\r\n", name)
	scanner.Scan()
	expect(scanner.Text(), fmt.Sprintf("VALUE %v", len(value)))
	scanner.Scan()
	expect(value, scanner.Text())
	fmt.Println("Testing behavior in situation without majority")
	kill(":8003") // Majority lost
	fmt.Fprintf(conn, "get %v\r\n", name)
	fail := make(chan bool)
	go expectation(scanner, fail)
	timer := time.After(time.Duration(time.Second * 10))
	select {
	case _ = <-fail:
		panic("Server replied even without consensus")
	case _ = <-timer:
		return
	}
}

func expectation(scanner *bufio.Scanner, fail chan bool) {
	scanner.Scan()
	// this should never scan
	fail <- true
}

// Test the TCP interface
func TestTCP() {
	fmt.Println("Running basic functionality and concurrency tests")
	fmt.Println("Warning: these will take time")
	done := make(chan bool, 10)
	// Simple interface checks
	go singleTCP(done, "hi", "bye")
	go singleTCP(done, "banana", "potato")
	go singleTCP(done, "watermelon", "cantaloupe")

	// Concurrent CAS check
	go concurrentTCP(done, "concurrent-banana", "concurrent-potato")
	go concurrentTCP(done, "concurrent-hi", "concurrent-bye")

	// Wait for tests to finish
	for i := 0; i < 7; i++ {
		<-done
	}
}

// Simple serial check of getting and setting
func singleTCP(done chan bool, name string, value string) {
	exptime := 300
	conn, err := net.Dial("tcp", fmt.Sprintf("localhost%v", PORT))

	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()

	// Blank get
	fmt.Fprintf(conn, "get %v\r\n", name)
	scanner := bufio.NewScanner(conn)
	scanner.Scan()
	expect(scanner.Text(), "ERR_NOT_FOUND")

	// Set then get
	fmt.Fprintf(conn, "set %v %v %v\r\n%v\r\n", name, exptime, len(value), value)
	scanner.Scan()

	resp := scanner.Text()
	arr := strings.Split(resp, " ")
	expect(arr[0], "OK")
	ver, err := strconv.Atoi(arr[1])
	if err != nil {
		panic("Non-numeric version found")
	}
	version := int64(ver)
	fmt.Fprintf(conn, "get %v\r\n", name)
	scanner.Scan()
	expect(scanner.Text(), fmt.Sprintf("VALUE %v", len(value)))
	scanner.Scan()
	expect(value, scanner.Text())
	fmt.Fprintf(conn, "getm %v\r\n", name)
	scanner.Scan()
	expect(scanner.Text(), fmt.Sprintf("VALUE %v %v %v", version, exptime, len(value)))
	scanner.Scan()
	expect(scanner.Text(), value)

	done <- true
}

// Tests binary capability
func SingleTCPBin() {
	fmt.Println("Testing binary capability")
	exptime := 300
	conn, err := net.Dial("tcp", fmt.Sprintf("localhost%v", PORT))

	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()
	name := "binary"
	value := "\r\n"
	reader := bufio.NewReader(conn)
	// Set then get
	fmt.Fprintf(conn, "set %v %v %v\r\n%v\r\n", name, exptime, len(value), value)

	resp, _ := raft.ReadString(reader)
	arr := strings.Split(resp, " ")
	expect(arr[0], "OK")
	_, err = strconv.Atoi(arr[1])
	if err != nil {
		panic("Non-numeric version found")
	}
	fmt.Fprintf(conn, "get %v\r\n", name)
	resp, _ = raft.ReadString(reader)
	expect(resp, fmt.Sprintf("VALUE %v", len(value)))
	val, _ := raft.ReadBytes(reader, 2)
	expect(value, string(val))
	fmt.Println("Binary capability verified")
}

// Sets up to CASes to race
func concurrentTCP(done chan bool, name string, value string) {
	exptime := 300
	conn, err := net.Dial("tcp", fmt.Sprintf("localhost%v", PORT))

	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()

	fmt.Fprintf(conn, "set %v %v %v\r\n%v\r\n", name, exptime, len(value), value)
	scanner := bufio.NewScanner(conn)
	scanner.Scan()
	resp := scanner.Text()
	arr := strings.Split(resp, " ")
	expect(arr[0], "OK")
	ver, err := strconv.Atoi(arr[1])
	if err != nil {
		panic("Non-numeric version found")
	}
	version := int64(ver)
	go concurrentTCPInner(done, name, exptime, version, "setby1", "setby2", "First")
	go concurrentTCPInner(done, name, exptime, version, "setby2", "setby1", "Second")
}

func concurrentTCPInner(done chan bool, name string, exptime int,
	version int64, newval string, otherval string, testname string) {
	conn, err := net.Dial("tcp", fmt.Sprintf("localhost%v", PORT))

	value := newval
	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()
	fmt.Fprintf(conn, "cas %v %v %v %v\r\n%v\r\n", name, exptime, version, len(value), value)
	scanner := bufio.NewScanner(conn)
	scanner.Scan()
	resp := scanner.Text()
	arr := strings.Split(resp, " ")
	if arr[0] == "OK" {
		fmt.Printf("%v goroutine won\n", testname)
	} else {
		expect(resp, "ERR_VERSION")
		value = otherval
	}
	fmt.Fprintf(conn, "get %v\r\n", name)
	scanner.Scan()
	expect(scanner.Text(), fmt.Sprintf("VALUE %v", len(value)))
	scanner.Scan()
	expect(scanner.Text(), value)
	done <- true
}

// Useful testing function
func expect(a string, b string) {
	if a != b {
		panic(fmt.Sprintf("Expected %v, found %v", b, a))
	}
}
