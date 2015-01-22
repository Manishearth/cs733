package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Serial tests for functioning of backend
func TestBackend(t *testing.T) {
	messages := make(chan Message, 100)
	go backend(messages)
	ack := make(chan string, 2)

	// Check that the store is empty
	message := Message{ack, Get{"banana"}}
	messages <- message
	resp := <-ack
	expect(t, resp, "ERR_NOT_FOUND")

	message.data = Getm{"banana"}
	messages <- message
	resp = <-ack
	expect(t, resp, "ERR_NOT_FOUND")

	message.data = Delete{"banana"}
	messages <- message
	resp = <-ack
	expect(t, resp, "ERR_NOT_FOUND")

	// Add a single k-v pair to the store
	message.data = Set{key: "banana", exptime: 42, noreply: false, value: "potato"}
	messages <- message
	resp = <-ack
	arr := strings.Split(resp, " ")
	expect(t, arr[0], "OK")
	ver, err := strconv.Atoi(arr[1])
	if err != nil {
		t.Error("Non-numeric version found")
	}
	version := int64(ver)

	// Test that the k-v pair exists and Get/Getm are working properly
	message.data = Get{"banana"}
	messages <- message
	resp = <-ack
	expect(t, resp, "VALUE 6\r\npotato")

	message.data = Getm{"banana"}
	messages <- message
	resp = <-ack
	expect(t, resp, fmt.Sprintf("VALUE %v 42 6\r\npotato", version))

	// Add another k-v pair to test for clashes
	message.data = Set{key: "watermelon", exptime: 100, noreply: false, value: "tomato"}
	messages <- message
	resp = <-ack

	// Check that they still work
	message.data = Get{"banana"}
	messages <- message
	resp = <-ack
	expect(t, resp, "VALUE 6\r\npotato")

	message.data = Getm{"banana"}
	messages <- message
	resp = <-ack
	expect(t, resp, fmt.Sprintf("VALUE %v 42 6\r\npotato", version))

	// CAS with bad version
	message.data = Cas{key: "banana", exptime: 42, noreply: false, value: "tomato", version: version + 100}
	messages <- message
	resp = <-ack
	expect(t, resp, "ERR_VERSION")

	// CAS with bad key
	message.data = Cas{key: "pineapple", exptime: 42, noreply: false, value: "tomato", version: version + 100}
	messages <- message
	resp = <-ack
	expect(t, resp, "ERR_NOT_FOUND")

	// CAS with good version
	message.data = Cas{key: "banana", exptime: 100, noreply: false, value: "kiwi", version: version}
	messages <- message
	resp = <-ack
	arr = strings.Split(resp, " ")
	expect(t, arr[0], "OK")
	ver, err = strconv.Atoi(arr[1])
	if err != nil {
		t.Error("Non-numeric version found")
	}
	version = int64(ver)

	// Check that it was updated
	message.data = Get{"banana"}
	messages <- message
	resp = <-ack
	expect(t, resp, "VALUE 4\r\nkiwi")

	message.data = Getm{"banana"}
	messages <- message
	resp = <-ack
	expect(t, resp, fmt.Sprintf("VALUE %v 100 4\r\nkiwi", version))

	// Remove watermelon
	message.data = Delete{"watermelon"}
	messages <- message
	resp = <-ack
	expect(t, resp, "DELETED")

	// `watermelon` should be gone
	message.data = Get{"watermelon"}
	messages <- message
	resp = <-ack
	expect(t, resp, "ERR_NOT_FOUND")

	message.data = Getm{"watermelon"}
	messages <- message
	resp = <-ack
	expect(t, resp, "ERR_NOT_FOUND")

	// `banana` should still exist
	message.data = Get{"banana"}
	messages <- message
	resp = <-ack
	expect(t, resp, "VALUE 4\r\nkiwi")

	message.data = Getm{"banana"}
	messages <- message
	resp = <-ack
	expect(t, resp, fmt.Sprintf("VALUE %v 100 4\r\nkiwi", version))

	// Remove `banana`
	message.data = Delete{"banana"}
	messages <- message
	resp = <-ack
	expect(t, resp, "DELETED")

	// `banana` should now be gone
	message.data = Get{"banana"}
	messages <- message
	resp = <-ack
	expect(t, resp, "ERR_NOT_FOUND")

	message.data = Getm{"banana"}
	messages <- message
	resp = <-ack
	expect(t, resp, "ERR_NOT_FOUND")

	// CAS with deleted key
	message.data = Cas{key: "banana", exptime: 42, noreply: false, value: "tomato", version: version + 100}
	messages <- message
	resp = <-ack
	expect(t, resp, "ERR_NOT_FOUND")
}

// This tests the backend's ability to hand lots
// of clients at once, with some racing attempts to CAS
func TestBackendConcurrent(t *testing.T) {
	done := make(chan bool, 10)
	messages := make(chan Message, 100)
	go backend(messages)
	// Add some background noise. This shouldn't race, but it tests
	// our ability to buffer and handle load
	for i := 0; i < 5; i++ {
		go interference(t, messages, done, "i"+strconv.Itoa(i),
			"tomato"+strconv.Itoa(i), int64(100+i))
	}

	// Each one of these is an concurrency test
	// which will try to race with itself
	go concurrent(t, messages, done, "prefix1")
	go concurrent(t, messages, done, "prefix2")
	go concurrent(t, messages, done, "prefix3")

	// Wait for tests to finish
	for i := 0; i < 9; i++ {
		<-done
	}
}

// An independent goroutine to be spawned (to trigger interference)
// Just sets, swaps, and deletes a single key
func interference(t *testing.T, messages chan Message, done chan bool, key string, value string, exptime int64) {
	for i := 0; i < 10; i++ {
		ack := make(chan string, 2)
		message := Message{ack, Set{key: key, exptime: exptime, noreply: false, value: value}}
		messages <- message
		resp := <-ack
		arr := strings.Split(resp, " ")
		expect(t, arr[0], "OK")
		ver, err := strconv.Atoi(arr[1])
		if err != nil {
			t.Error("Non-numeric version found")
		}
		version := int64(ver)

		message.data = Get{key}
		messages <- message
		resp = <-ack
		expect(t, resp, fmt.Sprintf("VALUE %v\r\n%v", len(value), value))

		message.data = Getm{key}
		messages <- message
		resp = <-ack
		expect(t, resp, fmt.Sprintf("VALUE %v %v %v\r\n%v", version, exptime, len(value), value))

		message.data = Cas{key: key, exptime: exptime, noreply: false, value: value, version: version}
		messages <- message
		resp = <-ack
		arr = strings.Split(resp, " ")
		expect(t, arr[0], "OK")
		ver, err = strconv.Atoi(arr[1])
		if err != nil {
			t.Error("Non-numeric version found")
		}
		version = int64(ver)

		message.data = Get{key}
		messages <- message
		resp = <-ack
		expect(t, resp, fmt.Sprintf("VALUE %v\r\n%v", len(value), value))

		message.data = Getm{key}
		messages <- message
		resp = <-ack
		expect(t, resp, fmt.Sprintf("VALUE %v %v %v\r\n%v", version, exptime, len(value), value))

		message.data = Delete{key}
		messages <- message
		resp = <-ack
		expect(t, resp, "DELETED")

		// `banana` should now be gone
		message.data = Get{key}
		messages <- message
		resp = <-ack
		expect(t, resp, "ERR_NOT_FOUND")

		message.data = Getm{key}
		messages <- message
		resp = <-ack
		expect(t, resp, "ERR_NOT_FOUND")
	}
	done <- true
}

// Sets a key with the given prefix, then spawns two threads competing to CAS it
func concurrent(t *testing.T, messages chan Message, done chan bool, prefix string) {
	key := prefix + "-concurrent"
	exptime := int64(100)
	value := "set"
	ack := make(chan string, 2)
	message := Message{ack, Set{key: key, exptime: exptime, noreply: false, value: value}}
	messages <- message
	resp := <-ack
	arr := strings.Split(resp, " ")
	expect(t, arr[0], "OK")
	ver, err := strconv.Atoi(arr[1])
	if err != nil {
		t.Error("Non-numeric version found")
	}
	version := int64(ver)
	// When run with GOMAXPROCS=4, this is
	go concurrentInner(t, messages, done, key, version, "setby1", "setby2", "First")
	go concurrentInner(t, messages, done, key, version, "setby2", "setby1", "Second")
}

// CAS the given key, report if successful, and check validity
func concurrentInner(t *testing.T, messages chan Message, done chan bool, key string,
	version int64, value string, othervalue string, name string) {
	ack := make(chan string, 2)
	exptime := int64(200)
	message := Message{ack, Cas{key: key, exptime: exptime, noreply: false, value: value, version: version}}
	messages <- message

	resp := <-ack
	arr := strings.Split(resp, " ")
	if arr[0] == "OK" {
		t.Logf("%v concurrent goroutine won", name)
	} else {
		value = othervalue
	}
	message.data = Get{key}
	messages <- message
	resp = <-ack
	expect(t, resp, fmt.Sprintf("VALUE %v\r\n%v", len(value), value))
	done <- true
}

// Test the TCP interface
func TestTCP(t *testing.T) {
	go main()
	// Wait for setup to happen, could take some time
	time.Sleep(time.Microsecond * time.Duration(100))

	done := make(chan bool, 2)
	// Simple interface checks
	go singleTCP(t, done, "hi", "bye")
	go singleTCP(t, done, "banana", "potato")
	go singleTCP(t, done, "watermelon", "cantaloupe")

	// Concurrent CAS check
	go concurrentTCP(t, done, "concurrent-banana", "concurrent-potato")
	go concurrentTCP(t, done, "concurrent-hi", "concurrent-bye")

	// Wait for tests to finish
	for i := 0; i < 7; i++ {
		<-done
	}
	// Everything will just exit here, no need to close goroutines from main() explicitly
}

// Simple serial check of getting and setting
func singleTCP(t *testing.T, done chan bool, name string, value string) {
	exptime := 300
	conn, err := net.Dial("tcp", fmt.Sprintf("localhost%v", PORT))
	if err != nil {
		t.Error(err.Error())
	}

	// Blank get
	fmt.Fprintf(conn, "get %v\r\n", name)
	scanner := bufio.NewScanner(conn)
	scanner.Scan()
	expect(t, scanner.Text(), "ERR_NOT_FOUND")

	// Set then get
	fmt.Fprintf(conn, "set %v %v %v\r\n%v\r\n", name, exptime, len(value), value)
	scanner.Scan()
	resp := scanner.Text()
	arr := strings.Split(resp, " ")
	expect(t, arr[0], "OK")
	ver, err := strconv.Atoi(arr[1])
	if err != nil {
		t.Error("Non-numeric version found")
	}
	version := int64(ver)
	fmt.Fprintf(conn, "get %v\r\n", name)
	scanner.Scan()
	expect(t, scanner.Text(), fmt.Sprintf("VALUE %v", len(value)))
	scanner.Scan()
	expect(t, value, scanner.Text())
	fmt.Fprintf(conn, "getm %v\r\n", name)
	scanner.Scan()
	expect(t, scanner.Text(), fmt.Sprintf("VALUE %v %v %v", version, exptime, len(value)))
	scanner.Scan()
	expect(t, scanner.Text(), value)

	// Test noreply (just test that the next reply is correct since testing for a blocking scan
	// is harder)
	fmt.Fprintf(conn, "set %v %v %v noreply\r\n%v\r\n", name+"-norep", exptime, len(value), value)
	fmt.Fprintf(conn, "get %v\r\n", name)
	scanner.Scan()
	expect(t, scanner.Text(), fmt.Sprintf("VALUE %v", len(value)))
	done <- true
}

// Sets up to CASes to race, similar to concurrent() but via TCP
func concurrentTCP(t *testing.T, done chan bool, name string, value string) {
	exptime := 300
	conn, err := net.Dial("tcp", fmt.Sprintf("localhost%v", PORT))
	if err != nil {
		t.Error(err.Error())
	}

	fmt.Fprintf(conn, "set %v %v %v\r\n%v\r\n", name, exptime, len(value), value)
	scanner := bufio.NewScanner(conn)
	scanner.Scan()
	resp := scanner.Text()
	arr := strings.Split(resp, " ")
	expect(t, arr[0], "OK")
	ver, err := strconv.Atoi(arr[1])
	if err != nil {
		t.Error("Non-numeric version found")
	}
	version := int64(ver)
	go concurrentTCPInner(t, done, name, exptime, version, "setby1", "setby2", "First")
	go concurrentTCPInner(t, done, name, exptime, version, "setby2", "setby1", "Second")
}

func concurrentTCPInner(t *testing.T, done chan bool, name string, exptime int,
	version int64, newval string, otherval string, testname string) {
	conn, err := net.Dial("tcp", fmt.Sprintf("localhost%v", PORT))
	value := newval
	if err != nil {
		t.Error(err.Error())
	}
	fmt.Fprintf(conn, "cas %v %v %v %v\r\n%v\r\n", name, exptime, version, len(value), value)
	scanner := bufio.NewScanner(conn)
	scanner.Scan()
	resp := scanner.Text()
	arr := strings.Split(resp, " ")
	if arr[0] == "OK" {
		t.Logf("%v goroutine won", testname)
	} else {
		expect(t, resp, "ERR_VERSION")
		value = otherval
	}
	fmt.Fprintf(conn, "get %v\r\n", name)
	scanner.Scan()
	expect(t, scanner.Text(), fmt.Sprintf("VALUE %v", len(value)))
	scanner.Scan()
	expect(t, scanner.Text(), value)
	done <- true
}

// Test for expiry of keys
func TestExpiry(t *testing.T) {
	messages := make(chan Message, 100)
	go backend(messages)
	ack := make(chan string, 2)

	// Check that the store is empty
	message := Message{ack, Set{key: "banana", exptime: 2, noreply: false, value: "potato"}}
	messages <- message
	<-ack

	time.Sleep(time.Duration(4) * time.Second)

	message.data = Get{key: "banana"}
	messages <- message
	resp := <-ack
	expect(t, resp, "ERR_NOT_FOUND")
}

// Useful testing function
func expect(t *testing.T, a string, b string) {
	if a != b {
		t.Error(fmt.Sprintf("Expected %v, found %v", b, a))
	}
}
