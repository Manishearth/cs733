package main

import (
	"fmt"
	"strconv"
	"strings"
	"testing"
)

// Tests for functioning of backend
func TestBackend(t *testing.T) {
	messages := make(chan Message)
	go backend(messages)
	ack := make(chan string)

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

func TestBackendConcurrent(t *testing.T) {

	messages := make(chan Message)
	done := make(chan bool)
	go backend(messages)
	// Add some background noise
	for i := 0; i < 5; i++ {
		go interference(t, messages, done, "i"+strconv.Itoa(i),
			"tomato"+strconv.Itoa(i), int64(100+i))
	}

	for i := 0; i < 5; i++ {
		<-done
	}
}

// An independent test to be spawned (to trigger interference)
// Just sets, swaps, and deletes a single key
func interference(t *testing.T, messages chan Message, done chan bool, key string, value string, exptime int64) {
	for i := 0; i < 10; i++ {
		ack := make(chan string)
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

func expect(t *testing.T, a string, b string) {
	if a != b {
		t.Error(fmt.Sprintf("Expected %v, found %v", b, a))
	}
}
