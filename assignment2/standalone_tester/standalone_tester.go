package main

import (
    "net"
    "strconv"
    "fmt"
    "bufio"
    "strings"
)

const PORT = ":8000"
func main () {
    TestTCP()
}



// Test the TCP interface
func TestTCP() {

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

// Sets up to CASes to race, similar to concurrent() but via TCP
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