package raft

import (
	"fmt"
	"math/rand"
	"strconv"
)

// This file contains most of the code for the
// backing keyvalue store

type IndexedAck struct {
	lsn Lsn
	ack chan []byte
}

// A value in the key-value store
type Value struct {
	val     []byte
	exptime int64
	version int64
}

// A generic message to be passed to the backend
type Message struct {
	// The actual command (Set, Get, Getm, Cas, Delete)
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

// Handles interactions with the kv store
func (t *Raft) Backend(cs chan IndexedAck) {
	// The actual kv store
	store := make(map[string]Value)
	// Those waiting for a response, with a handle
	// back to the original listener
	waiters := make(map[Lsn]chan []byte)
	for {
		select {
		// If we get another waiter, just hold on to it
		case message := <-cs:
			waiters[message.lsn] = message.ack
		// If we get a commit, take out the waiter ...
		case message := <-t.CommitCh:
			ack := waiters[message.Lsn]
			delete(waiters, message.Lsn)
			// ... destructure the data ...
			switch message.Data.Data.(type) {
			case Set:
				{
					data := message.Data.Data.(Set)
					version := rand.Int63()
					exptime := data.Exptime
					value := data.Value
					store[data.Key] = Value{value, exptime, version}
					// ... and send back the response
					ack <- ([]byte)("OK " + strconv.FormatInt(version, 10))
				}
			case Get:
				{
					data := message.Data.Data.(Get)
					value, ok := store[data.Key]
					if ok {
						val := value.val
						ack <- append(([]byte)(fmt.Sprintf("VALUE %v\r\n", len(val))), val...)
					} else {
						ack <- ([]byte)("ERR_NOT_FOUND")
					}
				}
			case Getm:
				{
					data := message.Data.Data.(Getm)
					value, ok := store[data.Key]
					if ok {
						val := value.val
						ack <- append(([]byte)(fmt.Sprintf("VALUE %v %v %v\r\n", value.version,
							value.exptime, len(val))), val...)
					} else {
						ack <- ([]byte)("ERR_NOT_FOUND")
					}
				}
			case Delete:
				{
					data := message.Data.Data.(Delete)
					_, ok := store[data.Key]
					if ok {
						delete(store, data.Key)
						ack <- ([]byte)("DELETED")
					} else {
						ack <- ([]byte)("ERR_NOT_FOUND")
					}
				}
			case Cas:
				{
					data := message.Data.Data.(Cas)
					val, ok := store[data.Key]
					if ok {
						if val.version == data.Version {
							version := rand.Int63()
							exptime := data.Exptime
							value := data.Value
							store[data.Key] = Value{value, exptime, version}
							ack <- ([]byte)("OK " + strconv.FormatInt(version, 10))
						} else {
							ack <- ([]byte)("ERR_VERSION")
						}
					} else {
						ack <- ([]byte)("ERR_NOT_FOUND")
					}
				}
			default:
				// this should never happen
				ack <- ([]byte)("ERR_INTERNAL")
				continue
			}
		}

	}
}
