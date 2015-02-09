package raft

import (
"math/rand"
"strconv"
"fmt"
)

type IndexedAck struct {
    lsn Lsn
    ack chan string
}

func (t *Raft) Backend(cs chan IndexedAck) {
    store := make(map[string]Value)
    waiters := make(map[Lsn] chan string)
    for {
        select {
        case message := <- cs:
            waiters[message.lsn] = message.ack
        case message := <- t.CommitCh:
            ack := waiters[message.Lsn]
            delete(waiters, message.Lsn)
            switch message.Data.Data.(type) {
                case Set:
                    {
                        data := message.Data.Data.(Set)
                        version := rand.Int63()
                        exptime := data.Exptime
                        value := data.Value
                        store[data.Key] = Value{value, exptime, version}
                        ack <- "OK " + strconv.FormatInt(version, 10)
                    }
                case Get:
                    {
                        data := message.Data.Data.(Get)
                        value, ok := store[data.Key]
                        if ok {
                            val := value.val
                            ack <- fmt.Sprintf("VALUE %v\r\n%v", len(val), string(val))
                        } else {
                            ack <- "ERR_NOT_FOUND"
                        }
                    }
                case Getm:
                    {
                        data := message.Data.Data.(Getm)
                        value, ok := store[data.Key]
                        if ok {
                            val := value.val
                            ack <- fmt.Sprintf("VALUE %v %v %v\r\n%v", value.version,
                                value.exptime, len(val), string(val))
                        } else {
                            ack <- "ERR_NOT_FOUND"
                        }
                    }
                case Delete:
                    {
                        data := message.Data.Data.(Delete)
                        _, ok := store[data.Key]
                        if ok {
                            delete(store, data.Key)
                            ack <- "DELETED"
                        } else {
                            ack <- "ERR_NOT_FOUND"
                        }
                    }
                case Cas:
                    {
                        data := message.Data.Data.(Cas)
                        val, ok := store[data.Key]
                        if ok {
                            if val.version == data.Version {
                                version := rand.Int63()
                                exptime :=data.Exptime
                                value := data.Value
                                store[data.Key] = Value{value, exptime, version}
                                ack <- "OK " + strconv.FormatInt(version, 10)
                            } else {
                                ack <- "ERR_VERSION"
                            }
                        } else {
                            ack <- "ERR_NOT_FOUND"
                        }
                    }
                default:
                    // this should never happen
                    continue
                }                
            }


    }
}