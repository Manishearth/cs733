package main

import ("net")

const PORT = ":9000"

func main () {
    l, _ := net.Listen("tcp", PORT);
    for {
        tx, _ := l.Accept();

        tx.Write([]byte("banana"));
        tx.Close();
    }
}