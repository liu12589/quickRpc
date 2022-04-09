package main

import (
	"../../quickRpc"
	"../effiCode"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"
)

func startServer(addr chan string) {
	l, err := net.Listen("tcp", ":0")
	if err != nil{
		log.Fatal("network error:", err)
	}
	log.Println("start rpc server on", l.Addr())
	addr <- l.Addr().String()
	quickRpc.Accept(l)
}

func main() {
	addr := make(chan string)
	go startServer(addr)

	conn,_ := net.Dial("tcp", <-addr)
	defer func() { _ = conn.Close() }()

	time.Sleep(time.Second)
	_ = json.NewEncoder(conn).Encode(quickRpc.DefaultOption)
	ec := effiCode.NewGobCodec(conn)
	for i := 0; i < 5; i++ {
		h := &effiCode.Header{
			ServiceMethod: "Foo.Sum",
			Seq:           uint64(i),
		}
		_ = ec.Write(h, fmt.Sprintf("quickRpc req %d", h.Seq))
		_ = ec.ReadHeader(h)
		var reply string
		_ = ec.ReadBody(&reply)
		log.Println("reply:", reply)
	}
}
