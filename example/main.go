package main

import (
	"fmt"
	"github.com/jaehue/qlib"
)

func setupNsq(q *qlib.Nsq) {
	q.Topic = "test"
	q.Channel = "qlib_test"
}

func main() {
	q, err := qlib.Setup("nsq", "192.168.81.43:4150", setupNsq)
	if err != nil {
		panic(err)
	}
	fmt.Println("setup nsq: ", q)

	recvCh := make(chan []byte, 100)
	err = q.BindRecvChan(recvCh)
	if err != nil {
		panic(err)
	}
	for m := range recvCh {
		fmt.Println(string(m))
	}
}
