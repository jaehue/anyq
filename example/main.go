package main

import (
	"fmt"
	"github.com/jaehue/qlib"
)

func setupNsq(q interface{}) interface{} {
	v, ok := q.(qlib.Nsq)
	if ok {
		fmt.Println("return")
		return nil
	}

	nsq := v

	nsq.Topic = "test"
	nsq.Channel = "qlib_test"
	fmt.Println("nsq", nsq)
	return &nsq
}

func main() {
	q, err := qlib.Setup("nsq", "192.168.81.43:4150", setupNsq)
	if err != nil {
		panic(err)
	}
	fmt.Println("new queue ", q)
	recvCh := make(chan []byte, 100)
	err = q.BindRecvChan(recvCh)
	if err != nil {
		panic(err)
	}
	for m := range recvCh {
		fmt.Println(string(m))
	}
}
