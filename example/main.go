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

	// consume
	recvCh := make(chan []byte, 100)
	err = q.BindRecvChan(recvCh)
	if err != nil {
		panic(err)
	}
	go func() {
		for m := range recvCh {
			fmt.Println("[receive]", string(m))
		}
	}()

	// produce
	sendCh := make(chan []byte, 100)
	err = q.BindSendChan(sendCh)

	for i := 0; i < 100; i++ {
		sendCh <- []byte(fmt.Sprintf("[%d]%s", i, "test message"))
	}

	c := make(chan struct{})
	<-c
}
