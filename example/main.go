package main

import (
	"fmt"
	"github.com/jaehue/qlib"
)

func main() {
	runNsq()
	c := make(chan struct{})
	<-c
}

func runNsq() {
	q, err := qlib.Setup("nsq", "192.168.81.43:4150", func(q *qlib.Nsq) {
		q.ConsumeChannel = "qlib"
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("setup nsq: ", q)

	// consume
	recvCh := make(chan []byte, 100)
	err = q.BindRecvChan("test", recvCh)
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
	err = q.BindSendChan("test", sendCh)

	for i := 0; i < 100; i++ {
		sendCh <- []byte(fmt.Sprintf("[%d]%s", i, "test message"))
	}

}
