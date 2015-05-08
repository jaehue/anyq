package main

import (
	"fmt"
	"github.com/jaehue/qlib"
)

func main() {
	runKafka()
	//runNsq()
	//runNats()
	c := make(chan struct{})
	<-c
}

func runKafka() {
	q, err := qlib.Setup("kafka", "192.168.81.43:32771,192.168.81.43:32772,192.168.81.43:32773")
	if err != nil {
		panic(err)
	}
	fmt.Println("setup kafka: ", q)
	runPubSub(q)
}

func runNats() {
	q, err := qlib.Setup("nats", "nats://192.168.81.43:4222")
	if err != nil {
		panic(err)
	}
	fmt.Println("setup nats: ", q)
	runPubSub(q)
}

func runNsq() {
	q, err := qlib.Setup("nsq", "192.168.81.43:4150", func(q *qlib.Nsq) {
		q.ConsumeChannel = "qlib"
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("setup nsq: ", q)
	runPubSub(q)
}

func runPubSub(q qlib.Queuer) {
	// consume
	recvCh := make(chan []byte, 100)
	err := q.BindRecvChan("test", recvCh)
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
