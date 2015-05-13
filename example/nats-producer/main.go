package main

import (
	"flag"
	"fmt"
	"github.com/jaehue/anyq"
)

var (
	url     = flag.String("url", "nats://127.0.0.1:4222", "REQUIRED: the nats url")
	subject = flag.String("subject", "test", "REQUIRED: the subject to consume")
)

func init() {

}

func main() {
	flag.Parse()

	q, err := anyq.New("nats", *url)
	if err != nil {
		panic(err)
	}

	p, err := q.NewProducer(anyq.NatsProducerArgs{Subject: *subject})
	if err != nil {
		panic(err)
	}

	sendCh := make(chan []byte, 1)
	p.BindSendChan(sendCh)

	var bytes []byte
	for {
		fmt.Scanln(&bytes)
		sendCh <- bytes
	}
}
