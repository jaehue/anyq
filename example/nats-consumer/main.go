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

	c, err := q.NewConsumer(anyq.NatsConsumerArgs{Subject: *subject})
	if err != nil {
		panic(err)
	}

	recvCh := make(chan *anyq.Message, 256)
	c.BindRecvChan(recvCh)
	for m := range recvCh {
		fmt.Println("[receive]", string(m.Body))
	}

}
