package main

import (
	"flag"
	"fmt"
	"github.com/jaehue/anyq"
)

var (
	url     = flag.String("url", "127.0.0.1:4150", "REQUIRED: the nsq url")
	topic   = flag.String("topic", "test", "REQUIRED: the topic to consume")
	channel = flag.String("channel", "test", "REQUIRED: the channel to consume")
)

func init() {

}

func main() {
	flag.Parse()

	q, err := anyq.New("nsq", *url)
	if err != nil {
		panic(err)
	}

	c, err := q.NewConsumer(anyq.NsqConsumerArgs{Topic: *topic, Channel: *channel})
	if err != nil {
		panic(err)
	}

	recvCh := make(chan *anyq.Message, 256)
	c.BindRecvChan(recvCh)
	for m := range recvCh {
		fmt.Println("[receive]", string(m.Body))
	}

}
