package main

import (
	"flag"
	"fmt"
	"github.com/jaehue/anyq"
)

var (
	url   = flag.String("url", "127.0.0.1:4150", "REQUIRED: the nsq url")
	topic = flag.String("topic", "test", "REQUIRED: the topic to consume")
)

func init() {

}

func main() {
	flag.Parse()

	q, err := anyq.New("nsq", *url)
	if err != nil {
		panic(err)
	}

	p, err := q.NewProducer(anyq.NsqProducerArgs{Topic: *topic})
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
