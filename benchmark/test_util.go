package main

import (
	"fmt"
	"github.com/jaehue/anyq"
	"testing"
)

var testMsg = `Go is an open source programming language that makes it easy to build simple, reliable, and efficient software.`

func produceBenchmark(b *testing.B, q anyq.Queuer, args interface{}) {
	p, err := q.NewProducer(args)
	if err != nil {
		b.Error(err)
	}

	sendCh := make(chan []byte)
	err = p.BindSendChan(sendCh)
	if err != nil {
		b.Error(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		body := fmt.Sprintf("[%d]%s", i, testMsg)
		sendCh <- []byte(body)
	}
}

func pubsubBenchmark(b *testing.B, q anyq.Queuer, producerArgs interface{}, consumerArgs interface{}) {
	// run producer
	p, err := q.NewProducer(producerArgs)
	if err != nil {
		b.Error(err)
	}
	sendCh := make(chan []byte)
	err = p.BindSendChan(sendCh)
	if err != nil {
		b.Error(err)
	}

	quit := make(chan struct{})
	go func() {
		i := 0
	produceloop:
		for {
			select {
			case <-quit:
				break produceloop
			default:
				body := fmt.Sprintf("[%d]%s", i, testMsg)
				sendCh <- []byte(body)
				i++
			}
		}
		// b.Logf("produce count: %d", i)
	}()

	// run consumer
	c, err := q.NewConsumer(consumerArgs)
	if err != nil {
		b.Error(err)
	}
	recvCh := make(chan *anyq.Message)
	err = c.BindRecvChan(recvCh)
	if err != nil {
		b.Error(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		<-recvCh
	}
	quit <- struct{}{}
}
