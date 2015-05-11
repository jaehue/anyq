package main

import (
	"fmt"
	"github.com/jaehue/qlib"
	"testing"
)

var testMsg = `Go is an open source programming language that makes it easy to build simple, reliable, and efficient software.`

func produceBenchmark(b *testing.B, q qlib.Queuer, args interface{}) {
	sendCh := make(chan []byte, 100)
	err := q.BindSendChan(sendCh, args)
	if err != nil {
		b.Error(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		body := fmt.Sprintf("[%d]%s", i, testMsg)
		sendCh <- []byte(body)
	}
}

func consumeBenchmark(b *testing.B, q qlib.Queuer, args interface{}) {
	recvCh := make(chan []byte, 100)
	err := q.BindRecvChan(recvCh, args)
	if err != nil {
		b.Error(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		<-recvCh
	}
}

func pubsubBenchmark(b *testing.B, q qlib.Queuer, produceArgs interface{}, consumeArgs interface{}) {
	recvCh := make(chan []byte, 100)
	err := q.BindRecvChan(recvCh, consumeArgs)
	if err != nil {
		b.Error(err)
	}

	cnt := 0
	go func() {
		for _ = range recvCh {
			cnt++
		}
	}()

	sendCh := make(chan []byte, 100)
	err = q.BindSendChan(sendCh, produceArgs)
	if err != nil {
		b.Error(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		body := fmt.Sprintf("[%d]%s", i, testMsg)
		sendCh <- []byte(body)
	}
	fmt.Println("receive count: ", cnt)
}
