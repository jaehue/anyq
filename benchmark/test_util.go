package main

import (
	"fmt"
	"github.com/jaehue/qlib"
	"testing"
)

func produceBenchmark(b *testing.B, q qlib.Queuer, args interface{}) {
	sendCh := make(chan []byte, 100)
	err := q.BindSendChan(sendCh, args)
	if err != nil {
		b.Error(err)
	}

	msg := "test message"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		body := fmt.Sprintf("[%d]%s", i, msg)
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
