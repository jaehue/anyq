package main

import (
	"fmt"
	"github.com/jaehue/qlib"
	"testing"
)

func consumeBenchmark(q qlib.Queuer, b *testing.B) {
	sendCh := make(chan []byte, 100)
	err := q.BindSendChan("test", sendCh)
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

func produceBenchmark(q qlib.Queuer, b *testing.B) {
	recvCh := make(chan []byte, 100)
	err := q.BindRecvChan("test", recvCh)
	if err != nil {
		b.Error(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		<-recvCh
	}
}
