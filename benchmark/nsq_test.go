package main

import (
	"fmt"
	"github.com/jaehue/qlib"
	"io/ioutil"
	"log"
	"testing"
)

func BenchmarkNsqProduce(b *testing.B) {
	b.StopTimer()

	log.SetOutput(ioutil.Discard)

	q, err := qlib.Setup("nsq", "192.168.81.43:4150", func(q *qlib.Nsq) {
		q.ConsumeChannel = "qlib"
	})
	if err != nil {
		b.Error(err)
	}

	sendCh := make(chan []byte, 100)
	err = q.BindSendChan("test", sendCh)
	if err != nil {
		b.Error(err)
	}

	msg := "test message"

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		body := fmt.Sprintf("[%d]%s", i, msg)
		sendCh <- []byte(body)
	}
}

func BenchmarkNsqConsume(b *testing.B) {
	b.StopTimer()

	log.SetOutput(ioutil.Discard)

	q, err := qlib.Setup("nsq", "192.168.81.43:4150", func(q *qlib.Nsq) {
		q.ConsumeChannel = "qlib"
	})
	if err != nil {
		b.Error(err)
	}

	recvCh := make(chan []byte, 100)
	err = q.BindRecvChan("test", recvCh)
	if err != nil {
		b.Error(err)
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		<-recvCh
	}

}
