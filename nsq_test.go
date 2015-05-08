package qlib

import (
	"fmt"
	"io/ioutil"
	"log"
	"testing"
)

func BenchmarkProduce(b *testing.B) {
	b.StopTimer()

	log.SetOutput(ioutil.Discard)

	q, err := Setup("nsq", "192.168.81.43:4150", func(q *Nsq) {
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

func BenchmarkConsume(b *testing.B) {
	b.StopTimer()

	log.SetOutput(ioutil.Discard)

	q, err := Setup("nsq", "192.168.81.43:4150", func(q *Nsq) {
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
