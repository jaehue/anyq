package main

import (
	"github.com/jaehue/qlib"
	"io/ioutil"
	"log"
	"testing"
)

func BenchmarkNsqProduce(b *testing.B) {
	log.SetOutput(ioutil.Discard)

	q, err := qlib.Setup("nsq", "192.168.81.43:4150", func(q *qlib.Nsq) {
		q.ConsumeChannel = "qlib"
	})
	if err != nil {
		b.Error(err)
	}
	consumeBenchmark(q, b)
}

func BenchmarkNsqConsume(b *testing.B) {
	log.SetOutput(ioutil.Discard)

	q, err := qlib.Setup("nsq", "192.168.81.43:4150", func(q *qlib.Nsq) {
		q.ConsumeChannel = "qlib"
	})
	if err != nil {
		b.Error(err)
	}
	produceBenchmark(q, b)
}
