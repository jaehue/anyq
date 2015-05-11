package main

import (
	"github.com/jaehue/qlib"
	"io/ioutil"
	"log"
	"testing"
)

func BenchmarkNsqProduce(b *testing.B) {
	log.SetOutput(ioutil.Discard)

	q, err := qlib.Setup("nsq", "192.168.81.43:4150")
	if err != nil {
		b.Error(err)
	}
	produceBenchmark(b, q, qlib.NsqProduceArgs{Topic: "test"})
}

func BenchmarkNsqConsume(b *testing.B) {
	log.SetOutput(ioutil.Discard)

	q, err := qlib.Setup("nsq", "192.168.81.43:4150")
	if err != nil {
		b.Error(err)
	}
	consumeBenchmark(b, q, qlib.NsqConsumeArgs{Topic: "test", Channel: "qlib"})
}
