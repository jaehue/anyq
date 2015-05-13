package main

import (
	"github.com/jaehue/anyq"
	"io/ioutil"
	"log"
	"testing"
)

func BenchmarkNsqProduce(b *testing.B) {
	log.SetOutput(ioutil.Discard)

	q, err := anyq.Setup("nsq", "192.168.81.43:4150")
	if err != nil {
		b.Error(err)
	}
	produceBenchmark(b, q, anyq.NsqProducerArgs{Topic: "test"})
}

func BenchmarkNsqConsume(b *testing.B) {
	log.SetOutput(ioutil.Discard)

	q, err := anyq.Setup("nsq", "192.168.81.43:4150")
	if err != nil {
		b.Error(err)
	}
	pubsubBenchmark(b, q, anyq.NsqProducerArgs{Topic: "test"}, anyq.NsqConsumerArgs{Topic: "test", Channel: "anyq"})
}
