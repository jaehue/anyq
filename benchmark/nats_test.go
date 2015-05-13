package main

import (
	"github.com/jaehue/anyq"
	"io/ioutil"
	"log"
	"testing"
)

func BenchmarkNatsProduce(b *testing.B) {
	log.SetOutput(ioutil.Discard)

	q, err := anyq.New("nats", "nats://192.168.81.43:4222")
	if err != nil {
		b.Error(err)
	}
	produceBenchmark(b, q, anyq.NatsProducerArgs{Subject: "test"})
}

func BenchmarkNatsConsume(b *testing.B) {
	log.SetOutput(ioutil.Discard)

	q, err := anyq.New("nats", "nats://192.168.81.43:4222")
	if err != nil {
		b.Error(err)
	}

	pubsubBenchmark(b, q, anyq.NatsProducerArgs{Subject: "test"}, anyq.NatsConsumerArgs{Subject: "test"})
}
