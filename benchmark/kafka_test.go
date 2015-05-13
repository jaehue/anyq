package main

import (
	"github.com/jaehue/anyq"
	"io/ioutil"
	"log"
	"testing"
)

func BenchmarkKafkaAsyncProduce(b *testing.B) {
	log.SetOutput(ioutil.Discard)

	q, err := anyq.New("kafka", "localhost:9092")
	if err != nil {
		b.Error(err)
	}

	produceBenchmark(b, q, anyq.KafkaProducerArgs{Topic: "test"})
}

func BenchmarkKafkaSyncProduce(b *testing.B) {
	log.SetOutput(ioutil.Discard)

	q, err := anyq.New("kafka", "localhost:9092")
	if err != nil {
		b.Error(err)
	}

	produceBenchmark(b, q, anyq.KafkaProducerArgs{Topic: "test", Sync: true})
}

func BenchmarkKafkaConsume(b *testing.B) {
	log.SetOutput(ioutil.Discard)

	q, err := anyq.New("kafka", "localhost:9092")
	if err != nil {
		b.Error(err)
	}

	pubsubBenchmark(b, q, anyq.KafkaProducerArgs{Topic: "test"}, anyq.KafkaConsumerArgs{Topic: "test", Partitions: "all"})
}
