package main

import (
	"github.com/jaehue/qlib"
	"io/ioutil"
	"log"
	"testing"
)

func BenchmarkKafkaPubsub(b *testing.B) {
	log.SetOutput(ioutil.Discard)

	q, err := qlib.Setup("kafka", "192.168.81.43:32785")
	if err != nil {
		b.Error(err)
	}

	pubsubBenchmark(b, q, qlib.KafkaProduceArgs{Topic: "test"}, qlib.KafkaConsumeArgs{Topic: "test", Partitions: "all"})
}

func BenchmarkKafkaAsyncProduce(b *testing.B) {
	log.SetOutput(ioutil.Discard)

	q, err := qlib.Setup("kafka", "192.168.81.43:32785")
	if err != nil {
		b.Error(err)
	}

	produceBenchmark(b, q, qlib.KafkaProduceArgs{Topic: "test"})
}

func BenchmarkKafkaSyncProduce(b *testing.B) {
	log.SetOutput(ioutil.Discard)

	q, err := qlib.Setup("kafka", "192.168.81.43:32785")
	if err != nil {
		b.Error(err)
	}

	produceBenchmark(b, q, qlib.KafkaProduceArgs{Topic: "test", Sync: true})
}

func BenchmarkKafkaConsume(b *testing.B) {
	log.SetOutput(ioutil.Discard)

	q, err := qlib.Setup("kafka", "192.168.81.43:32785")
	if err != nil {
		b.Error(err)
	}

	consumeBenchmark(b, q, qlib.KafkaConsumeArgs{Topic: "test", Partitions: "all", Offset: "oldest"})
}
