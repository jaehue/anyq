package main

import (
	"github.com/jaehue/qlib"
	"io/ioutil"
	"log"
	"testing"
)

func BenchmarkKafkaProduce(b *testing.B) {
	log.SetOutput(ioutil.Discard)

	q, err := qlib.Setup("kafka", "192.168.81.43:32771,192.168.81.43:32772,192.168.81.43:32773")
	if err != nil {
		b.Error(err)
	}

	consumeBenchmark(q, b)
}

func BenchmarkKafkaConsume(b *testing.B) {
	log.SetOutput(ioutil.Discard)

	q, err := qlib.Setup("kafka", "192.168.81.43:32771,192.168.81.43:32772,192.168.81.43:32773")
	if err != nil {
		b.Error(err)
	}

	produceBenchmark(q, b)
}
