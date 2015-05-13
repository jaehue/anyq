package main

import (
	"github.com/apcera/nats"
	"github.com/jaehue/anyq"
	"io/ioutil"
	"log"
	"strconv"
	"testing"
	"time"
)

func BenchmarkNatsProduce(b *testing.B) {
	log.SetOutput(ioutil.Discard)

	q, err := anyq.New("nats", "nats://127.0.0.1:4222")
	if err != nil {
		b.Error(err)
	}
	produceBenchmark(b, q, anyq.NatsProducerArgs{Subject: "test"})
}

func BenchmarkNatsConsume(b *testing.B) {
	log.SetOutput(ioutil.Discard)

	q, err := anyq.New("nats", "nats://127.0.0.1:4222")
	if err != nil {
		b.Error(err)
	}

	pubsubBenchmark(b, q, anyq.NatsProducerArgs{Subject: "test"}, anyq.NatsConsumerArgs{Subject: "test"})
}

func BenchmarkNatsReply(b *testing.B) {
	log.SetOutput(ioutil.Discard)

	q, err := anyq.New("nats", "nats://127.0.0.1:4222")
	if err != nil {
		panic(err)
	}

	conn, err := q.Conn()
	if err != nil {
		b.Error(err)
	}
	natsConn, ok := conn.(*nats.Conn)
	if !ok {
		log.Fatalf("invalid conn type(%T)\n", conn)
	}

	// set consumer for reply
	natsConn.Subscribe("test", func(m *nats.Msg) {
		natsConn.Publish(m.Reply, m.Data)
		log.Println("[receive and reply]", string(m.Data))
	})

	// set producer for request
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		body := strconv.Itoa(i)

		m, err := natsConn.Request("test", []byte(body), 10*time.Second)
		if err != nil {
			log.Fatalln(err)
			return
		}
		log.Println("[replied]", string(m.Data))
	}
}
