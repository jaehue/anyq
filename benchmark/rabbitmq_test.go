package main

import (
	"fmt"
	"github.com/jaehue/anyq"
	"github.com/streadway/amqp"
	"io/ioutil"
	"log"
	"testing"
)

var (
	ex    = "test-exchange"
	qname = "test"
	key   = "test-key"
)

func setExchange(q *anyq.Rabbitmq) {
	log.Println("declaring Exchange: ", ex)
	if err := q.ExchangeDeclare(ex, "direct", false, false, false, false, nil); err != nil {
		log.Fatal(err)
	}
	log.Println("declared Exchange")
}

func BenchmarkRabbitmqProduce(b *testing.B) {
	log.SetOutput(ioutil.Discard)

	q, err := anyq.New("rabbitmq", "amqp://guest:guest@127.0.0.1:5672/", setExchange)
	if err != nil {
		panic(err)
	}

	produceBenchmark(b, q, anyq.RabbitmqProducerArgs{Exchange: ex, RoutingKey: key})
}

func BenchmarkRabbitmqConsume(b *testing.B) {
	log.SetOutput(ioutil.Discard)

	q, err := anyq.New("rabbitmq", "amqp://guest:guest@localhost:5672/", setExchange)
	if err != nil {
		b.Error(err)
	}

	// run producer
	p, err := q.NewProducer(anyq.RabbitmqProducerArgs{Exchange: ex, RoutingKey: key})
	if err != nil {
		b.Error(err)
	}
	sendCh := make(chan []byte)
	err = p.BindSendChan(sendCh)
	if err != nil {
		b.Error(err)
	}

	quit := make(chan struct{}, 1)
	go func() {
		i := 0
	produceloop:
		for {
			select {
			case <-quit:
				break produceloop
			default:
				body := fmt.Sprintf("[%d]%s", i, "testMsg")
				sendCh <- []byte(body)
				i++
			}
		}
		// b.Logf("produce count: %d", i)
	}()

	// run consumer
	c, err := q.NewConsumer(anyq.RabbitmqConsumerArgs{Queue: qname, RoutingKey: key, Exchange: ex})
	if err != nil {
		b.Error(err)
	}
	recvCh := make(chan *anyq.Message)
	err = c.BindRecvChan(recvCh)
	if err != nil {
		b.Error(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m := <-recvCh
		delivery := m.Origin.(amqp.Delivery)
		delivery.Ack(false)
	}
	quit <- struct{}{}
	q.Close()
}
