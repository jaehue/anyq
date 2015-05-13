package main

import (
	"flag"
	"fmt"
	"github.com/jaehue/anyq"
	"log"
)

var (
	uri          = flag.String("uri", "amqp://guest:guest@localhost:5672/", "AMQP URI")
	exchange     = flag.String("exchange", "test-exchange", "Durable, non-auto-deleted AMQP exchange name")
	exchangeType = flag.String("exchange-type", "direct", "Exchange type - direct|fanout|topic|x-custom")
	queueName    = flag.String("queue", "test-queue", "Ephemeral AMQP queue name")
	bindingKey   = flag.String("key", "test-key", "AMQP binding key")
	consumerTag  = flag.String("consumer-tag", "simple-consumer", "AMQP consumer tag (should not be blank)")
)

func init() {
	flag.Parse()
}

func main() {
	q, err := anyq.New("rabbitmq", *uri, setExchange)
	if err != nil {
		panic(err)
	}

	c, err := q.NewConsumer(anyq.RabbitmqConsumerArgs{Queue: *queueName, RoutingKey: *bindingKey, Exchange: *exchange, ConsumerTag: *consumerTag})
	if err != nil {
		panic(err)
	}

	recvCh := make(chan *anyq.Message)
	c.BindRecvChan(recvCh)
	for m := range recvCh {
		fmt.Println("[receive]", string(m.Body))
	}
}

func setExchange(q *anyq.Rabbitmq) {
	log.Println("declaring Exchange: ", *exchange)
	if err := q.ExchangeDeclare(*exchange, *exchangeType, false, false, false, false, nil); err != nil {
		log.Fatal(err)
	}
	log.Println("declared Exchange")
}
