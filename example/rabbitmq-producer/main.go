package main

import (
	"flag"
	"fmt"
	"github.com/jaehue/anyq"
	"log"
)

var (
	uri          = flag.String("uri", "amqp://guest:guest@localhost:5672/", "AMQP URI")
	exchangeName = flag.String("exchange", "test-exchange", "Durable AMQP exchange name")
	exchangeType = flag.String("exchange-type", "direct", "Exchange type - direct|fanout|topic|x-custom")
	routingKey   = flag.String("key", "test-key", "AMQP routing key")
	body         = flag.String("body", "foobar", "Body of message")
	reliable     = flag.Bool("reliable", true, "Wait for the publisher confirmation before exiting")
)

func init() {
	flag.Parse()
}

func main() {
	q, err := anyq.New("rabbitmq", *uri, func(q *anyq.Rabbitmq) {
		log.Println("declaring Exchange: ", *exchangeName)
		if err := q.ExchangeDeclare(*exchangeName, *exchangeType, false, false, false, false, nil); err != nil {
			log.Fatal(err)
		}
		log.Println("declared Exchange")
	})
	if err != nil {
		panic(err)
	}

	p, err := q.NewProducer(anyq.RabbitmqProducerArgs{Exchange: *exchangeName, RoutingKey: *routingKey})
	if err != nil {
		panic(err)
	}

	sendCh := make(chan []byte)
	p.BindSendChan(sendCh)
	sendCh <- []byte(*body)

	fmt.Println("[send]", *body)

	q.Close()
}
