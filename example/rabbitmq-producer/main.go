package main

import (
	"flag"
	"fmt"
	"github.com/jaehue/qlib"
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
	q, err := qlib.Setup("rabbitmq", *uri, func(q *qlib.Rabbitmq) {
		log.Println("declaring Exchange: ", *exchangeName)
		if err := q.ExchangeDeclare(*exchangeName, *exchangeType, false, false, false, false, nil); err != nil {
			log.Fatal(err)
		}
		log.Println("declared Exchange")
	})
	if err != nil {
		panic(err)
	}

	sendCh := make(chan []byte)
	q.BindSendChan(sendCh, qlib.RabbitmqProduceArgs{Exchange: *exchangeName, RoutingKey: *routingKey})
	sendCh <- []byte(*body)

	fmt.Println("[send]", *body)

	q.Cleanup()
}
