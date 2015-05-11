package main

import (
	"flag"
	"fmt"
	"github.com/jaehue/qlib"
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
	q, err := qlib.Setup("rabbitmq", *uri, setExchange, setQueue)
	if err != nil {
		panic(err)
	}

	recvCh := make(chan []byte, 100)
	q.BindRecvChan(recvCh, qlib.RabbitmqConsumeArgs{Queue: *queueName, Consumer: *consumerTag})
	for m := range recvCh {
		fmt.Println("[receive]", string(m))
	}
}

func setExchange(q *qlib.Rabbitmq) {
	log.Println("declaring Exchange: ", *exchange)
	if err := q.ExchangeDeclare(*exchange, *exchangeType, false, false, false, false, nil); err != nil {
		log.Fatal(err)
	}
	log.Println("declared Exchange")
}

func setQueue(q *qlib.Rabbitmq) {
	log.Println("declaring Queue: ", *queueName)
	queue, err := q.QueueDeclare(*queueName, true, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("declared Queue (%q %d messages, %d consumers)\n", queue.Name, queue.Messages, queue.Consumers)

	log.Printf("binding to Exchange (key %q)\n", *bindingKey)
	if err := q.QueueBind(queue.Name, *bindingKey, *exchange, false, nil); err != nil {
		log.Fatal(err)
	}
	log.Println("Queue bound to Exchange")
}
