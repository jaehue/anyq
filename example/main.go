package main

import (
	"fmt"
	"github.com/jaehue/qlib"
	"log"
)

func main() {
	runRabbitmq()
	//runKafka()
	//runNsq()
	//runNats()
	<-make(chan struct{})
}

func runRabbitmq() {
	var (
		ex         = "test-exchange"
		queueName  = "test"
		bindingKey = "test-key"
	)

	q, err := qlib.Setup("rabbitmq", "amqp://guest:guest@localhost:5672/", func(q *qlib.Rabbitmq) {

		if err := q.Qos(100, 0, false); err != nil {
			log.Fatal(err)
		}

		log.Println("declaring Exchange ", ex)
		if err := q.ExchangeDeclare(
			ex,       // name of the exchange
			"direct", // exchange type - direct|fanout|topic|x-custom
			false,    // durable
			false,    // delete when complete
			false,    // internal
			false,    // noWait
			nil,      // arguments
		); err != nil {
			log.Fatal(err)
		}
		log.Println("declared Exchange, declaring Queue ", queueName)
		queue, err := q.QueueDeclare(
			queueName, // name of the queue
			true,      // durable
			false,     // delete when usused
			false,     // exclusive
			false,     // noWait
			nil,       // arguments
		)
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("declared Queue (%q %d messages, %d consumers), binding to Exchange (key %q)\n",
			queue.Name, queue.Messages, queue.Consumers, bindingKey)

		if err := q.QueueBind(
			queue.Name, // name of the queue
			bindingKey, // bindingKey
			ex,         // sourceExchange
			false,      // noWait
			nil,        // arguments
		); err != nil {
			log.Fatal(err)
		}

		log.Println("Queue bound to Exchange")
	})
	if err != nil {
		panic(err)
	}

	fmt.Println("setup rabbitmq: ", q)
	runPubSub(q)
}

func runKafka() {
	q, err := qlib.Setup("kafka", "192.168.81.43:32771,192.168.81.43:32772,192.168.81.43:32773")
	if err != nil {
		panic(err)
	}
	fmt.Println("setup kafka: ", q)
	runPubSub(q)
}

func runNats() {
	q, err := qlib.Setup("nats", "nats://192.168.81.43:4222")
	if err != nil {
		panic(err)
	}
	fmt.Println("setup nats: ", q)
	runPubSub(q)
}

func runNsq() {
	q, err := qlib.Setup("nsq", "192.168.81.43:4150", func(q *qlib.Nsq) {
		q.ConsumeChannel = "qlib"
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("setup nsq: ", q)
	runPubSub(q)
}

func runPubSub(q qlib.Queuer) {
	// consume
	recvCh := make(chan []byte, 100)
	err := q.BindRecvChan("test", recvCh)
	if err != nil {
		panic(err)
	}
	go func() {
		for m := range recvCh {
			fmt.Println("[receive]", string(m))
		}
	}()

	// produce
	sendCh := make(chan []byte, 100)
	err = q.BindSendChan("test", sendCh)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 100; i++ {
		msg := fmt.Sprintf("[%d]%s", i, "test message")
		fmt.Println("[send]", msg)
		sendCh <- []byte(msg)
	}
}
