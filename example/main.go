package main

import (
	"fmt"
	"github.com/jaehue/qlib"
	"log"
)

func main() {
	//runRabbitmq()
	runKafka()
	//runNsq()
	//runNats()

	<-make(chan struct{})
}

func runKafka() qlib.Queuer {
	q, err := qlib.Setup("kafka", "192.168.81.43:32785", func(q *qlib.Kafka) {
		q.Zookeepers = []string{"192.168.81.43:32784"}
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("setup kafka: ", q)
	runPubSub(q, qlib.KafkaConsumerArgs{Topic: "test", Partitions: "all", Group: "group1"},
		qlib.KafkaProducerArgs{Topic: "test", Sync: true})
	return q
}

func runRabbitmq() qlib.Queuer {
	var (
		ex    = "test-exchange"
		qname = "test"
		key   = "test-key"
	)

	setQos := func(q *qlib.Rabbitmq) {
		if err := q.Qos(100, 0, false); err != nil {
			log.Fatal(err)
		}
	}

	setExchange := func(q *qlib.Rabbitmq) {
		log.Println("declaring Exchange: ", ex)
		if err := q.ExchangeDeclare(ex, "direct", false, false, false, false, nil); err != nil {
			log.Fatal(err)
		}
		log.Println("declared Exchange")
	}

	q, err := qlib.Setup("rabbitmq", "amqp://guest:guest@127.0.0.1:5672/", setQos, setExchange)
	if err != nil {
		panic(err)
	}
	fmt.Println("setup rabbitmq: ", q)

	runPubSub(q, qlib.RabbitmqConsumerArgs{Queue: qname, RoutingKey: key, Exchange: ex}, qlib.RabbitmqProducerArgs{Exchange: ex, RoutingKey: key})
	return q
}

func runNats() qlib.Queuer {
	q, err := qlib.Setup("nats", "nats://192.168.81.43:4222")
	if err != nil {
		panic(err)
	}
	fmt.Println("setup nats: ", q)
	runPubSub(q, qlib.NatsConsumerArgs{Subject: "test"}, qlib.NatsProducerArgs{Subject: "test"})
	return q
}

func runNsq() qlib.Queuer {
	q, err := qlib.Setup("nsq", "192.168.81.43:4150")
	if err != nil {
		panic(err)
	}
	fmt.Println("setup nsq: ", q)
	runPubSub(q, qlib.NsqConsumerArgs{Topic: "test", Channel: "qlib"}, qlib.NsqProducerArgs{Topic: "test"})

	return q
}

func runPubSub(q qlib.Queuer, consumeArgs, produceArgs interface{}) {
	c, err := q.NewConsumer(consumeArgs)
	if err != nil {
		panic(err)
	}

	p, err := q.NewProducer(produceArgs)
	if err != nil {
		panic(err)
	}

	// Consume
	recvCh := make(chan *qlib.Message, 100)
	err = c.BindRecvChan(recvCh)
	if err != nil {
		panic(err)
	}
	go func() {
		for m := range recvCh {
			fmt.Println("[receive]", string(m.Body))
		}
	}()

	// produce
	sendCh := make(chan []byte, 100)
	err = p.BindSendChan(sendCh)
	if err != nil {
		panic(err)
	}

	for i := 0; i < 100; i++ {
		msg := fmt.Sprintf("[%d]%s", i, "test message")
		fmt.Println("[send]", msg)
		sendCh <- []byte(msg)
	}
}
