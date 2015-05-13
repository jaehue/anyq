package main

import (
	"fmt"
	"github.com/jaehue/anyq"
	"log"
)

func main() {
	runRabbitmq()
	//runKafka()
	//runNsq()
	//runNats()

	<-make(chan struct{})
}

func runKafka() anyq.Queuer {
	q, err := anyq.New("kafka", "192.168.81.43:32785", func(q *anyq.Kafka) {
		q.Zookeepers = []string{"192.168.81.43:32784"}
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("setup kafka: ", q)
	runPubSub(q, anyq.KafkaConsumerArgs{Topic: "test", Partitions: "all", Group: "group1"},
		anyq.KafkaProducerArgs{Topic: "test", Sync: true})
	return q
}

func runRabbitmq() anyq.Queuer {
	var (
		ex    = "test-exchange"
		qname = "test"
		key   = "test-key"
	)

	setQos := func(q *anyq.Rabbitmq) {
		if err := q.Qos(100, 0, false); err != nil {
			log.Fatal(err)
		}
	}

	setExchange := func(q *anyq.Rabbitmq) {
		log.Println("declaring Exchange: ", ex)
		if err := q.ExchangeDeclare(ex, "direct", false, false, false, false, nil); err != nil {
			log.Fatal(err)
		}
		log.Println("declared Exchange")
	}

	q, err := anyq.New("rabbitmq", "amqp://guest:guest@127.0.0.1:5672/", setQos, setExchange)
	if err != nil {
		panic(err)
	}
	fmt.Println("setup rabbitmq: ", q)

	runPubSub(q, anyq.RabbitmqConsumerArgs{Queue: qname, RoutingKey: key, Exchange: ex}, anyq.RabbitmqProducerArgs{Exchange: ex, RoutingKey: key})
	return q
}

func runNats() anyq.Queuer {
	q, err := anyq.New("nats", "nats://192.168.81.43:4222")
	if err != nil {
		panic(err)
	}
	fmt.Println("setup nats: ", q)
	runPubSub(q, anyq.NatsConsumerArgs{Subject: "test"}, anyq.NatsProducerArgs{Subject: "test"})
	return q
}

func runNsq() anyq.Queuer {
	q, err := anyq.New("nsq", "192.168.81.43:4150")
	if err != nil {
		panic(err)
	}
	fmt.Println("setup nsq: ", q)
	runPubSub(q, anyq.NsqConsumerArgs{Topic: "test", Channel: "anyq"}, anyq.NsqProducerArgs{Topic: "test"})

	return q
}

func runPubSub(q anyq.Queuer, consumeArgs, produceArgs interface{}) {
	c, err := q.NewConsumer(consumeArgs)
	if err != nil {
		panic(err)
	}

	p, err := q.NewProducer(produceArgs)
	if err != nil {
		panic(err)
	}

	// Consume
	recvCh := make(chan *anyq.Message, 100)
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
