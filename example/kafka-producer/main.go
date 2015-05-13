package main

import (
	"flag"
	"fmt"
	"github.com/jaehue/anyq"
)

var (
	brokerList  = flag.String("brokers", "localhost:9092", "The comma separated list of brokers in the Kafka cluster. You can also set the KAFKA_PEERS environment variable")
	topic       = flag.String("topic", "test", "REQUIRED: the topic to produce to")
	key         = flag.String("key", "", "The key of the message to produce. Can be empty.")
	value       = flag.String("value", "", "REQUIRED: the value of the message to produce. You can also provide the value on stdin.")
	partitioner = flag.String("partitioner", "", "The partitioning scheme to use. Can be `hash`, `manual`, or `random`")
	partition   = flag.Int("partition", -1, "The partition to produce to.")
)

func init() {

}

func main() {
	flag.Parse()

	q, err := anyq.New("kafka", *brokerList)
	if err != nil {
		panic(err)
	}

	p, err := q.NewProducer(anyq.KafkaProducerArgs{Topic: *topic, Key: *key, Partitioner: *partitioner, Partition: int32(*partition)})
	if err != nil {
		panic(err)
	}

	sendCh := make(chan []byte, 1)
	p.BindSendChan(sendCh)

	var bytes []byte
	for {
		fmt.Scanln(&bytes)
		sendCh <- bytes
	}
}
