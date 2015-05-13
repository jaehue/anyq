package main

import (
	"flag"
	"fmt"
	"github.com/jaehue/anyq"
	"strings"
)

var (
	brokerList = flag.String("brokers", "localhost:9092", "The comma separated list of brokers in the Kafka cluster")
	zookeeper  = flag.String("zookeeper", "localhost:2181", "A comma-separated Zookeeper connection string")
	group      = flag.String("group", "", "The name of the consumer group, used for coordination and load balancing")
	topic      = flag.String("topic", "test", "REQUIRED: the topic to consume")
	partitions = flag.String("partitions", "all", "The partitions to consume, can be 'all' or comma-separated numbers")
	offset     = flag.String("offset", "newest", "The offset to start with. Can be `oldest`, `newest`")
)

func init() {

}

func main() {
	flag.Parse()

	q, err := anyq.New("kafka", *brokerList, func(q *anyq.Kafka) {
		q.Zookeepers = strings.Split(*zookeeper, ",")
	})
	if err != nil {
		panic(err)
	}

	c, err := q.NewConsumer(anyq.KafkaConsumerArgs{Topic: *topic, Partitions: *partitions, Offset: *offset, Group: *group})
	if err != nil {
		panic(err)
	}

	recvCh := make(chan *anyq.Message, 256)
	c.BindRecvChan(recvCh)
	for m := range recvCh {
		fmt.Println("[receive]", string(m.Body))
	}

}
