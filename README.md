# AnyQ
[![GoDoc](https://godoc.org/github.com/jaehue/anyq?status.svg)](https://godoc.org/github.com/jaehue/anyq)


This is queue library wrapper for widely popular queues.
AnyQ provide one way to handle various queues.

Supporting Queues
- RabbitMQ  
  https://www.rabbitmq.com
- Kafka  
  https://kafka.apache.org
- NSQ  
  http://nsq.io
- NATS  
  http://nats.io

## Basic usage

Go get:

```
$ go get -u github.com/jaehue/anyq
```

Import the package:

```
import (
	"github.com/jaehue/anyq"
)
```

Create new queue:

```
q, _ := anyq.New("nsq", "127.0.0.1:4150")
```

Create consumer:

```
c, _ := q.NewConsumer(anyq.NsqConsumerArgs{Topic: "test", Channel: "anyq"})
```

Create producer:

```
p, _ := q.NewProducer(anyq.NsqProducerArgs{Topic: "test"})
```

Consume:

```
recvCh := make(chan *anyq.Message, 256)
c.BindRecvChan(recvCh)
for m := range recvCh {
	fmt.Println("[receive]", string(m.Body))
}
```

Produce:

```
sendCh := make(chan []byte, 1)
p.BindSendChan(sendCh)
sendCh <- []byte("test message")
```

Close:
```
q.Close()
```

## Advanced usage

#### Optional setup function

set QoS and Exchange of RabbitMQ

```
setQos := func(q *anyq.Rabbitmq) {
	if err := q.Qos(100, 0, false); err != nil {
		log.Fatal(err)
	}
}

setExchange := func(q *anyq.Rabbitmq) {
	if err := q.ExchangeDeclare("test-ex", "direct", false, false, false, false, nil); err != nil {
		log.Fatal(err)
	}
	log.Println("declared Exchange")
}

q, err := anyq.New("rabbitmq", "amqp://guest:guest@127.0.0.1:5672/", setQos, setExchange)
```

set zookeeper urls of Kafka

```
q, err := anyq.New("kafka", "localhost:9092", func(q *anyq.Kafka) {
	q.Zookeepers = []string{"localhost:2181", "localhost:2182"}
})
```

#### Retrieve original conn object

```
q, err := anyq.New("nats", "nats://127.0.0.1:4222")
if err != nil {
	panic(err)
}

conn, err := q.Conn()
if err != nil {
	b.Error(err)
}
natsConn, ok := conn.(*nats.Conn)
if !ok {
	log.Fatalf("invalid conn type(%T)\n", conn)
}

natsConn.Subscribe("test", func(m *nats.Msg) {
	natsConn.Publish(m.Reply, m.Data)
	log.Println("[receive and reply]", string(m.Data))
})
```

## Test

Unit test:

```
$ go test github.com/jaehue/anyq
ok  	github.com/jaehue/anyq	1.136s
```

Benchmark test:

```
$ go test github.com/jaehue/anyq/benchmark -test.bench=. -test.benchmem
testing: warning: no tests to run
PASS
BenchmarkKafkaAsyncProduce	  300000	      4111 ns/op	     700 B/op	      10 allocs/op
BenchmarkKafkaSyncProduce	   20000	    100699 ns/op	    3080 B/op	      58 allocs/op
BenchmarkKafkaConsume	   30000	    151092 ns/op	   27805 B/op	     405 allocs/op
BenchmarkNatsProduce	  500000	      3468 ns/op	     280 B/op	       4 allocs/op
BenchmarkNatsConsume	  200000	     10199 ns/op	    1429 B/op	      21 allocs/op
BenchmarkNatsReply	    5000	    256335 ns/op	    2128 B/op	      88 allocs/op
BenchmarkNsqProduce	  100000	     14261 ns/op	     852 B/op	      17 allocs/op
BenchmarkNsqConsume	     100	  13415530 ns/op	  824936 B/op	   17322 allocs/op
BenchmarkRabbitmqProduce	   30000	     38698 ns/op	    1739 B/op	      53 allocs/op
BenchmarkRabbitmqConsume	       1	2421170045 ns/op	97861152 B/op	 1966673 allocs/op
ok  	github.com/jaehue/anyq/benchmark	23.944s
```

## Examples

- kafka  
  [kafka-consumer](https://github.com/jaehue/anyq/tree/master/example/kafka-consumer)  
  [kafka-producer](https://github.com/jaehue/anyq/tree/master/example/kafka-producer)  
- rabbitmq  
  [rabbitmq-consumer](https://github.com/jaehue/anyq/tree/master/example/rabbitmq-consumer)  
  [rabbitmq-producer](https://github.com/jaehue/anyq/tree/master/example/rabbitmq-producer)  
- nats  
  [nats-consumer](https://github.com/jaehue/anyq/tree/master/example/nats-consumer)  
  [nats-producer](https://github.com/jaehue/anyq/tree/master/example/nats-producer)  
- nsq  
  [nsq-consumer](https://github.com/jaehue/anyq/tree/master/example/nsq-consumer)  
  [nsq-producer](https://github.com/jaehue/anyq/tree/master/example/nsq-producer)  
