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
        log.Println("declaring Exchange: ", ex)
        if err := q.ExchangeDeclare(ex, "direct", false, false, false, false, nil); err != nil {
                log.Fatal(err)
        }
        log.Println("declared Exchange")
}

q, err := anyq.New("rabbitmq", "amqp://guest:guest@127.0.0.1:5672/", setQos, setExchange)
```

set zookeeper urls of Kafka

```
q, err := anyq.New("kafka", *brokerList, func(q *anyq.Kafka) {
        q.Zookeepers = strings.Split(*zookeeper, ",")
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
