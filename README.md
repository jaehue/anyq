# AnyQ

This is queue library wrapper for widely popular queues.
AnyQ provide one way to handle various queues.

Supporting Queues
- rabbitmq  
  https://www.rabbitmq.com
- kafka  
  https://kafka.apache.org
- nsq  
  http://nsq.io
- nats  
  http://nats.io

## Usage

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
