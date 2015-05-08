package qlib

import (
	"fmt"
)

var queues = make(map[string]Queuer)

func Register(name string, q Queuer) {
	if q == nil {
		panic("queue: Register queue is nil")
	}
	if _, dup := queues[name]; dup {
		panic("queue: Register called twice for queue " + name)
	}
	queues[name] = q
}

func Setup(qname, url string, options ...func(interface{}) interface{}) (Queuer, error) {
	q, ok := queues[qname]
	if !ok {
		return nil, fmt.Errorf("queue: unknown queue %q (forgotten import?)", qname)
	}
	fmt.Println("setup before ", q)
	q.Setup(url, options...)
	fmt.Println("setup complete ", q)
	return q, nil
}

type Queuer interface {
	Setup(string, ...func(interface{}) interface{})
	Consumer
	Producer
}

type Consumer interface {
	BindRecvChan(chan<- []byte) error
}

type Producer interface {
	BindSendChan(<-chan []byte) error
}
