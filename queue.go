package qlib

import (
	"fmt"
	"reflect"
)

var queues = make(map[string]Queuer)

type Queuer interface {
	Setup(string) error
	Consumer
	Producer
}

type Consumer interface {
	BindRecvChan(chan<- []byte, interface{}) error
}

type Producer interface {
	BindSendChan(<-chan []byte, interface{}) error
}

func Register(name string, q Queuer) {
	if q == nil {
		panic("queue: Register queue is nil")
	}
	if _, dup := queues[name]; dup {
		panic("queue: Register called twice for queue " + name)
	}
	queues[name] = q
}

func Setup(qname, url string, setupFn ...interface{}) (Queuer, error) {
	q, ok := queues[qname]
	if !ok {
		return nil, fmt.Errorf("queue: unknown queue %q (forgotten import?)", qname)
	}

	if err := q.Setup(url); err != nil {
		return nil, err
	}

	for _, f := range setupFn {
		fn := reflect.ValueOf(f)
		fn.Call([]reflect.Value{reflect.ValueOf(q)})
	}

	return q, nil
}
