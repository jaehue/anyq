package qlib

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"reflect"
)

var queues = make(map[string]Queuer)

type Queuer interface {
	Setup(string) error
	cleanup() error
	Quit() <-chan struct{}

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

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Interrupt)

		<-signals
		log.Print("cleaning... ")
		if err := q.cleanup(); err != nil {
			log.Fatalln("cleaning error: ", err)
		} else {

			log.Println("clean complate")
		}
		os.Exit(0)
	}()

	return q, nil

}
