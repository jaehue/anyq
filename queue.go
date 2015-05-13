package anyq

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"reflect"
)

const _MESSAGE_BUF_COUNT = 100

var queues = make(map[string]Queuer)

type Message struct {
	Body   []byte
	Origin interface{}
}

type Queuer interface {
	Setup(string) error
	NewConsumer(args interface{}) (Consumer, error)
	NewProducer(args interface{}) (Producer, error)
	SetLogger(logger, LogLevel)
	closer
}

type Consumer interface {
	BindRecvChan(messages chan<- *Message) error
	closer
}

type Producer interface {
	BindSendChan(messages <-chan []byte) error
	// Publish(message *Message) error
	closer
}

// LogLevel specifies the severity of a given log message
type LogLevel int

// Log levels
const (
	LogLevelDebug LogLevel = iota
	LogLevelInfo
	LogLevelWarning
	LogLevelError
)

type closer interface {
	Close() error
}

type logger interface {
	Output(calldepth int, s string) error
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

func New(qname, url string, setupFn ...interface{}) (Queuer, error) {
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
		if err := q.Close(); err != nil {
			log.Fatalln("cleaning error: ", err)
		} else {

			log.Println("clean complate")
		}
		os.Exit(0)
	}()

	return q, nil

}
