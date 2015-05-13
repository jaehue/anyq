// This is queue library wrapper for widely popular queues. AnyQ provide one way to handle various queues.
//
// Supporting Queues
//   - RabbitMQ(https://www.rabbitmq.com)
//   - Kafka(https://kafka.apache.org)
//   - NSQ(http://nsq.io)
//   - NATS(http://nats.io)
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

// Queuer provide generic method to handle queue
type Queuer interface {
	// Conn returns original connection object.
	Conn() (interface{}, error)

	// NewConsumer create new consumer.
	// You MUST pass valid argument such as RabbitmqConsumerArgs, KafkaConsumerArgs, NsqConsumerArgs, and NatsConsumerArgs
	NewConsumer(args interface{}) (Consumer, error)

	// NewProducer create new producer.
	// You MUST pass valid argument such as RabbitmqProducerArgs, KafkaProducerArgs, NsqProducerArgs, and NatsProducerArgs
	NewProducer(args interface{}) (Producer, error)

	// SetLogger assigns the logger to use as well as a level.
	// The logger parameter is an interface that requires the following method to be implemented (such as the the stdlib log.Logger):
	//
	//    Output(calldepth int, s string)
	//
	SetLogger(logger, LogLevel)

	Setup(string) error
	closer
}

// Consumer process messages from Queue.
type Consumer interface {
	// Consumer returns original consumer object
	Consumer() (interface{}, error)

	// BindRecvChan bind a channel for receive operations from queue.
	BindRecvChan(messages chan<- *Message) error
	closer
}

// Producer publish messages to Queue.
type Producer interface {
	// Producer returns original producer object
	Producer() (interface{}, error)

	// Producer bind a channel for send operations to queue.
	BindSendChan(messages <-chan []byte) error
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

// Register makes a queue available by the provided name.
// If Register is called twice with the same name or if queue is nil, it panics.
func Register(name string, queue Queuer) {
	if queue == nil {
		panic("queue: Register queue is nil")
	}
	if _, dup := queues[name]; dup {
		panic("queue: Register called twice for queue " + name)
	}
	queues[name] = queue
}

// New creates a queue specified by its queue name and a queue url,
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
