package qlib

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"reflect"
)

func init() {
	Register("rabbitmq", &Rabbitmq{})
}

type Rabbitmq struct {
	*amqp.Channel
	consumerTag string
	conn        *amqp.Connection
	quit        chan struct{}
}

type RabbitmqConsumeArgs struct {
	Queue                               string `validate:"nonzero"`
	Consumer                            string
	AutoAck, Exclusive, NoLocal, NoWait bool
	Args                                map[string]interface{}
}

type RabbitmqProduceArgs struct {
	Exchange             string          `validate:"nonzero"`
	RoutingKey           string          `validate:"nonzero"`
	Msg                  amqp.Publishing `validate:"nonzero"`
	Mandatory, Immediate bool
}

func (q *Rabbitmq) Setup(url string) error {
	log.Println("dialing amqp ", url)
	conn, err := amqp.Dial(url)
	if err != nil {
		return err
	}
	q.conn = conn

	log.Println("got Connection, getting Channel")
	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	log.Println("got Channel")
	q.Channel = ch

	q.quit = make(chan struct{})

	return nil
}

func (q *Rabbitmq) cleanup() error {
	defer func() {
		log.Printf("Rabbitmq shutdown OK")
		q.quit <- struct{}{}
	}()

	if err := q.Cancel(q.consumerTag, true); err != nil {
		return fmt.Errorf("Consumer cancel failed: %s", err)
	}

	if err := q.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	return nil
}

func (q *Rabbitmq) Quit() <-chan struct{} {
	return q.quit
}

func (q *Rabbitmq) BindRecvChan(recvCh chan<- []byte, args interface{}) error {
	c, ok := args.(RabbitmqConsumeArgs)
	if !ok {
		return fmt.Errorf("invalid consume arguments(%v)", args)
	}

	deliveries, err := q.Consume(c.Queue, c.Consumer, c.AutoAck, c.Exclusive, c.NoLocal, c.NoWait, c.Args)
	if err != nil {
		return err
	}

	log.Println("starting consume")
	go func() {

		for d := range deliveries {
			recvCh <- d.Body
		}
	}()

	return nil
}

func (q *Rabbitmq) BindSendChan(sendCh <-chan []byte, args interface{}) error {
	p, ok := args.(RabbitmqProduceArgs)
	if !ok {
		return fmt.Errorf("invalid produce arguments(%v)", args)
	}

	if reflect.DeepEqual(p.Msg, amqp.Publishing{}) {
		p.Msg = amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "text/plain",
			ContentEncoding: "",
			DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
			Priority:        0,              // 0-9
		}
	}

	// Reliable publisher confirms require confirm.select support from the
	// connection.
	log.Printf("enabling publishing confirms.")
	if err := q.Confirm(false); err != nil {
		return fmt.Errorf("Channel could not be put into confirm mode: %s", err)
	}

	// ack, nack := q.NotifyConfirm(make(chan uint64, 1), make(chan uint64, 1))

	// defer confirmOne(ack, nack)
	go func() {
		for body := range sendCh {
			p.Msg.Body = body
			if err := q.Publish(p.Exchange, p.RoutingKey, p.Mandatory, p.Immediate, p.Msg); err != nil {
				log.Fatalf("Exchange Publish: %s", err)
			}
		}
	}()
	return nil
}

func confirmOne(ack, nack chan uint64) {
	log.Printf("waiting for confirmation of one publishing")

	select {
	case tag := <-ack:
		log.Printf("confirmed delivery with delivery tag: %d", tag)
	case tag := <-nack:
		log.Printf("failed delivery of delivery tag: %d", tag)
	}
}
