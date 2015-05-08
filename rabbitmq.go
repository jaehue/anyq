package qlib

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
)

func init() {
	Register("rabbitmq", &Rabbitmq{})
}

type Rabbitmq struct {
	*amqp.Channel
}

func (q *Rabbitmq) Setup(url string) error {
	log.Println("dialing amqp ", url)
	conn, err := amqp.Dial(url)
	if err != nil {
		return err
	}

	log.Println("got Connection, getting Channel")
	ch, err := conn.Channel()
	if err != nil {
		return err
	}

	log.Println("got Channel")
	q.Channel = ch

	return nil
}

func (q *Rabbitmq) BindRecvChan(qname string, recvCh chan<- []byte) error {
	log.Println("starting consume")
	deliveries, err := q.Consume(
		qname, // name
		"",    // consumerTag,
		false, // noAck
		false, // exclusive
		false, // noLocal
		false, // noWait
		nil,   // arguments
	)
	if err != nil {
		return err
	}

	go func() {

		for d := range deliveries {
			recvCh <- d.Body
		}
	}()

	return nil
}

func (q *Rabbitmq) BindSendChan(topic string, sendCh <-chan []byte) error {
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
			if err := q.Publish(
				"test-exchange", // publish to an exchange
				"test-key",      // routing to 0 or more queues
				false,           // mandatory
				false,           // immediate
				amqp.Publishing{
					Headers:         amqp.Table{},
					ContentType:     "text/plain",
					ContentEncoding: "",
					Body:            []byte(body),
					DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
					Priority:        0,              // 0-9
					// a bunch of application/implementation-specific fields
				},
			); err != nil {
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
