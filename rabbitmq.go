package anyq

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
	conn    *amqp.Connection
	closers []closer
}

type RabbitmqConsumerArgs struct {
	Queue                               string `validate:"nonzero"`
	RoutingKey                          string
	Exchange                            string
	ConsumerTag                         string
	AutoAck, Exclusive, NoLocal, NoWait bool
	Args                                map[string]interface{}
}

type RabbitmqProducerArgs struct {
	Exchange             string `validate:"nonzero"`
	RoutingKey           string `validate:"nonzero"`
	Mandatory, Immediate bool
	DeliveryMode         uint8
}

type rabbitmqConsumer struct {
	channel *amqp.Channel
	args    *RabbitmqConsumerArgs
}

type rabbitmqProducer struct {
	channel *amqp.Channel
	args    *RabbitmqProducerArgs
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

	return nil
}

func (q *Rabbitmq) SetLogger(l logger, level LogLevel) {}

func (q *Rabbitmq) Close() error {
	for _, c := range q.closers {
		c.Close()
	}

	if err := q.Channel.Close(); err != nil {
		return fmt.Errorf("AMQP channel close error: %s", err)
	}

	if err := q.conn.Close(); err != nil {
		return fmt.Errorf("AMQP connection close error: %s", err)
	}

	log.Printf("Rabbitmq shutdown OK")

	return nil
}

func (q *Rabbitmq) NewConsumer(v interface{}) (Consumer, error) {
	args, ok := v.(RabbitmqConsumerArgs)
	if !ok {
		return nil, fmt.Errorf("invalid consumer arguments(%v)", v)
	}

	log.Println("declaring Queue: ", args.Queue)
	mq, err := q.QueueDeclare(args.Queue, true, false, false, false, nil)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("declared Queue (%q %d messages, %d consumers)\n", mq.Name, mq.Messages, mq.Consumers)

	log.Printf("binding to Exchange (key %q)\n", args.RoutingKey)
	if err := q.QueueBind(mq.Name, args.RoutingKey, args.Exchange, false, nil); err != nil {
		log.Fatal(err)
	}
	log.Println("Queue bound to Exchange")

	c := &rabbitmqConsumer{q.Channel, &args}
	q.closers = append(q.closers, c)
	return c, nil
}

func (q *Rabbitmq) NewProducer(v interface{}) (Producer, error) {
	args, ok := v.(RabbitmqProducerArgs)
	if !ok {
		return nil, fmt.Errorf("invalid produce arguments(%v)", v)
	}

	p := &rabbitmqProducer{q.Channel, &args}
	q.closers = append(q.closers, p)
	return p, nil
}

func (c *rabbitmqConsumer) BindRecvChan(messages chan<- *Message) error {
	deliveries, err := c.channel.Consume(c.args.Queue, c.args.ConsumerTag, c.args.AutoAck, c.args.Exclusive, c.args.NoLocal, c.args.NoWait, c.args.Args)
	if err != nil {
		return err
	}

	go func() {
		for d := range deliveries {
			messages <- &Message{Body: d.Body, Origin: d}
		}
	}()
	return nil
}

func (c *rabbitmqConsumer) Close() error {
	if err := c.channel.Cancel(c.args.ConsumerTag, true); err != nil {
		return fmt.Errorf("Consumer cancel failed: %s", err)
	}
	return nil
}

func (p *rabbitmqProducer) BindSendChan(messages <-chan []byte) error {
	if err := p.channel.Confirm(false); err != nil {
		return fmt.Errorf("Channel could not be put into confirm mode: %s", err)
	}

	go func() {
		msg := amqp.Publishing{DeliveryMode: p.args.DeliveryMode}
		for m := range messages {
			msg.Body = m
			if err := p.channel.Publish(p.args.Exchange, p.args.RoutingKey, p.args.Mandatory, p.args.Immediate, msg); err != nil {
				log.Fatalf("Exchange Publish: %s", err)
			}
		}
	}()
	return nil
}

func (p *rabbitmqProducer) Close() error {
	return nil
}
