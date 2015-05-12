package qlib

import (
	"fmt"
	"github.com/bitly/go-nsq"
	"log"
)

func init() {
	Register("nsq", &Nsq{})
}

type Nsq struct {
	url     string
	closers []closer
}

type NsqConsumerArgs struct {
	Topic, Channel string
}
type NsqProducerArgs struct {
	Topic string
}

type nsqConsumer struct {
	*nsq.Consumer
	url string
}

type nsqProducer struct {
	*nsq.Producer
	topic string
}

func (q *Nsq) Setup(url string) error {
	q.url = url
	return nil
}

func (q *Nsq) NewConsumer(v interface{}) (Consumer, error) {
	args, ok := v.(NsqConsumerArgs)
	if !ok {
		return nil, fmt.Errorf("invalid consume arguments(%v)", v)
	}

	c, err := nsq.NewConsumer(args.Topic, args.Channel, nsq.NewConfig())
	if err != nil {
		return nil, err
	}
	consumer := &nsqConsumer{c, q.url}
	q.closers = append(q.closers, consumer)
	return consumer, nil
}

func (q *Nsq) NewProducer(v interface{}) (Producer, error) {
	args, ok := v.(NsqProducerArgs)
	if !ok {
		return nil, fmt.Errorf("invalid consume arguments(%T)", v)
	}

	p, err := nsq.NewProducer(q.url, nsq.NewConfig())
	if err != nil {
		return nil, err
	}

	producer := &nsqProducer{p, args.Topic}
	q.closers = append(q.closers, producer)
	return producer, nil
}

func (q *Nsq) Close() error {
	for _, c := range q.closers {
		c.Close()
	}

	log.Printf("NSQ shutdown OK")

	return nil
}

func (c *nsqConsumer) BindRecvChan(messages chan<- *Message) error {
	c.AddConcurrentHandlers(nsq.HandlerFunc(func(m *nsq.Message) error {
		log.Println("receive message: ", string(m.Body))
		messages <- &Message{Body: m.Body, Origin: m}
		return nil
	}), 1)

	if err := c.ConnectToNSQD(c.url); err != nil {
		return err
	}
	return nil
}

func (c *nsqConsumer) Close() error {
	c.Stop()
	return nil
}

func (p *nsqProducer) BindSendChan(messages <-chan []byte) error {
	done := make(chan *nsq.ProducerTransaction, 1000000)
	go func() {
		for {
			<-done
		}
	}()
	go func() {
		for body := range messages {
			log.Println("send message: ", string(body))
			p.PublishAsync(p.topic, body, done)
		}
	}()

	return nil
}

func (p *nsqProducer) Close() error {
	p.Stop()
	return nil
}
