package qlib

import (
	"github.com/bitly/go-nsq"
	"log"
	"reflect"
)

func init() {
	Register("nsq", &Nsq{})
}

type Nsq struct {
	Url, Topic, Channel string
}

func (q *Nsq) Setup(url string, options ...func(interface{}) interface{}) {
	for _, option := range options {
		if v := option(q); v != nil {
			if x, ok := v.(*Nsq); ok {
				q = x
			}
		}
	}
	q.Url = url
	log.Println("setup q: ", q)

}

func (q *Nsq) Setup2(options ...func(*Nsq)) {
	for _, option := range options {
		option(q)
	}
}

func NewNsqWithOption(options ...func(*Nsq)) *Nsq {
	q := &Nsq{}

	for _, option := range options {
		option(q)
	}

	return q
}

func NewNsq(options ...func(interface{}) interface{}) *Nsq {
	q := Nsq{}

	log.Println(reflect.TypeOf(q))
	for _, option := range options {
		if v := option(&q); v != nil {
			if x, ok := v.(*Nsq); ok {
				log.Println("set nsq")
				q = *x
			}
		}
	}

	log.Println(q)
	return &q
}

func (q *Nsq) BindRecvChan(recvCh chan<- []byte) error {
	log.Println("recv ", q)
	c, err := nsq.NewConsumer(q.Topic, q.Channel, nsq.NewConfig())
	if err != nil {
		return err
	}

	c.AddConcurrentHandlers(nsq.HandlerFunc(func(m *nsq.Message) error {
		log.Println(string(m.Body))
		recvCh <- m.Body
		return nil
	}), 1)

	return c.ConnectToNSQD(q.Url)
}

func (q *Nsq) BindSendChan(sendCh <-chan []byte) error {
	p, err := nsq.NewProducer(q.Url, nsq.NewConfig())
	if err != nil {
		return err
	}
	done := make(chan *nsq.ProducerTransaction, 1000000)
	go func() {
		for {
			<-done
		}
	}()
	go func() {
		for m := range sendCh {
			p.PublishAsync(q.Topic, m, done)
		}
	}()

	return nil
}
