package qlib

import (
	"github.com/bitly/go-nsq"
	"log"
)

type Nsq struct {
	Url string
}

func NewNsq(url string) *Nsq {
	q := &Nsq{url}
	return q
}

func (q *Nsq) BindRecvChan(topic, channel string, recvCh chan<- []byte) error {
	c, err := nsq.NewConsumer(topic, channel, nsq.NewConfig())
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

func (q *Nsq) BindSendChan(topic string, sendCh <-chan []byte) error {
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
			p.PublishAsync(topic, m, done)
		}
	}()

	return nil
}
