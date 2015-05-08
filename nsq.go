package qlib

import (
	"github.com/bitly/go-nsq"
	"log"
)

func init() {
	Register("nsq", &Nsq{})
}

type Nsq struct {
	url, ConsumeChannel string
}

func (q *Nsq) Setup(url string) error {
	q.url = url
	return nil
}

func (q *Nsq) BindRecvChan(topic string, recvCh chan<- []byte) error {
	c, err := nsq.NewConsumer(topic, q.ConsumeChannel, nsq.NewConfig())
	if err != nil {
		return err
	}

	c.AddConcurrentHandlers(nsq.HandlerFunc(func(m *nsq.Message) error {
		log.Println("receive message: ", string(m.Body))
		recvCh <- m.Body
		return nil
	}), 1)

	return c.ConnectToNSQD(q.url)
}

func (q *Nsq) BindSendChan(topic string, sendCh <-chan []byte) error {
	p, err := nsq.NewProducer(q.url, nsq.NewConfig())
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
		for body := range sendCh {
			log.Println("send message: ", string(body))
			p.PublishAsync(topic, body, done)
		}
	}()

	return nil
}
