package qlib

import (
	"github.com/bitly/go-nsq"
	"log"
)

func init() {
	Register("nsq", &Nsq{})
}

type Nsq struct {
	Url, Topic, Channel string
}

func (q *Nsq) Setup(url string) {
	q.Url = url
}

func (q *Nsq) BindRecvChan(recvCh chan<- []byte) error {
	c, err := nsq.NewConsumer(q.Topic, q.Channel, nsq.NewConfig())
	if err != nil {
		return err
	}

	c.AddConcurrentHandlers(nsq.HandlerFunc(func(m *nsq.Message) error {
		log.Println("receive message: ", string(m.Body))
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
		for body := range sendCh {
			log.Println("send message: ", string(body))
			p.PublishAsync(q.Topic, body, done)
		}
	}()

	return nil
}
