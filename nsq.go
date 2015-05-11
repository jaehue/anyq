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
	url string

	quits []chan struct{}
	quit  chan struct{}
}

type NsqConsumeArgs struct {
	Topic, Channel string
}
type NsqProduceArgs struct {
	Topic string
}

func (q *Nsq) Setup(url string) error {
	q.url = url
	q.quit = make(chan struct{})
	return nil
}

func (q *Nsq) Cleanup() error {
	defer func() {
		log.Printf("NSQ shutdown OK")
		q.quit <- struct{}{}
	}()

	for _, quit := range q.quits {
		quit <- struct{}{}
	}

	return nil
}

func (q *Nsq) Quit() <-chan struct{} {
	return q.quit
}

func (q *Nsq) BindRecvChan(recvCh chan<- []byte, args interface{}) error {
	consumeArgs, ok := args.(NsqConsumeArgs)
	if !ok {
		return fmt.Errorf("invalid consume arguments(%v)", args)
	}

	c, err := nsq.NewConsumer(consumeArgs.Topic, consumeArgs.Channel, nsq.NewConfig())
	if err != nil {
		return err
	}

	c.AddConcurrentHandlers(nsq.HandlerFunc(func(m *nsq.Message) error {
		log.Println("receive message: ", string(m.Body))
		recvCh <- m.Body
		return nil
	}), 1)

	go func() {
		quit := make(chan struct{})
		q.quits = append(q.quits, quit)

		<-quit
		c.Stop()
	}()

	return c.ConnectToNSQD(q.url)
}

func (q *Nsq) BindSendChan(sendCh <-chan []byte, v interface{}) error {
	args, ok := v.(NsqProduceArgs)
	if !ok {
		return fmt.Errorf("invalid consume arguments(%T)", v)
	}

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
			p.PublishAsync(args.Topic, body, done)
		}
	}()

	go func() {
		quit := make(chan struct{})
		q.quits = append(q.quits, quit)

		<-quit
		p.Stop()
	}()

	return nil
}
