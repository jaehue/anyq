package qlib

import (
	"fmt"
	"github.com/apcera/nats"
	"log"
	"strings"
)

func init() {
	Register("nats", &Nats{})
}

type Nats struct {
	*nats.Conn

	quits []chan struct{}
	quit  chan struct{}
}

type NatsConsumeArgs struct {
	Subject string
}

type NatsProduceArgs struct {
	Subject string
}

func (q *Nats) Setup(url string) error {
	opts := nats.DefaultOptions
	opts.Servers = strings.Split(url, ",")
	for i, s := range opts.Servers {
		opts.Servers[i] = strings.Trim(s, " ")
	}
	nc, err := opts.Connect()
	if err != nil {
		return err
	}
	q.Conn = nc

	q.quit = make(chan struct{})

	return nil
}

func (q *Nats) Cleanup() error {
	defer func() {
		log.Printf("NATS shutdown OK")
		q.quit <- struct{}{}
	}()

	for _, quit := range q.quits {
		quit <- struct{}{}
	}

	return nil
}

func (q *Nats) Quit() <-chan struct{} {
	return q.quit
}

func (q *Nats) BindRecvChan(ch chan<- []byte, v interface{}) error {
	args, ok := v.(NatsConsumeArgs)
	if !ok {
		return fmt.Errorf("invalid consume arguments(%v)", v)
	}

	s, err := q.Subscribe(args.Subject, func(m *nats.Msg) {
		log.Println("receive message: ", string(m.Data))
		ch <- m.Data
	})
	if err != nil {
		return err
	}
	log.Println("start consume: ", s.Subject)
	return nil
}

func (q *Nats) BindSendChan(ch <-chan []byte, v interface{}) error {
	args, ok := v.(NatsProduceArgs)
	if !ok {
		return fmt.Errorf("invalid consume arguments(%v)", v)
	}

	go func() {
		for {
			for body := range ch {
				q.Publish(args.Subject, body)
			}
		}
	}()
	return nil
}
