package qlib

import (
	"github.com/apcera/nats"
	"log"
	"strings"
)

func init() {
	Register("nats", &Nats{})
}

type Nats struct {
	*nats.Conn
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
	return nil
}

func (q *Nats) BindRecvChan(topic string, ch chan<- []byte) error {
	s, err := q.Subscribe(topic, func(m *nats.Msg) {
		log.Println("receive message: ", string(m.Data))
		ch <- m.Data
	})
	if err != nil {
		return err
	}
	log.Println("start consume: ", s.Subject)
	return nil
}

func (q *Nats) BindSendChan(topic string, ch <-chan []byte) error {
	go func() {
		for {
			for body := range ch {
				q.Publish(topic, body)
			}
		}
	}()
	return nil
}
