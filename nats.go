package anyq

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
	conn *nats.Conn
}

type NatsConsumerArgs struct {
	Subject string
}

type NatsProducerArgs struct {
	Subject string
}

type natsConsumer struct {
	*nats.Conn
	subject string
}

type natsProducer struct {
	*nats.Conn
	subject string
}

func (c *natsConsumer) Consumer() (interface{}, error) {
	return nil, fmt.Errorf("unsupported method")
}

func (c *natsConsumer) BindRecvChan(messages chan<- *Message) error {
	s, err := c.Conn.Subscribe(c.subject, func(m *nats.Msg) {
		log.Println("receive message: ", string(m.Data))
		messages <- &Message{Body: m.Data, Origin: m}
	})
	if err != nil {
		return err
	}
	log.Println("start consume: ", s.Subject)
	return nil
}

func (c *natsConsumer) Close() error {
	return nil
}

func (p *natsProducer) Producer() (interface{}, error) {
	return nil, fmt.Errorf("unsupported method")
}

func (p *natsProducer) BindSendChan(messages <-chan []byte) error {
	go func() {
		for {
			for body := range messages {
				p.Conn.Publish(p.subject, body)
			}
		}
	}()
	return nil
}

func (p *natsProducer) Close() error {
	return nil
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
	q.conn = nc
	return nil
}

func (q *Nats) Conn() (interface{}, error) {
	return q.conn, nil
}

func (q *Nats) SetLogger(l logger, level LogLevel) {}

func (q *Nats) Close() error {
	q.conn.Close()
	log.Printf("NATS shutdown OK")
	return nil
}

func (q *Nats) NewConsumer(v interface{}) (Consumer, error) {
	args, ok := v.(NatsConsumerArgs)
	if !ok {
		return nil, fmt.Errorf("invalid consume arguments(%v)", v)
	}

	return &natsConsumer{q.conn, args.Subject}, nil
}

func (q *Nats) NewProducer(v interface{}) (Producer, error) {
	args, ok := v.(NatsProducerArgs)
	if !ok {
		return nil, fmt.Errorf("invalid consume arguments(%v)", v)
	}
	return &natsProducer{q.conn, args.Subject}, nil
}
