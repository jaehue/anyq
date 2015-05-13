package anyq

import (
	"github.com/bitly/go-nsq"
	"testing"
)

func TestNsqConsumer(t *testing.T) {
	q, err := New("nsq", "127.0.0.1:4150")
	if err != nil {
		t.Error(err)
	}

	c, err := q.NewConsumer(NsqConsumerArgs{Topic: "test", Channel: "anyq"})
	if err != nil {
		t.Error(err)
	}

	consumer, err := c.Consumer()
	if err != nil {
		t.Error(err)
	}

	if _, ok := consumer.(*nsq.Consumer); !ok {
		t.Errorf("invalid consumer type(%T)\n", consumer)
	}
}

func TestNsqProducer(t *testing.T) {
	q, err := New("nsq", "127.0.0.1:4150")
	if err != nil {
		t.Error(err)
	}

	p, err := q.NewProducer(NsqProducerArgs{Topic: "test"})
	if err != nil {
		t.Error(err)
	}

	producer, err := p.Producer()
	if err != nil {
		t.Error(err)
	}

	if _, ok := producer.(*nsq.Producer); !ok {
		t.Errorf("invalid producer type(%T)\n", producer)
	}
}
