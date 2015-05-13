package anyq

import (
	"github.com/wvanbergen/kafka/consumergroup"
	"gopkg.in/Shopify/sarama.v1"
	"testing"
)

func TestKafkaSetZookeeper(t *testing.T) {
	q, err := New("kafka", "localhost:9092")
	if err != nil {
		t.Error(err)
	}

	_, err = q.NewConsumer(KafkaConsumerArgs{Topic: "test", Partitions: "all", Group: "group1"})
	if err == nil {
		t.Error("zookeeper is required for group consumer")
	}
}

func TestKafkaSingleConsumer(t *testing.T) {
	q, err := New("kafka", "localhost:9092")
	if err != nil {
		t.Error(err)
	}

	c, err := q.NewConsumer(KafkaConsumerArgs{Topic: "test", Partitions: "all"})
	if err != nil {
		t.Error(err)
	}

	consumer, err := c.Consumer()
	if err != nil {
		t.Error(err)
	}

	if _, ok := consumer.(sarama.Consumer); !ok {
		t.Errorf("invalid consumer type(%T)\n", consumer)
	}
}

func TestKafkaGroupConsumer(t *testing.T) {
	q, err := New("kafka", "localhost:9092", func(q *Kafka) {
		q.Zookeepers = []string{"localhost:2181"}
	})
	if err != nil {
		t.Error(err)
	}

	c, err := q.NewConsumer(KafkaConsumerArgs{Topic: "test", Partitions: "all", Group: "group1"})
	if err != nil {
		t.Error(err)
	}

	consumer, err := c.Consumer()
	if err != nil {
		t.Error(err)
	}

	if _, ok := consumer.(*consumergroup.ConsumerGroup); !ok {
		t.Errorf("invalid consumer type(%T)\n", consumer)
	}
}

func TestKafkaSyncProducer(t *testing.T) {
	q, err := New("kafka", "localhost:9092")
	if err != nil {
		t.Error(err)
	}

	p, err := q.NewProducer(KafkaProducerArgs{Topic: "test", Sync: true})
	if err != nil {
		t.Error(err)
	}

	producer, err := p.Producer()
	if err != nil {
		t.Error(err)
	}

	if _, ok := producer.(sarama.SyncProducer); !ok {
		t.Errorf("invalid producer type(%T)\n", producer)
	}
}

func TestKafkaAsyncProducer(t *testing.T) {
	q, err := New("kafka", "localhost:9092")
	if err != nil {
		t.Error(err)
	}

	p, err := q.NewProducer(KafkaProducerArgs{Topic: "test"})
	if err != nil {
		t.Error(err)
	}

	producer, err := p.Producer()
	if err != nil {
		t.Error(err)
	}

	if _, ok := producer.(sarama.AsyncProducer); !ok {
		t.Errorf("invalid producer type(%T)\n", producer)
	}
}
