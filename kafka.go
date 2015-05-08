package qlib

import (
	"github.com/Shopify/sarama"
	"log"
	"strings"
)

func init() {
	Register("kafka", &Kafka{})
}

type Kafka struct {
	brokers []string
	*sarama.Config
}

func (q *Kafka) Setup(url string) error {
	q.brokers = strings.Split(url, ",")
	q.Config = sarama.NewConfig()
	return nil
}

func (q *Kafka) BindRecvChan(topic string, ch chan<- []byte) error {
	c, err := sarama.NewConsumer(q.brokers, q.Config)
	if err != nil {
		return err
	}
	partitions, err := c.Partitions(topic)
	if err != nil {
		return err
	}
	for _, p := range partitions {
		pc, err := c.ConsumePartition(topic, p, sarama.OffsetOldest)
		if err != nil {
			return err
		}
		go func(pc sarama.PartitionConsumer) {
			for m := range pc.Messages() {
				ch <- m.Value
			}
		}(pc)
	}
	return nil

}

func (q *Kafka) BindSendChan(topic string, ch <-chan []byte) error {
	p, err := sarama.NewSyncProducer(q.brokers, q.Config)
	if err != nil {
		return err
	}
	message := &sarama.ProducerMessage{Topic: topic}
	go func() {
		for body := range ch {
			message.Value = sarama.ByteEncoder(body)
			_, _, err := p.SendMessage(message)
			if err != nil {
				log.Printf("[Error]%v\n", err)
			}
			log.Println("send message: ", string(body))
		}
	}()
	return nil
}
