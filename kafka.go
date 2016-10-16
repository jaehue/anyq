package anyq

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/wvanbergen/kafka/consumergroup"
)

func init() {
	Register("kafka", &Kafka{})
}

type Kafka struct {
	Brokers    []string // The comma separated list of brokers in the Kafka cluster
	Zookeepers []string

	closers []closer
}

type KafkaConsumerArgs struct {
	Topic      string `validate:"nonzero"`
	Group      string
	Partitions string
	Offset     string `validate:"regexp=^(oldest|newest)$"`
}

type KafkaProducerArgs struct {
	Topic       string `validate:"nonzero"`
	Key         string
	Partitioner string `validate:"regexp=^(hash|manual|random)$"` // The partitioning scheme to usep
	Partition   int32  // The partition to produce to
	Sync        bool
}

type kafkaSingleConsumer struct {
	consumer           sarama.Consumer
	partitionConsumers []sarama.PartitionConsumer
	messages           chan *sarama.ConsumerMessage
	quits              []chan struct{}
}

type kafkaGroupConsumer struct {
	consumer *consumergroup.ConsumerGroup
	quit     chan struct{}
}

type kafkaSyncProducer struct {
	producer sarama.SyncProducer

	topic     string
	partition int32
	key       string

	quit chan struct{}
}

type kafkaAsyncProducer struct {
	producer sarama.AsyncProducer

	topic     string
	partition int32
	key       string

	quit chan struct{}
}

func (q *Kafka) Setup(url string) error {
	q.Brokers = strings.Split(url, ",")
	return nil
}

func (q *Kafka) Conn() (interface{}, error) {
	return nil, fmt.Errorf("unsupported method")
}

func (q *Kafka) SetLogger(l logger, level LogLevel) {}

func (q *Kafka) Close() error {
	for _, c := range q.closers {
		if err := c.Close(); err != nil {
			log.Fatalln(err)
		}
	}

	log.Printf("Kafka shutdown OK")

	return nil
}

func (q *Kafka) NewConsumer(v interface{}) (Consumer, error) {
	args, ok := v.(KafkaConsumerArgs)
	if !ok {
		return nil, fmt.Errorf("invalid consumer arguments(%v)", v)
	}

	if args.Group != "" {
		if len(q.Zookeepers) == 0 {
			return nil, fmt.Errorf("zookeeper url is required.")
		}
		config := consumergroup.NewConfig()
		config.Offsets.Initial = args.getOffset()
		config.Offsets.ProcessingTimeout = 10 * time.Second

		cg, err := consumergroup.JoinConsumerGroup(args.Group, strings.Split(args.Topic, ","), q.Zookeepers, config)
		if err != nil {
			return nil, err
		}

		c := &kafkaGroupConsumer{consumer: cg}
		q.closers = append(q.closers, c)

		return c, nil

	}

	c, err := sarama.NewConsumer(q.Brokers, sarama.NewConfig())
	if err != nil {
		return nil, err
	}
	sc := &kafkaSingleConsumer{consumer: c}

	partitions, err := args.getPartitions(c)
	if err != nil {
		return nil, err
	}

	for _, p := range partitions {
		pc, err := c.ConsumePartition(args.Topic, p, args.getOffset())
		if err != nil {
			return nil, err
		}
		sc.partitionConsumers = append(sc.partitionConsumers, pc)
	}
	q.closers = append(q.closers, sc)
	return sc, err
}

func (q *Kafka) NewProducer(v interface{}) (Producer, error) {
	args, ok := v.(KafkaProducerArgs)
	if !ok {
		return nil, fmt.Errorf("invalid consume arguments(%v)", v)
	}

	config := sarama.NewConfig()
	if partitioner, err := args.getPartitioner(); err == nil {
		config.Producer.Partitioner = partitioner
	}

	if args.Sync {
		config.Producer.RequiredAcks = sarama.WaitForAll

		producer, err := sarama.NewSyncProducer(q.Brokers, config)
		if err != nil {
			return nil, fmt.Errorf("Failed to open Kafka producer: %s", err)
		}

		p := &kafkaSyncProducer{
			producer:  producer,
			topic:     args.Topic,
			partition: args.Partition,
		}
		q.closers = append(q.closers, p)

		return p, nil
	}

	producer, err := sarama.NewAsyncProducer(q.Brokers, config)
	if err != nil {
		return nil, fmt.Errorf("Failed to open Kafka producer: %s", err)
	}

	p := &kafkaAsyncProducer{
		producer:  producer,
		topic:     args.Topic,
		partition: args.Partition,
	}
	q.closers = append(q.closers, p)

	return p, nil
}

func (c *kafkaSingleConsumer) Consumer() (interface{}, error) {
	return c.consumer, nil
}

func (c *kafkaSingleConsumer) BindRecvChan(messages chan<- *Message) error {
	if c.messages == nil {
		c.messages = make(chan *sarama.ConsumerMessage, _MESSAGE_BUF_COUNT)
	}

	for _, pc := range c.partitionConsumers {
		go func(pc sarama.PartitionConsumer) {
			quit := make(chan struct{}, 1)
			c.quits = append(c.quits, quit)
		SingleConsumerLoop:
			for {
				select {
				case c.messages <- <-pc.Messages():
				case err := <-pc.Errors():
					log.Fatalln(err)
				case <-quit:
					break SingleConsumerLoop
				}
			}
		}(pc)
	}

	go func() {
		for m := range c.messages {
			messages <- &Message{Body: m.Value, Origin: m}
		}
	}()
	return nil
}

func (c *kafkaSingleConsumer) Close() error {
	if c.consumer == nil {
		return nil
	}

	for _, q := range c.quits {
		q <- struct{}{}
	}
	log.Println("break consume loop")

	for _, pc := range c.partitionConsumers {
		pc.AsyncClose()
	}
	log.Println("close partition consumers")

	if err := c.consumer.Close(); err != nil {
		return nil
	}
	log.Println("close single consumer")

	return nil
}
func (c *kafkaGroupConsumer) Consumer() (interface{}, error) {
	return c.consumer, nil
}
func (c *kafkaGroupConsumer) BindRecvChan(messages chan<- *Message) error {
	go func() {
		if c.quit == nil {
			c.quit = make(chan struct{}, 1)
		}

	GroupConsumeLoop:
		for {
			select {
			case m := <-c.consumer.Messages():
				messages <- &Message{Body: m.Value, Origin: m}
				if err := c.consumer.CommitUpto(m); err != nil {
					log.Println("Consume commit error: ", err)
				}
			case err := <-c.consumer.Errors():
				log.Fatalln(err)
			case <-c.quit:
				break GroupConsumeLoop
			}
		}
	}()
	return nil
}

func (c *kafkaGroupConsumer) Subscribe(handler func(*Message)) error {
	go func() {
		if c.quit == nil {
			c.quit = make(chan struct{}, 1)
		}

	GroupSubscribeLoop:
		for {
			select {
			case m := <-c.consumer.Messages():
				handler(&Message{Body: m.Value, Origin: m})
				if err := c.consumer.CommitUpto(m); err != nil {
					log.Println("Consume commit error: ", err)
				}
			case err := <-c.consumer.Errors():
				log.Fatalln(err)
			case <-c.quit:
				break GroupSubscribeLoop
			}
		}
	}()
	return nil
}

func (c *kafkaGroupConsumer) Close() error {
	if c.consumer == nil {
		return nil
	}

	c.quit <- struct{}{}
	log.Println("break consume loop")

	if err := c.consumer.Close(); err != nil {
		return err
	}
	log.Println("close group consumer")

	return nil
}

func (p *kafkaSyncProducer) Producer() (interface{}, error) {
	return p.producer, nil
}

func (p *kafkaSyncProducer) BindSendChan(messages <-chan []byte) error {
	go func() {
		if p.quit == nil {
			p.quit = make(chan struct{}, 1)
		}

	SyncProduceLoop:
		for {
			select {
			case <-p.quit:
				break SyncProduceLoop
			case body := <-messages:
				message := &sarama.ProducerMessage{Topic: p.topic, Partition: p.partition}
				if p.key != "" {
					message.Key = sarama.StringEncoder(p.key)
				}

				message.Value = sarama.ByteEncoder(body)

				partition, offset, err := p.producer.SendMessage(message)
				if err != nil {
					log.Fatalf("[Error]%v\n", err)
				}
				log.Printf("[sent]topic=%s\tpartition=%d\toffset=%d\n", p.topic, partition, offset)
			}
		}

	}()
	return nil
}

func (p *kafkaSyncProducer) Close() error {
	if p.producer == nil {
		return nil
	}

	if p.quit != nil {
		p.quit <- struct{}{}
	}

	if err := p.producer.Close(); err != nil {
		return err
	}

	return nil
}

func (p *kafkaAsyncProducer) Producer() (interface{}, error) {
	return p.producer, nil
}
func (p *kafkaAsyncProducer) BindSendChan(messages <-chan []byte) error {
	go func() {
		if p.quit == nil {
			p.quit = make(chan struct{}, 1)
		}

	AsyncProduceLoop:
		for {
			select {
			case <-p.quit:
				break AsyncProduceLoop
			case body := <-messages:
				message := &sarama.ProducerMessage{Topic: p.topic, Partition: int32(p.partition)}
				if p.key != "" {
					message.Key = sarama.StringEncoder(p.key)
				}

				message.Value = sarama.ByteEncoder(body)
				p.producer.Input() <- message
			case err := <-p.producer.Errors():
				log.Fatalln("Failed to produce message", err)
			}
		}
	}()
	return nil
}

func (p *kafkaAsyncProducer) Close() error {
	if p.producer == nil {
		return nil
	}

	if p.quit != nil {
		p.quit <- struct{}{}
	}

	if err := p.producer.Close(); err != nil {
		return err
	}

	return nil
}

func (args *KafkaConsumerArgs) getOffset() int64 {
	switch args.Offset {
	case "oldest":
		return sarama.OffsetOldest
	case "newest":
		return sarama.OffsetNewest
	}
	return sarama.OffsetNewest
}

func (args *KafkaConsumerArgs) getPartitions(c sarama.Consumer) ([]int32, error) {
	if args.Partitions == "all" {
		return c.Partitions(args.Topic)
	}

	tmp := strings.Split(args.Partitions, ",")
	var pList []int32
	for i := range tmp {
		val, err := strconv.ParseInt(tmp[i], 10, 32)
		if err != nil {
			return nil, err
		}
		pList = append(pList, int32(val))
	}

	return pList, nil
}

func (args *KafkaProducerArgs) getPartitioner() (func(string) sarama.Partitioner, error) {
	switch args.Partitioner {
	case "":
		if args.Partition >= 0 {
			return sarama.NewManualPartitioner, nil
		} else {
			return sarama.NewHashPartitioner, nil
		}
	case "hash":
		return sarama.NewHashPartitioner, nil
	case "random":
		return sarama.NewRandomPartitioner, nil
	case "manual":
		if args.Partition == -1 {
			return nil, fmt.Errorf("partition is required when partitioning manually")
		}
		return sarama.NewManualPartitioner, nil
	default:
		return nil, fmt.Errorf("Partitioner %s not supported.", args.Partitioner)
	}
}
