package qlib

import (
	"fmt"
	"github.com/wvanbergen/kafka/consumergroup"
	"gopkg.in/Shopify/sarama.v1"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

func init() {
	Register("kafka", &Kafka{})
}

type Kafka struct {
	brokers []string // The comma separated list of brokers in the Kafka cluster

	quitWg sync.WaitGroup
	quits  []chan struct{}
	quit   chan struct{}
}

type KafkaConsumeArgs struct {
	Topic      string `validate:"nonzero"`
	Partitions string // The partitions to consume, can be 'all' or comma-separated numbers
	Offset     string `validate:"regexp=^(oldest|newest)$"`
	Zookeeper  string
	Group      string
}

type KafkaProduceArgs struct {
	Topic       string `validate:"nonzero"`
	Key         string
	Partitioner string `validate:"regexp=^(hash|manual|random)$"` // The partitioning scheme to usep
	Partition   int    // The partition to produce to
	Sync        bool
}

func (q *Kafka) Setup(url string) error {
	q.brokers = strings.Split(url, ",")
	q.quit = make(chan struct{}, 1)
	return nil
}

func (q *Kafka) Quit() <-chan struct{} {
	return q.quit
}

func (q *Kafka) Cleanup() error {
	defer func() {
		log.Printf("Kafka shutdown OK")
		q.quit <- struct{}{}
	}()

	for _, quit := range q.quits {
		q.quitWg.Add(1)
		quit <- struct{}{}
	}

	q.quitWg.Wait()
	return nil
}

type kafkaAbstractConsumer interface {
	Close() error
	Messages() <-chan *sarama.ConsumerMessage
	Errors() <-chan *sarama.ConsumerError
}

type kafkaAbstractConsumers []kafkaAbstractConsumer

func (cs kafkaAbstractConsumers) bindRecvChan(q *Kafka, ch chan<- []byte) {
	var wg sync.WaitGroup
	for _, c := range cs {
		wg.Add(1)
		go func(c kafkaAbstractConsumer) {
			defer wg.Done()

			quit := make(chan struct{})
			q.quits = append(q.quits, quit)

		consumeLoop:
			for {
				select {
				case err := <-c.Errors():
					log.Println(err)
				case m := <-c.Messages():
					ch <- m.Value
				case <-quit:
					c.Close()
					break consumeLoop
				}
			}
		}(c)
	}

	go func() {
		wg.Wait()
		close(ch)
		log.Println("consumer closed.")
	}()
}

func (q *Kafka) BindRecvChan(ch chan<- []byte, args interface{}) error {
	consumeArgs, ok := args.(KafkaConsumeArgs)
	if !ok {
		return fmt.Errorf("invalid consume arguments(%v)", args)
	}

	var consumers kafkaAbstractConsumers

	if consumeArgs.Group != "" {
		config := consumergroup.NewConfig()
		config.Offsets.Initial = consumeArgs.getOffset()
		config.Offsets.ProcessingTimeout = 10 * time.Second

		c, err := consumergroup.JoinConsumerGroup(consumeArgs.Group, []string{consumeArgs.Topic}, strings.Split(consumeArgs.Zookeeper, ","), config)
		if err != nil {
			return err
		}

		consumers = []kafkaAbstractConsumer{c}

	} else {
		c, err := sarama.NewConsumer(q.brokers, sarama.NewConfig())
		if err != nil {
			return err
		}

		quit := make(chan struct{})
		q.quits = append(q.quits, quit)

		go func() {
			<-quit
			if err := c.Close(); err != nil {
				log.Fatalln(err)
			}
		}()
		partitions, err := consumeArgs.getPartitions(c)
		if err != nil {
			return err
		}

		for _, p := range partitions {
			pc, err := c.ConsumePartition(consumeArgs.Topic, p, consumeArgs.getOffset())
			if err != nil {
				return err
			}
			consumers = append(consumers, pc)
		}
	}

	consumers.bindRecvChan(q, ch)

	// if err := c.Close(); err != nil {
	// 	log.Fatalln("Failed to close consumer: ", err)
	// 	return
	// }
	//log.Println("consumer closed.")
	//q.quitWg.Done()
	return nil

}

func (q *Kafka) BindSendChan(ch <-chan []byte, args interface{}) error {
	produceArgs, ok := args.(KafkaProduceArgs)
	if !ok {
		return fmt.Errorf("invalid consume arguments(%v)", args)
	}

	config := sarama.NewConfig()
	// set partitioner
	if partitioner, err := produceArgs.getPartitioner(); err == nil {
		config.Producer.Partitioner = partitioner
	}

	if produceArgs.Sync {
		config.Producer.RequiredAcks = sarama.WaitForAll
		producer, err := sarama.NewSyncProducer(q.brokers, config)
		if err != nil {
			return fmt.Errorf("Failed to open Kafka producer: %s", err)
		}

		go func() {
			quit := make(chan struct{})
			q.quits = append(q.quits, quit)

		SyncProduceLoop:
			for {
				select {
				case <-quit:
					break SyncProduceLoop
				case body := <-ch:
					message := &sarama.ProducerMessage{Topic: produceArgs.Topic, Partition: int32(produceArgs.Partition)}
					if produceArgs.Key != "" {
						message.Key = sarama.StringEncoder(produceArgs.Key)
					}

					message.Value = sarama.ByteEncoder(body)

					partition, offset, err := producer.SendMessage(message)
					if err != nil {
						log.Fatalf("[Error]%v\n", err)
					}
					log.Printf("[sent]topic=%s\tpartition=%d\toffset=%d\n", produceArgs.Topic, partition, offset)
				}
			}

			if err := producer.Close(); err != nil {
				log.Fatalln("Failed to close Kafka producer cleanly:", err)
			}
			q.quitWg.Done()
		}()

	} else {
		asyncproducer, err := sarama.NewAsyncProducer(q.brokers, config)
		if err != nil {
			return fmt.Errorf("Failed to open Kafka producer: %s", err)
		}
		go func() {

			quit := make(chan struct{})
			q.quits = append(q.quits, quit)

			go func() {
				for err := range asyncproducer.Errors() {
					log.Fatalln("Failed to produce message", err)
				}
			}()

		AsyncProduceLoop:
			for {
				select {
				case <-quit:
					break AsyncProduceLoop
				case body := <-ch:
					message := &sarama.ProducerMessage{Topic: produceArgs.Topic, Partition: int32(produceArgs.Partition)}
					if produceArgs.Key != "" {
						message.Key = sarama.StringEncoder(produceArgs.Key)
					}

					message.Value = sarama.ByteEncoder(body)
					asyncproducer.Input() <- message
				}
			}

			if err := asyncproducer.Close(); err != nil {
				log.Fatalln("Failed to close Kafka producer cleanly:", err)
			}
			log.Println("async producer closed.")
			q.quitWg.Done()
		}()
	}

	return nil
}

func (args *KafkaConsumeArgs) getOffset() int64 {
	switch args.Offset {
	case "oldest":
		return sarama.OffsetOldest
	case "newest":
		return sarama.OffsetNewest
	}
	return sarama.OffsetNewest
}

func (args *KafkaConsumeArgs) getPartitions(c sarama.Consumer) ([]int32, error) {
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

func (args *KafkaProduceArgs) getPartitioner() (func(string) sarama.Partitioner, error) {
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
