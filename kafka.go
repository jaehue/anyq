package qlib

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"strconv"
	"strings"
	"sync"
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
	Verbose    bool
	BufferSize int
}

type KafkaProduceArgs struct {
	Topic       string `validate:"nonzero"`
	Key         string
	Partitioner string `validate:"regexp=^(hash|manual|random)$"` // The partitioning scheme to usep
	Partition   int    // The partition to produce to
	Verbose     bool
	Silent      bool
	Sync        bool
}

func (q *Kafka) Setup(url string) error {
	q.brokers = strings.Split(url, ",")
	q.quit = make(chan struct{})
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

func (q *Kafka) BindRecvChan(ch chan<- []byte, args interface{}) error {
	consumeArgs, ok := args.(KafkaConsumeArgs)
	if !ok {
		return fmt.Errorf("invalid consume arguments(%v)", args)
	}

	c, err := sarama.NewConsumer(q.brokers, sarama.NewConfig())
	if err != nil {
		return err
	}

	partitions, err := consumeArgs.getPartitions(c)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup

	for _, p := range partitions {
		pc, err := c.ConsumePartition(consumeArgs.Topic, p, consumeArgs.getOffset())
		if err != nil {
			return err
		}

		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()

			quit := make(chan struct{})
			q.quits = append(q.quits, quit)

		consumeLoop:
			for {
				select {
				case err := <-pc.Errors():
					log.Println(err)
				case m := <-pc.Messages():
					ch <- m.Value
				case <-quit:
					pc.Close()
					break consumeLoop
				}
			}
		}(pc)
	}

	go func() {
		wg.Wait()
		close(ch)
		if err := c.Close(); err != nil {
			log.Fatalln("Failed to close consumer: ", err)
			return
		}
		log.Println("consumer closed.")
		q.quitWg.Done()
	}()
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
