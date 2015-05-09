package qlib

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
)

func init() {
	Register("kafka", &Kafka{})
}

type Kafka struct {
	brokers []string // The comma separated list of brokers in the Kafka cluster
	*sarama.Config
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
	q.Config = sarama.NewConfig()
	return nil
}

func (q *Kafka) BindRecvChan(ch chan<- []byte, args interface{}) error {
	consumeArgs, ok := args.(KafkaConsumeArgs)
	if !ok {
		return fmt.Errorf("invalid consume arguments(%v)", args)
	}

	c, err := sarama.NewConsumer(q.brokers, q.Config)
	if err != nil {
		return err
	}
	partitions, err := consumeArgs.getPartitions(c)
	if err != nil {
		return err
	}
	for _, p := range partitions {
		pc, err := c.ConsumePartition(consumeArgs.Topic, p, consumeArgs.getOffset())
		if err != nil {
			return err
		}

		go func() {
			signals := make(chan os.Signal, 1)
			signal.Notify(signals, os.Interrupt)

		consumerLoop:
			for {
				select {
				case err := <-pc.Errors():
					log.Println(err)
				case m := <-pc.Messages():
					ch <- m.Value
				case <-signals:
					log.Println("Received interrupt")
					os.Exit(1)
					break consumerLoop
				}
			}
		}()
	}
	return nil

}

func (q *Kafka) BindSendChan(ch <-chan []byte, args interface{}) error {
	produceArgs, ok := args.(KafkaProduceArgs)
	if !ok {
		return fmt.Errorf("invalid consume arguments(%v)", args)
	}

	// set partitioner
	if partitioner, err := produceArgs.getPartitioner(); err == nil {
		q.Config.Producer.Partitioner = partitioner
	}

	if produceArgs.Sync {
		q.Config.Producer.RequiredAcks = sarama.WaitForAll
		producer, err := sarama.NewSyncProducer(q.brokers, q.Config)
		if err != nil {
			return fmt.Errorf("Failed to open Kafka producer: %s", err)
		}
		// defer func() {
		// 	if err := producer.Close(); err != nil {
		// 		log.Println("Failed to close Kafka producer cleanly:", err)
		// 	}
		// }()
		go func() {
			for body := range ch {
				// make producer message
				message := &sarama.ProducerMessage{Topic: produceArgs.Topic, Partition: int32(produceArgs.Partition)}
				if produceArgs.Key != "" {
					message.Key = sarama.StringEncoder(produceArgs.Key)
				}

				message.Value = sarama.ByteEncoder(body)
				_, _, err = producer.SendMessage(message)
				if err != nil {
					log.Printf("[Error]%v\n", err)
				}
				log.Println("send message: ", string(body))
			}
		}()

	} else {
		asyncproducer, err := sarama.NewAsyncProducer(q.brokers, q.Config)
		if err != nil {
			return fmt.Errorf("Failed to open Kafka producer: %s", err)
		}
		go func() {

			signals := make(chan os.Signal, 1)
			signal.Notify(signals, os.Interrupt)

		ProducerLoop:
			for body := range ch {
				// make producer message
				message := &sarama.ProducerMessage{Topic: produceArgs.Topic, Partition: int32(produceArgs.Partition)}
				if produceArgs.Key != "" {
					message.Key = sarama.StringEncoder(produceArgs.Key)
				}

				message.Value = sarama.ByteEncoder(body)

				select {
				case asyncproducer.Input() <- message:
				case err := <-asyncproducer.Errors():
					log.Println("Failed to produce message", err)
				case <-signals:
					break ProducerLoop
				}
			}
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
