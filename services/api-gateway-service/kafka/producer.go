package kafka

import (
	"log"
	"github.com/IBM/sarama"
)

type Producer struct {
	SyncProducer sarama.SyncProducer
	Topic 		 string
}

type ProducerMessage struct {
	Key   string
	Data  []byte
}

func NewProducer(brokers []string, topic string) (*Producer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
    config.Producer.Retry.Max = 5
	config.Producer.Partitioner = sarama.NewHashPartitioner
	
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Printf("Error creating producer: %v\n", err)
		return nil, err
	}

	return &Producer{SyncProducer: producer, Topic: topic}, nil
}

func (p *Producer) SendMessage(key string, value []byte) {
	msg := &sarama.ProducerMessage{
		Topic: p.Topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.ByteEncoder(value),
	}
	partition, offset, err := p.SyncProducer.SendMessage(msg)
	if err != nil {
		log.Printf("Error while producer send: %v\n", err);
	}
	log.Printf("Message stored in partition %d, offset %d\n", partition, offset)
}

func (p *Producer) Close() error {
	return p.SyncProducer.Close()
}

func StartProducer(ch chan ProducerMessage, brokers []string, topic string) {
	producer, err := NewProducer(brokers, topic)
	if err != nil {
		log.Printf("Failed to start producer: %v\n", err)
		return
	}
	for {
		msg := <-ch
		producer.SendMessage(msg.Key, msg.Data)
	}
}