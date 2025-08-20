package main

import (
	"context"
	"log"
	"time"

	"github.com/ArtemChepenkov/golang-crud-s3-storage/services/distribute-service/kafka"
	"github.com/ArtemChepenkov/golang-crud-s3-storage/pkg/config"
	"github.com/IBM/sarama"

)

func main() {
	time.Sleep(10 * time.Second) // пока Kafka поднимется

	cfg := config.LoadConfig("./pkg/config")

	fileConsumer := kafka.NewFileConsumer(cfg.StorageDir)

	kafkaCfg := sarama.NewConfig()
	kafkaCfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumerGroup, err := sarama.NewConsumerGroup(cfg.KafkaBrokers, cfg.GroupID, kafkaCfg)
	if err != nil {
		log.Fatalf("Error creating consumer group: %v", err)
	}
	defer consumerGroup.Close()

	handler := consumer.ConsumerHandler{FileConsumer: fileConsumer}

	ctx := context.Background()
	for {
		if err := consumerGroup.Consume(ctx, []string{cfg.Topic}, handler); err != nil {
			log.Printf("Consume error: %v", err)
			time.Sleep(5 * time.Second)
		}
	}
}
