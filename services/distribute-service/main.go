package main

import (
	"context"
	"log"
	"time"
	"os"
	"os/signal"
	"syscall"
	"github.com/ArtemChepenkov/golang-crud-s3-storage/services/distribute-service/kafka"
	"github.com/ArtemChepenkov/golang-crud-s3-storage/pkg/config"
	"github.com/IBM/sarama"

)

func main() {
	cfg := config.LoadConfig("./pkg/config")

	time.Sleep(10 * time.Second)

	fileConsumer := kafka.NewFileConsumer(cfg)

	kafkaCfg := sarama.NewConfig()
	kafkaCfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	consumerGroup, err := sarama.NewConsumerGroup(
		cfg.Kafka.Brokers,
		"distribute-service-group",
		kafkaCfg,
	)
	if err != nil {
		log.Fatalf("Error creating consumer group: %v", err)
	}
	defer consumerGroup.Close()

	handler := kafka.ConsumerHandler{Consumer: fileConsumer}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		for {
			if err := consumerGroup.Consume(ctx, []string{cfg.Kafka.Topic}, handler); err != nil {
				log.Printf("Consume error: %v", err)
				time.Sleep(5 * time.Second)
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	<-sigchan
	log.Println("Shutting down consumer...")
	cancel()
	
	time.Sleep(1 * time.Second)
}