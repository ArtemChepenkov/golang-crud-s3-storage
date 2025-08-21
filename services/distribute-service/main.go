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
	// Загружаем конфиг
	cfg := config.LoadConfig("./pkg/config")

	// Немного ждем, пока Kafka поднимется (лучше потом health-checkами заменить)
	time.Sleep(10 * time.Second)

	// Инициализация FileConsumer
	fileConsumer := kafka.NewFileConsumer(cfg)

	// Настройка Kafka consumer
	kafkaCfg := sarama.NewConfig()
	kafkaCfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	// Создаем consumer group
	consumerGroup, err := sarama.NewConsumerGroup(
		cfg.Kafka.Brokers,
		"distribute-service-group", // Более осмысленное имя группы
		kafkaCfg,
	)
	if err != nil {
		log.Fatalf("Error creating consumer group: %v", err)
	}
	defer consumerGroup.Close()

	handler := kafka.ConsumerHandler{Consumer: fileConsumer}

	// Контекст с отменой по сигналу (graceful shutdown)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Запускаем потребление в отдельной горутине
	go func() {
		for {
			if err := consumerGroup.Consume(ctx, []string{cfg.Kafka.Topic}, handler); err != nil {
				log.Printf("Consume error: %v", err)
				time.Sleep(5 * time.Second)
			}
			// если контекст отменён — выходим
			if ctx.Err() != nil {
				return
			}
		}
	}()

	// Ждём SIGINT/SIGTERM
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	<-sigchan
	log.Println("Shutting down consumer...")
	cancel()
	
	// Даем время на graceful shutdown
	time.Sleep(1 * time.Second)
}