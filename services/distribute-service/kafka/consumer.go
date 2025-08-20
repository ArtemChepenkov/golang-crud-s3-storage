package kafka

import (
	"fmt"
	"log"
	"sync"
	"time"
	"context"
	"os"
	"os/signal"
	"syscall"
	"github.com/IBM/sarama"
	"google.golang.org/protobuf/proto"

	pb "github.com/ArtemChepenkov/golang-crud-s3-storage/services/distribute-service/proto"
	"github.com/ArtemChepenkov/golang-crud-s3-storage/services/distribute-service/client"
	"github.com/ArtemChepenkov/golang-crud-s3-storage/services/distribute-service/client"
	"github.com/ArtemChepenkov/golang-crud-s3-storage/pkg/config"
)

type FileConsumer struct {
	cfg             *config.Config
	activeUploaders map[string]*client.Uploader
	mu              sync.Mutex
}

func NewFileConsumer(cfg *config.Config) *FileConsumer {
	return &FileConsumer{
		cfg:             cfg,
		activeUploaders: make(map[string]*client.Uploader),
	}
}

// HandleMessage вызывается при получении сообщения из Kafka
func (c *FileConsumer) HandleMessage(msg *sarama.ConsumerMessage) error {
	var chunk pb.FileChunk
	if err := proto.Unmarshal(msg.Value, &chunk); err != nil {
		return fmt.Errorf("protobuf decode error: %v\n", err)
	}

	key := fmt.Sprintf("%s-%s", chunk.UserId, chunk.Filename)

	c.mu.Lock()
	defer c.mu.Unlock()

	// проверяем, есть ли активный стрим
	uploader, exists := c.activeUploaders[key]
	if !exists {
		u, err := client.NewUploader(c.cfg)
		if err != nil {
			return fmt.Errorf("failed to create uploader: %v\n", err)
		}
		c.activeUploaders[key] = u
		uploader = u
		log.Printf("Started upload for %s\n", key)
	}

	// отправляем чанк в gRPC
	if err := uploader.SendChunk(&chunk); err != nil {
		return fmt.Errorf("failed to send chunk: %v\n", err)
	}

	// если последний чанк, то закрываем стрим и очищаем карту
	if chunk.IsLast {
		if _, err := uploader.Close(); err != nil {
			return fmt.Errorf("failed to close uploader: %v\n", err)
		}
		delete(c.activeUploaders, key)
		log.Printf("Completed upload for %s\n", key)
	}

	return nil
}


func main() {
	// Загружаем конфиг
	cfg := config.LoadConfig("./pkg/config")

	// Немного ждем, пока Kafka поднимется (лучше потом health-checkами заменить)
	time.Sleep(5 * time.Second)

	// Инициализация FileConsumer
	fileConsumer := NewFileConsumer(cfg)

	// Настройка Kafka consumer
	kafkaCfg := sarama.NewConfig()
	kafkaCfg.Consumer.Offsets.Initial = sarama.OffsetOldest

	// Создаем consumer group
	consumerGroup, err := sarama.NewConsumerGroup(
		cfg.Kafka.Brokers,
		"1",
		kafkaCfg,
	)
	if err != nil {
		log.Fatalf("Error creating consumer group: %v", err)
	}
	defer consumerGroup.Close()

	handler := consumerHandler{consumer: fileConsumer}

	// Контекст с отменой по сигналу (graceful shutdown)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		for {
			if err := consumerGroup.Consume(ctx, []string{cfg.KafkaTopic}, handler); err != nil {
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
}