package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	pb "github.com/ArtemChepenkov/golang-crud-s3-storage/services/distribute-service/proto"
	"github.com/IBM/sarama"
	"google.golang.org/protobuf/proto"
)

// FileChunk - структура сообщения (должна совпадать с продюсером)
type FileChunk struct {
	UserId    string
	Filename  string
	ChunkData []byte
	ChunkIndex int32
	IsLast    bool
}

// FileConsumer - собирает файлы из чанков
type FileConsumer struct {
	storageDir  string
	activeFiles map[string]*os.File // Ключ: userId-filename
	mu          sync.Mutex
}

func NewConsumer(storageDir string) *FileConsumer {
	if err := os.MkdirAll(storageDir, 0755); err != nil {
		log.Fatalf("Failed to create storage dir: %v", err)
	}
	return &FileConsumer{
		storageDir:  storageDir,
		activeFiles: make(map[string]*os.File),
	}
}

func (c *FileConsumer) HandleMessage(msg *sarama.ConsumerMessage) error {
	var chunk pb.FileChunk
	if err := proto.Unmarshal(msg.Value, &chunk); err != nil {
		return fmt.Errorf("protobuf decode error: %v", err)
	}

	key := fmt.Sprintf("%s-%s", chunk.UserId, chunk.Filename)
	filePath := filepath.Join(c.storageDir, chunk.UserId, chunk.Filename)

	c.mu.Lock()
	defer c.mu.Unlock()

	// Открываем или создаем файл
	file, exists := c.activeFiles[key]
	if !exists {
		if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
			return fmt.Errorf("create dir error: %v", err)
		}
		f, err := os.Create(filePath)
		if err != nil {
			return fmt.Errorf("file create error: %v", err)
		}
		c.activeFiles[key] = f
		file = f
		log.Printf("Started new file: %s", filePath)
	}

	// Записываем чанк
	if _, err := file.Write(chunk.ChunkData); err != nil {
		return fmt.Errorf("write error: %v", err)
	}

	// Если последний чанк - закрываем файл
	if chunk.IsLast {
		if err := file.Close(); err != nil {
			return fmt.Errorf("file close error: %v", err)
		}
		delete(c.activeFiles, key)
		log.Printf("Completed file: %s (%d bytes)", filePath, chunk.ChunkIndex+1)
	}

	return nil
}

func main() {
	// Конфигурация
	time.Sleep(10*time.Second)
	kafkaBrokers := []string{"kafka:9092"} // Укажите свои брокеры
	topic := "object.events"                    // Укажите свой топик
	storageDir := "./storage"                  // Директория для сохранения

	// Инициализация консьюмера
	consumer := NewConsumer(storageDir)

	// Настройка Kafka consumer
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = sarama.OffsetOldest // Читать с начала

	// Создаем consumer group
	consumerGroup, err := sarama.NewConsumerGroup(kafkaBrokers, "file-consumer-group", config)
	if err != nil {
		log.Fatalf("Error creating consumer group: %v", err)
	}
	defer consumerGroup.Close()

	// Обработчик для Sarama
	handler := consumerHandler{consumer}

	// Запускаем бесконечный цикл потребления
	ctx := context.Background()
	for {
		if err := consumerGroup.Consume(ctx, []string{topic}, handler); err != nil {
			log.Printf("Consume error: %v", err)
			time.Sleep(5 * time.Second)
		}
	}
}

// Реализация интерфейса sarama.ConsumerGroupHandler
type consumerHandler struct {
	*FileConsumer
}

func (h consumerHandler) Setup(sarama.ConsumerGroupSession) error {
	log.Println("Consumer is starting...")
	return nil
}

func (h consumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("Consumer is shutting down...")
	return nil
}

func (h consumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if err := h.HandleMessage(msg); err != nil {
			log.Printf("Failed to process message (offset %d): %v", msg.Offset, err)
			continue
		}
		session.MarkMessage(msg, "")
	}
	return nil
}