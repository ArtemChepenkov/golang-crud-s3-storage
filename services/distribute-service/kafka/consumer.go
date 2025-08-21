package kafka

import (
	_ "context"
	"strings"
	"fmt"
	"log"
	_ "os"
	_ "os/signal"
	"sync"
	_ "syscall"
	_ "time"
	_ "context"
	"github.com/IBM/sarama"
	"google.golang.org/protobuf/proto"

	pb "github.com/ArtemChepenkov/golang-crud-s3-storage/services/distribute-service/proto"
	//pb_metadata "github.com/ArtemChepenkov/golang-crud-s3-storage/services/metadata-service/proto"
	"github.com/ArtemChepenkov/golang-crud-s3-storage/services/distribute-service/client/storage"
	"github.com/ArtemChepenkov/golang-crud-s3-storage/services/distribute-service/client/metadata"
	"github.com/ArtemChepenkov/golang-crud-s3-storage/pkg/config"
)

type FileConsumer struct {
	cfg             *config.Config
	activeUploaders map[string]*storage.Uploader
	mu              sync.Mutex
}

func NewFileConsumer(cfg *config.Config) *FileConsumer {
	return &FileConsumer{
		cfg:             cfg,
		activeUploaders: make(map[string]*storage.Uploader),
	}
}

// HandleMessage вызывается при получении сообщения из Kafka
func (c *FileConsumer) HandleMessage(msg *sarama.ConsumerMessage) error {
	var chunk pb.FileChunk
	if err := proto.Unmarshal(msg.Value, &chunk); err != nil {
		return fmt.Errorf("protobuf decode error: %v", err)
	}

	key := fmt.Sprintf("%s-%s", chunk.UserId, chunk.Filename)

	c.mu.Lock()
	defer c.mu.Unlock()

	// проверяем, есть ли активный стрим
	uploader, exists := c.activeUploaders[key]
	if !exists {
		u, err := storage.NewUploader(c.cfg)
		if err != nil {
			return fmt.Errorf("failed to create uploader: %v", err)
		}
		c.activeUploaders[key] = u
		uploader = u
		log.Printf("Started upload for %s\n", key)
	}

	// отправляем чанк в gRPC (MinIO сервис)
	if err := uploader.SendChunk(&chunk); err != nil {
		return fmt.Errorf("failed to send chunk: %v", err)
	}

	// если последний чанк, то закрываем стрим и очищаем карту
	if chunk.IsLast {
		resp, err := uploader.Close()
		if err != nil {
			return fmt.Errorf("failed to close uploader: %v", err)
		}
		delete(c.activeUploaders, key)
		log.Printf("Completed upload for %s, stored as: %s\n", key, resp.Message)

		// вызов Metadata Client
		metaClient := metadata.NewMetadataClient(c.cfg.Deps.MetadataServiceAddr)

		err = metaClient.SaveFileMetadata(chunk.UserId, ExtractFilename(chunk.Filename), chunk.Filename)
		if err != nil {
			return fmt.Errorf("failed to save metadata: %v", err)
		}
		log.Printf("Saved metadata for %s\n", key)
	}

	return nil
}


// consumerHandler реализует интерфейс sarama.ConsumerGroupHandler
type ConsumerHandler struct {
	Consumer *FileConsumer
}

// Setup вызывается перед началом потребления
func (h ConsumerHandler) Setup(sarama.ConsumerGroupSession) error {
	log.Println("Consumer group setup")
	return nil
}

// Cleanup вызывается после завершения потребления
func (h ConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("Consumer group cleanup")
	return nil
}

// ConsumeClaim - основной метод обработки сообщений
func (h ConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		log.Printf("Received message: Topic=%s, Partition=%d, Offset=%d",
			message.Topic, message.Partition, message.Offset)

		// Обрабатываем сообщение
		if err := h.Consumer.HandleMessage(message); err != nil {
			log.Printf("Error processing message: %v", err)
			// Здесь можно добавить логику повторной обработки или dead letter queue
		}

		// Подтверждаем обработку сообщения
		session.MarkMessage(message, "")
	}
	return nil
}


func ExtractFilename(s string) string {
    firstDash := strings.Index(s, "-")
    lastDash := strings.LastIndex(s, "-")
    if firstDash == -1 || lastDash == -1 || firstDash == lastDash {
        return ""
    }
    return s[firstDash+1 : lastDash]
}