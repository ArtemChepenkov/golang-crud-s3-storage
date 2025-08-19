package gateway

import (
    "io"
    pb "github.com/ArtemChepenkov/golang-crud-s3-storage/services/api-gateway-service/proto"
    "google.golang.org/protobuf/proto"
    "fmt"
    "log"
    "encoding/json"
    "strings"
	"net/http"
	"github.com/ArtemChepenkov/golang-crud-s3-storage/pkg/config"
    "github.com/ArtemChepenkov/golang-crud-s3-storage/services/api-gateway-service/kafka"
    "sync"
    "runtime"
)

type Server struct {
	Cfg             *config.Config
    WorkerChan      chan kafka.ProducerMessage 
    ProducersAmount int
    ShutdownChan    chan struct{}
    Wg              sync.WaitGroup
}

func NewServer(cfg *config.Config) *Server {

    producersAmount := cfg.Kafka.ProducersAmount
    if producersAmount <= 0 {
        producersAmount = runtime.NumCPU()
    }

    server := &Server{
        Cfg:             cfg,
        WorkerChan:      make(chan kafka.ProducerMessage),
        ProducersAmount: producersAmount,
        ShutdownChan:    make(chan struct{}),
    }

    for range producersAmount {
        go kafka.StartProducer(server.WorkerChan, cfg.Kafka.Brokers, cfg.Kafka.Topic)
    }

	return server
}

func generateObjectName(userId,filename string) string { 
    cleanFilename := strings.ReplaceAll(filename, "/", "_")
    return fmt.Sprintf("%s-%s-%s", userId, cleanFilename, userId)

}

func (s *Server) RegisterRoutes() {
	http.HandleFunc(s.Cfg.URLs.Handlers["upload_file"], s.UploadFileHandler)
	http.HandleFunc(s.Cfg.URLs.Handlers["download_file"], s.DownloadFileHandler)
	http.HandleFunc(s.Cfg.URLs.Handlers["list_files"], s.ListFilesHandler)
	http.HandleFunc(s.Cfg.URLs.Handlers["delete_file"], s.DeleteFileHandler)
	http.HandleFunc(s.Cfg.URLs.Handlers["health"], s.HealthHandler)
}

func (s *Server) UploadFileHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if r.Header.Get("Content-Type") != "application/octet-stream" {
		http.Error(w, "Expected Content-Type: application/octet-stream", http.StatusBadRequest)
		return
	}

	filename := r.URL.Query().Get("filename")
	userId := r.URL.Query().Get("userId")

	if filename == "" || userId == "" {
		http.Error(w, "Missing filename or userId parameters", http.StatusBadRequest)
		return
	}

    filename = generateObjectName(userId, filename)

	chunkSize := 1 << 20
    buf := make([]byte, chunkSize)
    var totalBytes int64
    chunkIndex := 0

	for {
        n, err := r.Body.Read(buf)
        if n > 0 {
            totalBytes += int64(n)

            chunk := &pb.FileChunk{
                UserId:    userId,
                Filename:  filename,
                ChunkData: buf[:n],
                ChunkIndex: int32(chunkIndex),
                IsLast:    err == io.EOF,
            }

            data, err := proto.Marshal(chunk)
            if err != nil {
                log.Printf("Protobuf marshal error: %v", err)
                continue
            }

            s.WorkerChan <- kafka.ProducerMessage{userId + "-" + filename, data}
            chunkIndex++
        }

        if err != nil {
            if err != io.EOF {
                log.Printf("Read error: %v", err)
                http.Error(w, "File read error", http.StatusInternalServerError)
            }
            break
        }
    }

	w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]interface{}{
        "status":      "success",
        "filename":    filename,
        "bytesReceived": totalBytes,
    })
	
}

func (s *Server) DownloadFileHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
}

func (s *Server) ListFilesHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
}

func (s *Server) DeleteFileHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
}

func (s *Server) HealthHandler(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}