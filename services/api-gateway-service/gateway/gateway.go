package gateway

import (
    "io"
    pb "github.com/ArtemChepenkov/golang-crud-s3-storage/services/storage-service/proto"
    "google.golang.org/protobuf/proto"
    "fmt"
    "time"
    "context"
    "log"
    "encoding/json"
    "strings"
	"net/http"
    "google.golang.org/grpc"
	"github.com/ArtemChepenkov/golang-crud-s3-storage/pkg/config"
    "github.com/ArtemChepenkov/golang-crud-s3-storage/services/api-gateway-service/kafka"
    "github.com/ArtemChepenkov/golang-crud-s3-storage/services/api-gateway-service/client"
    pbMeta "github.com/ArtemChepenkov/golang-crud-s3-storage/services/metadata-service/proto"
    
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

func newMetadataClient(addr string, timeout time.Duration) (pbMeta.MetadataServiceClient, *grpc.ClientConn, error) {
    ctx, cancel := context.WithTimeout(context.Background(), timeout)
    defer cancel()
    conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithBlock())
    if err != nil {
        return nil, nil, err
    }
    return pbMeta.NewMetadataServiceClient(conn), conn, nil
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

    userId := r.URL.Query().Get("userId")
    origFilename := r.URL.Query().Get("filename")
    if userId == "" || origFilename == "" {
        http.Error(w, "missing userId or filename", http.StatusBadRequest)
        return
    }

    metaClient, metaConn, err := newMetadataClient(s.Cfg.Deps.MetadataServiceAddr, 5*time.Second)
    if err != nil {
        log.Printf("metadata dial error: %v", err)
        http.Error(w, "metadata service unavailable", http.StatusInternalServerError)
        return
    }
    defer metaConn.Close()

    metaResp, err := metaClient.GetFileMetadata(context.Background(), &pbMeta.GetFileMetadataRequest{
        UserId:           userId,
        OriginalFilename: origFilename,
    })
    if err != nil {
        log.Printf("metadata get error: %v", err)
        http.Error(w, "metadata lookup failed", http.StatusInternalServerError)
        return
    }
    if !metaResp.Found {
        http.Error(w, "file not found", http.StatusNotFound)
        return
    }
    storedName := metaResp.StoredFilename

    w.Header().Set("Content-Type", "application/octet-stream")
    w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", origFilename))
    w.WriteHeader(http.StatusOK)

    if err := client.DownloadToWriter(s.Cfg.Deps.FileServiceAddr, userId, storedName, w); err != nil {
        log.Printf("download failed: %v", err)
    }
}

func (s *Server) ListFilesHandler(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodGet {
        http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
        return
    }

    userId := r.URL.Query().Get("userId")
    if userId == "" {
        http.Error(w, "missing userId", http.StatusBadRequest)
        return
    }

    metaClient, metaConn, err := newMetadataClient(s.Cfg.Deps.MetadataServiceAddr, 5*time.Second)
    if err != nil {
        log.Printf("metadata dial error: %v", err)
        http.Error(w, "metadata service unavailable", http.StatusInternalServerError)
        return
    }
    defer metaConn.Close()

    resp, err := metaClient.ListUserFiles(context.Background(), &pbMeta.ListUserFilesRequest{
        UserId: userId,
    })
    if err != nil {
        log.Printf("metadata list error: %v", err)
        http.Error(w, "failed to list files", http.StatusInternalServerError)
        return
    }

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]interface{}{
        "filenames": resp.OriginalFilenames,
    })
}



func (s *Server) DeleteFileHandler(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodDelete {
        http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
        return
    }

    userId := r.URL.Query().Get("userId")
    origFilename := r.URL.Query().Get("filename")
    if userId == "" || origFilename == "" {
        http.Error(w, "missing userId or filename", http.StatusBadRequest)
        return
    }

    metaClient, metaConn, err := newMetadataClient(s.Cfg.Deps.MetadataServiceAddr, 5*time.Second)
    if err != nil {
        log.Printf("metadata dial error: %v", err)
        http.Error(w, "metadata service unavailable", http.StatusInternalServerError)
        return
    }
    defer metaConn.Close()

    metaResp, err := metaClient.GetFileMetadata(context.Background(), &pbMeta.GetFileMetadataRequest{
        UserId:           userId,
        OriginalFilename: origFilename,
    })
    if err != nil {
        log.Printf("metadata get error: %v", err)
        http.Error(w, "metadata lookup failed", http.StatusInternalServerError)
        return
    }
    if !metaResp.Found {
        http.Error(w, "file not found", http.StatusNotFound)
        return
    }
    storedName := metaResp.StoredFilename
    if storedName == "" {
        log.Printf("metadata returned empty stored filename for delete: %s/%s", userId, origFilename)
        http.Error(w, "file not found", http.StatusNotFound)
        return
    }


    if err := client.DeleteFile(s.Cfg.Deps.FileServiceAddr, userId, storedName); err != nil {
        log.Printf("storage delete error: %v", err)
        http.Error(w, "failed to delete from storage", http.StatusInternalServerError)
        return
    }

    delMetaResp, err := metaClient.DeleteFileMetadata(context.Background(), &pbMeta.DeleteFileMetadataRequest{
        UserId:           userId,
        OriginalFilename: origFilename,
    })
    if err != nil {
        log.Printf("metadata delete error: %v", err)
        http.Error(w, "failed to delete metadata", http.StatusInternalServerError)
        return
    }
    if !delMetaResp.Success {
        log.Printf("metadata delete failed: %s", delMetaResp.Message)
        http.Error(w, "failed to delete metadata", http.StatusInternalServerError)
        return
    }

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(map[string]bool{"success": true})
}



func (s *Server) HealthHandler(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}