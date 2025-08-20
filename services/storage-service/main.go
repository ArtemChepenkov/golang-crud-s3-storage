package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	pb "github.com/ArtemChepenkov/golang-crud-s3-storage/services/storage-service/proto"
	pb "github.com/ArtemChepenkov/golang-crud-s3-storage/pkg/config"
	"google.golang.org/grpc"
)

type StorageServer struct {
	pb.UnimplementedStorageServiceServer
	minioClient *minio.Client
	bucketName  string
}

func NewStorageServer(endpoint, accessKey, secretKey, bucketName string, useSSL bool) (*StorageServer, error) {
	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to init minio client: %v", err)
	}

	// создаём бакет, если нет
	ctx := context.Background()
	exists, err := minioClient.BucketExists(ctx, bucketName)
	if err != nil {
		return nil, fmt.Errorf("failed to check bucket: %v", err)
	}
	if !exists {
		if err := minioClient.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{}); err != nil {
			return nil, fmt.Errorf("failed to create bucket: %v", err)
		}
		log.Printf("Bucket %s created", bucketName)
	}

	return &StorageServer{
		minioClient: minioClient,
		bucketName:  bucketName,
	}, nil
}

func (s *StorageServer) UploadFile(stream pb.StorageService_UploadFileServer) error {
	var (
		tmpFile   *os.File
		filePath  string
		userID    string
		finalName string
	)

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			// закрываем и отправляем в minio
			if tmpFile != nil {
				tmpFile.Close()
				objectName := filepath.Base(finalName)

				_, err := s.minioClient.FPutObject(
					context.Background(),
					s.bucketName,
					objectName,
					filePath,
					minio.PutObjectOptions{},
				)
				if err != nil {
					return stream.SendAndClose(&pb.FileUploadResponse{
						Success: false,
						Message: fmt.Sprintf("upload error: %v", err),
					})
				}

				fileURL := fmt.Sprintf("http://%s/%s/%s", s.minioClient.EndpointURL().Host, s.bucketName, objectName)

				return stream.SendAndClose(&pb.FileUploadResponse{
					Success: true,
					Message: "file uploaded",
					FileUrl: fileURL,
				})
			}
			return stream.SendAndClose(&pb.FileUploadResponse{
				Success: false,
				Message: "empty upload",
			})
		}
		if err != nil {
			return fmt.Errorf("recv error: %v", err)
		}

		// инициализация временного файла при первом чанке
		if tmpFile == nil {
			userID = chunk.UserId
			finalName = chunk.Filename
			tmpDir := filepath.Join(os.TempDir(), "uploads", userID)
			if err := os.MkdirAll(tmpDir, 0755); err != nil {
				return fmt.Errorf("mkdir error: %v", err)
			}
			filePath = filepath.Join(tmpDir, filepath.Base(finalName))

			tmpFile, err = os.Create(filePath)
			if err != nil {
				return fmt.Errorf("create temp file error: %v", err)
			}
		}

		// пишем чанк в файл
		if _, err := tmpFile.Write(chunk.ChunkData); err != nil {
			return fmt.Errorf("write chunk error: %v", err)
		}
	}
}

func main() {
	cfg := config.LoadConfig("./pkg/config")
	endpoint := cfg.Minio.Endpoint
	accessKey := cfg.Minio.AccessKey
	secretKey := cfg.Minio.SecretKey
	bucketName := "files"
	useSSL := false

	storageServer, err := NewStorageServer(endpoint, accessKey, secretKey, bucketName, useSSL)
	if err != nil {
		log.Fatalf("failed to init storage server: %v", err)
	}

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterStorageServiceServer(grpcServer, storageServer)

	log.Println("Storage gRPC server started on :50051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
