package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"time"
	"path/filepath"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	pb "github.com/ArtemChepenkov/golang-crud-s3-storage/services/storage-service/proto"
	"github.com/ArtemChepenkov/golang-crud-s3-storage/pkg/config"
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


				return stream.SendAndClose(&pb.FileUploadResponse{
					Success: true,
					Message: "file uploaded",
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

		if _, err := tmpFile.Write(chunk.ChunkData); err != nil {
			return fmt.Errorf("write chunk error: %v", err)
		}
	}
}

func (s *StorageServer) DownloadFile(req *pb.FileDownloadRequest, stream pb.StorageService_DownloadFileServer) error {
	ctx := context.Background()
	objectName := req.Filename

	obj, err := s.minioClient.GetObject(ctx, s.bucketName, objectName, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("minio getobject error: %w", err)
	}
	defer obj.Close()

	buf := make([]byte, 1024*1024)
	var idx int32 = 0
	for {
		n, rerr := obj.Read(buf)
		if n > 0 {
			ch := &pb.FileChunk{
				UserId:     req.UserId,
				Filename:   objectName,
				ChunkData:  buf[:n],
				ChunkIndex: idx,
				IsLast:     false,
			}
			if rerr == io.EOF {
				ch.IsLast = true
			}
			if err := stream.Send(ch); err != nil {
				return fmt.Errorf("stream send error: %w", err)
			}
			idx++
		}
		if rerr != nil {
			if rerr == io.EOF {
				return nil
			}
			return fmt.Errorf("read object error: %w", rerr)
		}
	}
}

func (s *StorageServer) DeleteFile(ctx context.Context, req *pb.DeleteFileRequest) (*pb.DeleteFileResponse, error) {
	err := s.minioClient.RemoveObject(ctx, s.bucketName, req.Filename, minio.RemoveObjectOptions{})
	if err != nil {
		return &pb.DeleteFileResponse{Success: false, Message: err.Error()}, nil
	}
	return &pb.DeleteFileResponse{Success: true, Message: "deleted"}, nil
}


func (s *StorageServer) ListFiles(ctx context.Context, req *pb.ListFilesRequest) (*pb.ListFilesResponse, error) {
	objectCh := s.minioClient.ListObjects(ctx, s.bucketName, minio.ListObjectsOptions{
		Recursive: true,
	})

	var names []string
	for obj := range objectCh {
		if obj.Err != nil {
			log.Printf("list object error: %v", obj.Err)
			return nil, obj.Err
		}
		names = append(names, obj.Key)
	}

	return &pb.ListFilesResponse{Filenames: names}, nil
}



func main() {
	time.Sleep(10*time.Second)
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
