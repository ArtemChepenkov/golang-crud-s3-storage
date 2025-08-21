package storage

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "github.com/ArtemChepenkov/golang-crud-s3-storage/services/distribute-service/proto"
	"github.com/ArtemChepenkov/golang-crud-s3-storage/pkg/config"
	"google.golang.org/grpc"
)

type Uploader struct {
	client pb.StorageServiceClient
	stream pb.StorageService_UploadFileClient
	ctx    context.Context
	cancel context.CancelFunc
}

func NewUploader(cfg *config.Config) (*Uploader, error) {
	conn, err := grpc.Dial(cfg.Deps.FileServiceAddr, grpc.WithInsecure())
	if err != nil {
		return nil, fmt.Errorf("failed to connect to storage service: %v", err)
	}

	client := pb.NewStorageServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	stream, err := client.UploadFile(ctx)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("cant open stream: %v", err)
	}

	return &Uploader{client: client, stream: stream, ctx: ctx, cancel: cancel}, nil
}

func (u *Uploader) SendChunk(chunk *pb.FileChunk) error {
	return u.stream.Send(chunk)
}

func (u *Uploader) Close() (*pb.FileUploadResponse, error) {
	defer u.cancel()
	resp, err := u.stream.CloseAndRecv()
	if err != nil {
		return nil, fmt.Errorf("response receive error: %v", err)
	}

	log.Printf("Upload finished: success=%v, msg=%s",
		resp.Success, resp.Message)

	return resp, nil
}
