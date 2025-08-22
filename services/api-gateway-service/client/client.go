package client

import (
	"context"
	"fmt"
	"io"
	"time"

	pb "github.com/ArtemChepenkov/golang-crud-s3-storage/services/storage-service/proto"
	pb_metadata "github.com/ArtemChepenkov/golang-crud-s3-storage/services/metadata-service/proto"
	"github.com/ArtemChepenkov/golang-crud-s3-storage/pkg/config"
	"google.golang.org/grpc"
)


func NewConn(addr string, timeout time.Duration) (*grpc.ClientConn, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func DownloadToWriter(addr, userID, storedFilename string, w io.Writer) error {
	conn, err := NewConn(addr, time.Minute * 10)
	if err != nil {
		return fmt.Errorf("dial error: %w", err)
	}
	defer conn.Close()

	client := pb.NewStorageServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	stream, err := client.DownloadFile(ctx, &pb.FileDownloadRequest{
		UserId:   userID,
		Filename: storedFilename,
	})
	if err != nil {
		return fmt.Errorf("download rpc error: %w", err)
	}

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("recv error: %w", err)
		}
		if _, werr := w.Write(chunk.ChunkData); werr != nil {
			return fmt.Errorf("write error: %w", werr)
		}
	}
}

func DeleteFile(addr, userID, storedFilename string) error {
	conn, err := NewConn(addr, time.Minute * 10)
	if err != nil {
		return fmt.Errorf("dial error: %w", err)
	}
	defer conn.Close()

	client := pb.NewStorageServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := client.DeleteFile(ctx, &pb.DeleteFileRequest{
		UserId:   userID,
		Filename: storedFilename,
	})
	if err != nil {
		return fmt.Errorf("delete rpc error: %w", err)
	}
	if !resp.Success {
		return fmt.Errorf("delete failed: %s", resp.Message)
	}
	return nil
}

func ListFiles(cfg config.Config, userID string) ([]string, error) {
	conn, err := NewConn(cfg.Deps.MetadataServiceAddr, time.Minute*1)
	if err != nil {
		return nil, fmt.Errorf("dial error: %w", err)
	}
	defer conn.Close()

	client := pb_metadata.NewMetadataServiceClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := client.ListUserFiles(ctx, &pb_metadata.ListUserFilesRequest{UserId: userID})
	if err != nil {
		return nil, fmt.Errorf("list rpc error: %w", err)
	}

	return resp.OriginalFilenames, nil
}

