package storage

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	pb "github.com/ArtemChepenkov/golang-crud-s3-storage/services/distribute-service/proto"
    "github.com/ArtemChepenkov/golang-crud-s3-storage/pkg/config"
	"google.golang.org/grpc"
)


func UploadFileToStorage(
    cfg *config.Config,
    r io.Reader,
    filename string,
    userID string,
) (*pb.FileUploadResponse, error) {

    conn, err := grpc.Dial(cfg.Deps.FileServiceAddr, grpc.WithInsecure())
    if err != nil {
        return nil, fmt.Errorf("failed to connect to storage service: %v\n", err)
    }
    defer conn.Close()

    client := pb.NewStorageServiceClient(conn)

    ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
    defer cancel()

    stream, err := client.UploadFile(ctx)
    if err != nil {
        return nil, fmt.Errorf("cant open stream: %v\n", err)
    }

    buffer := make([]byte, cfg.Service.ChunkSize)
    index := int32(0)

    for {
        n, err := r.Read(buffer)
        if err != nil && err != io.EOF {
            return nil, fmt.Errorf("stream read error: %v\n", err)
        }
        if n == 0 {
            break
        }

        chunk := &pb.FileChunk{
            UserId:     userID,
            Filename:   filename,
            ChunkData:  buffer[:n],
            ChunkIndex: index,
            IsLast:     false,
        }

        if err := stream.Send(chunk); err != nil {
            return nil, fmt.Errorf("cant send chunk: %v\n", err)
        }
        index++
    }

    lastChunk := &pb.FileChunk{
        UserId:   userID,
        Filename: filename,
        IsLast:   true,
    }
    if err := stream.Send(lastChunk); err != nil {
        return nil, fmt.Errorf("cant send last chunk: %v\n", err)
    }

    resp, err := stream.CloseAndRecv()
    if err != nil {
        return nil, fmt.Errorf("response receive error: %v\n", err)
    }

    log.Printf("Download ended: success=%v, msg=%s",
        resp.Success, resp.Message)

    return resp, nil
}
