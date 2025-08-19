package client

import (
	"log"
	pb "github.com/ArtemChepenkov/golang-crud-s3-storage/services/api-gateway-service/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type StorageServiceClient struct {
	conn   *grpc.ClientConn
	client pb.StorageServiceClient
}

func New(addr string) (*StorageServiceClient, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("Storage service client error: %v\n", err)
		return nil, err
	}

	return &StorageServiceClient{
		conn:   conn,
		client: pb.NewStorageServiceClient(conn),
	}, nil
}

func (c *StorageServiceClient) UploadStream(ctx context.Context,
	userId string,
	filename string,
	reader io.Reader,
	chunkSize int) (*pb.FileUploadResponse, error) {
	stream, err := c.client.UploadFile(ctx)
	if err != nil {
		log.Printf("Client UploadFile error: %v\n", err)
		return nil, err
	}

	buf := make([]byte, chunkSize)
	chunkIndex := 0

	for {
		n, err := reader.Read(buf)
		if n > 0 {
			if sendErr := stream.Send(&pb.FileChunk{
				UserId:    userId,
				Filename:  filename,
				ChunkData: buf[:n],
				ChunkIndex: int32(chunkIndex),
				IsLast:    err == io.EOF,
			}); sendErr != nil {
				return nil, sendErr
			}
			chunkIndex++
		}

		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
	}

	return stream.CloseAndRecv()
}

func (c *StorageServiceClient) DownloadFile(ctx context.Context, userId, filename string) ([]byte, error) {
	stream, err := c.client.DownloadFile(ctx, &pb.FileDownloadRequest{
		UserId:   userId,
		Filename: filename,
	})
	if err != nil {
		return nil, err
	}

	var fileContent []byte
	for {
		chunk, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		fileContent = append(fileContent, chunk.ChunkData...)
	}

	return fileContent, nil
}

func (c *StorageServiceClient) Close() error {
	return c.conn.Close()
}