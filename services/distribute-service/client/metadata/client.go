package metadata

import (
	"context"
	"log"
	"time"

	pb "github.com/ArtemChepenkov/golang-crud-s3-storage/services/metadata-service/proto"
	"google.golang.org/grpc"
)

type MetadataClient struct {
	client pb.MetadataServiceClient
}

func NewMetadataClient(addr string) *MetadataClient {
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	return &MetadataClient{
		client: pb.NewMetadataServiceClient(conn),
	}
}

func (c *MetadataClient) SaveFileMetadata(userID, original, stored string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := c.client.SaveFileMetadata(ctx, &pb.SaveFileMetadataRequest{
		UserId:           userID,
		OriginalFilename: original,
		StoredFilename:   stored,
	})
	if err != nil {
		log.Printf("SaveFileMetadata error: %v", err)
		return err
	}
	return nil
}
