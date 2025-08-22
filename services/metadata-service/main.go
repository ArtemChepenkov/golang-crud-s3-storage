package main

import (
	"fmt"
	"log"
	"net"
	"time"
	"github.com/ArtemChepenkov/golang-crud-s3-storage/pkg/config"
	"google.golang.org/grpc"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	pb "github.com/ArtemChepenkov/golang-crud-s3-storage/services/metadata-service/proto"
	metadataServer "github.com/ArtemChepenkov/golang-crud-s3-storage/services/metadata-service/server"
)

func main() {
	time.Sleep(10*time.Second)
	cfg := config.LoadConfig("./pkg/config")

	dsn := fmt.Sprintf(
		"host=%s user=%s password=%s dbname=%s port=%d sslmode=%s",
		cfg.Postgres.Host,
		cfg.Postgres.User,
		cfg.Postgres.Password,
		cfg.Postgres.DB,
		cfg.Postgres.Port,
		cfg.Postgres.SSLMode,
	)
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("failed to connect db: %v", err)
	}
	db.AutoMigrate(&metadataServer.FileMetadata{})

	addr := cfg.Deps.MetadataServiceAddr
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterMetadataServiceServer(grpcServer, metadataServer.NewMetadataServer(db))

	log.Printf("Metadata gRPC server started on %s", addr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
