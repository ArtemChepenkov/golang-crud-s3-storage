package server

import (
	"context"

	pb "github.com/ArtemChepenkov/golang-crud-s3-storage/services/metadata-service/proto"
	"gorm.io/gorm"
)

type FileMetadata struct {
	ID              uint   `gorm:"primaryKey"`
	UserID          string `gorm:"index"`
	OriginalFilename string
	StoredFilename   string
}

type metadataServer struct {
	pb.UnimplementedMetadataServiceServer
	db *gorm.DB
}

func NewMetadataServer(db *gorm.DB) *metadataServer {
	return &metadataServer{db: db}
}

func (s *metadataServer) SaveFileMetadata(ctx context.Context, req *pb.SaveFileMetadataRequest) (*pb.SaveFileMetadataResponse, error) {
	meta := FileMetadata{
		UserID:          req.UserId,
		OriginalFilename: req.OriginalFilename,
		StoredFilename:   req.StoredFilename,
	}
	if err := s.db.Create(&meta).Error; err != nil {
		return &pb.SaveFileMetadataResponse{Success: false, Message: err.Error()}, nil
	}
	return &pb.SaveFileMetadataResponse{Success: true, Message: "Saved"}, nil
}

func (s *metadataServer) GetFileMetadata(ctx context.Context, req *pb.GetFileMetadataRequest) (*pb.GetFileMetadataResponse, error) {
	var meta FileMetadata
	if err := s.db.Where("user_id = ? AND original_filename = ?", req.UserId, req.OriginalFilename).First(&meta).Error; err != nil {
		return &pb.GetFileMetadataResponse{Found: false}, nil
	}
	return &pb.GetFileMetadataResponse{StoredFilename: meta.StoredFilename, Found: true}, nil
}

func (s *metadataServer) ListUserFiles(ctx context.Context, req *pb.ListUserFilesRequest) (*pb.ListUserFilesResponse, error) {
	var metas []FileMetadata
	if err := s.db.Where("user_id = ?", req.UserId).Find(&metas).Error; err != nil {
		return &pb.ListUserFilesResponse{}, err
	}
	files := make([]string, len(metas))
	for i, m := range metas {
		files[i] = m.OriginalFilename
	}
	return &pb.ListUserFilesResponse{OriginalFilenames: files}, nil
}

func (s *metadataServer) DeleteFileMetadata(ctx context.Context, req *pb.DeleteFileMetadataRequest) (*pb.DeleteFileMetadataResponse, error) {
	if err := s.db.Where("user_id = ? AND original_filename = ?", req.UserId, req.OriginalFilename).Delete(&FileMetadata{}).Error; err != nil {
		return &pb.DeleteFileMetadataResponse{Success: false, Message: err.Error()}, nil
	}
	return &pb.DeleteFileMetadataResponse{Success: true, Message: "Deleted"}, nil
}

