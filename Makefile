generate:
	protoc --go_out=./services/file-service/proto --go_opt=paths=source_relative \
	--go-grpc_out=./services/file-service/proto --go-grpc_opt=paths=source_relative \
	-I=./api/protobuf storage.proto

	protoc --go_out=./services/distribute-service/proto --go_opt=paths=source_relative \
	--go-grpc_out=./services/distribute-service/proto --go-grpc_opt=paths=source_relative \
	-I=./api/protobuf storage.proto