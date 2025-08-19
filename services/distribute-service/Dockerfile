FROM golang:1.24-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o distribute-service ./services/distribute-service/kafka

FROM alpine:latest
WORKDIR /app

COPY --from=builder /app/distribute-service .
COPY --from=builder /app/pkg/config/config.yaml ./config.yaml

RUN mkdir -p /data/storage
CMD ["./distribute-service"]