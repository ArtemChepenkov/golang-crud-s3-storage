FROM golang:1.24-alpine

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o distribute-service ./services/distribute-service

RUN chmod +x distribute-service

RUN mkdir -p /data/storage

ENTRYPOINT ["./distribute-service"]