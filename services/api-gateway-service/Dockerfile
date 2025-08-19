FROM golang:1.24-alpine

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o api-gateway ./services/api-gateway-service

EXPOSE 8080
ENTRYPOINT ["./api-gateway"]
