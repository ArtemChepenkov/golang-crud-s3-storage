FROM golang:1.24-alpine


WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

RUN go build -o metadata-service ./services/metadata-service


ENTRYPOINT ["./metadata-service"]
