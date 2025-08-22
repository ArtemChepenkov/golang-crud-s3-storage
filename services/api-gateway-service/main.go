package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ArtemChepenkov/golang-crud-s3-storage/pkg/config"
	"github.com/ArtemChepenkov/golang-crud-s3-storage/services/api-gateway-service/gateway"
)

func main() {
	time.Sleep(time.Second * 10)
	cfg := config.LoadConfig("./pkg/config")

	server := gateway.NewServer(cfg)

	server.RegisterRoutes()

	httpServer := &http.Server{
		Addr:    cfg.Service.HTTPListen,
		Handler: nil,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Printf("🚀 API Gateway running on %s", cfg.Service.HTTPListen)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	<-stop
	log.Println("Shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		log.Fatalf("server forced to shutdown: %v", err)
	}

	close(server.WorkerChan)
	server.Wg.Wait()

	log.Println("✅ Shutdown complete")
}
