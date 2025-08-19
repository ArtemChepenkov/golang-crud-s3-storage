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
	// Загружаем конфиг
	cfg := config.LoadConfig("./pkg/config")

	// Создаём сервер
	server := gateway.NewServer(cfg)

	// Регистрируем роуты
	server.RegisterRoutes()

	// HTTP сервер
	httpServer := &http.Server{
		Addr:    cfg.Service.HTTPListen,
		Handler: nil, // используем глобальный DefaultServeMux
	}

	// Канал для graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	// Запуск сервера
	go func() {
		log.Printf("🚀 API Gateway running on %s", cfg.Service.HTTPListen)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	// Ждём сигналов
	<-stop
	log.Println("Shutting down...")

	// Контекст с таймаутом для shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Останавливаем HTTP сервер
	if err := httpServer.Shutdown(ctx); err != nil {
		log.Fatalf("server forced to shutdown: %v", err)
	}

	// Останавливаем продюсеров
	close(server.WorkerChan)
	server.Wg.Wait()

	log.Println("✅ Shutdown complete")
}
