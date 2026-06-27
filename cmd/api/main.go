package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	httpSwagger "github.com/swaggo/http-swagger/v2"
	_ "github.com/xnxq1/go-kafka-test/docs"
	config_module "github.com/xnxq1/go-kafka-test/internal/config"
	http_server "github.com/xnxq1/go-kafka-test/internal/http-server"
	messages_http "github.com/xnxq1/go-kafka-test/internal/http-server/messages"
	"github.com/xnxq1/go-kafka-test/internal/infra/postgres"
	logic "github.com/xnxq1/go-kafka-test/internal/logic/messages"
)

// @title           Go Kafka Test API
// @version         1.0
// @description     API сервиса сообщений с transactional outbox.
// @host            localhost:8080
// @BasePath        /
func main() {
	if err := run(); err != nil {
		slog.Error("приложение завершилось с ошибкой", "err", err)
		os.Exit(1)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	config, err := config_module.LoadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	initCtx, initCancel := context.WithTimeout(ctx, 5*time.Second)
	defer initCancel()
	dbPool, err := postgres.NewPool(initCtx, config.DatabaseURL)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	defer dbPool.Close()

	transactor := postgres.NewTransactor(dbPool)
	messageRepo := postgres.NewMessageRepo(dbPool)
	outboxMessageRepo := postgres.NewMessageOutboxRepo(dbPool)
	messageService := logic.NewMessageService(transactor, messageRepo, outboxMessageRepo, config)
	messageHandler := messages_http.NewMessageHandler(messageService)

	router := chi.NewRouter()
	router.Use(http_server.ErrorMapMiddleware)
	router.Mount("/", messageHandler.Init())
	router.Get("/swagger/*", httpSwagger.WrapHandler)

	server := &http.Server{
		Addr:    ":8080",
		Handler: router,
	}

	serverErr := make(chan error, 1)
	go func() {
		slog.Info("listening", "addr", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
	}()

	select {
	case err := <-serverErr:
		return fmt.Errorf("server failed: %w", err)
	case <-ctx.Done():
		slog.Info("shutting down")
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful shutdown: %w", err)
	}
	return nil
}
