package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	config_module "github.com/xnxq1/go-kafka-test/internal/config"
	"github.com/xnxq1/go-kafka-test/internal/infra/postgres"
	logic "github.com/xnxq1/go-kafka-test/internal/logic/messages"
	"github.com/xnxq1/go-kafka-test/internal/workers"
)

func main() {
	if err := run(); err != nil {
		slog.Error("приложение завершилось с ошибкой", "err", err)
		os.Exit(1)
	}
}

func run() error {
	config, err := config_module.LoadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	initCtx, InitCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer InitCancel()
	dbPool, err := postgres.NewPool(initCtx, config.DatabaseURL)
	if err != nil {
		return fmt.Errorf("init db pool: %w", err)
	}
	defer dbPool.Close()
	outboxMessageRepo := postgres.NewMessageOutboxRepo(dbPool)
	executor := logic.NewOutboxExecutor(outboxMessageRepo, config)
	worker := workers.NewOutboxWorker(executor, config.OutboxDelay)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		worker.Run(ctx)
	}()
	<-ctx.Done()
	wg.Wait()
	return nil
}
