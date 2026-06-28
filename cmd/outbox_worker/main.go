package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
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

	slog.SetDefault(setupLogger(config.LogLevel, config.LogFormat))
	slog.Info("конфигурация загружена", "outbox_delay_sec", config.OutboxDelay, "outbox_limit", config.OutboxLimit)

	initCtx, initCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer initCancel()
	dbPool, err := postgres.NewPool(initCtx, config.DatabaseURL)
	if err != nil {
		return fmt.Errorf("init db pool: %w", err)
	}
	defer dbPool.Close()
	outboxMessageRepo := postgres.NewMessageOutboxRepo(dbPool)
	transactor := postgres.NewTransactor(dbPool)
	executor := logic.NewOutboxExecutor(outboxMessageRepo, config, transactor)
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
	slog.Info("получен сигнал завершения, ожидаем остановки воркера")
	wg.Wait()
	slog.Info("приложение остановлено")
	return nil
}

func setupLogger(level, format string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: lvl}
	var handler slog.Handler
	if strings.ToLower(format) == "json" {
		handler = slog.NewJSONHandler(os.Stdout, opts)
	} else {
		handler = slog.NewTextHandler(os.Stdout, opts)
	}
	return slog.New(handler)
}
