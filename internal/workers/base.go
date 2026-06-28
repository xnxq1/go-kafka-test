package workers

import (
	"context"
	"log/slog"
	"time"
)

type BaseWorker struct {
	delay    int
	executor IExecutor
}

func (worker *BaseWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(worker.delay) * time.Second)
	defer ticker.Stop()
	slog.InfoContext(ctx, "воркер запущен", "delay_sec", worker.delay)
	for {
		select {
		case <-ctx.Done():
			slog.InfoContext(ctx, "воркер остановлен", "reason", ctx.Err())
			return
		case <-ticker.C:
			if err := worker.executor.Execute(ctx); err != nil {
				slog.ErrorContext(ctx, "ошибка выполнения задачи воркера", "err", err)
			}
		}

	}
}
