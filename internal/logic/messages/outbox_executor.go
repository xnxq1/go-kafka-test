package logic

import (
	"context"
	"log/slog"
)

type OutboxMessageExecutor struct {
	messageOutboxRepo IMessageOutboxRepo
	config            IConfig
}

func (executor *OutboxMessageExecutor) Execute(ctx context.Context) error {
	limit := executor.config.GetOutboxLimit()
	msgs, err := executor.messageOutboxRepo.GetUnPublishedMessages(ctx, limit, 0)
	if err != nil {
		slog.ErrorContext(ctx, "не удалось получить неопубликованные сообщения", "limit", limit, "err", err)
		return err
	}
	if len(msgs) == 0 {
		slog.DebugContext(ctx, "неопубликованных сообщений нет")
		return nil
	}
	slog.InfoContext(ctx, "сообщения опубликованы", "count", len(msgs))
	return nil
}
func NewOutboxExecutor(messageOutboxRepo IMessageOutboxRepo, config IConfig) *OutboxMessageExecutor {
	return &OutboxMessageExecutor{messageOutboxRepo: messageOutboxRepo, config: config}
}
