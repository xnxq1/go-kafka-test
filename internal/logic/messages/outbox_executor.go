package logic

import (
	"context"
	"log/slog"
)

type OutboxMessageExecutor struct {
	messageOutboxRepo IMessageOutboxRepo
	config            IConfig
	transactor        ITransactor
}

func (executor *OutboxMessageExecutor) Execute(ctx context.Context) error {
	limit := executor.config.GetOutboxLimit()
	err := executor.transactor.WithTx(ctx, func(ctx context.Context) error {
		msgs, err := executor.messageOutboxRepo.GetUnPublishedMessages(ctx, limit)
		if err != nil {
			slog.ErrorContext(ctx, "не удалось получить неопубликованные сообщения", "limit", limit, "err", err)
			return err
		}
		if len(msgs) == 0 {
			slog.DebugContext(ctx, "неопубликованных сообщений нет")
			return nil
		}
		slog.InfoContext(ctx, "сообщения опубликованы", "count", len(msgs)) // mock
		return nil
	})
	return err
}
func NewOutboxExecutor(messageOutboxRepo IMessageOutboxRepo, config IConfig, transactor ITransactor) *OutboxMessageExecutor {
	return &OutboxMessageExecutor{messageOutboxRepo: messageOutboxRepo, config: config, transactor: transactor}
}
