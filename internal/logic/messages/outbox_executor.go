package logic

import (
	"context"
	"fmt"
)

type OutboxMessageExecutor struct {
	messageOutboxRepo IMessageOutboxRepo
	config            IConfig
}

func (executor *OutboxMessageExecutor) Execute(ctx context.Context) error {
	msgs, err := executor.messageOutboxRepo.GetUnPublishedMessages(ctx, executor.config.GetOutboxLimit(), 0)
	if err != nil {
		return err
	}
	fmt.Printf("%d messages have been published\n", len(msgs))
	return nil
}
func NewOutboxExecutor(messageOutboxRepo IMessageOutboxRepo, config IConfig) *OutboxMessageExecutor {
	return &OutboxMessageExecutor{messageOutboxRepo: messageOutboxRepo, config: config}
}
