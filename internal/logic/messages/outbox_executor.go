package logic

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/google/uuid"
	"github.com/xnxq1/go-kafka-test/internal/domain"
)

type OutboxMessageExecutor struct {
	messageOutboxRepo IMessageOutboxRepo
	config            IConfig
	transactor        ITransactor
	producer          IProducer
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
		var jsonMessages [][]byte
		jsonMessages, err = executor.prepareMessagesToProduce(msgs)
		if err != nil {
			return err
		}
		produceMsgs := executor.producer.Produce(ctx, jsonMessages)
		var doneMsgs []uuid.UUID
		for _, msg := range produceMsgs {
			var doneMsg domain.MessageOutbox
			_ = json.Unmarshal(msg, &doneMsg)
			doneMsgs = append(doneMsgs, doneMsg.MessageId)
		}
		err = executor.messageOutboxRepo.MarkMessagesDone(ctx, doneMsgs)
		return err
	})
	return err
}
func (executor *OutboxMessageExecutor) prepareMessagesToProduce(messages []domain.MessageOutbox) ([][]byte, error) {
	res := make([][]byte, len(messages))
	for _, msg := range messages {
		jsonMsg, err := json.Marshal(msg)
		if err != nil {
			return res, err
		}
		res = append(res, jsonMsg)
	}
	return res, nil
}
func NewOutboxExecutor(
	messageOutboxRepo IMessageOutboxRepo,
	config IConfig,
	transactor ITransactor,
	producer IProducer,
) *OutboxMessageExecutor {
	return &OutboxMessageExecutor{
		messageOutboxRepo: messageOutboxRepo,
		config:            config,
		transactor:        transactor,
		producer:          producer,
	}
}
