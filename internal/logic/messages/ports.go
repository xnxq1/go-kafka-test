package logic

import (
	"context"

	"github.com/google/uuid"
	"github.com/xnxq1/go-kafka-test/internal/domain"
)

type ITransactor interface {
	WithTx(ctx context.Context, fn func(ctx context.Context) error) error
}

type IMessageRepo interface {
	Create(ctx context.Context, content string) (*domain.Message, error)
	GetMessages(ctx context.Context, limit int, offset int) ([]domain.Message, error)
}

type IMessageOutboxRepo interface {
	Create(ctx context.Context, messageId uuid.UUID, maxRetryCount int) error
}

type IConfig interface {
	GetOutboxMaxRetryCount() int
}
