package http_server

import (
	"context"

	"github.com/xnxq1/go-kafka-test/internal/domain"
)

type IMessageService interface {
	CreateMessage(ctx context.Context, content string) (*domain.Message, error)
	GetMessages(ctx context.Context, limit int, offset int) ([]domain.Message, error)
}

type IConfig interface {
	GetLimit() int
}
