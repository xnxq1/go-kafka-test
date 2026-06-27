package http_server

import (
	"context"

	"github.com/xnxq1/go-kafka-test/internal/domain"
)

type IMessageService interface {
	CreateMessage(ctx context.Context, content string) (*domain.Message, error)
}
