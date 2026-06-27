package logic

import (
	"context"

	"github.com/xnxq1/go-kafka-test/internal/domain"
)

type MessageService struct {
	transactor        ITransactor
	messageRepo       IMessageRepo
	messageOutboxRepo IMessageOutboxRepo
	config            IConfig
}

func (service *MessageService) CreateMessage(ctx context.Context, content string) (*domain.Message, error) {
	var msg *domain.Message
	err := service.transactor.WithTx(ctx, func(ctx context.Context) error {
		var err error
		msg, err = service.messageRepo.Create(ctx, content)
		if err != nil {
			return err
		}
		err = service.messageOutboxRepo.Create(ctx, msg.Id, service.config.GetOutboxMaxRetryCount())
		return err
	})
	return msg, err
}

func NewMessageService(transactor ITransactor, messageRepo IMessageRepo, messageOutboxRepo IMessageOutboxRepo, config IConfig) *MessageService {
	return &MessageService{transactor, messageRepo, messageOutboxRepo, config}
}
