package kafka

import "context"

type IConsumerHandler interface {
	Handle(ctx context.Context, message []byte) error
}
