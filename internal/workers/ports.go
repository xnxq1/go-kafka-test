package workers

import "context"

type IExecutor interface {
	Execute(ctx context.Context) error
}
