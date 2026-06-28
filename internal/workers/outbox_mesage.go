package workers

type OutboxMessageWorker struct {
	*BaseWorker
}

func NewOutboxWorker(executor IExecutor, delay int) *OutboxMessageWorker {
	return &OutboxMessageWorker{&BaseWorker{delay: delay, executor: executor}}
}
