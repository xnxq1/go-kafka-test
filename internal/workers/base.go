package workers

import (
	"context"
	"fmt"
	"time"
)

type BaseWorker struct {
	delay    int
	executor IExecutor
}

func (worker *BaseWorker) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(worker.delay) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			fmt.Println("worker exit")
			return
		case <-ticker.C:
			if err := worker.executor.Execute(ctx); err != nil {
				fmt.Println("worker execute error:", err)
			}
		}

	}
}
