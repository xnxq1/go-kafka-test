package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
)

type KafkaConsumer struct {
	client  *kgo.Client
	handler IConsumerHandler
}

func (c *KafkaConsumer) Consume(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		fetches := c.client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, err := range errs {
				if errors.Is(err.Err, context.Canceled) {
					return nil
				}
				slog.ErrorContext(ctx, "ошибка при чтении из kafka",
					"topic", err.Topic, "partition", err.Partition, "err", err.Err)
			}
		}
		fetches.EachRecord(func(record *kgo.Record) {
			if err := c.handler.Handle(ctx, record.Value); err != nil {
				slog.ErrorContext(ctx, "не удалось обработать сообщение",
					"topic", record.Topic, "offset", record.Offset, "err", err)
			}
		})
	}
}

func (c *KafkaConsumer) PoolConsume(ctx context.Context) error {
	workers := map[string]*Worker{}
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		fetches := c.client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, err := range errs {
				if errors.Is(err.Err, context.Canceled) {
					return nil
				}
				slog.ErrorContext(ctx, "ошибка при чтении из kafka",
					"topic", err.Topic, "partition", err.Partition, "err", err.Err)
			}
		}
		fetches.EachPartition(func(p kgo.FetchTopicPartition) {
			key := fmt.Sprintf("%s-%d", p.Topic, p.Partition)
			w, ok := workers[key]
			if !ok {
				w = &Worker{
					Channel: make(chan *kgo.Record, 10),
				}
				workers[key] = w
				go c.RunWorker(ctx, w)
			}
			for _, record := range p.Records {
				w.Channel <- record
			}
		})
	}
}

type Worker struct {
	Channel chan *kgo.Record
}

func (c *KafkaConsumer) RunWorker(ctx context.Context, w *Worker) {
	for {
		record, ok := <-w.Channel
		if !ok {
			slog.Info("Shutdown worker")
			return
		}
		if err := c.handler.Handle(ctx, record.Value); err != nil {
			slog.ErrorContext(ctx, "не удалось обработать сообщение",
				"topic", record.Topic, "offset", record.Offset, "err", err)
		}
		c.client.MarkCommitRecords(record)
	}
}
