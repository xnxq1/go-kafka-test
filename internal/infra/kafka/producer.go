package kafka

import (
	"context"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
)

type KafkaProducer struct {
	client *kgo.Client
	topic  string
}

func (producer *KafkaProducer) Produce(ctx context.Context, messages [][]byte) [][]byte {
	records := make([]*kgo.Record, len(messages))
	for i, m := range messages {
		records[i] = &kgo.Record{Topic: producer.topic, Value: m}
	}

	slog.DebugContext(ctx, "публикуем сообщения в kafka", "topic", producer.topic, "count", len(records))

	produceMessages := producer.client.ProduceSync(ctx, records...)
	result := make([][]byte, 0, len(produceMessages))
	for _, msg := range produceMessages {
		if msg.Err != nil {
			slog.ErrorContext(ctx, "не удалось опубликовать сообщение в kafka", "topic", producer.topic, "err", msg.Err)
			continue
		}
		result = append(result, msg.Record.Value)
	}

	slog.InfoContext(ctx, "сообщения опубликованы в kafka",
		"topic", producer.topic, "ok", len(result), "failed", len(produceMessages)-len(result))
	return result
}
