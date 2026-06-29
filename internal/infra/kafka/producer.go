package kafka

import (
	"context"

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
	produceMessages := producer.client.ProduceSync(ctx, records...)
	var result [][]byte
	for _, msg := range produceMessages {
		if msg.Err == nil {
			result = append(result, msg.Record.Value)
		}
	}
	return result
}
