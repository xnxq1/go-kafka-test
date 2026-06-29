package postgres

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xnxq1/go-kafka-test/internal/domain"
)

type MessageOutboxRepo struct {
	*BaseRepo
}

func (repo *MessageOutboxRepo) Create(ctx context.Context, messageId uuid.UUID, maxRetryCount int) error {
	_, err := repo.db(ctx).Exec(
		ctx,
		`INSERT INTO messages_outbox(message_id, max_retry_count) VALUES ($1, $2)`, messageId, maxRetryCount)
	return err
}

func (repo *MessageOutboxRepo) GetUnPublishedMessages(ctx context.Context, limit int) ([]domain.MessageOutbox, error) {
	rows, err := repo.db(ctx).Query(ctx,
		`SELECT id, message_id, max_retry_count, created_at, published_at, retry_count
			FROM messages_outbox
			WHERE published_at IS NULL
			LIMIT $1
			FOR UPDATE SKIP LOCKED`, limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	outboxMsgs := []domain.MessageOutbox{}
	for rows.Next() {
		var outboxMsg domain.MessageOutbox
		err := rows.Scan(
			&outboxMsg.Id,
			&outboxMsg.MessageId,
			&outboxMsg.MaxRetryCount,
			&outboxMsg.CreatedAt,
			&outboxMsg.PublishedAt,
			&outboxMsg.RetryCount,
		)
		if err != nil {
			return nil, err
		}
		outboxMsgs = append(outboxMsgs, outboxMsg)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return outboxMsgs, nil

}

func (repo *MessageOutboxRepo) MarkMessagesDone(ctx context.Context, msg_ids []uuid.UUID) error {
	_, err := repo.db(ctx).Exec(
		ctx,
		`UPDATE messages_outbox SET published_at = now() WHERE id IN ($1)`,
		msg_ids,
	)
	return err

}
func NewMessageOutboxRepo(dbPool *pgxpool.Pool) *MessageOutboxRepo {
	return &MessageOutboxRepo{&BaseRepo{dbPool}}
}
