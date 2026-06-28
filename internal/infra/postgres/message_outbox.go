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

func (repo *MessageOutboxRepo) GetUnPublishedMessages(ctx context.Context, limit int, offset int) ([]domain.MessageOutbox, error) {
	rows, err := repo.db(ctx).Query(ctx,
		`SELECT id, message_id, max_retry_count, created_at, published_at, retry_count
			FROM messages_outbox
			WHERE processed_at IS NULL
			ORDER BY processed_at DESC
			LIMIT $1 OFFSET $2`, limit, offset,
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
func NewMessageOutboxRepo(dbPool *pgxpool.Pool) *MessageOutboxRepo {
	return &MessageOutboxRepo{&BaseRepo{dbPool}}
}
