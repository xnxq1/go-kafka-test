package postgres

import (
	"context"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
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

func NewMessageOutboxRepo(dbPool *pgxpool.Pool) *MessageOutboxRepo {
	return &MessageOutboxRepo{&BaseRepo{dbPool}}
}
