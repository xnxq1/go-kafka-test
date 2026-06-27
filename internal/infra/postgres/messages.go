package postgres

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/xnxq1/go-kafka-test/internal/domain"
)

type MessageRepository struct {
	*BaseRepo
}

func (repo *MessageRepository) Create(ctx context.Context, content string) (*domain.Message, error) {
	var msg domain.Message
	err := repo.db(ctx).QueryRow(ctx,
		`INSERT INTO messages (content)
			VALUES ($1)
			RETURNING id, content, status, created_at, processed_at`, content,
	).Scan(&msg.Id, &msg.Content, &msg.Status, &msg.CreatedAt, &msg.ProcessedAt)
	return &msg, err
}

func NewMessageRepo(dbPool *pgxpool.Pool) *MessageRepository {
	return &MessageRepository{&BaseRepo{dbPool: dbPool}}
}
