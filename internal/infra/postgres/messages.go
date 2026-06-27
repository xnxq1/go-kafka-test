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

func (repo *MessageRepository) GetMessages(ctx context.Context, limit int, offset int) ([]domain.Message, error) {
	rows, err := repo.db(ctx).Query(ctx,
		`SELECT id, content, status, created_at, processed_at
			FROM messages
			ORDER BY id DESC
			LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var messages []domain.Message
	for rows.Next() {
		var msg domain.Message
		if err := rows.Scan(&msg.Id, &msg.Content, &msg.Status, &msg.CreatedAt, &msg.ProcessedAt); err != nil {
			return nil, err
		}
		messages = append(messages, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return messages, nil
}

func NewMessageRepo(dbPool *pgxpool.Pool) *MessageRepository {
	return &MessageRepository{&BaseRepo{dbPool: dbPool}}
}
