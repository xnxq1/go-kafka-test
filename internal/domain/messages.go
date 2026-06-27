package domain

import (
	"time"

	"github.com/google/uuid"
)

type Message struct {
	Id          uuid.UUID  `json:"id"`
	Content     string     `json:"content"`
	Status      Status     `json:"status"`
	CreatedAt   time.Time  `json:"created_at"`
	ProcessedAt *time.Time `json:"processed_at"`
}
