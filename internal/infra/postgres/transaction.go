package postgres

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type TransactionKey struct {
}
type Transactor struct {
	pgPool *pgxpool.Pool
}

func (t *Transactor) WithTx(ctx context.Context, fn func(ctx context.Context) error) error {
	return pgx.BeginFunc(ctx, t.pgPool, func(tx pgx.Tx) error {
		return fn(context.WithValue(ctx, TransactionKey{}, tx))
	})
}

func NewTransactor(pgPool *pgxpool.Pool) *Transactor {
	return &Transactor{pgPool}
}
