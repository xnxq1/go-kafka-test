package postgres

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var SetupPoolError = errors.New("setup pool error")

func NewPool(ctx context.Context, connString string) (*pgxpool.Pool, error) {
	var err error
	var config *pgxpool.Config
	var pool *pgxpool.Pool
	config, err = pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, SetupPoolError
	}
	pool, err = pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, SetupPoolError
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, SetupPoolError
	}
	return pool, nil
}

type BaseRepo struct {
	dbPool *pgxpool.Pool
}
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (commandTag pgconn.CommandTag, err error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func (repo *BaseRepo) db(ctx context.Context) Querier {
	if tx, ok := ctx.Value(TransactionKey{}).(pgx.Tx); ok {
		return tx
	}
	return repo.dbPool
}
