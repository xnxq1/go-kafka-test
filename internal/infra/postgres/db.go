package postgres

import (
	"context"
	"errors"
	"log/slog"

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
		slog.ErrorContext(ctx, "не удалось разобрать строку подключения к БД", "err", err)
		return nil, SetupPoolError
	}
	pool, err = pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		slog.ErrorContext(ctx, "не удалось создать пул соединений с БД", "err", err)
		return nil, SetupPoolError
	}
	if err := pool.Ping(ctx); err != nil {
		slog.ErrorContext(ctx, "БД не отвечает на ping", "err", err)
		return nil, SetupPoolError
	}
	slog.InfoContext(ctx, "пул соединений с БД готов")
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
