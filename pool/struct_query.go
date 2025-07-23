package pool

import (
	"context"
	"github.com/jackc/pgx/v5"
)

type structQuery[T any] struct {
	e *dbEngine
}

func Struct[T any](e IDB) *structQuery[T] {
	return &structQuery[T]{
		e: e.(*dbEngine),
	}
}

func (s *structQuery[T]) Query(ctx context.Context, label, q string, args ...any) ([]*T, error) {
	r, err := s.e.Query(ctx, label, q, args...)
	if err != nil {
		return nil, err
	}

	items, err := pgx.CollectRows(r, pgx.RowToAddrOfStructByNameLax[T])
	if err != nil {
		return nil, err
	}

	return items, nil
}

func (s *structQuery[T]) QueryOne(ctx context.Context, label, q string, args ...any) (*T, error) {
	r, err := s.e.Query(ctx, label, q, args...)
	if err != nil {
		return nil, err
	}

	item, err := pgx.CollectOneRow(r, pgx.RowToAddrOfStructByNameLax[T])
	if err != nil {
		return nil, err
	}

	return item, nil
}

func (s *structQuery[T]) TxQuery(ctx context.Context, tx pgx.Tx, label, q string, args ...any) ([]*T, error) {
	r, err := tx.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}

	items, err := pgx.CollectRows(r, pgx.RowToAddrOfStructByNameLax[T])
	if err != nil {
		return nil, err
	}

	return items, nil
}

func (s *structQuery[T]) TxQueryOne(ctx context.Context, tx pgx.Tx, label, q string, args ...any) (*T, error) {

	r, err := tx.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}

	item, err := pgx.CollectOneRow(r, pgx.RowToAddrOfStructByNameLax[T])
	if err != nil {
		return nil, err
	}

	return item, nil
}
