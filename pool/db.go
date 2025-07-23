package pool

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type IDB interface {
	Exec(ctx context.Context, label, sql string, arguments ...any) (tag pgconn.CommandTag, err error)
	Query(ctx context.Context, label, query string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, label, query string, args ...any) pgx.Row
	Ping(ctx context.Context) error
	Begin(ctx context.Context) (pgx.Tx, error)
	BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error)
	Close()
}

type dbEngine struct {
	cfg *Config
	db  *pgxpool.Pool
}

func NewDB(
	ctx context.Context,
	cfg *Config,
) (_ IDB, err error) {
	e := &dbEngine{
		cfg: cfg,
	}

	if err = e.connect(ctx); err != nil {
		return nil, err
	}

	return e, nil
}

func (e *dbEngine) Ping(ctx context.Context) error {
	return e.db.Ping(ctx)
}

func (e *dbEngine) Close() {
	e.db.Close()
}

func (e *dbEngine) Query(ctx context.Context, label, query string, args ...any) (pgx.Rows, error) {
	var err error

	r := &rows{
		label: label,
		start: time.Now(),
	}

	r.rows, err = e.db.Query(ctx, query, args...)

	return r, err
}

func (e *dbEngine) QueryRow(ctx context.Context, label, query string, args ...any) pgx.Row {
	s := &scanner{
		label: label,
		start: time.Now(),
	}

	s.row = e.db.QueryRow(ctx, query, args...)
	return s
}

func (e *dbEngine) Exec(ctx context.Context, label, sql string, arguments ...any) (tag pgconn.CommandTag, err error) {
	return e.db.Exec(ctx, sql, arguments...)
}

func (e *dbEngine) Begin(ctx context.Context) (pgx.Tx, error) {
	return e.db.Begin(ctx)
}

func (e *dbEngine) BeginTx(ctx context.Context, opts pgx.TxOptions) (pgx.Tx, error) {
	return e.db.BeginTx(ctx, opts)
}

type scanner struct {
	label string
	start time.Time
	row   pgx.Row
}

func (s *scanner) Scan(targets ...any) error {
	var err error

	err = s.row.Scan(targets...)
	return err
}

type rows struct {
	label string
	start time.Time
	rows  pgx.Rows
	err   error
}

func (r *rows) Close() {
	r.rows.Close()
}

func (r *rows) Err() error {
	return r.rows.Err()
}

func (r *rows) CommandTag() pgconn.CommandTag {
	return r.rows.CommandTag()
}

func (r *rows) FieldDescriptions() []pgconn.FieldDescription {
	return r.rows.FieldDescriptions()
}

func (r *rows) Next() bool {
	return r.rows.Next()
}

func (r *rows) Scan(dest ...any) error {
	return r.rows.Scan(dest...)
}

func (r *rows) Values() ([]any, error) {
	return r.rows.Values()
}

func (r *rows) RawValues() [][]byte {
	return r.rows.RawValues()
}

func (r *rows) Conn() *pgx.Conn {
	return r.rows.Conn()
}
