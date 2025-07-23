package pool

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func (e *dbEngine) connect(ctx context.Context) error {
	pcfg, err := pgxpool.ParseConfig(formConnectionString(e.cfg))
	if err != nil {
		return err
	}
	pcfg.AfterConnect = afterConnect(e.cfg)

	pcfg.MaxConns = e.cfg.MaxConn
	pcfg.MinConns = e.cfg.MinConn
	pcfg.MaxConnIdleTime = e.cfg.MaxIdleTime
	if e.cfg.DisableTLS {
		pcfg.ConnConfig.TLSConfig = nil
	}

	e.db, err = pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return err
	}

	if err = e.Ping(ctx); err != nil {
		return err
	}

	if err = checkTimeZone(ctx, e.cfg, e); err != nil {
		return err
	}

	return nil
}

func formConnectionString(cfg *Config) string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s",
		cfg.Username, cfg.Password, cfg.Host, cfg.Port,
		cfg.Database,
	)
}

type afterFunc func(ctx context.Context, conn *pgx.Conn) error

func afterConnect(cfg *Config) afterFunc {
	return func(ctx context.Context, conn *pgx.Conn) error {
		if _, err := conn.Exec(ctx, fmt.Sprintf(`set time zone '%s';`, cfg.TimeZone)); err != nil {
			return err
		}

		return nil
	}
}

func checkTimeZone(ctx context.Context, cfg *Config, db IDB) error {
	var tz string

	if err := db.QueryRow(ctx, `check_time_zone`, `select current_setting('TIMEZONE')`).Scan(&tz); err != nil {
		return err
	}

	if strings.ToLower(tz) != strings.ToLower(cfg.TimeZone) {
		return fmt.Errorf(`db server timezone not match with pgsql config. cfg tz: %s, server tz: %s`,
			cfg.TimeZone, tz)
	}

	return nil
}
