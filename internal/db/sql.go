package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
)

const createTable = `
CREATE TABLE IF NOT EXISTS stash (
	id         TEXT PRIMARY KEY,
	namespace  TEXT NOT NULL,
	name       TEXT NOT NULL DEFAULT '',
	key        TEXT NOT NULL DEFAULT '',
	data       TEXT NOT NULL DEFAULT '',
	created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now'))
);
CREATE INDEX IF NOT EXISTS idx_stash_namespace ON stash(namespace);
CREATE INDEX IF NOT EXISTS idx_stash_key       ON stash(namespace, key);
`

const insertSQL = `INSERT INTO stash (id, namespace, name, key, data) VALUES (?, ?, ?, ?, ?)`

const queryBase = `SELECT id, namespace, name, key, data, created_at FROM stash WHERE 1=1`

const dropByNamespace = `DELETE FROM stash WHERE namespace = ?`

const dropAll = `DELETE FROM stash`

type Row struct {
	ID        string
	Namespace string
	Name      string
	Key       string
	Data      string
	CreatedAt time.Time
}

type Filter struct {
	Namespace string
	Key       string
	Since     time.Time
	Limit     int
}

func EnsureTable(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, createTable)
	if err != nil {
		return fmt.Errorf("tempstash: ensure table: %w", err)
	}
	return nil
}

func Insert(ctx context.Context, db *sql.DB, namespace, name, key, data string) (string, error) {
	id := uuid.NewString()
	_, err := db.ExecContext(ctx, insertSQL, id, namespace, name, key, data)
	if err != nil {
		return "", fmt.Errorf("tempstash: insert: %w", err)
	}
	return id, nil
}

func Query(ctx context.Context, db *sql.DB, f Filter) ([]Row, error) {
	q := queryBase
	var args []any

	if f.Namespace != "" {
		q += " AND namespace = ?"
		args = append(args, f.Namespace)
	}
	if f.Key != "" {
		q += " AND key = ?"
		args = append(args, f.Key)
	}
	if !f.Since.IsZero() {
		q += " AND created_at >= ?"
		args = append(args, f.Since.UTC().Format(time.RFC3339Nano))
	}

	q += " ORDER BY created_at DESC"

	limit := f.Limit
	if limit <= 0 {
		limit = 100
	}
	q += fmt.Sprintf(" LIMIT %d", limit)

	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("tempstash: query: %w", err)
	}
	defer rows.Close()

	var out []Row
	for rows.Next() {
		var r Row
		var ts string
		if err := rows.Scan(&r.ID, &r.Namespace, &r.Name, &r.Key, &r.Data, &ts); err != nil {
			return nil, fmt.Errorf("tempstash: scan: %w", err)
		}
		r.CreatedAt, _ = time.Parse(time.RFC3339Nano, ts)
		out = append(out, r)
	}
	return out, rows.Err()
}

func Drop(ctx context.Context, db *sql.DB, namespace string) error {
	if namespace == "" {
		_, err := db.ExecContext(ctx, dropAll)
		return err
	}
	_, err := db.ExecContext(ctx, dropByNamespace, namespace)
	return err
}
