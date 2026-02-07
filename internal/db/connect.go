package db

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/tursodatabase/libsql-client-go/libsql"
)

func Connect(url string) (*sql.DB, error) {
	db, err := sql.Open("libsql", url)
	if err != nil {
		return nil, fmt.Errorf("tempstash: open: %w", err)
	}

	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("tempstash: ping: %w", err)
	}

	return db, nil
}
