package tempstash

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/mhpenta/tempstash/internal/db"
)

type Stash struct {
	conn  *sql.DB
	log   *slog.Logger
	retry retryConfig
}

type Option func(*Stash)

func WithLogger(l *slog.Logger) Option {
	return func(s *Stash) { s.log = l }
}

func New(url string, opts ...Option) (*Stash, error) {
	conn, err := db.Connect(url)
	if err != nil {
		return nil, err
	}

	s := &Stash{
		conn:  conn,
		log:   slog.Default(),
		retry: defaultRetry(),
	}
	for _, o := range opts {
		o(s)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.EnsureTable(ctx, conn); err != nil {
		_ = conn.Close()
		return nil, err
	}

	return s, nil
}

type Stashed struct {
	Name      string
	Namespace string
	Key       string
	Data      any
}

type Record struct {
	ID        string
	Namespace string
	Name      string
	Key       string
	Data      string
	CreatedAt time.Time
}

type QueryFilter struct {
	Namespace string
	Key       string
	Since     time.Time
	Limit     int
}

func (s *Stash) Put(ctx context.Context, item Stashed) {
	go s.safeDo(func() {
		data, err := marshal(item.Data)
		if err != nil {
			s.log.Error("tempstash: marshal", "err", err)
			return
		}

		_ = retry(ctx, s.retry, func(ctx context.Context) error {
			_, err := db.Insert(ctx, s.conn, item.Namespace, item.Name, item.Key, data)
			return err
		})
	})
}

func (s *Stash) PutSync(ctx context.Context, item Stashed) (string, error) {
	data, err := marshal(item.Data)
	if err != nil {
		return "", fmt.Errorf("tempstash: marshal: %w", err)
	}

	var id string
	err = retry(ctx, s.retry, func(ctx context.Context) error {
		var e error
		id, e = db.Insert(ctx, s.conn, item.Namespace, item.Name, item.Key, data)
		return e
	})
	return id, err
}

func (s *Stash) Query(ctx context.Context, f QueryFilter) ([]Record, error) {
	rows, err := db.Query(ctx, s.conn, db.Filter{
		Namespace: f.Namespace,
		Key:       f.Key,
		Since:     f.Since,
		Limit:     f.Limit,
	})
	if err != nil {
		return nil, err
	}

	out := make([]Record, len(rows))
	for i, r := range rows {
		out[i] = Record{
			ID:        r.ID,
			Namespace: r.Namespace,
			Name:      r.Name,
			Key:       r.Key,
			Data:      r.Data,
			CreatedAt: r.CreatedAt,
		}
	}
	return out, nil
}

func (s *Stash) Drop(ctx context.Context, namespace string) error {
	return db.Drop(ctx, s.conn, namespace)
}

func (s *Stash) Close() error {
	if s.conn != nil {
		return s.conn.Close()
	}
	return nil
}

func (s *Stash) safeDo(fn func()) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("tempstash: recovered panic", "err", r)
		}
	}()
	fn()
}

func marshal(v any) (string, error) {
	switch d := v.(type) {
	case string:
		return d, nil
	case []byte:
		return string(d), nil
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return "", err
		}
		return string(b), nil
	}
}
