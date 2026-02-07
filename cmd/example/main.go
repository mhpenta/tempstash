package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/mhpenta/tempstash"
)

func main() {
	url := os.Getenv("TURSO_URL")
	if url == "" {
		fmt.Fprintln(os.Stderr, "set TURSO_URL")
		os.Exit(1)
	}

	s, err := tempstash.New(url, tempstash.WithLogger(slog.Default()))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer s.Close()

	ctx := context.Background()

	src, err := os.ReadFile("cmd/example/main.go")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	id, err := s.PutSync(ctx, tempstash.Stashed{
		Namespace: "examples",
		Name:      "self-stash",
		Key:       "main.go",
		Data:      string(src),
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("stashed:", id)

	records, err := s.Query(ctx, tempstash.QueryFilter{
		Namespace: "examples",
		Since:     time.Now().Add(-1 * time.Minute),
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	for _, r := range records {
		fmt.Printf("--- %s [%s] %s ---\n%s\n", r.Name, r.Key, r.CreatedAt.Format(time.RFC3339), r.Data)
	}
}
