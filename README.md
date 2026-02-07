# tempstash

A tiny, disposable stash for structured observations. Built for cases where you want to temporarily capture data — like LLM-generated scripts — query it after some real-world use to spot patterns, and then throw it away. Not a database, not a logger. Just a place to toss things, look at them, and drop them. Backed by Turso/libsql.

## Usage

```go
s, err := tempstash.New("libsql://your-db.turso.io?authToken=...")

// fire-and-forget (background goroutine, panic-safe, auto-retry)
s.Put(ctx, tempstash.Stashed{
    Namespace: "go-scripts",
    Key:       "http-request",
    Data:      scriptContent,
})

// query later
records, _ := s.Query(ctx, tempstash.QueryFilter{
    Namespace: "go-scripts",
    Since:     time.Now().Add(-24 * time.Hour),
})

// throw it away
s.Drop(ctx, "go-scripts")
s.Close()
```

## License

MIT
