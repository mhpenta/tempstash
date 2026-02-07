package tempstash

import (
	"context"
	"math/rand/v2"
	"time"
)

type retryConfig struct {
	times   int
	backoff func(attempt int) time.Duration
}

func defaultRetry() retryConfig {
	return retryConfig{
		times:   3,
		backoff: backoffWithJitter,
	}
}

func retry(ctx context.Context, cfg retryConfig, fn func(ctx context.Context) error) error {
	var last error
	for i := 0; i < cfg.times; i++ {
		if err := fn(ctx); err != nil {
			last = err
		} else {
			return nil
		}

		if i == cfg.times-1 {
			break
		}

		delay := cfg.backoff(i + 1)
		select {
		case <-ctx.Done():
			return last
		case <-time.After(delay):
		}
	}
	return last
}

func backoffWithJitter(attempt int) time.Duration {
	base := time.Duration(500*(1<<attempt)) * time.Millisecond
	jitter := time.Duration(rand.N(int64(base / 2)))
	return base + jitter
}
