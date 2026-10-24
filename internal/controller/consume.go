package controller

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/mirzanajafov/portalgun-router/internal/distrib"
	"github.com/redis/go-redis/v9"
)

func Consume(ctx context.Context, rdb *redis.Client, agg *Aggregator, logger *slog.Logger) {
	last := "$"
	var skipped int
	var warned time.Time
	for ctx.Err() == nil {
		streams, err := rdb.XRead(ctx, &redis.XReadArgs{Streams: []string{distrib.SnapshotStream, last}, Count: 200, Block: time.Second}).Result()
		if errors.Is(err, redis.Nil) {
			continue
		}
		if err != nil {
			if ctx.Err() == nil {
				logger.Warn("reading snapshots", "err", err)
				time.Sleep(time.Second)
			}
			continue
		}
		for _, s := range streams {
			for _, msg := range s.Messages {
				last = msg.ID
				snap, err := distrib.DecodeSnapshot(msg)
				if err != nil {
					skipped++
					if time.Since(warned) >= time.Minute {
						logger.Warn("snapshots that can't be decoded are skipped", "skipped", skipped, "entry", msg.ID, "err", err)
						skipped, warned = 0, time.Now()
					}
					continue
				}
				agg.Add(snap)
			}
		}
	}
}

func FlushLoop(ctx context.Context, agg *Aggregator) {
	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			agg.Flush(now)
		}
	}
}
