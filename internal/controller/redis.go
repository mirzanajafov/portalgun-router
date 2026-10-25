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
	var skipLog everyMinute
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
					if skipLog.allow(time.Now()) {
						logger.Warn("snapshots that can't be decoded are skipped", "skipped", skipped, "entry", msg.ID, "err", err)
						skipped = 0
					}
					continue
				}
				agg.Add(snap)
			}
		}
	}
}

type RedisCounter struct{ RDB *redis.Client }

func (r RedisCounter) Hit(ctx context.Context, key string, window time.Duration) (int64, error) {
	var incr *redis.IntCmd
	_, err := r.RDB.TxPipelined(ctx, func(p redis.Pipeliner) error {
		incr = p.Incr(ctx, key)
		p.ExpireNX(ctx, key, window)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return incr.Val(), nil
}
