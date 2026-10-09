package queueredis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/rachmanzz/fiber-extras/v3/queue"
	"github.com/redis/go-redis/v9"
)

// DefaultPrefix is the default key prefix for Redis queue data structures.
const DefaultPrefix = "fiber:queue:"

// RedisDriver implements queue.Adapter (and queue.DelayedEnqueuer) using Redis Sorted Sets and Hashes.
type RedisDriver struct {
	client redis.Cmdable
	prefix string
}

// RedisAdapter is an alias for RedisDriver under the Adapter architecture.
type RedisAdapter = RedisDriver

// Option configures RedisDriver parameters.
type Option func(*RedisDriver)

// WithPrefix sets the Redis key prefix for the queue.
func WithPrefix(prefix string) Option {
	return func(r *RedisDriver) {
		if prefix != "" {
			r.prefix = prefix
		}
	}
}

// New constructs a RedisDriver with options.
func New(client redis.Cmdable, opts ...Option) *RedisDriver {
	return NewRedisDriver(client, DefaultPrefix, opts...)
}

// NewRedisDriver constructs a new Redis queue driver with a specified key prefix and options.
func NewRedisDriver(client redis.Cmdable, prefix string, opts ...Option) *RedisDriver {
	if prefix == "" {
		prefix = DefaultPrefix
	}
	r := &RedisDriver{
		client: client,
		prefix: prefix,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// NewRedisAdapter constructs the official Redis Adapter for the Queue Core.
func NewRedisAdapter(client redis.Cmdable, prefix string, opts ...Option) *RedisAdapter {
	return NewRedisDriver(client, prefix, opts...)
}

// Name returns the driver identifier.
func (r *RedisDriver) Name() string {
	return "redis"
}

// Capabilities returns the capability profile for the Redis adapter.
func (r *RedisDriver) Capabilities() queue.Capabilities {
	return queue.Capabilities{
		Durable:           false,
		Persistent:        false,
		NativeRedelivery:  false,
		NativePriority:    queue.PrioritySoft,
		DelayedEnqueue:    true,
		StrictGlobalOrder: false,
		Replay:            false,
		Dedup:             true,
		MaxBatchSize:      500,
		Guarantee:         queue.AtLeastOnce,
	}
}

func (r *RedisDriver) queueKey() string {
	return r.prefix + "priority_zset"
}

func (r *RedisDriver) delayedKey() string {
	return r.prefix + "delayed_zset"
}

func (r *RedisDriver) dataKey() string {
	return r.prefix + "messages_hash"
}

// Enqueue serializes and pushes a message into the Redis priority ZSet and message hash.
func (r *RedisDriver) Enqueue(ctx context.Context, msg *queue.Message) error {
	raw, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("queue-redis: failed to marshal message: %w", err)
	}

	// Score is Priority (higher priority = higher score).
	// Sub-score tie-breaker: timestamp unix seconds deducted
	score := float64(msg.Priority)*1e9 - float64(msg.CreatedAt.Unix())

	pipe := r.client.Pipeline()
	pipe.HSet(ctx, r.dataKey(), msg.ID, raw)
	pipe.ZAdd(ctx, r.queueKey(), redis.Z{
		Score:  score,
		Member: msg.ID,
	})
	_, err = pipe.Exec(ctx)
	return err
}

// EnqueueAt schedules a message to become active at a specific future timestamp.
func (r *RedisDriver) EnqueueAt(ctx context.Context, msg *queue.Message, at time.Time) error {
	raw, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("queue-redis: failed to marshal message: %w", err)
	}

	// In delayed ZSet, score is unix timestamp with fractional seconds
	score := float64(at.UnixNano()) / 1e9

	pipe := r.client.Pipeline()
	pipe.HSet(ctx, r.dataKey(), msg.ID, raw)
	pipe.ZAdd(ctx, r.delayedKey(), redis.Z{
		Score:  score,
		Member: msg.ID,
	})
	_, err = pipe.Exec(ctx)
	return err
}

// Dequeue pops the highest-priority messages from Redis, migrating matured delayed messages first.
func (r *RedisDriver) Dequeue(ctx context.Context, req queue.DequeueRequest) ([]*queue.Delivery, error) {
	nowScore := float64(time.Now().UnixNano()) / 1e9

	// 1. Migrate any matured delayed messages from delayedKey to queueKey
	delayedMembers, _ := r.client.ZRangeByScore(ctx, r.delayedKey(), &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%f", nowScore),
	}).Result()

	if len(delayedMembers) > 0 {
		pipe := r.client.Pipeline()
		for _, mID := range delayedMembers {
			pipe.ZRem(ctx, r.delayedKey(), mID)
			score := float64(queue.PriorityDefault)*1e9 - nowScore
			pipe.ZAdd(ctx, r.queueKey(), redis.Z{
				Score:  score,
				Member: mID,
			})
		}
		_, _ = pipe.Exec(ctx)
	}

	batchSize := req.BatchSize
	if batchSize <= 0 {
		batchSize = 20
	}

	// 2. Pop highest priority members
	items, err := r.client.ZPopMax(ctx, r.queueKey(), int64(batchSize)).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	if len(items) == 0 {
		return nil, nil
	}

	msgIDs := make([]string, 0, len(items))
	for _, it := range items {
		if id, ok := it.Member.(string); ok {
			msgIDs = append(msgIDs, id)
		}
	}

	rawItems, err := r.client.HMGet(ctx, r.dataKey(), msgIDs...).Result()
	if err != nil {
		return nil, err
	}

	deliveries := make([]*queue.Delivery, 0, len(rawItems))
	for _, raw := range rawItems {
		if raw == nil {
			continue
		}
		var msg queue.Message
		var str string
		switch v := raw.(type) {
		case string:
			str = v
		case []byte:
			str = string(v)
		}
		if err := json.Unmarshal([]byte(str), &msg); err == nil {
			deliveries = append(deliveries, &queue.Delivery{
				Message: &msg,
				Token:   msg.ID,
			})
		}
	}

	return deliveries, nil
}

// Ack removes the processed message payload from the Redis hash.
func (r *RedisDriver) Ack(ctx context.Context, d *queue.Delivery) error {
	if d == nil || d.Token == nil {
		return nil
	}
	msgID, ok := d.Token.(string)
	if !ok {
		return fmt.Errorf("queue-redis: invalid delivery token type: %T", d.Token)
	}
	return r.client.HDel(ctx, r.dataKey(), msgID).Err()
}

// Nack restores the message back into the priority queue for retry.
func (r *RedisDriver) Nack(ctx context.Context, d *queue.Delivery, reason error) error {
	if d == nil || d.Token == nil {
		return nil
	}
	msgID, ok := d.Token.(string)
	if !ok {
		return fmt.Errorf("queue-redis: invalid delivery token type: %T", d.Token)
	}
	prio := queue.PriorityDefault
	if d.Message != nil {
		prio = d.Message.Priority
	}
	score := float64(prio)*1e9 - float64(time.Now().Unix())
	return r.client.ZAdd(ctx, r.queueKey(), redis.Z{
		Score:  score,
		Member: msgID,
	}).Err()
}

// Close gracefully closes driver resources.
func (r *RedisDriver) Close() error {
	return nil
}
