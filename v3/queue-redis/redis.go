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

// DefaultVisibilityTimeout is the default lease held on in-flight messages.
// A delivered message stays in-flight until Ack/Nack; if the consumer crashes
// and the lease expires, the next Dequeue redelivers it (at-least-once).
const DefaultVisibilityTimeout = 30 * time.Second

// RedisDriver implements queue.Adapter (and queue.DelayedEnqueuer) using Redis Sorted Sets and Hashes.
type RedisDriver struct {
	client redis.Cmdable
	prefix string
	lease  time.Duration
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

// WithVisibilityTimeout sets how long a delivered-but-unacked message stays
// in-flight before it becomes eligible for redelivery after a consumer crash.
func WithVisibilityTimeout(d time.Duration) Option {
	return func(r *RedisDriver) {
		if d > 0 {
			r.lease = d
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
		lease:  DefaultVisibilityTimeout,
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
		Durable: false,
		// Persistent = broker redelivers unacknowledged messages on consumer
		// crash. The inflight lease provides exactly that (reclaimExpired), so
		// this must be true to stay consistent with Guarantee: AtLeastOnce.
		Persistent:        true,
		NativeRedelivery:  false,
		NativePriority:    queue.PrioritySoft,
		DelayedEnqueue:    true,
		StrictGlobalOrder: false,
		Replay:            false,
		Dedup:             true, // Effectively idempotent per message ID via last-write-wins (hash overwrite, single ZSet member); not a time window.
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

// inflightKey tracks delivered-but-unacked members. The score is the lease
// expiry (unix seconds). Unlike the ready/delayed ZSets, entries here are not
// popped; Dequeue reclaims members whose lease expired so a crashed consumer's
// messages are redelivered (the at-least-once guarantee Redis lacks natively).
func (r *RedisDriver) inflightKey() string {
	return r.prefix + "inflight_zset"
}

// inflightScoreKey remembers each in-flight member's original ready-queue score
// so a reclaimed (crashed-consumer) message returns to the ready ZSet at its
// original position instead of losing its priority.
func (r *RedisDriver) inflightScoreKey() string {
	return r.prefix + "inflight_scores"
}

// popToInflight atomically moves up to N highest-priority members from the ready
// ZSet into the in-flight ZSet stamped with the lease score. Doing the pop and
// lease-stamp in one script closes the window where a crash between the two
// would strand a message that is neither ready nor reclaimable. The member's
// original ready score is saved so reclaimExpired can restore its position.
var popToInflightScript = redis.NewScript(`
local items = redis.call('ZPOPMAX', KEYS[1], ARGV[1])
local out = {}
for i = 1, #items, 2 do
  local member = items[i]
  local score = items[i+1]
  redis.call('ZADD', KEYS[2], ARGV[2], member)
  redis.call('HSET', KEYS[3], member, score)
  out[#out+1] = member
end
return out
`)

// requeueInflight atomically removes a member from the in-flight ZSet and puts
// it back into the ready ZSet with the given score (used by Nack).
var requeueInflightScript = redis.NewScript(`
redis.call('ZREM', KEYS[2], ARGV[2])
redis.call('HDEL', KEYS[3], ARGV[2])
redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
return 1
`)

// reclaimExpiredScript atomically returns every expired in-flight member to the
// ready ZSet, restoring its original score when known. Because the ZRem and the
// ZAdd happen in one script, a consumer crash cannot lose a message between the
// two (the window the previous non-atomic implementation left open). The count
// of reclaimed members is returned.
var reclaimExpiredScript = redis.NewScript(`
local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[1])
local out = {}
for i = 1, #expired do
  local member = expired[i]
  local score = redis.call('HGET', KEYS[3], member)
  if not score then
    score = ARGV[2]
  end
  redis.call('ZADD', KEYS[1], score, member)
  redis.call('HDEL', KEYS[3], member)
  redis.call('ZREM', KEYS[2], member)
  out[#out+1] = member
end
return out
`)

// reclaimExpired redelivers in-flight members whose lease elapsed. The whole
// reclaim is performed by a single Lua script, so the removal from the in-flight
// ZSet and the reinsertion into the ready ZSet are atomic: a crash can never
// drop a message that was removed from in-flight but not yet requeued.
func (r *RedisDriver) reclaimExpired(ctx context.Context, nowScore float64) error {
	defaultScore := float64(queue.PriorityDefault)*1e9 - nowScore
	err := reclaimExpiredScript.Run(ctx, r.client,
		[]string{r.queueKey(), r.inflightKey(), r.inflightScoreKey()},
		fmt.Sprintf("%f", nowScore), defaultScore,
	).Err()
	if err == redis.Nil {
		return nil
	}
	return err
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
	now := time.Now()
	nowScore := float64(now.UnixNano()) / 1e9

	// 0. Reclaim in-flight messages whose lease expired (consumer crash recovery).
	if err := r.reclaimExpired(ctx, nowScore); err != nil {
		return nil, err
	}

	// 1. Migrate any matured delayed messages from delayedKey to queueKey.
	// The migration runs in a transaction (MULTI/EXEC) so a matured member is
	// never removed from the delayed set without also appearing in the ready
	// set, and errors are surfaced instead of silently swallowed. Each message's
	// payload is read first so its original priority is preserved.
	delayedMembers, err := r.client.ZRangeByScore(ctx, r.delayedKey(), &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%f", nowScore),
	}).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}

	if len(delayedMembers) > 0 {
		raws, err := r.client.HMGet(ctx, r.dataKey(), delayedMembers...).Result()
		if err != nil && err != redis.Nil {
			return nil, err
		}
		tx := r.client.TxPipeline()
		for i, mID := range delayedMembers {
			score := float64(queue.PriorityDefault)*1e9 - nowScore
			if i < len(raws) && raws[i] != nil {
				var msg queue.Message
				if err := json.Unmarshal(rawValue(raws[i]), &msg); err == nil {
					score = float64(msg.Priority)*1e9 - float64(msg.CreatedAt.Unix())
				}
			}
			tx.ZRem(ctx, r.delayedKey(), mID)
			tx.ZAdd(ctx, r.queueKey(), redis.Z{
				Score:  score,
				Member: mID,
			})
		}
		if _, err := tx.Exec(ctx); err != nil {
			return nil, err
		}
	}

	batchSize := req.BatchSize
	if batchSize <= 0 {
		batchSize = 20
	}

	// 2. Atomically pop highest-priority members into the in-flight ZSet with a lease.
	leaseScore := float64(now.Add(r.lease).UnixNano()) / 1e9
	res, err := popToInflightScript.Run(ctx, r.client,
		[]string{r.queueKey(), r.inflightKey(), r.inflightScoreKey()},
		batchSize, leaseScore).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, err
	}
	popped, _ := res.([]interface{})
	if len(popped) == 0 {
		return nil, nil
	}

	msgIDs := make([]string, 0, len(popped))
	for _, v := range popped {
		if id, ok := v.(string); ok {
			msgIDs = append(msgIDs, id)
		}
	}
	if len(msgIDs) == 0 {
		return nil, nil
	}

	rawItems, err := r.client.HMGet(ctx, r.dataKey(), msgIDs...).Result()
	if err != nil {
		return nil, err
	}

	deliveries := make([]*queue.Delivery, 0, len(rawItems))
	for i, raw := range rawItems {
		msgID := ""
		if i < len(msgIDs) {
			msgID = msgIDs[i]
		}
		if raw == nil {
			// Hash entry is missing while the ZSet member was already popped:
			// the payload is unrecoverable. Surface a poison delivery so the
			// consumer routes the loss to the DLQ instead of silently dropping it.
			deliveries = append(deliveries, &queue.Delivery{
				Message: queue.NewPoisonMessage(msgID, nil, fmt.Errorf("queue-redis: missing payload for message id %q", msgID), r.Name()),
				Token:   msgID,
			})
			continue
		}
		var msg queue.Message
		str := rawValue(raw)
		if err := json.Unmarshal(str, &msg); err != nil {
			// Corrupt payload: the ZSet member is already gone, so surface a
			// poison delivery instead of dropping the corrupt message silently.
			deliveries = append(deliveries, &queue.Delivery{
				Message: queue.NewPoisonMessage(msgID, str, err, r.Name()),
				Token:   msgID,
			})
			continue
		}
		deliveries = append(deliveries, &queue.Delivery{
			Message: &msg,
			Token:   msg.ID,
		})
	}

	return deliveries, nil
}

// Reenqueue reschedules a delivery by upserting the message into the delayed set.
// The message hash entry is overwritten in place (same message ID) so there is no
// ack-then-enqueue gap, and the in-flight entry is cleared so the lease cannot
// later reclaim it into the ready set as a duplicate.
func (r *RedisDriver) Reenqueue(ctx context.Context, d *queue.Delivery, msg *queue.Message, at time.Time) error {
	if d == nil || d.Token == nil {
		return nil
	}
	if err := r.EnqueueAt(ctx, msg, at); err != nil {
		return err
	}
	if msgID, ok := d.Token.(string); ok {
		pipe := r.client.Pipeline()
		pipe.ZRem(ctx, r.inflightKey(), msgID)
		pipe.HDel(ctx, r.inflightScoreKey(), msgID)
		if _, err := pipe.Exec(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Ack removes the processed message payload from the Redis hash and clears its
// in-flight lease.
func (r *RedisDriver) Ack(ctx context.Context, d *queue.Delivery) error {
	if d == nil || d.Token == nil {
		return nil
	}
	msgID, ok := d.Token.(string)
	if !ok {
		return fmt.Errorf("queue-redis: invalid delivery token type: %T", d.Token)
	}
	pipe := r.client.Pipeline()
	pipe.HDel(ctx, r.dataKey(), msgID)
	pipe.ZRem(ctx, r.inflightKey(), msgID)
	pipe.HDel(ctx, r.inflightScoreKey(), msgID)
	_, err := pipe.Exec(ctx)
	return err
}

// Nack restores the message back into the priority queue for retry and clears
// its in-flight lease in the same atomic step.
func (r *RedisDriver) Nack(ctx context.Context, d *queue.Delivery, reason error) error {
	if d == nil || d.Token == nil {
		return nil
	}
	msgID, ok := d.Token.(string)
	if !ok {
		return fmt.Errorf("queue-redis: invalid delivery token type: %T", d.Token)
	}
	prio := queue.PriorityDefault
	// Score the redelivery exactly like Enqueue does (priority*1e9 - CreatedAt),
	// so a nacked message returns to its original position within its priority
	// class instead of being pushed behind every newer message by time.Now().
	createdAt := time.Now()
	if d.Message != nil {
		prio = d.Message.Priority
		if !d.Message.CreatedAt.IsZero() {
			createdAt = d.Message.CreatedAt
		}
	}
	score := float64(prio)*1e9 - float64(createdAt.Unix())
	return requeueInflightScript.Run(ctx, r.client,
		[]string{r.queueKey(), r.inflightKey(), r.inflightScoreKey()}, score, msgID).Err()
}

// Close gracefully closes driver resources.
func (r *RedisDriver) Close() error {
	return nil
}

// rawValue normalizes a Redis bulk string (string or []byte) to []byte.
func rawValue(v interface{}) []byte {
	switch s := v.(type) {
	case string:
		return []byte(s)
	case []byte:
		return s
	default:
		return nil
	}
}
