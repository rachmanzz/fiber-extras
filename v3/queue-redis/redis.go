package queueredis

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rachmanzz/gosuite/v3/queue"
	"github.com/redis/go-redis/v9"
	"github.com/vmihailenco/msgpack/v5"
)

// ErrNilMessage is returned when attempting to enqueue a nil message.
var ErrNilMessage = errors.New("queue-redis: cannot enqueue nil message")

// DefaultPrefix is the default key prefix for Redis queue data structures.
const DefaultPrefix = "gosuite:queue:"

// DefaultVisibilityTimeout is the default lease held on in-flight messages.
// A delivered message stays in-flight until Ack/Nack; if the consumer crashes
// and the lease expires, the next Dequeue redelivers it (at-least-once).
const DefaultVisibilityTimeout = 30 * time.Second

// Wire format (split transport): the message hash stores the user payload as
// raw bytes under "<id>:p" and the adapter metadata msgpack-encoded under
// "<id>:m". The payload never passes through any text encoding, so no base64
// tax applies. This is the only wire format: a record that is missing or
// cannot be decoded surfaces as a poison delivery instead of being guessed.
const (
	payloadFieldSuffix = ":p"
	metaFieldSuffix    = ":m"
)

func payloadField(id string) string { return id + payloadFieldSuffix }

func metaField(id string) string { return id + metaFieldSuffix }

// wireMeta is the v2 adapter-side metadata envelope. It is msgpack-encoded so
// metadata values stay binary-safe: header values (e.g. a poison message's
// raw_payload) round-trip losslessly, unlike the v1 JSON envelope which
// replaces invalid UTF-8.
type wireMeta struct {
	ID          string
	Topic       string
	Priority    queue.Priority
	GroupID     string
	GroupOrder  int
	Headers     map[string]string
	CreatedAt   time.Time
	Attempt     int
	MaxAttempts int
	Driver      string
}

func metaFromMessage(msg *queue.Message) wireMeta {
	return wireMeta{
		ID:          msg.ID,
		Topic:       msg.Topic,
		Priority:    msg.Priority,
		GroupID:     msg.GroupID,
		GroupOrder:  msg.GroupOrder,
		Headers:     msg.Headers,
		CreatedAt:   msg.CreatedAt,
		Attempt:     msg.Attempt,
		MaxAttempts: msg.MaxAttempts,
		Driver:      msg.Driver,
	}
}

func (m wireMeta) toMessage(payload []byte) *queue.Message {
	return &queue.Message{
		ID:          m.ID,
		Topic:       m.Topic,
		Payload:     payload,
		Priority:    m.Priority,
		GroupID:     m.GroupID,
		GroupOrder:  m.GroupOrder,
		Headers:     m.Headers,
		CreatedAt:   m.CreatedAt,
		Attempt:     m.Attempt,
		MaxAttempts: m.MaxAttempts,
		Driver:      m.Driver,
	}
}

// encodeMeta serializes the v2 metadata; the payload itself is not touched
// (it is stored raw under its own hash field, zero-copy).
func encodeMeta(msg *queue.Message) ([]byte, error) {
	return msgpack.Marshal(metaFromMessage(msg))
}

func decodeMeta(b []byte) (wireMeta, error) {
	var m wireMeta
	if err := msgpack.Unmarshal(b, &m); err != nil {
		return wireMeta{}, err
	}
	return m, nil
}

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

// migrateDelayedScript atomically migrates matured delayed members to the
// ready ZSet. For each (member, score) argument it first ZREMs the member from
// the delayed set and only ZADDs it to the ready set when that removal actually
// happened. This makes concurrent migrations safe: when two Dequeue calls both
// observe the same matured member, exactly one of them removes it (ZREM
// returns 1) and therefore exactly one copy ever reaches the ready set. The
// previous two-step implementation (ZRangeByScore -> TxPipeline ZRem+ZAdd)
// allowed a second, overlapping migration to re-add a member that had already
// been popped, delivered and Acked -- whose hash entry was then already gone
// -- producing a duplicate delivery followed by a poison "missing payload".
var migrateDelayedScript = redis.NewScript(`
local out = {}
for i = 1, #ARGV, 2 do
  local member = ARGV[i]
  local score = ARGV[i+1]
  if redis.call('ZREM', KEYS[1], member) == 1 then
    redis.call('ZADD', KEYS[2], score, member)
    out[#out+1] = member
  end
end
return out
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

// enqueue writes the v2 split record (raw payload + msgpack metadata) into the
// message hash and adds the message to the given ZSet with the given score.
func (r *RedisDriver) enqueue(ctx context.Context, msg *queue.Message, key string, score float64) error {
	if msg == nil {
		return ErrNilMessage
	}
	meta, err := encodeMeta(msg)
	if err != nil {
		return fmt.Errorf("queue-redis: failed to encode metadata: %w", err)
	}
	payload := msg.Payload
	if payload == nil {
		payload = []byte{}
	}

	pipe := r.client.Pipeline()
	pipe.HSet(ctx, r.dataKey(),
		payloadField(msg.ID), payload,
		metaField(msg.ID), meta,
	)
	pipe.ZAdd(ctx, key, redis.Z{Score: score, Member: msg.ID})
	_, err = pipe.Exec(ctx)
	return err
}

// Enqueue serializes and pushes a message into the Redis priority ZSet and message hash.
func (r *RedisDriver) Enqueue(ctx context.Context, msg *queue.Message) error {
	if msg == nil {
		return ErrNilMessage
	}
	// Score is Priority (higher priority = higher score).
	// Sub-score tie-breaker: timestamp unix seconds deducted
	score := float64(msg.Priority)*1e9 - float64(msg.CreatedAt.Unix())
	return r.enqueue(ctx, msg, r.queueKey(), score)
}

// EnqueueAt schedules a message to become active at a specific future timestamp.
func (r *RedisDriver) EnqueueAt(ctx context.Context, msg *queue.Message, at time.Time) error {
	if msg == nil {
		return ErrNilMessage
	}
	// In delayed ZSet, score is unix timestamp with fractional seconds
	score := float64(at.UnixNano()) / 1e9
	return r.enqueue(ctx, msg, r.delayedKey(), score)
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
		// Read each message's msgpack metadata to preserve its original
		// priority during migration.
		fields := make([]string, 0, len(delayedMembers))
		for _, mID := range delayedMembers {
			fields = append(fields, metaField(mID))
		}
		raws, err := r.client.HMGet(ctx, r.dataKey(), fields...).Result()
		if err != nil && err != redis.Nil {
			return nil, err
		}
		args := make([]interface{}, 0, len(delayedMembers)*2)
		for i, mID := range delayedMembers {
			score := float64(queue.PriorityDefault)*1e9 - nowScore
			if i < len(raws) {
				if metaRaw := rawValue(raws[i]); metaRaw != nil {
					if meta, err := decodeMeta(metaRaw); err == nil {
						score = float64(meta.Priority)*1e9 - float64(meta.CreatedAt.Unix())
					}
				}
			}
			args = append(args, mID, fmt.Sprintf("%f", score))
		}
		// The migration is conditional on the ZREM (see migrateDelayedScript):
		// an overlapping migration that lost the race contributes nothing, so
		// a member can never be re-added to the ready set after it was
		// already delivered and Acked.
		if _, err := migrateDelayedScript.Run(ctx, r.client,
			[]string{r.delayedKey(), r.queueKey()}, args...).Result(); err != nil {
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

	// Fetch the split record for each popped ID: "<id>:p" (raw payload) and
	// "<id>:m" (msgpack metadata).
	fields := make([]string, 0, len(msgIDs)*2)
	for _, id := range msgIDs {
		fields = append(fields, payloadField(id), metaField(id))
	}
	rawItems, err := r.client.HMGet(ctx, r.dataKey(), fields...).Result()
	if err != nil {
		return nil, err
	}

	deliveries := make([]*queue.Delivery, 0, len(msgIDs))
	for i, msgID := range msgIDs {
		v2Payload := rawValue(rawItems[i*2])
		v2Meta := rawValue(rawItems[i*2+1])
		if v2Payload == nil || v2Meta == nil {
			// The split record is incomplete while the ZSet member was already
			// popped: the payload is unrecoverable. Surface a poison delivery
			// so the consumer routes the loss to the DLQ instead of silently
			// dropping it.
			deliveries = append(deliveries, &queue.Delivery{
				Message: queue.NewPoisonMessage(msgID, nil, fmt.Errorf("queue-redis: missing payload for message id %q", msgID), r.Name()),
				Token:   msgID,
			})
			continue
		}
		meta, err := decodeMeta(v2Meta)
		if err != nil {
			deliveries = append(deliveries, &queue.Delivery{
				Message: queue.NewPoisonMessage(msgID, v2Payload, fmt.Errorf("queue-redis: corrupt metadata: %w", err), r.Name()),
				Token:   msgID,
			})
			continue
		}
		msg := meta.toMessage(v2Payload)
		deliveries = append(deliveries, &queue.Delivery{Message: msg, Token: msg.ID})
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
	pipe.HDel(ctx, r.dataKey(), payloadField(msgID), metaField(msgID))
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
