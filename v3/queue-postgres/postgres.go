package queuepostgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rachmanzz/fiber-extras/v3/queue"
)

// DefaultTableName is the default table name for storing queue messages.
const DefaultTableName = "queue_messages"

// PostgresDriver implements queue.Adapter, queue.DelayedEnqueuer, and queue.Replayer
// using PostgreSQL SELECT ... FOR UPDATE SKIP LOCKED pattern.
type PostgresDriver struct {
	pool      *pgxpool.Pool
	tableName string
}

// Option configures PostgresDriver.
type Option func(*PostgresDriver)

// WithTableName sets a custom table name for queue storage.
func WithTableName(name string) Option {
	return func(p *PostgresDriver) {
		if name != "" {
			p.tableName = name
		}
	}
}

// New constructs a PostgresDriver from a pgxpool.Pool and options.
func New(pool *pgxpool.Pool, opts ...Option) *PostgresDriver {
	p := &PostgresDriver{
		pool:      pool,
		tableName: DefaultTableName,
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

// Name returns the driver identifier.
func (p *PostgresDriver) Name() string {
	return "postgres"
}

// Capabilities declares capabilities of PostgreSQL transactional queue.
func (p *PostgresDriver) Capabilities() queue.Capabilities {
	return queue.Capabilities{
		Durable:           true,
		Persistent:        true,
		NativeRedelivery:  false,
		NativePriority:    queue.PriorityHard,
		DelayedEnqueue:    true,
		StrictGlobalOrder: true,
		Replay:            true,
		Dedup:             true,
		MaxBatchSize:      1000,
		Guarantee:         queue.AtLeastOnce,
	}
}

// EnsureTable creates the queue table and indexes if they do not exist.
func (p *PostgresDriver) EnsureTable(ctx context.Context) error {
	schema := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		id TEXT PRIMARY KEY,
		topic TEXT NOT NULL,
		payload BYTEA NOT NULL,
		priority INT NOT NULL DEFAULT 50,
		group_id TEXT,
		group_order INT DEFAULT 0,
		attempts INT DEFAULT 0,
		status TEXT NOT NULL DEFAULT 'pending',
		process_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		headers JSONB NOT NULL DEFAULT '{}'::jsonb
	);
	ALTER TABLE %s ADD COLUMN IF NOT EXISTS headers JSONB NOT NULL DEFAULT '{}'::jsonb;
	CREATE INDEX IF NOT EXISTS idx_%s_priority ON %s (status, process_at ASC, priority DESC, created_at ASC);
	`, p.tableName, p.tableName, p.tableName, p.tableName)

	_, err := p.pool.Exec(ctx, schema)
	return err
}

// Enqueue inserts a message into the queue table with immediate processing time.
func (p *PostgresDriver) Enqueue(ctx context.Context, msg *queue.Message) error {
	return p.EnqueueAt(ctx, msg, time.Now())
}

// EnqueueAt inserts a message scheduled to be processed at a future timestamp.
func (p *PostgresDriver) EnqueueAt(ctx context.Context, msg *queue.Message, at time.Time) error {
	query := fmt.Sprintf(`
	INSERT INTO %s (id, topic, payload, priority, group_id, group_order, status, process_at, created_at, headers)
	VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7, $8, $9::jsonb)
	ON CONFLICT (id) DO UPDATE
	SET status = 'pending', process_at = EXCLUDED.process_at, updated_at = now()
	`, p.tableName)

	_, err := p.pool.Exec(ctx, query,
		msg.ID, msg.Topic, msg.Payload, int(msg.Priority), msg.GroupID, msg.GroupOrder, at, msg.CreatedAt, marshalHeaders(msg.Headers),
	)
	return err
}

// Replay streams messages created since a given timestamp for auditing or recovery.
func (p *PostgresDriver) Replay(ctx context.Context, from time.Time) (<-chan *queue.Delivery, error) {
	ch := make(chan *queue.Delivery, 100)
	go func() {
		defer close(ch)
		query := fmt.Sprintf(`
		SELECT id, topic, payload, priority, group_id, group_order, created_at, attempts, headers
		FROM %s
		WHERE created_at >= $1
		ORDER BY created_at ASC
		`, p.tableName)

		rows, err := p.pool.Query(ctx, query, from)
		if err != nil {
			return
		}
		defer rows.Close()

		for rows.Next() {
			var msg queue.Message
			var prio int
			var groupID *string
			var groupOrder *int
			var headersRaw []byte
			if err := rows.Scan(
				&msg.ID, &msg.Topic, &msg.Payload, &prio, &groupID, &groupOrder, &msg.CreatedAt, &msg.Attempt, &headersRaw,
			); err != nil {
				return
			}
			msg.Headers = unmarshalHeaders(headersRaw)
			msg.Priority = queue.Priority(prio)
			if groupID != nil {
				msg.GroupID = *groupID
			}
			if groupOrder != nil {
				msg.GroupOrder = *groupOrder
			}
			ch <- &queue.Delivery{Message: &msg, Token: msg.ID}
		}
	}()
	return ch, nil
}

// Dequeue pulls available messages using SELECT ... FOR UPDATE SKIP LOCKED.
func (p *PostgresDriver) Dequeue(ctx context.Context, req queue.DequeueRequest) ([]*queue.Delivery, error) {
	batchSize := req.BatchSize
	if batchSize <= 0 {
		batchSize = 20
	}

	query := fmt.Sprintf(`
	WITH cte AS (
		SELECT id FROM %s
		WHERE status = 'pending' AND process_at <= now()
		ORDER BY priority DESC, group_order ASC, created_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	)
	UPDATE %s q
	SET status = 'processing', updated_at = now()
	FROM cte
	WHERE q.id = cte.id
	RETURNING q.id, q.topic, q.payload, q.priority, q.group_id, q.group_order, q.created_at, q.attempts, q.headers;
	`, p.tableName, p.tableName)

	rows, err := p.pool.Query(ctx, query, batchSize)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var deliveries []*queue.Delivery
	for rows.Next() {
		var msg queue.Message
		var prio int
		var groupID *string
		var groupOrder *int
		var headersRaw []byte
		if err := rows.Scan(
			&msg.ID, &msg.Topic, &msg.Payload, &prio, &groupID, &groupOrder, &msg.CreatedAt, &msg.Attempt, &headersRaw,
		); err != nil {
			return nil, err
		}
		msg.Headers = unmarshalHeaders(headersRaw)
		msg.Priority = queue.Priority(prio)
		if groupID != nil {
			msg.GroupID = *groupID
		}
		if groupOrder != nil {
			msg.GroupOrder = *groupOrder
		}
		deliveries = append(deliveries, &queue.Delivery{
			Message: &msg,
			Token:   msg.ID,
		})
	}
	return deliveries, nil
}

// Ack removes the message from the queue table upon successful completion.
func (p *PostgresDriver) Ack(ctx context.Context, d *queue.Delivery) error {
	if d == nil || d.Token == nil {
		return nil
	}
	msgID := d.Token
	query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", p.tableName)
	_, err := p.pool.Exec(ctx, query, msgID)
	return err
}

// Nack restores the message status to 'pending' for redelivery.
func (p *PostgresDriver) Nack(ctx context.Context, d *queue.Delivery, reason error) error {
	if d == nil || d.Token == nil {
		return nil
	}
	msgID := d.Token
	query := fmt.Sprintf("UPDATE %s SET status = 'pending', updated_at = now() WHERE id = $1", p.tableName)
	_, err := p.pool.Exec(ctx, query, msgID)
	return err
}

// Close gracefully closes the driver.
func (p *PostgresDriver) Close() error {
	return nil
}

// marshalHeaders encodes message headers as a JSON object for the JSONB column.
func marshalHeaders(h map[string]string) string {
	if len(h) == 0 {
		return "{}"
	}
	b, err := json.Marshal(h)
	if err != nil {
		return "{}"
	}
	return string(b)
}

// unmarshalHeaders decodes a JSONB header column back into a map.
func unmarshalHeaders(raw []byte) map[string]string {
	if len(raw) == 0 {
		return nil
	}
	var h map[string]string
	if err := json.Unmarshal(raw, &h); err != nil {
		return nil
	}
	return h
}
