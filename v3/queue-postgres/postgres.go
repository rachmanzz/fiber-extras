package queuepostgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rachmanzz/gosuite/v3/queue"
)

// DefaultTableName is the default table name for storing queue messages.
const DefaultTableName = "queue_messages"

// identifierRe validates SQL identifiers (table names) before they are
// interpolated into queries, preventing SQL injection via configuration.
var identifierRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// claim is the broker-opaque delivery token for PostgreSQL. It carries the row
// id together with the exact lease deadline stamped at claim time, so Ack, Nack
// and Reenqueue can verify they still own the row (status unchanged, lease not
// superseded by another worker) before mutating it.
type claim struct {
	id    string
	lease time.Time
}

// PostgresDriver implements queue.Adapter, queue.DelayedEnqueuer, and queue.Replayer
// using PostgreSQL SELECT ... FOR UPDATE SKIP LOCKED pattern. In-flight messages are
// guarded by a lease (visibility timeout); once the lease expires they are redelivered,
// so deliveries are never lost if a worker crashes.
type PostgresDriver struct {
	pool       *pgxpool.Pool
	tableName  string
	lease      time.Duration
	tableMu    sync.Mutex
	tableReady bool
}

// Option configures PostgresDriver.
type Option func(*PostgresDriver)

// DefaultVisibilityTimeout is the default lease duration held on in-flight messages.
const DefaultVisibilityTimeout = 3 * time.Minute

// WithTableName sets a custom table name for queue storage.
// The name must be a plain SQL identifier (letters, digits, underscore; not
// starting with a digit); otherwise it panics at construction time rather than
// risking SQL injection through interpolated queries.
func WithTableName(name string) Option {
	return func(p *PostgresDriver) {
		if name == "" {
			return
		}
		if !identifierRe.MatchString(name) {
			panic(fmt.Sprintf("queue-postgres: invalid table name %q: must match %s", name, identifierRe.String()))
		}
		// PostgreSQL truncates identifiers longer than NAMEDATALEN-1 (63) bytes,
		// which would silently point the driver at a different table.
		if len(name) > 63 {
			panic(fmt.Sprintf("queue-postgres: invalid table name %q: exceeds 63-byte identifier limit", name))
		}
		p.tableName = name
	}
}

// WithVisibilityTimeout sets how long a message may stay claimed (in-flight) before
// it becomes eligible for redelivery. A worker that crashes without Acking will have
// its messages redelivered once the lease expires.
func WithVisibilityTimeout(d time.Duration) Option {
	return func(p *PostgresDriver) {
		if d > 0 {
			p.lease = d
		}
	}
}

// New constructs a PostgresDriver from a pgxpool.Pool and options.
func New(pool *pgxpool.Pool, opts ...Option) *PostgresDriver {
	p := &PostgresDriver{
		pool:      pool,
		tableName: DefaultTableName,
		lease:     DefaultVisibilityTimeout,
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
		Durable:          true,
		Persistent:       true,
		NativeRedelivery: false,
		NativePriority:   queue.PriorityHard,
		DelayedEnqueue:   true,
		// Dequeue claims rows in priority order, but FOR UPDATE SKIP LOCKED lets
		// concurrent consumers claim out of order and independent deliveries are
		// dispatched to a parallel worker pool, so global FIFO is not guaranteed.
		StrictGlobalOrder: false,
		Replay:            true,
		Dedup:             false, // Primary key prevents duplicate inserts, but there is no dedup window.
		MaxBatchSize:      1000,
		Guarantee:         queue.AtLeastOnce,
	}
}

// EnsureTable creates the queue table and indexes if they do not exist. The DDL
// is executed at most once per driver instance: after a successful run the
// result is memoized, so repeated calls (e.g. on every reconnect) do not reissue
// the CREATE/ALTER/INDEX statements.
func (p *PostgresDriver) EnsureTable(ctx context.Context) error {
	p.tableMu.Lock()
	defer p.tableMu.Unlock()
	if p.tableReady {
		return nil
	}

	schema := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS %s (
		id TEXT PRIMARY KEY,
		topic TEXT NOT NULL,
		payload BYTEA NOT NULL,
		priority INT NOT NULL DEFAULT 50,
		group_id TEXT,
		group_order INT DEFAULT 0,
		attempts INT NOT NULL DEFAULT 0,
		max_attempts INT NOT NULL DEFAULT 3,
		status TEXT NOT NULL DEFAULT 'pending',
		process_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
		headers JSONB NOT NULL DEFAULT '{}'::jsonb,
		lease_until TIMESTAMPTZ,
		acked_at TIMESTAMPTZ
	);
	ALTER TABLE %s ADD COLUMN IF NOT EXISTS headers JSONB NOT NULL DEFAULT '{}'::jsonb;
	ALTER TABLE %s ADD COLUMN IF NOT EXISTS max_attempts INT NOT NULL DEFAULT 3;
	ALTER TABLE %s ADD COLUMN IF NOT EXISTS lease_until TIMESTAMPTZ;
	ALTER TABLE %s ADD COLUMN IF NOT EXISTS acked_at TIMESTAMPTZ;
	CREATE INDEX IF NOT EXISTS idx_%s_priority ON %s (status, process_at ASC, priority DESC, created_at ASC);
	CREATE INDEX IF NOT EXISTS idx_%s_active ON %s (priority DESC, group_order ASC, created_at ASC) WHERE status IN ('pending', 'processing');
	CREATE INDEX IF NOT EXISTS idx_%s_lease ON %s (status, lease_until);
	CREATE INDEX IF NOT EXISTS idx_%s_acked ON %s (status, acked_at);
	`, p.tableName, p.tableName, p.tableName, p.tableName, p.tableName, p.tableName, p.tableName, p.tableName, p.tableName, p.tableName, p.tableName, p.tableName, p.tableName)

	if _, err := p.pool.Exec(ctx, schema); err != nil {
		return err
	}
	p.tableReady = true
	return nil
}

// Enqueue inserts a message into the queue table with immediate processing time.
func (p *PostgresDriver) Enqueue(ctx context.Context, msg *queue.Message) error {
	return p.EnqueueAt(ctx, msg, time.Now())
}

// EnqueueAt inserts a message scheduled to be processed at a future timestamp.
func (p *PostgresDriver) EnqueueAt(ctx context.Context, msg *queue.Message, at time.Time) error {
	headers, err := marshalHeaders(msg.Headers)
	if err != nil {
		return fmt.Errorf("queue-postgres: failed to encode headers: %w", err)
	}

	// attempts never moves backwards on conflict: GREATEST keeps the higher of
	// the stored and incoming values, so re-enqueuing a message with a stale
	// attempt count cannot reset the retry counter (which would allow an
	// infinite retry loop).
	query := fmt.Sprintf(`
	INSERT INTO %s (id, topic, payload, priority, group_id, group_order, status, process_at, created_at, headers, attempts, max_attempts)
	VALUES ($1, $2, $3, $4, $5, $6, 'pending', $7, $8, $9::jsonb, $10, $11)
	ON CONFLICT (id) DO UPDATE
	SET status = 'pending', process_at = EXCLUDED.process_at, updated_at = now(),
		attempts = GREATEST(%s.attempts, EXCLUDED.attempts), max_attempts = EXCLUDED.max_attempts,
		lease_until = NULL
	`, p.tableName, p.tableName)

	_, err = p.pool.Exec(ctx, query,
		msg.ID, msg.Topic, nonNilPayload(msg.Payload), int(msg.Priority), msg.GroupID, msg.GroupOrder, at, msg.CreatedAt, headers, msg.Attempt, msg.MaxAttempts,
	)
	return err
}

// nonNilPayload normalizes a nil payload to an empty (but non-nil) byte slice.
// The payload column is NOT NULL, and pgx encodes a nil slice as SQL NULL,
// so enqueuing a message with an empty payload (valid for every other driver)
// would otherwise fail. An empty non-nil slice encodes as an empty bytea.
func nonNilPayload(b []byte) []byte {
	if b == nil {
		return []byte{}
	}
	return b
}

// Replay streams messages created since a given timestamp for auditing or
// recovery. It includes completed (acked, soft-deleted) rows — Ack retains them
// until PurgeAcked — so a replay can surface already-processed messages.
// The query is executed synchronously so connection/query failures are returned to
// the caller; streaming then honours ctx cancellation so a receiver that stops
// reading cannot leak the producer goroutine.
func (p *PostgresDriver) Replay(ctx context.Context, from time.Time) (<-chan *queue.Delivery, error) {
	query := fmt.Sprintf(`
	SELECT id, topic, payload, priority, group_id, group_order, created_at, attempts, max_attempts, headers
	FROM %s
	WHERE created_at >= $1
	ORDER BY created_at ASC, id ASC
	`, p.tableName)

	rows, err := p.pool.Query(ctx, query, from)
	if err != nil {
		return nil, fmt.Errorf("queue-postgres: replay query failed: %w", err)
	}
	defer rows.Close()

	// Rows are materialised synchronously so a Scan or iteration failure is
	// returned to the caller rather than silently swallowed inside a streaming
	// goroutine. The channel then streams the already-decoded deliveries.
	var deliveries []*queue.Delivery
	for rows.Next() {
		var msg queue.Message
		var prio int
		var groupID *string
		var groupOrder *int
		var headersRaw []byte
		if err := rows.Scan(
			&msg.ID, &msg.Topic, &msg.Payload, &prio, &groupID, &groupOrder, &msg.CreatedAt, &msg.Attempt, &msg.MaxAttempts, &headersRaw,
		); err != nil {
			return nil, fmt.Errorf("queue-postgres: replay scan failed: %w", err)
		}
		headers, err := unmarshalHeaders(headersRaw)
		if err != nil {
			return nil, err
		}
		msg.Headers = headers
		msg.Priority = queue.Priority(prio)
		if groupID != nil {
			msg.GroupID = *groupID
		}
		if groupOrder != nil {
			msg.GroupOrder = *groupOrder
		}
		deliveries = append(deliveries, &queue.Delivery{
			Message: &msg,
			Token:   claim{id: msg.ID},
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("queue-postgres: replay iteration failed: %w", err)
	}

	ch := make(chan *queue.Delivery, len(deliveries))
	for _, d := range deliveries {
		ch <- d
	}
	close(ch)
	return ch, nil
}

// Dequeue pulls available messages using SELECT ... FOR UPDATE SKIP LOCKED.
// Claimed messages are stamped with a lease (now() + visibility timeout); once the
// lease expires the row becomes eligible again, enabling crash-safe redelivery.
func (p *PostgresDriver) Dequeue(ctx context.Context, req queue.DequeueRequest) ([]*queue.Delivery, error) {
	batchSize := req.BatchSize
	if batchSize <= 0 {
		batchSize = 20
	}

	// The lease is expressed in milliseconds so sub-second visibility timeouts
	// are honoured rather than truncated to whole seconds by int64(Seconds()).
	leaseExpr := fmt.Sprintf("%d milliseconds", p.lease.Milliseconds())

	query := fmt.Sprintf(`
	WITH cte AS (
		SELECT id FROM %s
		WHERE status IN ('pending', 'processing')
		AND process_at <= now()
		AND (lease_until IS NULL OR lease_until < now())
		ORDER BY priority DESC, group_order ASC, created_at ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	)
	UPDATE %s q
	SET status = 'processing', lease_until = now() + $2::interval, updated_at = now()
	FROM cte
	WHERE q.id = cte.id
	RETURNING q.id, q.topic, q.payload, q.priority, q.group_id, q.group_order, q.created_at, q.attempts, q.max_attempts, q.headers, q.lease_until;
	`, p.tableName, p.tableName)

	rows, err := p.pool.Query(ctx, query, batchSize, leaseExpr)
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
		var leaseUntil time.Time
		if err := rows.Scan(
			&msg.ID, &msg.Topic, &msg.Payload, &prio, &groupID, &groupOrder, &msg.CreatedAt, &msg.Attempt, &msg.MaxAttempts, &headersRaw, &leaseUntil,
		); err != nil {
			return nil, err
		}
		headers, hErr := unmarshalHeaders(headersRaw)
		if hErr != nil {
			// Corrupt header JSON: the row is already claimed (status
			// 'processing'), so surface a poison delivery to route it to the DLQ
			// instead of failing the whole batch or dropping it silently.
			deliveries = append(deliveries, &queue.Delivery{
				Message: queue.NewPoisonMessage(msg.ID, headersRaw, hErr, p.Name()),
				Token:   claim{id: msg.ID, lease: leaseUntil},
			})
			continue
		}
		msg.Headers = headers
		msg.Priority = queue.Priority(prio)
		if groupID != nil {
			msg.GroupID = *groupID
		}
		if groupOrder != nil {
			msg.GroupOrder = *groupOrder
		}
		deliveries = append(deliveries, &queue.Delivery{
			Message: &msg,
			Token:   claim{id: msg.ID, lease: leaseUntil},
		})
	}
	return deliveries, nil
}

// ErrDeliveryLost is returned when a delivery token no longer owns its row,
// e.g. the lease expired and another worker already reclaimed it.
var ErrDeliveryLost = errors.New("queue-postgres: delivery is no longer owned (lease expired or already settled)")

// ErrNilDelivery is returned when Ack/Nack/Reenqueue is called with a nil
// delivery or a delivery that carries no token, surfacing a caller bug instead
// of silently succeeding.
var ErrNilDelivery = errors.New("queue-postgres: nil delivery or missing token")

// deliveryClaim extracts and validates the PostgreSQL claim token.
func deliveryClaim(d *queue.Delivery) (claim, error) {
	if d == nil || d.Token == nil {
		return claim{}, ErrNilDelivery
	}
	c, ok := d.Token.(claim)
	if !ok {
		return claim{}, fmt.Errorf("queue-postgres: invalid delivery token type: %T", d.Token)
	}
	return c, nil
}

// Reenqueue reschedules an in-flight delivery to a future time without the
// ack-then-enqueue crash window: the guarded UPDATE atomically rewrites the
// claimed row back to 'pending' with the new execution time (and clears its
// lease), so the message cannot be lost even if the process dies right after.
// It only succeeds while the caller still owns the row (status 'processing' and
// the same lease deadline); otherwise ErrDeliveryLost is returned so a stale
// worker cannot clobber a row another worker has since reclaimed.
func (p *PostgresDriver) Reenqueue(ctx context.Context, d *queue.Delivery, msg *queue.Message, at time.Time) error {
	c, err := deliveryClaim(d)
	if err != nil {
		return err
	}
	if c.id == "" {
		return nil
	}
	query := fmt.Sprintf(`
	UPDATE %s
	SET status = 'pending', process_at = $2, attempts = $3, max_attempts = $4,
		lease_until = NULL, updated_at = now()
	WHERE id = $1 AND status = 'processing' AND lease_until = $5
	`, p.tableName)
	tag, err := p.pool.Exec(ctx, query, c.id, at, msg.Attempt, msg.MaxAttempts, c.lease)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrDeliveryLost
	}
	return nil
}

// Ack marks the message as completed. The row is soft-deleted (status 'acked')
// rather than removed so Replay can still surface processed messages for
// auditing/recovery; use PurgeAcked to reclaim storage. Ack only affects the row
// while the caller still owns it (status 'processing' and matching lease).
func (p *PostgresDriver) Ack(ctx context.Context, d *queue.Delivery) error {
	c, err := deliveryClaim(d)
	if err != nil {
		return err
	}
	if c.id == "" {
		return nil
	}
	query := fmt.Sprintf("UPDATE %s SET status = 'acked', lease_until = NULL, acked_at = now(), updated_at = now() WHERE id = $1 AND status = 'processing' AND lease_until = $2", p.tableName)
	_, err = p.pool.Exec(ctx, query, c.id, c.lease)
	return err
}

// PurgeAcked permanently deletes completed (acked) rows acked before the given
// time, returning the number of rows removed. It is the retention mechanism for
// the soft-deleted rows Ack leaves behind for Replay.
func (p *PostgresDriver) PurgeAcked(ctx context.Context, before time.Time) (int64, error) {
	query := fmt.Sprintf("DELETE FROM %s WHERE status = 'acked' AND acked_at IS NOT NULL AND acked_at < $1", p.tableName)
	tag, err := p.pool.Exec(ctx, query, before)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// Nack releases the message back to 'pending' for redelivery and clears its
// lease. Like Ack it only affects the row while the caller still owns it.
func (p *PostgresDriver) Nack(ctx context.Context, d *queue.Delivery, reason error) error {
	c, err := deliveryClaim(d)
	if err != nil {
		return err
	}
	if c.id == "" {
		return nil
	}
	query := fmt.Sprintf("UPDATE %s SET status = 'pending', lease_until = NULL, updated_at = now() WHERE id = $1 AND status = 'processing' AND lease_until = $2", p.tableName)
	_, err = p.pool.Exec(ctx, query, c.id, c.lease)
	return err
}

// Close gracefully closes the driver.
func (p *PostgresDriver) Close() error {
	return nil
}

// marshalHeaders encodes message headers as a JSON object for the JSONB column.
// A marshal failure is returned to the caller instead of being masked as "{}".
func marshalHeaders(h map[string]string) (string, error) {
	if len(h) == 0 {
		return "{}", nil
	}
	b, err := json.Marshal(h)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// unmarshalHeaders decodes a JSONB header column back into a map. A corrupt
// header value is reported as an error instead of being silently dropped.
func unmarshalHeaders(raw []byte) (map[string]string, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var h map[string]string
	if err := json.Unmarshal(raw, &h); err != nil {
		return nil, fmt.Errorf("queue-postgres: corrupt headers JSON: %w", err)
	}
	return h, nil
}
