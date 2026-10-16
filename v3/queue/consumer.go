package queue

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rachmanzz/gosuite/v3/worker"
)

var (
	ErrNoHandlerFound = errors.New("no handler registered for topic")
)

type groupBatch struct {
	task       worker.Task
	deliveries []*Delivery
	release    func()
}

type groupRetryHead struct {
	id, topic    string
	order        int
	sequence     uint64
	blockedUntil time.Time
}

type groupLockEntry struct {
	unsettled         []groupRetryHead
	running           bool
	pending           []groupBatch
	mu                sync.Mutex
	refs              int
	blockedUntil      time.Time
	retryHeadID       string
	retryHeadTopic    string
	retryHeadOrder    int
	retryHeadSequence uint64
	nextSequence      uint64
}

type pendingRetry struct {
	msg     *Message
	del     *Delivery
	release func()
}

// Consumer handles polling messages from the driver and executing them via worker.Dispatcher.
type Consumer struct {
	driver                 Driver
	router                 *Router
	workerDispatcher       worker.Dispatcher
	batchSize              int
	pollInterval           time.Duration
	retryPolicy            RetryPolicy
	logger                 Logger
	quit                   chan struct{}
	ctx                    context.Context
	cancel                 context.CancelFunc
	wg                     sync.WaitGroup
	closed                 bool
	mu                     sync.Mutex
	closeOnce              sync.Once
	groupLocks             map[string]*groupLockEntry
	groupMu                sync.Mutex
	retryTimers            map[*time.Timer]pendingRetry
	retryCallbacks         sync.WaitGroup
	inFlight               sync.WaitGroup
	memoryBudget           *MemoryBudget
	memoryEstimate         int64
	memoryEstimateExplicit bool
	settlementSlots        chan struct{}
}

// ConsumerOption configures parameters of a Consumer.
type ConsumerOption func(*Consumer)

func WithBatchSize(size int) ConsumerOption {
	return func(c *Consumer) {
		if size > 0 {
			c.batchSize = size
		}
	}
}

func WithPollInterval(interval time.Duration) ConsumerOption {
	return func(c *Consumer) {
		if interval > 0 {
			c.pollInterval = interval
		}
	}
}

func WithRetryPolicy(policy RetryPolicy) ConsumerOption {
	return func(c *Consumer) {
		c.retryPolicy = policy
	}
}

func WithLogger(l Logger) ConsumerOption {
	return func(c *Consumer) {
		if l != nil {
			c.logger = l
		}
	}
}

func WithWorkerDispatcher(disp worker.Dispatcher) ConsumerOption {
	return func(c *Consumer) {
		if disp != nil {
			c.workerDispatcher = disp
		}
	}
}

// NewConsumer constructs a new queue consumer for a specific driver.
func NewConsumer(d Driver, r *Router, disp worker.Dispatcher, opts ...ConsumerOption) *Consumer {
	if disp == nil {
		disp = worker.GetDispatcher()
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &Consumer{
		driver:           d,
		router:           r,
		workerDispatcher: disp,
		batchSize:        50,
		memoryEstimate:   64 * 1024,
		settlementSlots:  make(chan struct{}, 1),
		pollInterval:     50 * time.Millisecond,
		retryPolicy:      DefaultRetryPolicy(),
		logger:           DefaultLogger(),
		quit:             make(chan struct{}),
		ctx:              ctx,
		cancel:           cancel,
		groupLocks:       make(map[string]*groupLockEntry),
		retryTimers:      make(map[*time.Timer]pendingRetry),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Start launches the consumer polling loop in a background goroutine.
func (c *Consumer) Start() {
	c.wg.Add(1)
	go c.pollLoop()
	c.logger.Info("queue consumer started",
		"driver", c.driver.Name(),
		"poll_interval", c.pollInterval,
	)
}

func (c *Consumer) pollLoop() {
	defer c.wg.Done()
	ticker := time.NewTicker(c.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.quit:
			return
		case <-ticker.C:
			// Adaptive drain loop: if a batch is full, immediately pull the next batch
			// without waiting for the next timer tick. Only sleep when empty or partial.
			for {
				select {
				case <-c.quit:
					return
				default:
				}

				count := c.fetchAndProcess()
				drainSize := c.batchSize
				if c.memoryBudget != nil {
					drainSize = 1
				}
				if count < drainSize {
					break
				}
			}
		}
	}
}

func (c *Consumer) fetchAndProcess() int {
	ctx := c.ctx
	req := DequeueRequest{
		BatchSize: c.batchSize,
	}
	var admission *MemoryReservation
	if c.memoryBudget != nil {
		req.BatchSize = 1
		estimate := c.admissionMemoryEstimate()
		var err error
		admission, err = c.memoryBudget.Claim(ctx, estimate)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				c.logger.Error("queue memory admission rejected", "estimate", estimate, "limit", c.memoryBudget.Limit(), "error", err)
			}
			return 0
		}
	}
	deliveries, dequeueErr := c.driver.Dequeue(ctx, req)
	if dequeueErr != nil && !errors.Is(dequeueErr, context.Canceled) {
		c.logger.Error("queue consumer dequeue error", "error", dequeueErr, "driver", c.driver.Name())
	}
	// A broker may have claimed deliveries before a later fetch failed. They
	// still require admission, ordering and settlement, even with an error.
	if len(deliveries) == 0 {
		if admission != nil {
			_ = admission.Close()
		}
		return 0
	}
	// Keep the logical admission position until settlement, including Nacks
	// before execution. Temporary references protect entries during handoff.
	type admissionRef struct {
		gid   string
		entry *groupLockEntry
	}
	var refs []admissionRef
	entries := make(map[*Delivery]*groupLockEntry)
	defer func() {
		for _, ref := range refs {
			c.releaseGroupRef(ref.gid, ref.entry)
		}
	}()
	for _, d := range deliveries {
		if d == nil || d.Message == nil || d.Message.GroupID == "" {
			continue
		}
		m := d.Message
		entry := c.acquireGroupLock(m.GroupID)
		refs = append(refs, admissionRef{gid: m.GroupID, entry: entry})
		entries[d] = entry
		c.rememberGroupDelivery(entry, m)
	}
	// Stop may cancel polling after the broker claimed this partial batch.
	// Remember group positions before returning it so an unsettled original
	// cannot be bypassed; cancellation must not disable the Nack operation.
	if cancelErr := ctx.Err(); cancelErr != nil {
		if admission != nil {
			_ = admission.Close()
		}
		settleCtx := context.WithoutCancel(ctx)
		for _, d := range deliveries {
			if d == nil || d.Message == nil {
				continue
			}
			c.updateGroupSettlement(entries[d], d.Message, true, c.pollInterval)
			if err := c.driver.Nack(settleCtx, d, cancelErr); err != nil {
				c.logger.Error("failed to return delivery after dequeue cancellation", "topic", d.Message.Topic, "msg_id", d.Message.ID, "error", err)
			}
		}
		return 0
	}
	var scopes map[*Delivery]*memoryScope
	if c.memoryBudget != nil {
		scopes = make(map[*Delivery]*memoryScope)
		accepted := make([]*Delivery, 0, len(deliveries))
		blockedGroups := make(map[string]bool)
		for _, del := range deliveries {
			if del == nil || del.Message == nil {
				continue
			}
			m := del.Message
			size := messageMemorySize(m)
			estimate := c.admissionMemoryEstimate()
			if size < estimate {
				size = estimate
			}
			var claim *MemoryReservation
			var claimErr error
			if m.GroupID != "" && blockedGroups[m.GroupID] {
				claimErr = ErrMemoryBudgetExceeded
			} else if admission != nil {
				claim = admission
				admission = nil
				if size > claim.Bytes() {
					claimErr = claim.TryGrow(size - claim.Bytes())
				}
			} else {
				claim, claimErr = c.memoryBudget.TryClaim(size)
			}
			if claimErr != nil {
				if claim != nil {
					_ = claim.Close()
				}
				if m.GroupID != "" {
					blockedGroups[m.GroupID] = true
				}
				settleCtx := context.WithoutCancel(ctx)
				if errors.Is(claimErr, ErrMemoryClaimTooLarge) {
					if err := c.settleAdmissionFailure(settleCtx, del, claimErr); err != nil {
						c.logger.Error("failed to settle rejected message", "topic", m.Topic, "msg_id", m.ID, "error", err)
					}
				} else {
					c.updateGroupSettlement(entries[del], m, true, c.pollInterval)
					_ = c.driver.Nack(settleCtx, del, claimErr)
				}
				continue
			}
			scopes[del] = newMemoryScope(claim)
			accepted = append(accepted, del)
		}
		if admission != nil {
			_ = admission.Close()
		}
		deliveries = accepted
	}

	// Group deliveries by GroupID
	grouped := make(map[string][]*Delivery)
	var independent []*Delivery

	for _, del := range deliveries {
		if del == nil {
			continue
		}
		if del.Message != nil && del.Message.GroupID != "" {
			grouped[del.Message.GroupID] = append(grouped[del.Message.GroupID], del)
		} else {
			independent = append(independent, del)
		}
	}

	// dispatched counts deliveries successfully handed off to the worker engine.
	// Backpressure rejects (ErrQueueFull) are Nacked and NOT counted, so the
	// adaptive drain loop stops pulling new batches when the engine is saturated
	// instead of hot-looping on immediately-redelivered Nacked messages.
	dispatched := 0

	// 1. Process Independent Deliveries (Dispatched to worker pool)
	for _, del := range independent {
		d := del
		m := d.Message
		if m == nil {
			continue
		}
		scope := scopes[d]
		c.inFlight.Add(1)
		task := worker.NewTaskFunc(m.Topic, func(tCtx context.Context, subPool worker.SubWorkerPool) error {
			defer c.inFlight.Done()
			defer scope.finish()

			// Bridge consumer lifecycle context (c.ctx) into the worker's execution context (tCtx)
			execCtx, execCancel := context.WithCancel(memoryContext(tCtx, scope))
			defer execCancel()
			stopWatch := context.AfterFunc(c.ctx, execCancel)
			defer stopWatch()

			settleCtx := context.WithoutCancel(execCtx)

			handler, exists := c.router.Get(m.Topic)
			if !exists {
				_, _, err := c.handleFailure(settleCtx, d, ErrNoHandlerFound)
				return err
			}

			// Execute handler with access to SubWorkerPool for parallel fan-out.
			// A handler panic must flow through the failure path (retry/DLQ) so the
			// delivery is never silently leaked as stuck in-flight.
			var handlerErr error
			func() {
				defer func() {
					if r := recover(); r != nil {
						c.logger.Error("handler panic recovered",
							"topic", m.Topic, "msg_id", m.ID, "group", m.GroupID,
							"panic", fmt.Sprint(r), "stack", string(debug.Stack()),
						)
						handlerErr = fmt.Errorf("handler panic on topic %q: %v", m.Topic, r)
					}
				}()
				handlerErr = handler(execCtx, m, memoryPool(execCtx, subPool, scope))
			}()

			// Join the handler's fan-out before settling the delivery. The
			// handler may spawn sub-workers and (mistakenly) return without
			// calling pool.Wait(); settling first would Ack (or retry) the
			// delivery while its sub-workers are still running, so a crash in
			// between would lose the message's work (at-least-once violation).
			if waitErr := subPool.Wait(); waitErr != nil && handlerErr == nil {
				handlerErr = waitErr
			}

			if handlerErr != nil {
				_, _, err := c.handleFailure(settleCtx, d, handlerErr)
				return err
			}

			return c.driver.Ack(settleCtx, d)
		})

		err := c.workerDispatcher.Dispatch(ctx, task)
		if err != nil {
			c.inFlight.Done()
			scope.finish()
			// If worker engine task queue is full (backpressure), do not drop message!
			// Nack immediately so broker/adapter can redeliver later when worker has capacity.
			_ = c.driver.Nack(context.WithoutCancel(ctx), d, err)
			continue
		}
		dispatched++
	}

	// 2. Process Grouped Deliveries in strict GroupOrder (FIFO per group).
	// Members of a group are executed sequentially in ascending GroupOrder and a
	// per-group lock serializes the group across consecutive polls, so a later
	// batch can never overtake an in-flight one.
	for groupID, dels := range grouped {
		gID := groupID
		groupDeliveries := dels
		groupScopes := make(map[*Delivery]*memoryScope, len(dels))
		for _, d := range dels {
			groupScopes[d] = scopes[d]
		}
		releaseGroupMemory := func() {
			for _, s := range groupScopes {
				s.finish()
			}
		}

		entry := c.acquireGroupLock(gID)

		// The engine's per-task pool is intentionally unused here: grouped
		// members get a fresh pool each (see below) so a sub-worker failure in
		// one member cannot cancel the fan-out of the next.
		c.inFlight.Add(1)
		groupTask := worker.NewTaskFunc("group:"+gID, func(tCtx context.Context, _ worker.SubWorkerPool) error {
			defer c.inFlight.Done()
			defer releaseGroupMemory()

			// Bridge consumer lifecycle context (c.ctx) into the group execution context (tCtx)
			execCtx, execCancel := context.WithCancel(tCtx)
			defer execCancel()
			stopWatch := context.AfterFunc(c.ctx, execCancel)
			defer stopWatch()

			settleCtx := context.WithoutCancel(execCtx)

			// A single drain executes queued batches in poll order. The lock
			// protects the batch lifecycle and quarantine bookkeeping.
			entry.mu.Lock()
			defer c.releaseGroupLock(gID, entry)

			ordered := make([]*Delivery, len(groupDeliveries))
			copy(ordered, groupDeliveries)
			sort.SliceStable(ordered, func(i, j int) bool {
				mi, mj := ordered[i].Message, ordered[j].Message
				if mi == nil {
					return false
				}
				if mj == nil {
					return true
				}
				return mi.GroupOrder < mj.GroupOrder
			})

			for idx, item := range ordered {
				d := item
				m := d.Message
				if m == nil {
					continue
				}
				// Backoff expiry is not proof that the retry has returned from
				// the broker. Keep later members behind the unsettled head.
				c.groupMu.Lock()
				retryHeadID := entry.retryHeadID
				retryHeadTopic := entry.retryHeadTopic
				retryHeadOrder := entry.retryHeadOrder
				retryHeadSequence := entry.retryHeadSequence
				var groupSequence uint64
				for _, head := range entry.unsettled {
					if head.id == m.ID && head.topic == m.Topic {
						groupSequence = head.sequence
						break
					}
				}
				blockedUntil := entry.blockedUntil
				c.groupMu.Unlock()
				if (retryHeadID != "" || retryHeadTopic != "") && (m.GroupOrder > retryHeadOrder || (m.GroupOrder == retryHeadOrder && groupSequence >= retryHeadSequence)) && (m.ID != retryHeadID || m.Topic != retryHeadTopic || time.Now().Before(blockedUntil)) {
					delay := time.Until(blockedUntil)
					if delay < c.pollInterval {
						delay = c.pollInterval
					}
					reason := fmt.Errorf("group %q waiting for retry head %q", gID, retryHeadID)
					if err := c.delayDelivery(memoryContext(settleCtx, groupScopes[d]), d, delay, reason); err != nil {
						c.logger.Error("failed to delay group member behind retry head", "group", gID, "msg_id", m.ID, "error", err)
						if !errors.Is(err, ErrDeliveryLost) {
							_ = c.driver.Nack(settleCtx, d, err)
						}
					}
					continue
				}
				handler, exists := c.router.Get(m.Topic)
				if !exists {
					c.handleGroupFailure(memoryContext(settleCtx, groupScopes[d]), entry, ordered, idx, ErrNoHandlerFound, groupSequence, groupScopes)
					break
				}

				// A fresh sub-worker pool per member keeps one member's fan-out
				// isolated: its failure/cancellation cannot bleed into the next
				// member, and its sub-workers can be joined before that member is
				// settled (Acked/retried/DLQ'd).
				memberCtx := memoryContext(execCtx, groupScopes[d])
				memberPool := worker.NewSubWorkerPool(memberCtx, 0, m.Topic, c.logger)

				// Panics are converted to failures so retry/DLQ logic still runs.
				// Each member is settled (Ack/retry/DLQ) before the next member starts.
				var handlerErr error
				func() {
					defer func() {
						if r := recover(); r != nil {
							c.logger.Error("handler panic recovered",
								"topic", m.Topic, "msg_id", m.ID, "group", m.GroupID,
								"panic", fmt.Sprint(r), "stack", string(debug.Stack()),
							)
							handlerErr = fmt.Errorf("handler panic on topic %q: %v", m.Topic, r)
						}
					}()
					handlerErr = handler(memberCtx, m, memberPool)
				}()

				// Join this member's fan-out before settling it, mirroring the
				// independent-delivery path: a handler that spawns sub-workers
				// and forgets pool.Wait() must not have its delivery Acked (or
				// retried) while the sub-workers are still running.
				if waitErr := memberPool.Wait(); waitErr != nil && handlerErr == nil {
					handlerErr = waitErr
				}

				if handlerErr != nil {
					c.handleGroupFailure(memoryContext(settleCtx, groupScopes[d]), entry, ordered, idx, handlerErr, groupSequence, groupScopes)
					break
				}
				if err := c.driver.Ack(settleCtx, d); err != nil {
					c.logger.Error("failed to ack message", "topic", m.Topic, "msg_id", m.ID, "group", m.GroupID, "error", err)
					c.updateGroupSettlement(entry, m, true, c.pollInterval, groupSequence)
					// The broker still owns the failed settlement. Later members
					// will be delayed by the head gate on the next iteration.
					continue
				}
				c.updateGroupSettlement(entry, m, false, 0)
			}
			return nil
		})

		err := c.dispatchGroupBatch(ctx, gID, entry, groupBatch{task: groupTask, deliveries: groupDeliveries, release: releaseGroupMemory})
		if err != nil {
			continue
		}
		dispatched += len(groupDeliveries)
	}

	if dequeueErr != nil {
		// Work was accepted, but defer the next fetch to the next poll interval.
		return 0
	}
	return dispatched
}

// acquireGroupLock fetches or allocates a group lock entry and increments its reference count.
func (c *Consumer) acquireGroupLock(gid string) *groupLockEntry {
	c.groupMu.Lock()
	defer c.groupMu.Unlock()
	if c.groupLocks == nil {
		c.groupLocks = make(map[string]*groupLockEntry)
	}
	l, ok := c.groupLocks[gid]
	if !ok {
		l = &groupLockEntry{}
		c.groupLocks[gid] = l
	} else if !l.blockedUntil.IsZero() && time.Now().After(l.blockedUntil) {
		l.blockedUntil = time.Time{}
	}
	l.refs++
	return l
}

// releaseGroupLock unlocks the group mutex and cleans up the map entry when all referencing tasks finish.
func (c *Consumer) releaseGroupLock(gid string, entry *groupLockEntry) {
	c.releaseGroupRef(gid, entry)
	entry.mu.Unlock()
}

func (c *Consumer) releaseGroupRef(gid string, entry *groupLockEntry) {
	c.groupMu.Lock()
	entry.refs--
	// A retry head must outlive its backoff timer and any gap in delivery.
	// The active drain removes idle entries when it has no batches left.
	if entry.refs <= 0 && !entry.running && !entry.hasRetryHead() && c.groupLocks[gid] == entry {
		delete(c.groupLocks, gid)
	}
	c.groupMu.Unlock()

}

func (c *Consumer) handleGroupFailure(ctx context.Context, entry *groupLockEntry, ordered []*Delivery, failedIdx int, reason error, sequence uint64, scopes ...map[*Delivery]*memoryScope) {
	d := ordered[failedIdx]
	m := d.Message
	retried, backoff, err := c.handleFailure(ctx, d, reason)
	if err != nil && m != nil {
		c.logger.Error("failed to settle group message after failure",
			"topic", m.Topic, "msg_id", m.ID, "group", m.GroupID, "error", err,
		)
	}

	c.updateGroupSettlement(entry, m, retried || err != nil, backoff, sequence)

	remaining := ordered[failedIdx+1:]
	if len(remaining) == 0 {
		return
	}

	if retried {
		// If the failed member is scheduled for retry with backoff, subsequent members
		// must be delayed by the same window so they do not execute out of order before
		// the failed member's retry attempt.
		for _, rem := range remaining {
			if rem.Message == nil {
				_ = c.driver.Nack(ctx, rem, reason)
				continue
			}
			if delayErr := c.delayDelivery(groupMemoryContext(ctx, rem, scopes), rem, backoff, reason); delayErr != nil {
				c.logger.Error("failed to delay downstream group member",
					"topic", rem.Message.Topic, "msg_id", rem.Message.ID, "group", rem.Message.GroupID, "error", delayErr,
				)
				_ = c.driver.Nack(ctx, rem, reason)
			}
		}
	} else {
		// If the failed member cannot be retried (routed to DLQ), it has been settled.
		// Nack remaining members in the batch back to the driver so they do not execute
		// out of order ahead of the failed member in the current iteration.
		for _, rem := range remaining {
			_ = c.driver.Nack(ctx, rem, reason)
		}
	}
}

func (c *Consumer) delayDelivery(ctx context.Context, d *Delivery, delay time.Duration, reason error) error {
	m := d.Message
	if m == nil {
		return c.driver.Nack(ctx, d, reason)
	}

	at := time.Now().Add(delay)
	if reenqueuer, ok := c.driver.(Reenqueuer); ok {
		return reenqueuer.Reenqueue(ctx, d, m, at)
	}

	if delayedAdapter, ok := c.driver.(DelayedEnqueuer); ok {
		if err := delayedAdapter.EnqueueAt(ctx, m, at); err != nil {
			return err
		}
		if err := c.driver.Ack(ctx, d); err != nil {
			c.logger.Error("failed to ack delivery after delay enqueue",
				"topic", m.Topic, "msg_id", m.ID, "error", err,
			)
			return err
		}
		return nil
	}

	// Core delayed dispatch fallback
	var timer *time.Timer
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return c.driver.Nack(ctx, d, reason)
	}
	c.retryCallbacks.Add(1)
	release := scopeFromContext(ctx).retain()
	timer = time.AfterFunc(delay, func() {
		defer c.retryCallbacks.Done()
		// Reclaim is part of callback completion: Stop must wait for it too.
		defer release()
		c.mu.Lock()
		delete(c.retryTimers, timer)
		closed := c.closed
		c.mu.Unlock()
		if closed {
			return
		}
		if err := c.driver.Enqueue(context.Background(), m); err == nil {
			_ = c.driver.Ack(context.Background(), d)
		} else {
			c.logger.Error("failed to enqueue delayed message in fallback",
				"topic", m.Topic, "msg_id", m.ID, "error", err,
			)
			_ = c.driver.Nack(context.Background(), d, err)
		}
	})
	c.retryTimers[timer] = pendingRetry{msg: m, del: d, release: release}
	c.mu.Unlock()
	return nil
}

func (c *Consumer) handleFailure(ctx context.Context, d *Delivery, reason error) (bool, time.Duration, error) {
	m := d.Message
	if m == nil {
		return false, 0, c.driver.Nack(ctx, d, reason)
	}

	maxAttempts := m.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = c.retryPolicy.MaxAttempts
	}
	if maxAttempts <= 0 {
		maxAttempts = 3
	}

	// 1. Non-retryable error check
	if !IsRetryable(reason) {
		c.logger.Warn("non-retryable error encountered, routing message directly to DLQ",
			"topic", m.Topic,
			"msg_id", m.ID,
			"error", reason,
		)
		return false, 0, c.routeToDLQ(ctx, d, reason)
	}

	// 2. Increment core-owned attempt count
	m.Attempt++

	// 3. Exceeded max attempts -> Route to DLQ
	if m.Attempt >= maxAttempts {
		c.logger.Error("message exceeded max attempts, routing to DLQ",
			"topic", m.Topic,
			"msg_id", m.ID,
			"attempts", m.Attempt,
			"max_attempts", maxAttempts,
			"error", reason,
		)
		return false, 0, c.routeToDLQ(ctx, d, reason)
	}

	// 4. Eligible for retry: calculate backoff
	backoff := c.retryPolicy.Backoff(m.Attempt)
	c.logger.Info("retrying failed message with exponential backoff",
		"topic", m.Topic,
		"msg_id", m.ID,
		"attempt", m.Attempt,
		"max_attempts", maxAttempts,
		"backoff", backoff,
		"error", reason,
	)

	err := c.delayDelivery(ctx, d, backoff, reason)
	if err != nil {
		if !errors.Is(err, ErrDeliveryLost) {
			err = errors.Join(err, c.driver.Nack(ctx, d, err))
		}
		return false, 0, err
	}
	return true, backoff, nil
}

func (c *Consumer) routeToDLQ(ctx context.Context, d *Delivery, reason error) error {
	m := d.Message
	if m == nil {
		return c.driver.Nack(ctx, d, reason)
	}

	if strings.HasSuffix(m.Topic, ":dlq") {
		// Delivery is already on a DLQ topic. Do not recursively append :dlq to avoid poison loop.
		c.logger.Error("message on DLQ topic failed with no DLQ consumer, dropping message to prevent poison loop",
			"topic", m.Topic,
			"msg_id", m.ID,
			"error", reason,
		)
		return c.driver.Ack(ctx, d)
	}

	dlqTopic := DLQTopic(m.Topic)

	// A DLQ publication is a separate stored message. Reusing the original ID
	// would overwrite ID-keyed storage and let the original Ack delete it.
	// Each transfer gets a fresh ID, including after an ambiguous/failed Ack;
	// DLQ handlers can deduplicate by original_topic/original_message_id.
	headers := make(map[string]string, len(m.Headers)+6)
	for k, v := range m.Headers {
		headers[k] = v
	}
	headers["dlq_reason"] = reason.Error()
	headers["dlq_failed_at"] = time.Now().Format(time.RFC3339)
	headers["original_topic"] = m.Topic
	headers["original_message_id"] = m.ID
	headers["original_group_id"] = m.GroupID
	headers["original_group_order"] = fmt.Sprint(m.GroupOrder)

	dlqMsg := &Message{
		ID:    NewUUID(),
		Topic: dlqTopic,
		// Handlers may run concurrently with original settlement/redelivery.
		// Give each DLQ publication its own backing buffer.
		Payload:  bytes.Clone(m.Payload),
		Priority: PriorityLow,
		// DLQ processing must not join the original work group: a low-priority
		// DLQ head could otherwise starve behind later, higher-priority members.
		// Original ordering information remains available in the headers.
		Headers:     headers,
		CreatedAt:   time.Now(),
		Attempt:     m.Attempt,
		MaxAttempts: m.MaxAttempts,
		Driver:      m.Driver,
	}

	err := c.driver.Enqueue(ctx, dlqMsg)
	if err != nil {
		c.logger.Error("failed to enqueue message to DLQ", "dlq_topic", dlqTopic, "error", err)
		return errors.Join(err, c.driver.Nack(ctx, d, reason))
	}

	return c.driver.Ack(ctx, d)
}

// Stop gracefully shuts down the consumer loop and driver.
func (c *Consumer) Stop() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	var pending []pendingRetry
	for t, p := range c.retryTimers {
		if t.Stop() {
			c.retryCallbacks.Done()
		}
		if p.msg != nil {
			pending = append(pending, p)
		}
	}
	c.retryTimers = nil
	c.mu.Unlock()

	// Stop dequeue before publishing flushed retries. Otherwise a still-active
	// poller can immediately consume a flushed message during shutdown.
	if c.cancel != nil {
		c.cancel()
	}
	close(c.quit)
	c.wg.Wait()

	// Flush any pending retry messages: enqueue the retry copy first, then ack the original.
	// If enqueue fails, nack the original so it stays in the broker.
	for _, p := range pending {
		if err := c.driver.Enqueue(context.Background(), p.msg); err == nil {
			_ = c.driver.Ack(context.Background(), p.del)
		} else {
			_ = c.driver.Nack(context.Background(), p.del, err)
		}

		if p.release != nil {
			p.release()
		}
	}

	c.inFlight.Wait()
	c.retryCallbacks.Wait()
	c.groupMu.Lock()
	c.groupLocks = make(map[string]*groupLockEntry)
	c.groupMu.Unlock()
	c.closeOnce.Do(func() {
		_ = c.driver.Close()
	})
	c.logger.Info("queue consumer stopped gracefully")
}

func (entry *groupLockEntry) hasRetryHead() bool {
	return entry.retryHeadID != "" || entry.retryHeadTopic != ""
}

// Record every unsettled logical delivery. GroupOrder determines which barrier
// is active; equal orders preserve arrival order. Copies on DLQ are distinct.
func (c *Consumer) updateGroupSettlement(entry *groupLockEntry, m *Message, pending bool, backoff time.Duration, sequence ...uint64) {
	if entry == nil || m == nil {
		return
	}
	c.groupMu.Lock()
	defer c.groupMu.Unlock()
	c.updateGroupSettlementLocked(entry, m, pending, backoff, sequence...)
}

func (c *Consumer) updateGroupSettlementLocked(entry *groupLockEntry, m *Message, pending bool, backoff time.Duration, sequence ...uint64) {
	idx := -1
	for i, head := range entry.unsettled {
		if head.id == m.ID && head.topic == m.Topic {
			idx = i
			break
		}
	}
	if pending {
		if backoff < c.pollInterval {
			backoff = c.pollInterval
		}
		until := time.Now().Add(backoff)
		if idx >= 0 {
			entry.unsettled[idx].blockedUntil = until
		} else {
			var seq uint64
			if len(sequence) > 0 {
				seq = sequence[0]
			} else {
				entry.nextSequence++
				seq = entry.nextSequence
			}
			entry.unsettled = append(entry.unsettled, groupRetryHead{id: m.ID, topic: m.Topic, order: m.GroupOrder, sequence: seq, blockedUntil: until})
			sort.SliceStable(entry.unsettled, func(i, j int) bool {
				if entry.unsettled[i].order != entry.unsettled[j].order {
					return entry.unsettled[i].order < entry.unsettled[j].order
				}
				return entry.unsettled[i].sequence < entry.unsettled[j].sequence
			})
		}
	} else if idx >= 0 {
		copy(entry.unsettled[idx:], entry.unsettled[idx+1:])
		entry.unsettled[len(entry.unsettled)-1] = groupRetryHead{}
		entry.unsettled = entry.unsettled[:len(entry.unsettled)-1]
	}
	c.refreshGroupHeadLocked(entry)
}

func (c *Consumer) refreshGroupHeadLocked(entry *groupLockEntry) {
	entry.retryHeadID, entry.retryHeadTopic = "", ""
	entry.retryHeadOrder = 0
	entry.retryHeadSequence = 0
	entry.blockedUntil = time.Time{}
	if len(entry.unsettled) > 0 {
		head := entry.unsettled[0]
		entry.retryHeadID, entry.retryHeadTopic = head.id, head.topic
		entry.retryHeadOrder = head.order
		entry.retryHeadSequence = head.sequence
		entry.blockedUntil = head.blockedUntil
	}
}

// ErrSettlementQueueFull signals that the bounded oversized settlement slot is busy.
var ErrSettlementQueueFull = errors.New("queue: oversized settlement slot is occupied")

// Reserve one bounded exception for an oversized delivery that has already
// been materialized by Dequeue. Never wait on an active group's lock in the
// poller. Further oversized deliveries are Nacked with their barrier retained.
func (c *Consumer) settleAdmissionFailure(ctx context.Context, d *Delivery, reason error) error {
	m := d.Message
	if m == nil {
		return c.driver.Nack(ctx, d, reason)
	}
	var entry *groupLockEntry
	if m.GroupID != "" {
		entry = c.acquireGroupLock(m.GroupID)
		c.updateGroupSettlement(entry, m, true, c.pollInterval)
	}
	select {
	case c.settlementSlots <- struct{}{}:
	default:
		if entry != nil {
			c.releaseGroupRef(m.GroupID, entry)
		}
		return errors.Join(ErrSettlementQueueFull, c.driver.Nack(ctx, d, ErrSettlementQueueFull))
	}
	release := func() { <-c.settlementSlots }
	c.inFlight.Add(1)
	task := worker.NewTaskFunc("settle:"+m.Topic, func(_ context.Context, _ worker.SubWorkerPool) error {
		defer c.inFlight.Done()
		defer release()
		if entry != nil {
			entry.mu.Lock()
			defer c.releaseGroupLock(m.GroupID, entry)
		}
		err := c.routeToDLQ(ctx, d, reason)
		c.updateGroupSettlement(entry, m, err != nil, c.pollInterval)
		if err != nil {
			c.logger.Error("failed to settle rejected message", "topic", m.Topic, "msg_id", m.ID, "error", err)
		}
		return err
	})
	if entry != nil {
		return c.dispatchGroupBatch(ctx, m.GroupID, entry, groupBatch{task: task, deliveries: []*Delivery{d}, release: release})
	}
	if err := c.workerDispatcher.Dispatch(c.ctx, task); err != nil {
		c.inFlight.Done()
		release()
		return errors.Join(err, c.driver.Nack(ctx, d, err))
	}
	return nil
}

// Both execution and terminal admission settlement use one ordered drain per
// group. Callers have already acquired an entry reference and inFlight count.
func (c *Consumer) dispatchGroupBatch(ctx context.Context, gid string, entry *groupLockEntry, batch groupBatch) error {
	c.groupMu.Lock()
	entry.pending = append(entry.pending, batch)
	if entry.running {
		c.groupMu.Unlock()
		return nil
	}
	entry.running = true
	c.groupMu.Unlock()
	drain := worker.NewTaskFunc("group-drain:"+gid, func(tCtx context.Context, pool worker.SubWorkerPool) error {
		for {
			c.groupMu.Lock()
			if len(entry.pending) == 0 {
				entry.running = false
				if entry.refs == 0 && !entry.hasRetryHead() && c.groupLocks[gid] == entry {
					delete(c.groupLocks, gid)
				}
				c.groupMu.Unlock()
				return nil
			}
			next := entry.pending[0]
			entry.pending[0] = groupBatch{}
			entry.pending = entry.pending[1:]
			c.groupMu.Unlock()
			_ = next.task.Execute(tCtx, pool)
		}
	})
	if err := c.workerDispatcher.Dispatch(c.ctx, drain); err != nil {
		c.groupMu.Lock()
		pending := entry.pending
		// Preserve every original delivery's receipt position before returning
		// it to the broker. A later group member must not bypass backpressure.
		for _, next := range pending {
			for _, d := range next.deliveries {
				if d != nil && d.Message != nil {
					c.updateGroupSettlementLocked(entry, d.Message, true, c.pollInterval)
				}
			}
		}
		entry.pending = nil
		entry.running = false
		entry.refs -= len(pending)
		if entry.refs == 0 && !entry.hasRetryHead() && c.groupLocks[gid] == entry {
			delete(c.groupLocks, gid)
		}
		c.groupMu.Unlock()
		for _, next := range pending {
			next.release()
			c.inFlight.Done()
			for _, d := range next.deliveries {
				_ = c.driver.Nack(context.WithoutCancel(ctx), d, err)
			}
		}
		return err
	}
	return nil
}

func (c *Consumer) rememberGroupDelivery(entry *groupLockEntry, m *Message) {
	c.groupMu.Lock()
	defer c.groupMu.Unlock()
	for _, head := range entry.unsettled {
		if head.id == m.ID && head.topic == m.Topic {
			return
		}
	}
	entry.nextSequence++
	entry.unsettled = append(entry.unsettled, groupRetryHead{id: m.ID, topic: m.Topic, order: m.GroupOrder, sequence: entry.nextSequence})
	sort.SliceStable(entry.unsettled, func(i, j int) bool {
		if entry.unsettled[i].order != entry.unsettled[j].order {
			return entry.unsettled[i].order < entry.unsettled[j].order
		}
		return entry.unsettled[i].sequence < entry.unsettled[j].sequence
	})
	c.refreshGroupHeadLocked(entry)
}
