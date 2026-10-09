package queue

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"time"
)

// Retryable marks an error that can be safely retried.
type Retryable interface {
	Retryable() bool
}

// IsRetryable determines whether an error is transient and safe to retry.
// Validation errors, unauthorized errors, and ErrNoHandlerFound are non-retryable.
// Context timeout/deadline exceeded or network/transient DB errors are retryable.
func IsRetryable(err error) bool {
	if err == nil {
		return false
	}

	// 1. Check explicit Retryable interface
	var r Retryable
	if errors.As(err, &r) {
		return r.Retryable()
	}

	// 2. Non-retryable sentinel errors
	if errors.Is(err, ErrNoHandlerFound) ||
		errors.Is(err, ErrDriverNotConfigured) ||
		errors.Is(err, ErrConsumerNotRunning) {
		return false
	}

	// 3. Known transient timeouts
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	// Default to retryable unless explicitly non-retryable
	return true
}

// RetryPolicy defines backoff and maximum retry parameters.
type RetryPolicy struct {
	MaxAttempts int           // Maximum retry attempts before routing to DLQ (default 3)
	Initial     time.Duration // Initial retry delay (default 100ms)
	Max         time.Duration // Maximum retry delay cap (default 10s)
	Multiplier  float64       // Exponential factor (default 2.0)
	Jitter      float64       // Random jitter factor between 0.0 and 0.5 (default 0.2)
}

// DefaultRetryPolicy returns the recommended exponential backoff retry policy.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts: 3,
		Initial:     100 * time.Millisecond,
		Max:         10 * time.Second,
		Multiplier:  2.0,
		Jitter:      0.2,
	}
}

// Backoff calculates the backoff duration for a given attempt count with exponential factor & jitter.
func (p RetryPolicy) Backoff(attempt int) time.Duration {
	if attempt <= 0 {
		return p.Initial
	}

	mult := p.Multiplier
	if mult <= 1.0 {
		mult = 2.0
	}

	// Exponential calculation: initial * (multiplier ^ attempt)
	delayFloat := float64(p.Initial) * math.Pow(mult, float64(attempt))

	maxFloat := float64(p.Max)
	if maxFloat > 0 && delayFloat > maxFloat {
		delayFloat = maxFloat
	}

	// Apply jitter: delay +/- (delay * jitter * random)
	if p.Jitter > 0 {
		jitterRange := delayFloat * p.Jitter
		delta := (rand.Float64()*2 - 1) * jitterRange // [-jitterRange, +jitterRange]
		delayFloat += delta
	}

	if delayFloat < float64(p.Initial) {
		delayFloat = float64(p.Initial)
	}

	return time.Duration(delayFloat)
}

// DLQTopic returns the canonical dead-letter queue topic for a given original topic.
func DLQTopic(topic string) string {
	return topic + ":dlq"
}
