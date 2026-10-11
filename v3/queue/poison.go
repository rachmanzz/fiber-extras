package queue

import "time"

// PoisonTopic is the fallback topic for broker payloads that cannot be decoded
// into a *Message (corrupt bytes, missing store entry). The core has no handler
// registered for this topic, so such deliveries flow through the normal failure
// path (ErrNoHandlerFound is non-retryable, retry.go) straight into the
// dead-letter queue instead of an adapter silently dropping them.
const PoisonTopic = "app.poison"

// NewPoisonMessage builds a decodable Message that surfaces an undecodable
// broker payload into the DLQ. id identifies the original store entry and is
// kept as the message ID so the entry can be acknowledged/overwritten away.
func NewPoisonMessage(id string, raw []byte, decodeErr error, adapter string) *Message {
	headers := map[string]string{
		"decode_error":     decodeErr.Error(),
		"decode_adapter":   adapter,
		"decode_failed_at": time.Now().Format(time.RFC3339),
	}
	if len(raw) > 0 {
		headers["raw_payload"] = string(raw)
	}
	return &Message{
		ID:          id,
		Topic:       PoisonTopic,
		Payload:     raw,
		Priority:    PriorityLow,
		Headers:     headers,
		CreatedAt:   time.Now(),
		MaxAttempts: 1,
	}
}
