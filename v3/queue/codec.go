package queue

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// Codec serializes and deserializes message payloads. Implementations let
// callers swap the payload wire encoding (JSON, MessagePack, ...) without
// touching any adapter: drivers only ever move opaque bytes.
type Codec interface {
	// ContentType returns the identifier written to the "content-type" header so
	// consumers can resolve the codec via Decode (e.g. "application/json").
	ContentType() string
	// Marshal encodes v into bytes.
	Marshal(v any) ([]byte, error)
	// Unmarshal decodes data into v.
	Unmarshal(data []byte, v any) error
}

// JSONCodec is the default Codec, encoding payloads as JSON.
type JSONCodec struct{}

func (JSONCodec) ContentType() string { return ContentTypeJSON }

func (JSONCodec) Marshal(v any) ([]byte, error) { return json.Marshal(v) }

func (JSONCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

// Well-known content types for the bundled codecs.
const (
	ContentTypeJSON = "application/json"
)

// contentTypeHeader is the message header key carrying the payload codec tag.
const contentTypeHeader = "content-type"

var defaultCodec Codec = JSONCodec{}

var (
	codecMu          sync.RWMutex
	registeredCodecs = map[string]Codec{}
)

// RegisterCodec registers a codec under its ContentType so Decode can resolve
// it from the "content-type" message header. Later registrations win.
func RegisterCodec(c Codec) {
	if c == nil {
		return
	}
	codecMu.Lock()
	defer codecMu.Unlock()
	registeredCodecs[c.ContentType()] = c
}

// CodecFor returns the codec registered for content type, if any.
func CodecFor(contentType string) (Codec, bool) {
	codecMu.RLock()
	defer codecMu.RUnlock()
	c, ok := registeredCodecs[contentType]
	return c, ok
}

// WithCodec sets the payload codec used by Dispatch for this message. The
// codec's ContentType is written to the "content-type" header (unless the caller
// already set one) so consumers can Decode it.
//
// WithCodec is a DispatchOption and only takes effect through Dispatch: it is
// ignored when a Message is built manually via NewMessage and passed straight to
// Enqueue, because that path never serializes the payload.
func WithCodec(c Codec) DispatchOption {
	return func(m *Message) {
		m.codec = c
	}
}

// defaultPayloadCodec returns the codec used when no explicit codec is selected.
// It honours a registration override for the JSON content type, falling back to
// the built-in JSONCodec.
func defaultPayloadCodec() Codec {
	if c, ok := CodecFor(ContentTypeJSON); ok {
		return c
	}
	return defaultCodec
}

// Decode deserializes a message payload into v using the codec referenced by the
// message's "content-type" header, falling back to the default codec (JSON) when
// the header is absent. It returns an error when the header names an
// unregistered codec.
func Decode(msg *Message, v any) error {
	if msg == nil {
		return errors.New("queue: cannot decode a nil message")
	}
	ct := ""
	if msg.Headers != nil {
		ct = msg.Headers[contentTypeHeader]
	}
	if ct == "" {
		return defaultPayloadCodec().Unmarshal(msg.Payload, v)
	}
	c, ok := CodecFor(ct)
	if !ok {
		return fmt.Errorf("queue: no codec registered for content-type %q", ct)
	}
	return c.Unmarshal(msg.Payload, v)
}

func init() {
	RegisterCodec(defaultCodec)
}
