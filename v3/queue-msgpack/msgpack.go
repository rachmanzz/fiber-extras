// Package queuemsgpack provides a queue.Codec implementation that encodes
// message payloads with MessagePack instead of JSON.
//
// Payloads are codec-tagged in the "content-type" message header, so any
// adapter can carry them and consumers can decode via queue.Decode without
// driver-specific changes.
package queuemsgpack

import (
	"github.com/rachmanzz/fiber-extras/v3/queue"
	"github.com/vmihailenco/msgpack/v5"
)

// ContentType identifies MessagePack-encoded payloads in the "content-type"
// message header.
const ContentType = "application/msgpack"

type codec struct{}

func (codec) ContentType() string { return ContentType }

func (codec) Marshal(v any) ([]byte, error) { return msgpack.Marshal(v) }

func (codec) Unmarshal(data []byte, v any) error { return msgpack.Unmarshal(data, v) }

var instance queue.Codec = codec{}

// Codec returns the shared MessagePack codec. It is registered with the queue
// core on package init, so queue.Decode resolves it from the "content-type"
// header.
func Codec() queue.Codec { return instance }

func init() {
	queue.RegisterCodec(instance)
}
