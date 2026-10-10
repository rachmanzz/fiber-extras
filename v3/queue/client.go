package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

var (
	ErrDriverNotConfigured = errors.New("queue: driver is not configured")
	ErrConsumerNotRunning  = errors.New("queue: consumer is not running")
)

// Producer defines the interface for enqueuing messages from services.
type Producer interface {
	Enqueue(ctx context.Context, msg *Message) error
	Dispatch(ctx context.Context, topic string, payload any, opts ...DispatchOption) error
}

type queueManager struct {
	mu            sync.RWMutex
	defaultDriver Driver
	drivers       map[string]Driver
	router        *Router
	consumers     map[string]*Consumer
	logger        Logger
}

var (
	globalManager = &queueManager{
		drivers:   make(map[string]Driver),
		router:    NewRouter(),
		consumers: make(map[string]*Consumer),
		logger:    DefaultLogger(),
	}
)

// SetLogger sets the logger for the global queue manager.
func SetLogger(l Logger) {
	globalManager.mu.Lock()
	defer globalManager.mu.Unlock()
	if l != nil {
		globalManager.logger = l
	}
}

// RegisterDriver registers a driver by its specific identifier name.
func RegisterDriver(name string, d Driver) {
	globalManager.mu.Lock()
	defer globalManager.mu.Unlock()
	globalManager.drivers[name] = d
	if globalManager.defaultDriver == nil {
		globalManager.defaultDriver = d
	}
}

// SetDefaultDriver sets the fallback driver used when no specific driver is requested.
func SetDefaultDriver(name string) error {
	globalManager.mu.Lock()
	defer globalManager.mu.Unlock()
	d, ok := globalManager.drivers[name]
	if !ok {
		return fmt.Errorf("queue: driver %q is not registered", name)
	}
	globalManager.defaultDriver = d
	return nil
}

// SetDriver sets the active broker driver (and marks it as default).
func SetDriver(d Driver) {
	globalManager.mu.Lock()
	defer globalManager.mu.Unlock()
	if d == nil {
		globalManager.defaultDriver = nil
		return
	}
	globalManager.drivers[d.Name()] = d
	globalManager.defaultDriver = d
}

// Reset clears all registered drivers, active consumers, and handlers. Useful for testing.
func Reset() {
	globalManager.mu.Lock()
	defer globalManager.mu.Unlock()
	for _, c := range globalManager.consumers {
		c.Stop()
	}
	globalManager.consumers = make(map[string]*Consumer)
	globalManager.drivers = make(map[string]Driver)
	globalManager.defaultDriver = nil
	globalManager.router = NewRouter()
}

// Attach plugs an Adapter into the active Queue Core.
func Attach(adapter Adapter) {
	SetDriver(adapter)
}

// AttachAdapter is an alias for Attach.
func AttachAdapter(adapter Adapter) {
	SetDriver(adapter)
}

// GetDriver returns the driver matching the name, or the default driver if name is empty.
func GetDriver(name ...string) Driver {
	globalManager.mu.RLock()
	defer globalManager.mu.RUnlock()

	if len(name) > 0 && name[0] != "" {
		if d, ok := globalManager.drivers[name[0]]; ok {
			return d
		}
	}
	return globalManager.defaultDriver
}

// GetRouter returns the global topic handler router.
func GetRouter() *Router {
	return globalManager.router
}

// RegisterHandler registers a message handler for a topic with optional configuration.
func RegisterHandler(topic string, handler Handler, opts ...HandlerOption) {
	globalManager.router.Register(topic, handler, opts...)
}

// Enqueue sends a Message into the appropriate driver queue based on msg.Driver, topic routing, or default.
func Enqueue(ctx context.Context, msg *Message) error {
	targetDriverName := msg.Driver

	if targetDriverName == "" {
		if route, exists := globalManager.router.GetRoute(msg.Topic); exists && route.DriverName != "" {
			targetDriverName = route.DriverName
		}
	}

	d := GetDriver(targetDriverName)
	if d == nil {
		return ErrDriverNotConfigured
	}
	return d.Enqueue(ctx, msg)
}

// Dispatch publishes a background queue job, serializing the payload with the
// selected codec (JSON by default, or the codec provided via WithCodec).
func Dispatch(ctx context.Context, topic string, payload any, opts ...DispatchOption) error {
	msg := NewMessage(topic, nil, opts...)

	codec := msg.codec
	if codec == nil {
		codec = defaultPayloadCodec()
	}

	var bytes []byte
	switch v := payload.(type) {
	case []byte:
		bytes = v
	case string:
		bytes = []byte(v)
	default:
		b, err := codec.Marshal(v)
		if err != nil {
			return fmt.Errorf("queue: failed to marshal payload: %w", err)
		}
		bytes = b
		if msg.codec != nil {
			if msg.Headers == nil {
				msg.Headers = make(map[string]string)
			}
			if _, exists := msg.Headers[contentTypeHeader]; !exists {
				msg.Headers[contentTypeHeader] = msg.codec.ContentType()
			}
		}
	}
	msg.Payload = bytes

	err := Enqueue(ctx, msg)
	if err != nil {
		globalManager.logger.Error("failed to dispatch queue message", "topic", topic, "error", err)
		return err
	}
	return nil
}

// StartConsumer starts background polling consumers for all registered drivers (or the default driver).
func StartConsumer(opts ...ConsumerOption) (*Consumer, error) {
	globalManager.mu.Lock()
	defer globalManager.mu.Unlock()

	if len(globalManager.drivers) == 0 && globalManager.defaultDriver == nil {
		return nil, ErrDriverNotConfigured
	}

	if globalManager.defaultDriver != nil {
		globalManager.drivers[globalManager.defaultDriver.Name()] = globalManager.defaultDriver
	}

	var firstConsumer *Consumer

	for name, d := range globalManager.drivers {
		if _, running := globalManager.consumers[name]; running {
			continue
		}
		consumerOpts := append([]ConsumerOption{WithLogger(globalManager.logger)}, opts...)
		c := NewConsumer(d, globalManager.router, nil, consumerOpts...)
		c.Start()
		globalManager.consumers[name] = c
		if firstConsumer == nil {
			firstConsumer = c
		}
	}

	if firstConsumer == nil && globalManager.defaultDriver != nil {
		firstConsumer = globalManager.consumers[globalManager.defaultDriver.Name()]
	}

	return firstConsumer, nil
}

// StopConsumer gracefully shuts down all active consumers.
func StopConsumer() {
	globalManager.mu.Lock()
	defer globalManager.mu.Unlock()

	for name, c := range globalManager.consumers {
		c.Stop()
		delete(globalManager.consumers, name)
	}
}
